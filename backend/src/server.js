import "dotenv/config";
import express from "express";
import cors from "cors";
import multer from "multer";
import { get, put } from "@vercel/blob";
import { Client as NotionClient } from "@notionhq/client";
import { createClient } from "@supabase/supabase-js";
import { Pool } from "pg";
import https from "https";
import http from "http";
import { createHash, createHmac, randomBytes, timingSafeEqual } from "crypto";
import { dirname, resolve } from "path";
import { fileURLToPath } from "url";
import { readFile, writeFile } from "fs/promises";

const app = express();
const port = Number(process.env.PORT || 3001);
const jsonBodyLimit = String(process.env.JSON_BODY_LIMIT || "2mb").trim() || "2mb";

app.use(cors());
app.use(express.json({ limit: jsonBodyLimit }));
app.use(express.urlencoded({ extended: true, limit: jsonBodyLimit }));
app.use((req, res, next) => {
  const incomingRequestId = String(req.headers["x-request-id"] || "").trim();
  const requestId = incomingRequestId || randomBytes(8).toString("hex");
  const start = Date.now();

  res.locals.requestId = requestId;
  ensureRequestTelemetry(res);
  res.setHeader("X-Request-Id", requestId);
  res.on("finish", () => {
    const telemetry = ensureRequestTelemetry(res);
    const durationMs = Date.now() - start;
    const slowRequestMsRaw = Number(process.env.REQUEST_TRACE_SLOW_MS || 1500);
    const slowRequestMs = Number.isFinite(slowRequestMsRaw)
      ? Math.max(100, Math.min(60000, Math.trunc(slowRequestMsRaw)))
      : 1500;
    const logAll = ["1", "true", "yes", "on"].includes(String(process.env.REQUEST_TRACE_LOG_ALL || "").trim().toLowerCase());
    const hasFailedSpan = telemetry.spans.some((span) => span && span.ok === false);
    const shouldLog = logAll || res.statusCode >= 500 || durationMs >= slowRequestMs || hasFailedSpan;

    if (shouldLog) {
      const level = res.statusCode >= 500 || hasFailedSpan ? "warn" : "log";
      console[level]("[request-trace]", {
        requestId,
        method: req.method,
        path: req.originalUrl,
        statusCode: res.statusCode,
        durationMs,
        backendTags: Array.from(telemetry.backendTags),
        spans: telemetry.spans.slice(0, 50)
      });
    }

    recordRequestPerfStat({
      requestId,
      method: req.method,
      path: req.originalUrl,
      statusCode: res.statusCode,
      durationMs,
      backendTags: Array.from(telemetry.backendTags)
    });

    void persistRequestPerfEvent({
      requestId,
      method: req.method,
      route: normalizePerfRoute(req.originalUrl),
      statusCode: res.statusCode,
      durationMs,
      backendTags: Array.from(telemetry.backendTags)
    });
  });

  next();
});
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 }
});

function normalizeNotionBlockId(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";

  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(raw)) {
    return raw.toLowerCase();
  }

  const hex = raw.replace(/[^0-9a-f]/gi, "").toLowerCase();
  if (hex.length !== 32) {
    return "";
  }

  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

// ── Server-Sent Events (delivery status ticks) ─────────────────────────────
const sseClients = new Set();
const softWarnTimestamps = new Map();
const requestPerfStats = new Map();
const requestTraceSamples = [];
const MAX_REQUEST_TRACE_SAMPLES = 200;
let requestPerfEventsTableReady = false;

function ensureRequestTelemetry(res) {
  if (!res.locals.requestTelemetry) {
    res.locals.requestTelemetry = {
      spans: [],
      backendTags: new Set()
    };
  }
  return res.locals.requestTelemetry;
}

function addRequestBackendTag(res, backendTag) {
  const tag = String(backendTag || "").trim();
  if (!tag) return;
  const telemetry = ensureRequestTelemetry(res);
  telemetry.backendTags.add(tag);
}

function recordRequestSpan(res, name, startMs, meta = {}, error = null) {
  const telemetry = ensureRequestTelemetry(res);
  const entry = {
    name: String(name || "span").trim() || "span",
    durationMs: Math.max(0, Date.now() - Number(startMs || Date.now())),
    ok: !error
  };

  if (meta && typeof meta === "object") {
    const safeMeta = { ...meta };
    if (safeMeta.backend) {
      addRequestBackendTag(res, safeMeta.backend);
    }
    entry.meta = safeMeta;
  }

  if (error) {
    entry.error = error instanceof Error ? error.message : String(error);
  }

  telemetry.spans.push(entry);
}

async function runWithRequestSpan(res, name, meta, fn) {
  const startMs = Date.now();
  try {
    const value = await fn();
    recordRequestSpan(res, name, startMs, meta, null);
    return value;
  } catch (error) {
    recordRequestSpan(res, name, startMs, meta, error);
    throw error;
  }
}

function normalizePerfRoute(pathname) {
  const path = String(pathname || "").split("?")[0] || "/";
  return path
    .replace(/[0-9a-f]{16,}/gi, ":id")
    .replace(/\b\d+\b/g, ":n");
}

function recordRequestPerfStat({ method, path, statusCode, durationMs, backendTags = [], requestId }) {
  const normalizedMethod = String(method || "GET").toUpperCase();
  const normalizedRoute = normalizePerfRoute(path);
  const key = `${normalizedMethod} ${normalizedRoute}`;
  const prev = requestPerfStats.get(key) || {
    key,
    method: normalizedMethod,
    route: normalizedRoute,
    count: 0,
    totalDurationMs: 0,
    maxDurationMs: 0,
    status2xx: 0,
    status4xx: 0,
    status5xx: 0,
    errorCount: 0,
    backendTagUsage: {},
    lastSeenAt: ""
  };

  prev.count += 1;
  prev.totalDurationMs += durationMs;
  prev.maxDurationMs = Math.max(prev.maxDurationMs, durationMs);
  if (statusCode >= 500) {
    prev.status5xx += 1;
    prev.errorCount += 1;
  } else if (statusCode >= 400) {
    prev.status4xx += 1;
  } else {
    prev.status2xx += 1;
  }

  for (const tag of backendTags) {
    const cleanTag = String(tag || "").trim();
    if (!cleanTag) continue;
    prev.backendTagUsage[cleanTag] = Number(prev.backendTagUsage[cleanTag] || 0) + 1;
  }

  prev.lastSeenAt = new Date().toISOString();
  requestPerfStats.set(key, prev);

  requestTraceSamples.push({
    requestId: String(requestId || ""),
    method: normalizedMethod,
    route: normalizedRoute,
    statusCode,
    durationMs,
    backendTags,
    at: prev.lastSeenAt
  });

  if (requestTraceSamples.length > MAX_REQUEST_TRACE_SAMPLES) {
    requestTraceSamples.splice(0, requestTraceSamples.length - MAX_REQUEST_TRACE_SAMPLES);
  }
}

function isRequestPerfDbEnabled() {
  const enabled = ["1", "true", "yes", "on"].includes(
    String(process.env.ADMIN_PERF_PERSIST_ENABLED || "").trim().toLowerCase()
  );
  return enabled && pgEnabled && !!pgPool;
}

async function ensureRequestPerfEventsTable() {
  if (!isRequestPerfDbEnabled() || requestPerfEventsTableReady) return;
  await pgPool.query(`
    create table if not exists public.request_perf_events (
      id bigserial primary key,
      request_id text,
      method text not null,
      route text not null,
      status_code integer not null,
      duration_ms integer not null,
      backend_tags jsonb not null default '[]'::jsonb,
      created_at timestamptz not null default now()
    );
  `);
  await pgPool.query(`
    create index if not exists idx_request_perf_events_created_at
    on public.request_perf_events (created_at desc);
  `);
  await pgPool.query(`
    create index if not exists idx_request_perf_events_route_created_at
    on public.request_perf_events (route, created_at desc);
  `);
  requestPerfEventsTableReady = true;
}

async function persistRequestPerfEvent({ requestId, method, route, statusCode, durationMs, backendTags }) {
  if (!isRequestPerfDbEnabled()) return;
  try {
    await ensureRequestPerfEventsTable();
    await pgPool.query(
      `insert into public.request_perf_events
      (request_id, method, route, status_code, duration_ms, backend_tags, created_at)
      values ($1, $2, $3, $4, $5, $6::jsonb, now())`,
      [
        String(requestId || ""),
        String(method || "GET").toUpperCase(),
        String(route || "/"),
        Number(statusCode || 0),
        Number(durationMs || 0),
        JSON.stringify(Array.isArray(backendTags) ? backendTags : [])
      ]
    );
  } catch (error) {
    warnSoftError("admin_perf.persist", error);
  }
}

function warnSoftError(key, error, context = {}) {
  const now = Date.now();
  const throttleMsRaw = Number(process.env.SOFT_WARN_THROTTLE_MS || 60000);
  const throttleMs = Number.isFinite(throttleMsRaw)
    ? Math.max(1000, Math.min(10 * 60 * 1000, Math.trunc(throttleMsRaw)))
    : 60000;
  const last = softWarnTimestamps.get(key) || 0;
  if (now - last < throttleMs) {
    return;
  }

  softWarnTimestamps.set(key, now);
  console.warn("[soft-error]", {
    key,
    message: error instanceof Error ? error.message : String(error || "unknown_error"),
    ...context
  });
}

function broadcastSSE(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const client of sseClients) {
    try {
      client.write(payload);
    } catch (error) {
      warnSoftError("sse.broadcast.write", error, { event });
    }
  }
}

// ── In-memory scheduled messages ──────────────────────────────────────────
const scheduledMessages = [];
const recentCallEvents = [];
const MAX_CALL_EVENTS = 100;
let autoNotificacaoEnvioRunning = false;
let autoNotificacaoEnvioLastRunDateKey = "";
let autoNotificacaoEnvioTransporteRunning = false;
let autoNotificacaoEnvioTransporteLastRunDateKey = "";
let autoNotificacaoEnvioStateHydrated = false;
let autoNotificacaoIncidenciaRunning = false;
let autoNotificacaoIncidenciaLastRunSlotKey = "";
let autoNotificacaoIncidenciaStateHydrated = false;
let autoNotificacaoIncidenciaInitialized = false;
const autoNotificacaoIncidenciaKnownKeys = new Set();
const autoNotificacaoIncidenciaSentKeys = new Set();
const autoNotificacaoIncidenciaPendingEntries = new Map();
const autoNotificacaoIncidenciaMetaByKey = new Map();
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const autoNotificacaoStateDir = String(process.env.AUTO_NOTIFICACAO_STATE_DIR || "").trim()
  || (process.env.VERCEL ? "/tmp" : resolve(__dirname, ".."));
const autoNotificacaoEnvioStateFile = resolve(
  autoNotificacaoStateDir,
  ".auto_notificacao_envio_state.json"
);
const autoNotificacaoIncidenciaStateFile = resolve(
  autoNotificacaoStateDir,
  ".auto_notificacao_incidencia_state.json"
);
const GOOGLE_OAUTH_STATE_TTL_MS = 15 * 60 * 1000;
const googleOauthSession = {
  accessToken: "",
  refreshToken: "",
  expiresAt: 0,
  scope: "",
  tokenType: "Bearer"
};
let googleOauthSessionHydrated = false;

function buildIncidenciaShipmentKey(row) {
  const parcelId = String(row?.parcelId || "").trim();
  const tracking = String(row?.providerTrackingCode || "").trim();
  if (!parcelId && !tracking) {
    return "";
  }
  return `${parcelId}|${tracking}`;
}

function normalizeIsoDateOrEmpty(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const time = Date.parse(raw);
  if (!Number.isFinite(time)) return "";
  return new Date(time).toISOString();
}

function getAutoNotificacaoIncidenciaRetentionDays() {
  const raw = Number(process.env.AUTO_NOTIFICACAO_INCIDENCIA_RETENTION_DAYS || 30);
  if (!Number.isFinite(raw)) return 30;
  return Math.max(1, Math.min(365, Math.trunc(raw)));
}

function setAutoNotificacaoIncidenciaKeyMeta(shipmentKey, updates = {}) {
  const key = String(shipmentKey || "").trim();
  if (!key) return;

  const prev = autoNotificacaoIncidenciaMetaByKey.get(key) || {};
  const next = {
    firstSeenAt: normalizeIsoDateOrEmpty(updates.firstSeenAt || prev.firstSeenAt || ""),
    lastSeenAt: normalizeIsoDateOrEmpty(updates.lastSeenAt || prev.lastSeenAt || ""),
    sentAt: normalizeIsoDateOrEmpty(updates.sentAt || prev.sentAt || "")
  };

  if (!next.firstSeenAt && next.lastSeenAt) {
    next.firstSeenAt = next.lastSeenAt;
  }

  autoNotificacaoIncidenciaMetaByKey.set(key, next);
}

function pruneAutoNotificacaoIncidenciaState() {
  const retentionDays = getAutoNotificacaoIncidenciaRetentionDays();
  const retentionMs = retentionDays * 24 * 60 * 60 * 1000;
  const cutoff = Date.now() - retentionMs;

  const candidates = new Set([
    ...autoNotificacaoIncidenciaKnownKeys,
    ...autoNotificacaoIncidenciaSentKeys,
    ...autoNotificacaoIncidenciaPendingEntries.keys(),
    ...autoNotificacaoIncidenciaMetaByKey.keys()
  ]);

  for (const shipmentKey of candidates) {
    const meta = autoNotificacaoIncidenciaMetaByKey.get(shipmentKey) || {};
    const sentAtMs = Date.parse(String(meta.sentAt || ""));
    const lastSeenAtMs = Date.parse(String(meta.lastSeenAt || ""));
    const seenReferenceMs = Number.isFinite(lastSeenAtMs) ? lastSeenAtMs : sentAtMs;
    const hasRecentSent = Number.isFinite(sentAtMs) && sentAtMs >= cutoff;
    const hasRecentSeen = Number.isFinite(seenReferenceMs) && seenReferenceMs >= cutoff;

    if (hasRecentSent || hasRecentSeen) {
      continue;
    }

    autoNotificacaoIncidenciaKnownKeys.delete(shipmentKey);
    autoNotificacaoIncidenciaSentKeys.delete(shipmentKey);
    autoNotificacaoIncidenciaPendingEntries.delete(shipmentKey);
    autoNotificacaoIncidenciaMetaByKey.delete(shipmentKey);
  }
}

function applyAutoNotificacaoIncidenciaState(input) {
  const parsed = input && typeof input === "object" ? input : {};
  const nowIso = new Date().toISOString();

  autoNotificacaoIncidenciaInitialized = Boolean(parsed.initialized);

  autoNotificacaoIncidenciaKnownKeys.clear();
  autoNotificacaoIncidenciaMetaByKey.clear();
  for (const key of Array.isArray(parsed.knownKeys) ? parsed.knownKeys : []) {
    const clean = String(key || "").trim();
    if (!clean) continue;
    autoNotificacaoIncidenciaKnownKeys.add(clean);
    setAutoNotificacaoIncidenciaKeyMeta(clean, { firstSeenAt: nowIso, lastSeenAt: nowIso });
  }

  autoNotificacaoIncidenciaSentKeys.clear();
  for (const key of Array.isArray(parsed.sentKeys) ? parsed.sentKeys : []) {
    const clean = String(key || "").trim();
    if (!clean) continue;
    autoNotificacaoIncidenciaSentKeys.add(clean);
    setAutoNotificacaoIncidenciaKeyMeta(clean, {
      firstSeenAt: nowIso,
      lastSeenAt: nowIso,
      sentAt: nowIso
    });
  }

  const keyMeta = parsed.keyMeta && typeof parsed.keyMeta === "object"
    ? parsed.keyMeta
    : {};
  for (const [key, value] of Object.entries(keyMeta)) {
    const clean = String(key || "").trim();
    if (!clean || !value || typeof value !== "object") continue;
    setAutoNotificacaoIncidenciaKeyMeta(clean, {
      firstSeenAt: value.firstSeenAt,
      lastSeenAt: value.lastSeenAt,
      sentAt: value.sentAt
    });
  }

  autoNotificacaoIncidenciaPendingEntries.clear();
  const pending = parsed.pendingEntries && typeof parsed.pendingEntries === "object"
    ? parsed.pendingEntries
    : {};
  for (const [key, value] of Object.entries(pending)) {
    const clean = String(key || "").trim();
    if (!clean || !value || typeof value !== "object") continue;
    autoNotificacaoIncidenciaPendingEntries.set(clean, {
      to: String(value.to || "").trim(),
      destinatario: String(value.destinatario || "").trim(),
      parcelId: String(value.parcelId || "").trim(),
      sender: String(value.sender || "").trim(),
      incidentReason: String(value.incidentReason || "").trim()
    });
    setAutoNotificacaoIncidenciaKeyMeta(clean, {
      firstSeenAt: nowIso,
      lastSeenAt: nowIso
    });
  }

  pruneAutoNotificacaoIncidenciaState();
}

function buildAutoNotificacaoIncidenciaStatePayload() {
  pruneAutoNotificacaoIncidenciaState();

  const pendingEntries = {};
  for (const [key, value] of autoNotificacaoIncidenciaPendingEntries.entries()) {
    pendingEntries[key] = value;
  }

  const keyMeta = {};
  for (const [key, value] of autoNotificacaoIncidenciaMetaByKey.entries()) {
    keyMeta[key] = {
      firstSeenAt: String(value.firstSeenAt || "").trim(),
      lastSeenAt: String(value.lastSeenAt || "").trim(),
      sentAt: String(value.sentAt || "").trim()
    };
  }

  return {
    initialized: autoNotificacaoIncidenciaInitialized,
    knownKeys: Array.from(autoNotificacaoIncidenciaKnownKeys),
    sentKeys: Array.from(autoNotificacaoIncidenciaSentKeys),
    pendingEntries,
    keyMeta
  };
}

async function hydrateAutoNotificacaoIncidenciaState() {
  if (autoNotificacaoIncidenciaStateHydrated) {
    return;
  }

  try {
    if (pgEnabled && pgPool) {
      try {
        await ensurePersistentStateTable();
      } catch (error) {
        warnSoftError("auto_incidencia.hydrate.ensure_table", error);
      }
    }

    if (supabaseEnabled && supabase) {
      try {
        const { data, error } = await supabase
          .from("workspace_state")
          .select("value")
          .eq("key", "auto_notificacao_incidencia_state")
          .limit(1)
          .maybeSingle();

        if (!error && data?.value && typeof data.value === "object") {
          applyAutoNotificacaoIncidenciaState(data.value);
          return;
        }
      } catch (error) {
        warnSoftError("auto_incidencia.hydrate.supabase", error);
      }
    }

    if (pgEnabled && pgPool) {
      try {
        const { rows } = await pgPool.query(
          `select value from public.workspace_state where key = $1 limit 1`,
          ["auto_notificacao_incidencia_state"]
        );
        const value = Array.isArray(rows) && rows.length > 0 ? rows[0]?.value : null;
        if (value && typeof value === "object") {
          applyAutoNotificacaoIncidenciaState(value);
          return;
        }
      } catch (error) {
        warnSoftError("auto_incidencia.hydrate.postgres", error);
      }
    }

    const raw = await readFile(autoNotificacaoIncidenciaStateFile, "utf8");
    const parsed = JSON.parse(raw || "{}") || {};
    applyAutoNotificacaoIncidenciaState(parsed);
  } catch (error) {
    warnSoftError("auto_incidencia.hydrate.file", error);
    // Ignore missing or invalid persisted state; scheduler will bootstrap.
  } finally {
    autoNotificacaoIncidenciaStateHydrated = true;
  }
}

async function persistAutoNotificacaoIncidenciaState() {
  const payload = buildAutoNotificacaoIncidenciaStatePayload();

  if (pgEnabled && pgPool) {
    try {
      await ensurePersistentStateTable();
    } catch (error) {
      warnSoftError("auto_incidencia.persist.ensure_table", error);
    }
  }

  if (supabaseEnabled && supabase) {
    try {
      const { error } = await supabase
        .from("workspace_state")
        .upsert([{ key: "auto_notificacao_incidencia_state", value: payload }], { onConflict: "key" });
      if (!error) {
        return;
      }
    } catch (error) {
      warnSoftError("auto_incidencia.persist.supabase", error);
    }
  }

  if (pgEnabled && pgPool) {
    try {
      await pgPool.query(
        `insert into public.workspace_state (key, value, updated_at)
        values ($1, $2::jsonb, now())
        on conflict (key) do update set value = excluded.value, updated_at = now()`,
        ["auto_notificacao_incidencia_state", JSON.stringify(payload)]
      );
      return;
    } catch (error) {
      warnSoftError("auto_incidencia.persist.postgres", error);
    }
  }

  try {
    await writeFile(autoNotificacaoIncidenciaStateFile, JSON.stringify(payload, null, 2), "utf8");
  } catch (error) {
    console.error(
      "[auto-notificacao-incidencia] failed to persist state",
      error instanceof Error ? error.message : error
    );
  }
}

async function processScheduledMessages() {
  const now = Date.now();
  let processed = 0;
  let sent = 0;
  let failed = 0;

  const concurrencyRaw = Number(process.env.SCHEDULED_MESSAGES_CONCURRENCY || 5);
  const concurrency = Number.isFinite(concurrencyRaw)
    ? Math.max(1, Math.min(20, Math.trunc(concurrencyRaw)))
    : 5;

  const dueItems = scheduledMessages.filter((item) => {
    if (item.status !== "pending") return false;
    return new Date(item.scheduledAt).getTime() <= now;
  });

  async function processItem(item) {
    processed += 1;
    item.status = "sending";

    try {
      const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
      const phoneNumberId = process.env.WHATSAPP_PHONE_NUMBER_ID || "";
      const token = process.env.WHATSAPP_ACCESS_TOKEN || "";
      if (!phoneNumberId || !token) {
        item.status = "failed";
        failed += 1;
        return;
      }

      const endpoint = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}/messages`;
      const components = [];
      if (item.bodyVariables && item.bodyVariables.length > 0) {
        components.push({
          type: "body",
          parameters: item.bodyVariables.map((text) => ({ type: "text", text }))
        });
      }

      const templatePayload = {
        name: item.templateName,
        language: { code: item.languageCode || "pt_PT" }
      };
      if (components.length > 0) templatePayload.components = components;

      const response = await fetch(endpoint, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
        body: JSON.stringify({
          messaging_product: "whatsapp",
          recipient_type: "individual",
          to: item.to,
          type: "template",
          template: templatePayload
        })
      });

      const responseBody = await response.json().catch(() => ({}));
      const messageId = responseBody?.messages?.[0]?.id || "";
      await safeSupabaseLog(() =>
        createSupabaseLogRow({
          direction: "out",
          channel: "template",
          to: item.to,
          messageText: Array.isArray(item.bodyVariables) ? item.bodyVariables.join(" | ") : "",
          templateName: item.templateName,
          status: response.ok ? "sent" : `failed_${response.status}`,
          apiMessageId: messageId,
          payload: {
            source: "scheduler",
            scheduledAt: item.scheduledAt,
            response: responseBody
          }
        })
      );

      if (!response.ok) {
        const fallbackMessage = buildTemplateFallbackText({
          templateName: item.templateName,
          bodyVariables: item.bodyVariables
        });

        item.smsFallback = await maybeSendAutomaticSmsFallback({
          to: item.to,
          message: fallbackMessage,
          source: "wa_template_scheduled_failure",
          templateName: item.templateName,
          waStatus: `failed_${response.status}`,
          waResponse: responseBody
        });
      }

      item.status = response.ok ? "sent" : "failed";
      if (response.ok) {
        sent += 1;
      } else {
        failed += 1;
      }
    } catch (error) {
      item.status = "failed";
      failed += 1;
      warnSoftError("scheduler.process_item", error, {
        id: item.id,
        to: item.to,
        templateName: item.templateName
      });
    } finally {
      broadcastSSE("scheduled_sent", { id: item.id, status: item.status });
    }
  }

  for (let i = 0; i < dueItems.length; i += concurrency) {
    const chunk = dueItems.slice(i, i + concurrency);
    await Promise.allSettled(chunk.map((item) => processItem(item)));
  }

  return { processed, sent, failed };
}

function getLisbonClockParts() {
  const formatter = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/Lisbon",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  });

  const parts = formatter.formatToParts(new Date());
  const lookup = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const dateKey = `${lookup.year || "0000"}-${lookup.month || "00"}-${lookup.day || "00"}`;

  return {
    dateKey,
    weekday: String(lookup.weekday || ""),
    hour: Number(lookup.hour || 0),
    minute: Number(lookup.minute || 0)
  };
}

function getAutoNotificacaoEnvioGraceMinutes() {
  const raw = Number(process.env.AUTO_NOTIFICACAO_ENVIO_GRACE_MINUTES || 30);
  if (!Number.isFinite(raw)) {
    return 30;
  }
  return Math.max(0, Math.min(30, Math.trunc(raw)));
}

function getAutoNotificacaoEnvioTransporteGraceMinutes() {
  const transporteRaw = process.env.AUTO_NOTIFICACAO_ENVIO_TRANSPORTE_GRACE_MINUTES;
  const fallback = getAutoNotificacaoEnvioGraceMinutes();
  if (typeof transporteRaw === "undefined") {
    return fallback;
  }

  const raw = Number(transporteRaw);
  if (!Number.isFinite(raw)) {
    return fallback;
  }

  return Math.max(0, Math.min(30, Math.trunc(raw)));
}

function getBoundedPositiveInt(value, fallback, min, max) {
  const raw = Number(value);
  if (!Number.isFinite(raw)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(raw)));
}

function getAutoNotificacaoEnvioFetchLimit() {
  return getBoundedPositiveInt(process.env.AUTO_NOTIFICACAO_ENVIO_FETCH_LIMIT, 120, 10, 250);
}

function getAutoNotificacaoEnvioFetchMaxPages() {
  return getBoundedPositiveInt(process.env.AUTO_NOTIFICACAO_ENVIO_FETCH_MAX_PAGES, 6, 1, 40);
}

function getAutoNotificacaoEnvioMaxSendsPerRun() {
  return getBoundedPositiveInt(process.env.AUTO_NOTIFICACAO_ENVIO_MAX_SENDS_PER_RUN, 20, 1, 500);
}

function getAutoNotificacaoEnvioTransporteFetchLimit() {
  return getBoundedPositiveInt(
    process.env.AUTO_NOTIFICACAO_ENVIO_TRANSPORTE_FETCH_LIMIT,
    getAutoNotificacaoEnvioFetchLimit(),
    10,
    250
  );
}

function getAutoNotificacaoEnvioTransporteFetchMaxPages() {
  return getBoundedPositiveInt(
    process.env.AUTO_NOTIFICACAO_ENVIO_TRANSPORTE_FETCH_MAX_PAGES,
    getAutoNotificacaoEnvioFetchMaxPages(),
    1,
    40
  );
}

function getAutoNotificacaoEnvioTransporteMaxSendsPerRun() {
  return getBoundedPositiveInt(
    process.env.AUTO_NOTIFICACAO_ENVIO_TRANSPORTE_MAX_SENDS_PER_RUN,
    getAutoNotificacaoEnvioMaxSendsPerRun(),
    1,
    500
  );
}

function getAutoNotificacaoIncidenciaFetchLimit() {
  return getBoundedPositiveInt(process.env.AUTO_NOTIFICACAO_INCIDENCIA_FETCH_LIMIT, 120, 10, 250);
}

function getAutoNotificacaoIncidenciaFetchMaxPages() {
  return getBoundedPositiveInt(process.env.AUTO_NOTIFICACAO_INCIDENCIA_FETCH_MAX_PAGES, 6, 1, 40);
}

function getAutoNotificacaoIncidenciaMaxSendsPerRun() {
  return getBoundedPositiveInt(process.env.AUTO_NOTIFICACAO_INCIDENCIA_MAX_SENDS_PER_RUN, 20, 1, 500);
}

function getAutoNotificacaoIncidenciaGraceMinutes() {
  const raw = Number(process.env.AUTO_NOTIFICACAO_INCIDENCIA_GRACE_MINUTES || 30);
  if (!Number.isFinite(raw)) {
    return 30;
  }
  return Math.max(0, Math.min(30, Math.trunc(raw)));
}

function isWithinClockWindow(parts, targetHour, targetMinute, graceMinutes) {
  const nowTotal = (parts.hour * 60) + parts.minute;
  const targetTotal = (targetHour * 60) + targetMinute;
  const delta = nowTotal - targetTotal;
  return delta >= 0 && delta <= graceMinutes;
}

async function hydrateAutoNotificacaoEnvioState() {
  if (autoNotificacaoEnvioStateHydrated) {
    return;
  }

  try {
    if (pgEnabled && pgPool) {
      try {
        await ensurePersistentStateTable();
      } catch (error) {
        warnSoftError("auto_envio.hydrate.ensure_table", error);
      }
    }

    if (supabaseEnabled && supabase) {
      try {
        const { data, error } = await supabase
          .from("workspace_state")
          .select("value")
          .eq("key", "auto_notificacao_envio_state")
          .limit(1)
          .maybeSingle();

        if (!error && data?.value && typeof data.value === "object") {
          autoNotificacaoEnvioLastRunDateKey = String(data.value.envioLastRunDateKey || "").trim();
          autoNotificacaoEnvioTransporteLastRunDateKey = String(data.value.transporteLastRunDateKey || "").trim();
          return;
        }
      } catch (error) {
        warnSoftError("auto_envio.hydrate.supabase", error);
      }
    }

    if (pgEnabled && pgPool) {
      try {
        const { rows } = await pgPool.query(
          `select value from public.workspace_state where key = $1 limit 1`,
          ["auto_notificacao_envio_state"]
        );
        const value = Array.isArray(rows) && rows.length > 0 ? rows[0]?.value : null;
        if (value && typeof value === "object") {
          autoNotificacaoEnvioLastRunDateKey = String(value.envioLastRunDateKey || "").trim();
          autoNotificacaoEnvioTransporteLastRunDateKey = String(value.transporteLastRunDateKey || "").trim();
          return;
        }
      } catch (error) {
        warnSoftError("auto_envio.hydrate.postgres", error);
      }
    }

    const raw = await readFile(autoNotificacaoEnvioStateFile, "utf8");
    const parsed = JSON.parse(raw || "{}") || {};

    autoNotificacaoEnvioLastRunDateKey = String(parsed.envioLastRunDateKey || "").trim();
    autoNotificacaoEnvioTransporteLastRunDateKey = String(parsed.transporteLastRunDateKey || "").trim();
  } catch (error) {
    warnSoftError("auto_envio.hydrate.file", error);
    // Ignore missing or invalid persisted state; scheduler will bootstrap.
  } finally {
    autoNotificacaoEnvioStateHydrated = true;
  }
}

async function persistAutoNotificacaoEnvioState() {
  const payload = {
    envioLastRunDateKey: autoNotificacaoEnvioLastRunDateKey,
    transporteLastRunDateKey: autoNotificacaoEnvioTransporteLastRunDateKey,
    updatedAt: new Date().toISOString()
  };

  if (pgEnabled && pgPool) {
    try {
      await ensurePersistentStateTable();
    } catch (error) {
      warnSoftError("auto_envio.persist.ensure_table", error);
    }
  }

  if (supabaseEnabled && supabase) {
    try {
      const { error } = await supabase
        .from("workspace_state")
        .upsert([{ key: "auto_notificacao_envio_state", value: payload }], { onConflict: "key" });
      if (!error) {
        return;
      }
    } catch (error) {
      warnSoftError("auto_envio.persist.supabase", error);
    }
  }

  if (pgEnabled && pgPool) {
    try {
      await pgPool.query(
        `insert into public.workspace_state (key, value, updated_at)
        values ($1, $2::jsonb, now())
        on conflict (key) do update set value = excluded.value, updated_at = now()`,
        ["auto_notificacao_envio_state", JSON.stringify(payload)]
      );
      return;
    } catch (error) {
      warnSoftError("auto_envio.persist.postgres", error);
    }
  }

  try {
    await writeFile(autoNotificacaoEnvioStateFile, JSON.stringify(payload, null, 2), "utf8");
  } catch (error) {
    console.error(
      "[auto-notificacao-envio] failed to persist state",
      error instanceof Error ? error.message : error
    );
  }
}

async function fetchAllTmsInDistributionShipmentsData({ limit = 250, maxPages = 40 } = {}) {
  const allRows = [];
  let page = 1;
  let totalPages = 1;

  while (page <= totalPages && page <= maxPages) {
    const pageData = await fetchTmsInDistributionShipmentsData({ page, limit });
    const rows = Array.isArray(pageData?.rows) ? pageData.rows : [];
    allRows.push(...rows);

    totalPages = Number(pageData?.meta?.totalPages || 1) || 1;
    page += 1;
  }

  return allRows;
}

async function fetchAllTmsDeliveredShipmentsData({ limit = 250, maxPages = 40 } = {}) {
  const allRows = [];
  let page = 1;
  let totalPages = 1;

  while (page <= totalPages && page <= maxPages) {
    const pageData = await fetchTmsDeliveredShipmentsData({ page, limit });
    const rows = Array.isArray(pageData?.rows) ? pageData.rows : [];
    allRows.push(...rows);

    totalPages = Number(pageData?.meta?.totalPages || 1) || 1;
    page += 1;
  }

  return allRows;
}

async function fetchAllTmsIncidenceShipmentsData({ limit = 250, maxPages = 40 } = {}) {
  const allRows = [];
  let page = 1;
  let totalPages = 1;

  while (page <= totalPages && page <= maxPages) {
    const pageData = await fetchTmsIncidenceShipmentsData({ page, limit });
    const rows = Array.isArray(pageData?.rows) ? pageData.rows : [];
    allRows.push(...rows);

    totalPages = Number(pageData?.meta?.totalPages || 1) || 1;
    page += 1;
  }

  return allRows;
}

async function fetchAllTmsInTransportShipmentsData({ limit = 250, maxPages = 40 } = {}) {
  const allRows = [];
  let page = 1;
  let totalPages = 1;

  while (page <= totalPages && page <= maxPages) {
    const pageData = await fetchTmsInTransportShipmentsData({ page, limit });
    const rows = Array.isArray(pageData?.rows) ? pageData.rows : [];
    allRows.push(...rows);

    totalPages = Number(pageData?.meta?.totalPages || 1) || 1;
    page += 1;
  }

  return allRows;
}

function shouldRunAutoNotificacaoIncidenciaAtClock(parts) {
  const isWeekend = ["Sat", "Sun"].includes(parts.weekday);
  // Monday-Friday only.
  if (isWeekend) {
    return false;
  }

  // Match GitHub cron slots exactly (Lisbon):
  // 14:30, 15:00, 15:30, 16:30, 17:00, 17:30, 18:00, 18:30, 19:00
  const allowedMinutesByHour = {
    14: [30],
    15: [0, 30],
    16: [30],
    17: [0, 30],
    18: [0, 30],
    19: [0]
  };

  const graceMinutes = getAutoNotificacaoIncidenciaGraceMinutes();
  const nowTotalMinutes = (parts.hour * 60) + parts.minute;

  for (const [hourRaw, minutes] of Object.entries(allowedMinutesByHour)) {
    const hour = Number(hourRaw);
    for (const minute of minutes) {
      const targetTotalMinutes = (hour * 60) + minute;
      const delta = nowTotalMinutes - targetTotalMinutes;
      if (delta >= 0 && delta <= graceMinutes) {
        return true;
      }
    }
  }

  return false;
}

async function maybeRunAutoNotificacaoIncidenciaSchedule(options = {}) {
  const forceRun = Boolean(options?.forceRun);
  const enabledRaw = String(process.env.AUTO_NOTIFICACAO_INCIDENCIA_ENABLED || "true").trim().toLowerCase();
  const enabled = !["0", "false", "no", "off"].includes(enabledRaw);
  if (!enabled) {
    return { ok: true, skipped: true, reason: "AUTO_NOTIFICACAO_INCIDENCIA_ENABLED=false" };
  }

  if (autoNotificacaoIncidenciaRunning) {
    return { ok: true, skipped: true, reason: "already_running" };
  }

  const parts = getLisbonClockParts();
  if (!forceRun && !shouldRunAutoNotificacaoIncidenciaAtClock(parts)) {
    return {
      ok: true,
      skipped: true,
      reason: "outside_schedule_window",
      lisbonClock: {
        dateKey: parts.dateKey,
        weekday: parts.weekday,
        hour: parts.hour,
        minute: parts.minute
      }
    };
  }

  const slotKey = `${parts.dateKey} ${String(parts.hour).padStart(2, "0")}:${String(parts.minute).padStart(2, "0")}`;
  if (!forceRun && autoNotificacaoIncidenciaLastRunSlotKey === slotKey) {
    return { ok: true, skipped: true, reason: "already_ran_slot", slotKey };
  }

  autoNotificacaoIncidenciaLastRunSlotKey = slotKey;

  autoNotificacaoIncidenciaRunning = true;
  try {
    await hydrateAutoNotificacaoIncidenciaState();
    const nowIso = new Date().toISOString();

    const fetchLimit = getAutoNotificacaoIncidenciaFetchLimit();
    const fetchMaxPages = getAutoNotificacaoIncidenciaFetchMaxPages();
    const maxSendsPerRun = getAutoNotificacaoIncidenciaMaxSendsPerRun();

    // Refresh source data each cycle to detect newly appeared incidence rows.
    const rows = await fetchAllTmsIncidenceShipmentsData({ limit: fetchLimit, maxPages: fetchMaxPages });
    const templateName = String(process.env.AUTO_NOTIFICACAO_INCIDENCIA_TEMPLATE || "notificacao_auto_incidencia").trim();
    const languageCode = String(process.env.AUTO_NOTIFICACAO_INCIDENCIA_LANGUAGE || "pt_PT").trim() || "pt_PT";

    const freshEntries = [];
    const currentEntriesByShipmentKey = new Map();
    for (const row of rows) {
      const shipmentKey = buildIncidenciaShipmentKey(row);
      if (!shipmentKey) continue;

      const to = normalizeRecipient(String(row?.finalClientPhone || ""));
      const destinatario = String(row?.recipient || "").trim();
      const parcelId = String(row?.parcelId || "").trim();
      const sender = String(row?.sender || "").trim();
      const incidentReason = String(row?.incidentReason || row?.incidence || "").trim();

      if (!to) continue;

      if (!currentEntriesByShipmentKey.has(shipmentKey)) {
        currentEntriesByShipmentKey.set(shipmentKey, {
          shipmentKey,
          to,
          destinatario,
          parcelId,
          sender,
          incidentReason
        });
      }

      setAutoNotificacaoIncidenciaKeyMeta(shipmentKey, {
        firstSeenAt: nowIso,
        lastSeenAt: nowIso
      });

      if (!autoNotificacaoIncidenciaKnownKeys.has(shipmentKey)) {
        autoNotificacaoIncidenciaKnownKeys.add(shipmentKey);
        freshEntries.push({
          shipmentKey,
          to,
          destinatario,
          parcelId,
          sender,
          incidentReason
        });
      }
    }

    if (!autoNotificacaoIncidenciaInitialized) {
      autoNotificacaoIncidenciaInitialized = true;
      await persistAutoNotificacaoIncidenciaState();
      console.log("[auto-notificacao-incidencia] bootstrap complete", {
        trackedKeys: autoNotificacaoIncidenciaKnownKeys.size,
        fetchedRows: rows.length
      });
      return {
        ok: true,
        skipped: true,
        reason: "bootstrap_initialized",
        fetchedRows: rows.length,
        trackedKeys: autoNotificacaoIncidenciaKnownKeys.size,
        lisbonClock: {
          dateKey: parts.dateKey,
          weekday: parts.weekday,
          hour: parts.hour,
          minute: parts.minute
        }
      };
    }

    let processed = 0;
    let sent = 0;
    let failed = 0;

    const queueByShipmentKey = new Map();
    for (const [shipmentKey, value] of autoNotificacaoIncidenciaPendingEntries.entries()) {
      queueByShipmentKey.set(shipmentKey, {
        shipmentKey,
        to: value.to,
        destinatario: value.destinatario,
        parcelId: value.parcelId,
        sender: value.sender,
        incidentReason: String(value.incidentReason || "").trim()
      });
    }
    for (const entry of currentEntriesByShipmentKey.values()) {
      if (!queueByShipmentKey.has(entry.shipmentKey)) {
        queueByShipmentKey.set(entry.shipmentKey, entry);
      }
    }

    for (const entry of queueByShipmentKey.values()) {
      if (autoNotificacaoIncidenciaSentKeys.has(entry.shipmentKey)) continue;
      if (processed >= maxSendsPerRun) break;

      const shipmentKey = entry.shipmentKey;
      processed += 1;
      const result = await sendGenericTemplateMessage({
        to: entry.to,
        templateName,
        languageCode,
        bodyVariables: [entry.destinatario, entry.parcelId, entry.sender],
        trackerContext: {
          clientName: entry.destinatario,
          parcelId: entry.parcelId,
          messageType: "Incidencia",
          notes: String(entry.incidentReason || entry.sender || "").trim()
        }
      });

      if (result.ok) {
        sent += 1;
        autoNotificacaoIncidenciaSentKeys.add(shipmentKey);
        setAutoNotificacaoIncidenciaKeyMeta(shipmentKey, {
          firstSeenAt: nowIso,
          lastSeenAt: nowIso,
          sentAt: nowIso
        });
        autoNotificacaoIncidenciaPendingEntries.delete(shipmentKey);
      } else {
        failed += 1;
        autoNotificacaoIncidenciaPendingEntries.set(shipmentKey, {
          to: entry.to,
          destinatario: entry.destinatario,
          parcelId: entry.parcelId,
          sender: entry.sender,
          incidentReason: String(entry.incidentReason || "").trim()
        });
      }
    }

    await persistAutoNotificacaoIncidenciaState();
    if (processed > 0 || freshEntries.length > 0) {
      console.log("[auto-notificacao-incidencia]", {
        processed,
        sent,
        failed,
        freshEntries: freshEntries.length,
        fetchedRows: rows.length,
        templateName,
        languageCode
      });
    }

    return {
      ok: true,
      mode: "weekday_send",
      processed,
      sent,
      failed,
      freshEntries: freshEntries.length,
      pendingTotal: autoNotificacaoIncidenciaPendingEntries.size,
      fetchedRows: rows.length,
      fetchLimit,
      fetchMaxPages,
      maxSendsPerRun,
      templateName,
      languageCode,
      lisbonClock: {
        dateKey: parts.dateKey,
        weekday: parts.weekday,
        hour: parts.hour,
        minute: parts.minute
      }
    };
  } catch (error) {
    console.error("[auto-notificacao-incidencia] failed", error instanceof Error ? error.message : error);
    return {
      ok: false,
      error: "Failed to run auto notificacao incidencia",
      details: error instanceof Error ? error.message : "Unknown error"
    };
  } finally {
    autoNotificacaoIncidenciaRunning = false;
  }
}

async function runAutoNotificacaoEnvioForInDistribution(options = {}) {
  const templateName = String(process.env.AUTO_NOTIFICACAO_ENVIO_TEMPLATE || "notificacao_de_envio").trim();
  const languageCode = String(process.env.AUTO_NOTIFICACAO_ENVIO_LANGUAGE || "pt_PT").trim() || "pt_PT";
  const fetchLimit = Number.isFinite(Number(options?.limit))
    ? Math.max(10, Math.min(250, Math.trunc(Number(options.limit))))
    : getAutoNotificacaoEnvioFetchLimit();
  const fetchMaxPages = Number.isFinite(Number(options?.maxPages))
    ? Math.max(1, Math.min(40, Math.trunc(Number(options.maxPages))))
    : getAutoNotificacaoEnvioFetchMaxPages();
  const maxSendsPerRun = Number.isFinite(Number(options?.maxSendsPerRun))
    ? Math.max(1, Math.min(500, Math.trunc(Number(options.maxSendsPerRun))))
    : getAutoNotificacaoEnvioMaxSendsPerRun();

  // Refresh source data first (equivalent to clicking "Atualizar em distribuicao").
  const rows = await fetchAllTmsInDistributionShipmentsData({ limit: fetchLimit, maxPages: fetchMaxPages });

  let processed = 0;
  let sent = 0;
  let failed = 0;
  let reachedCap = false;

  for (const row of rows) {
    if (processed >= maxSendsPerRun) {
      reachedCap = true;
      break;
    }

    const to = normalizeRecipient(String(row?.finalClientPhone || ""));
    if (!to) {
      continue;
    }

    const var1 = String(row?.recipient || "").trim();
    const var2 = String(row?.sender || "").trim();
    const var3 = String(row?.providerTrackingCode || row?.parcelId || "").trim();

    processed += 1;

    const result = await sendGenericTemplateMessage({
      to,
      templateName,
      languageCode,
      bodyVariables: [var1, var2, var3],
      trackerContext: {
        clientName: var1,
        parcelId: var3,
        messageType: "Em distribuicao",
        notes: var2
      }
    });

    if (result.ok) {
      sent += 1;
    } else {
      failed += 1;
    }
  }

  return {
    processed,
    sent,
    failed,
    reachedCap,
    maxSendsPerRun,
    fetchedRows: rows.length,
    fetchLimit,
    fetchMaxPages,
    templateName,
    languageCode
  };
}

async function runAutoNotificacaoEnvioForInTransport(options = {}) {
  const templateName = String(
    process.env.AUTO_NOTIFICACAO_ENVIO_TRANSPORTE_TEMPLATE ||
    process.env.AUTO_NOTIFICACAO_ENVIO_TEMPLATE ||
    "notificacao_de_envio"
  ).trim();
  const fallbackTemplateName = String(
    process.env.AUTO_NOTIFICACAO_ENVIO_TEMPLATE ||
    "notificacao_de_envio"
  ).trim() || "notificacao_de_envio";
  const languageCode = String(
    process.env.AUTO_NOTIFICACAO_ENVIO_TRANSPORTE_LANGUAGE ||
    process.env.AUTO_NOTIFICACAO_ENVIO_LANGUAGE ||
    "pt_PT"
  ).trim() || "pt_PT";
  const fetchLimit = Number.isFinite(Number(options?.limit))
    ? Math.max(10, Math.min(250, Math.trunc(Number(options.limit))))
    : getAutoNotificacaoEnvioTransporteFetchLimit();
  const fetchMaxPages = Number.isFinite(Number(options?.maxPages))
    ? Math.max(1, Math.min(40, Math.trunc(Number(options.maxPages))))
    : getAutoNotificacaoEnvioTransporteFetchMaxPages();
  const maxSendsPerRun = Number.isFinite(Number(options?.maxSendsPerRun))
    ? Math.max(1, Math.min(500, Math.trunc(Number(options.maxSendsPerRun))))
    : getAutoNotificacaoEnvioTransporteMaxSendsPerRun();

  // Refresh source data first (equivalent to clicking "Atualizar em transporte").
  const rows = await fetchAllTmsInTransportShipmentsData({ limit: fetchLimit, maxPages: fetchMaxPages });

  let processed = 0;
  let sent = 0;
  let failed = 0;
  let skippedMaritimoIlhas = 0;
  let reachedCap = false;

  for (const row of rows) {
    if (processed >= maxSendsPerRun) {
      reachedCap = true;
      break;
    }

    const serviceName = String(row?.service || "")
      .normalize("NFD")
      .replace(/\p{Diacritic}/gu, "")
      .toLowerCase();
    const compactServiceName = serviceName.replace(/\s+/g, "");
    const isMaritimoIlhasService =
      compactServiceName.includes("cttmaritimoilhas") ||
      (serviceName.includes("maritimo") && serviceName.includes("ilhas"));
    const isAereoIlhasService =
      compactServiceName.includes("cttaereoilhas") ||
      (serviceName.includes("aereo") && serviceName.includes("ilhas"));

    if (isMaritimoIlhasService || isAereoIlhasService) {
      skippedMaritimoIlhas += 1;
      continue;
    }

    const to = normalizeRecipient(String(row?.finalClientPhone || ""));
    if (!to) {
      continue;
    }

    const var1 = String(row?.recipient || "").trim();
    const var2 = String(row?.sender || "").trim();
    const var3 = String(row?.providerTrackingCode || row?.parcelId || "").trim();

    processed += 1;

    let result = await sendGenericTemplateMessage({
      to,
      templateName,
      languageCode,
      bodyVariables: [var1, var2, var3],
      trackerContext: {
        clientName: var1,
        parcelId: var3,
        messageType: "Em transporte",
        notes: var2
      }
    });

    if (!result.ok && fallbackTemplateName && fallbackTemplateName !== templateName) {
      result = await sendGenericTemplateMessage({
        to,
        templateName: fallbackTemplateName,
        languageCode,
        bodyVariables: [var1, var2, var3],
        trackerContext: {
          clientName: var1,
          parcelId: var3,
          messageType: "Em transporte",
          notes: var2
        }
      });
    }

    if (result.ok) {
      sent += 1;
    } else {
      failed += 1;
    }
  }

  return {
    processed,
    sent,
    failed,
    reachedCap,
    maxSendsPerRun,
    skippedMaritimoIlhas,
    fetchedRows: rows.length,
    fetchLimit,
    fetchMaxPages,
    templateName,
    fallbackTemplateName,
    languageCode
  };
}

async function buildAutoNotificacaoEnvioDryRunSummary(options = {}) {
  const templateName = String(process.env.AUTO_NOTIFICACAO_ENVIO_TEMPLATE || "notificacao_de_envio").trim();
  const languageCode = String(process.env.AUTO_NOTIFICACAO_ENVIO_LANGUAGE || "pt_PT").trim() || "pt_PT";
  const limit = Number.isFinite(Number(options?.limit)) ? Math.max(1, Number(options.limit)) : 250;
  const maxPages = Number.isFinite(Number(options?.maxPages)) ? Math.max(1, Number(options.maxPages)) : 40;
  const sampleSize = Number.isFinite(Number(options?.sampleSize)) ? Math.max(1, Number(options.sampleSize)) : 15;

  // Fetch source data exactly like the real auto run, but do not send anything.
  const rows = await fetchAllTmsInDistributionShipmentsData({ limit, maxPages });

  let eligibleToSend = 0;
  let skippedMissingPhone = 0;
  const sample = [];

  for (const row of rows) {
    const to = normalizeRecipient(String(row?.finalClientPhone || ""));
    if (!to) {
      skippedMissingPhone += 1;
      continue;
    }

    eligibleToSend += 1;
    if (sample.length < sampleSize) {
      sample.push({
        to,
        recipient: String(row?.recipient || "").trim(),
        sender: String(row?.sender || "").trim(),
        parcelId: String(row?.providerTrackingCode || row?.parcelId || "").trim()
      });
    }
  }

  return {
    templateName,
    languageCode,
    fetchedRows: rows.length,
    eligibleToSend,
    skippedMissingPhone,
    sample
  };
}

function shouldRunAutoNotificacaoEnvioAtClock(parts) {
  const isWeekday = !["Sat", "Sun"].includes(parts.weekday);
  const targetHour = Number(process.env.AUTO_NOTIFICACAO_ENVIO_HOUR || 9);
  const targetMinute = Number(process.env.AUTO_NOTIFICACAO_ENVIO_MINUTE || 30);
  const graceMinutes = getAutoNotificacaoEnvioGraceMinutes();
  return isWeekday && isWithinClockWindow(parts, targetHour, targetMinute, graceMinutes);
}

function shouldRunAutoNotificacaoEnvioTransporteAtClock(parts) {
  const isWeekday = !["Sat", "Sun"].includes(parts.weekday);
  const targetHour = Number(process.env.AUTO_NOTIFICACAO_ENVIO_TRANSPORTE_HOUR || 10);
  const targetMinute = Number(process.env.AUTO_NOTIFICACAO_ENVIO_TRANSPORTE_MINUTE || 0);
  const graceMinutes = getAutoNotificacaoEnvioTransporteGraceMinutes();
  return isWeekday && isWithinClockWindow(parts, targetHour, targetMinute, graceMinutes);
}

async function maybeRunAutoNotificacaoEnvioSchedule() {
  const enabledRaw = String(process.env.AUTO_NOTIFICACAO_ENVIO_ENABLED || "true").trim().toLowerCase();
  const enabled = !["0", "false", "no", "off"].includes(enabledRaw);
  if (!enabled) {
    return;
  }

  const parts = getLisbonClockParts();
  if (!shouldRunAutoNotificacaoEnvioAtClock(parts)) {
    return;
  }

  await hydrateAutoNotificacaoEnvioState();

  if (autoNotificacaoEnvioLastRunDateKey === parts.dateKey || autoNotificacaoEnvioRunning) {
    return;
  }

  autoNotificacaoEnvioRunning = true;
  try {
    const summary = await runAutoNotificacaoEnvioForInDistribution();
    autoNotificacaoEnvioLastRunDateKey = parts.dateKey;
    await persistAutoNotificacaoEnvioState();
    console.log("[auto-notificacao-envio]", summary);
  } catch (error) {
    console.error("[auto-notificacao-envio] failed", error instanceof Error ? error.message : error);
  } finally {
    autoNotificacaoEnvioRunning = false;
  }
}

async function maybeRunAutoNotificacaoEnvioTransporteSchedule() {
  const enabledRaw = String(process.env.AUTO_NOTIFICACAO_ENVIO_TRANSPORTE_ENABLED || "true").trim().toLowerCase();
  const enabled = !["0", "false", "no", "off"].includes(enabledRaw);
  if (!enabled) {
    return;
  }

  const parts = getLisbonClockParts();
  if (!shouldRunAutoNotificacaoEnvioTransporteAtClock(parts)) {
    return;
  }

  await hydrateAutoNotificacaoEnvioState();

  if (autoNotificacaoEnvioTransporteLastRunDateKey === parts.dateKey || autoNotificacaoEnvioTransporteRunning) {
    return;
  }

  autoNotificacaoEnvioTransporteRunning = true;
  try {
    const summary = await runAutoNotificacaoEnvioForInTransport();
    autoNotificacaoEnvioTransporteLastRunDateKey = parts.dateKey;
    await persistAutoNotificacaoEnvioState();
    console.log("[auto-notificacao-envio-em-transporte]", summary);
  } catch (error) {
    console.error("[auto-notificacao-envio-em-transporte] failed", error instanceof Error ? error.message : error);
  } finally {
    autoNotificacaoEnvioTransporteRunning = false;
  }
}

// Avoid perpetual background timers in serverless environments.
const internalSchedulerEnabledRaw = String(process.env.AUTO_NOTIFICACAO_INTERNAL_SCHEDULER_ENABLED || "true").trim().toLowerCase();
const internalSchedulerEnabled = ["1", "true", "yes", "on"].includes(internalSchedulerEnabledRaw);

if (!process.env.VERCEL && internalSchedulerEnabled) {
  // Keep the internal scheduler aligned with cron-like minute precision.
  setInterval(() => {
    void processScheduledMessages();
  }, 60_000);
  setInterval(() => {
    const parts = getLisbonClockParts();
    const shouldRunAnyAutoNotificacao =
      shouldRunAutoNotificacaoEnvioAtClock(parts)
      || shouldRunAutoNotificacaoEnvioTransporteAtClock(parts)
      || shouldRunAutoNotificacaoIncidenciaAtClock(parts);

    if (!shouldRunAnyAutoNotificacao) {
      return;
    }

    void maybeRunAutoNotificacaoEnvioSchedule();
    void maybeRunAutoNotificacaoEnvioTransporteSchedule();
    void maybeRunAutoNotificacaoIncidenciaSchedule();
  }, 60_000);
}

const logsResponseCache = {
  key: "",
  etag: "",
  expiresAt: 0,
  payload: null
};

const notion = new NotionClient({ auth: process.env.NOTION_API_KEY });
const notionEnabled = String(process.env.NOTION_ENABLED || "false").toLowerCase() === "true";
const notionTrackerApiKey = String(process.env.NOTION_TRACKER_API_KEY || process.env.NOTION_API_KEY || "").trim();
const notionTracker = notionTrackerApiKey ? new NotionClient({ auth: notionTrackerApiKey }) : null;
const notionTrackerDatabaseId = String(process.env.NOTION_TRACKER_DATABASE_ID || "").trim();
const notionTrackerEnabled = Boolean(notionTracker && notionTrackerDatabaseId);
const notionConsumiveisApiKey = String(process.env.NOTION_CONSUMIVEIS_API_KEY || process.env.NOTION_API_KEY || "").trim();
const notionConsumiveis = notionConsumiveisApiKey ? new NotionClient({ auth: notionConsumiveisApiKey }) : null;
const notionConsumiveisDatabaseId = String(process.env.NOTION_CONSUMIVEIS_DATABASE_ID || "").trim();
const notionConsumiveisPageId = normalizeNotionBlockId(process.env.NOTION_CONSUMIVEIS_PAGE_ID);
const notionConsumiveisEnabled = Boolean(notionConsumiveis && (notionConsumiveisDatabaseId || notionConsumiveisPageId));
const notionFeedbackApiKey = String(process.env.NOTION_FEEDBACK_API_KEY || "").trim();
const notionFeedback = notionFeedbackApiKey ? new NotionClient({ auth: notionFeedbackApiKey }) : null;
const notionFeedbackDatabaseId = String(process.env.NOTION_FEEDBACK_DATABASE_ID || "").trim();
const notionFeedbackPageId = String(process.env.NOTION_FEEDBACK_PAGE_ID || "").trim();
const notionFeedbackEnabled = Boolean(notionFeedback && (notionFeedbackDatabaseId || notionFeedbackPageId));
const botpressWebhookRelayUrl = String(process.env.BOTPRESS_WEBHOOK_RELAY_URL || "").trim();
const botpressApiKey = String(process.env.BOTPRESS_API_KEY || "").trim();
const blobReadWriteToken = String(process.env.BLOB_READ_WRITE_TOKEN || "").trim();
const blobMediaUploadEnabled = Boolean(blobReadWriteToken);
const blobMediaAccessRaw = String(process.env.BLOB_MEDIA_ACCESS || "private").trim().toLowerCase();
const blobMediaAccess = blobMediaAccessRaw === "public" ? "public" : "private";
const mediaBlobUrlSignSecret = String(process.env.MEDIA_BLOB_URL_SIGN_SECRET || process.env.CRON_SECRET || "").trim();
const mediaBlobUrlDefaultTtlSeconds = Math.max(
  60,
  Math.min(24 * 60 * 60, Number(process.env.MEDIA_BLOB_URL_DEFAULT_TTL_SECONDS || 900) || 900)
);
const supabaseUrl = String(process.env.SUPABASE_URL || "").trim();
const supabaseServiceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();
const supabaseEnabled = Boolean(supabaseUrl && supabaseServiceRoleKey);
const supabase = supabaseEnabled
  ? createClient(supabaseUrl, supabaseServiceRoleKey, { auth: { persistSession: false } })
  : null;
const supabaseDbUrl = String(process.env.SUPABASE_DB_URL || process.env.DATABASE_URL || "").trim();
const pgEnabled = Boolean(supabaseDbUrl);
const pgPool = pgEnabled
  ? new Pool({
    connectionString: supabaseDbUrl,
    ssl: { rejectUnauthorized: false }
  })
  : null;

const PERSISTENCE_KEYS = [
  "contacts",
  "contact_notes",
  "team_reminders",
  "personal_notes",
  "calendar_events",
  "clientes_email_templates"
];

const STATE_FALLBACK_CHANNEL = "workspace_state_snapshot";
const STATE_FALLBACK_TO = "workspace";

async function ensurePersistentStateTable() {
  if (!pgEnabled || !pgPool) {
    return;
  }

  await pgPool.query(`
    create table if not exists public.workspace_state (
      key text primary key,
      value jsonb not null default '{}'::jsonb,
      updated_at timestamptz not null default now()
    )
  `);
}

function defaultWorkspaceState() {
  return {
    contacts: {},
    contact_notes: {},
    team_reminders: [],
    personal_notes: [],
    calendar_events: [],
    clientes_email_templates: []
  };
}

function normalizeWorkspaceState(input) {
  const defaults = defaultWorkspaceState();
  const source = input && typeof input === "object" ? input : {};
  const next = { ...defaults };

  for (const key of PERSISTENCE_KEYS) {
    if (Object.prototype.hasOwnProperty.call(source, key)) {
      next[key] = source[key];
    }
  }

  if (!next.contacts || typeof next.contacts !== "object" || Array.isArray(next.contacts)) {
    next.contacts = {};
  }
  if (!next.contact_notes || typeof next.contact_notes !== "object" || Array.isArray(next.contact_notes)) {
    next.contact_notes = {};
  }
  if (!Array.isArray(next.team_reminders)) {
    next.team_reminders = [];
  }
  if (!Array.isArray(next.personal_notes)) {
    next.personal_notes = [];
  }
  if (!Array.isArray(next.calendar_events)) {
    next.calendar_events = [];
  }
  if (!Array.isArray(next.clientes_email_templates)) {
    next.clientes_email_templates = [];
  }

  return next;
}

function normalizeContactsMap(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return {};
  }

  const entries = Object.entries(input)
    .map(([phone, name]) => [String(phone || "").trim(), String(name || "").trim()])
    .filter(([phone, name]) => phone.length > 0 && name.length > 0);

  return Object.fromEntries(entries);
}

function contactsMapFromRows(rows) {
  if (!Array.isArray(rows)) {
    return {};
  }

  const entries = rows
    .map((row) => [String(row?.phone || "").trim(), String(row?.name || "").trim()])
    .filter(([phone, name]) => phone.length > 0 && name.length > 0);

  return Object.fromEntries(entries);
}

async function loadContactsFromDedicatedTable() {
  if (supabaseEnabled && supabase) {
    const { data, error } = await supabase.from("workspace_contacts").select("phone,name");
    if (error) {
      throw new Error(error.message || "Failed to read workspace_contacts");
    }
    return contactsMapFromRows(data || []);
  }

  if (pgEnabled && pgPool) {
    const { rows } = await pgPool.query(`select phone, name from public.workspace_contacts`);
    return contactsMapFromRows(rows || []);
  }

  return {};
}

async function syncContactsToDedicatedTable(contactsMapInput) {
  const contactsMap = normalizeContactsMap(contactsMapInput);
  const rows = Object.entries(contactsMap).map(([phone, name]) => ({ phone, name }));

  if (supabaseEnabled && supabase) {
    if (rows.length > 0) {
      const { error: upsertError } = await supabase
        .from("workspace_contacts")
        .upsert(rows, { onConflict: "phone" });
      if (upsertError) {
        throw new Error(upsertError.message || "Failed to upsert workspace_contacts");
      }
    }

    const { data: existing, error: existingError } = await supabase
      .from("workspace_contacts")
      .select("phone");
    if (existingError) {
      throw new Error(existingError.message || "Failed to list existing workspace_contacts");
    }

    const desiredPhones = new Set(Object.keys(contactsMap));
    const phonesToDelete = (existing || [])
      .map((row) => String(row?.phone || "").trim())
      .filter((phone) => phone.length > 0 && !desiredPhones.has(phone));

    if (phonesToDelete.length > 0) {
      const { error: deleteError } = await supabase
        .from("workspace_contacts")
        .delete()
        .in("phone", phonesToDelete);
      if (deleteError) {
        throw new Error(deleteError.message || "Failed to delete stale workspace_contacts rows");
      }
    }
    return;
  }

  if (pgEnabled && pgPool) {
    await pgPool.query(`
      create table if not exists public.workspace_contacts (
        id bigserial primary key,
        phone text not null unique,
        name text not null,
        created_at timestamptz not null default now(),
        updated_at timestamptz not null default now()
      )
    `);

    for (const [phone, name] of Object.entries(contactsMap)) {
      await pgPool.query(
        `insert into public.workspace_contacts (phone, name, updated_at)
        values ($1, $2, now())
        on conflict (phone) do update set name = excluded.name, updated_at = now()`,
        [phone, name]
      );
    }

    const desiredPhones = Object.keys(contactsMap);
    if (desiredPhones.length === 0) {
      await pgPool.query(`delete from public.workspace_contacts`);
    } else {
      await pgPool.query(
        `delete from public.workspace_contacts where not (phone = any($1::text[]))`,
        [desiredPhones]
      );
    }
  }
}

async function getWorkspaceStateFromLogsFallback() {
  if ((!supabaseEnabled || !supabase) && (!pgEnabled || !pgPool)) {
    return null;
  }

  if (supabaseEnabled && supabase) {
    const { data, error } = await supabase
      .from("whatsapp_logs")
      .select("payload")
      .eq("channel", STATE_FALLBACK_CHANNEL)
      .eq("to_number", STATE_FALLBACK_TO)
      .order("created_at", { ascending: false })
      .limit(1);

    if (error) {
      throw new Error(error.message || "Failed to read fallback workspace state");
    }

    const payload = Array.isArray(data) && data.length > 0 ? data[0]?.payload : null;
    const snapshot = payload && typeof payload === "object" ? payload.state : null;
    return normalizeWorkspaceState(snapshot);
  }

  const { rows } = await pgPool.query(
    `select payload
    from public.whatsapp_logs
    where channel = $1 and to_number = $2
    order by created_at desc
    limit 1`,
    [STATE_FALLBACK_CHANNEL, STATE_FALLBACK_TO]
  );

  const payload = Array.isArray(rows) && rows.length > 0 ? rows[0]?.payload : null;
  const snapshot = payload && typeof payload === "object" ? payload.state : null;
  return normalizeWorkspaceState(snapshot);
}

async function writeWorkspaceStateToLogsFallback(nextState) {
  const state = normalizeWorkspaceState(nextState);

  if (supabaseEnabled && supabase) {
    const { error } = await supabase.from("whatsapp_logs").insert({
      direction: "system",
      channel: STATE_FALLBACK_CHANNEL,
      to_number: STATE_FALLBACK_TO,
      contact_name: null,
      message_text: null,
      template_name: null,
      status: "snapshot",
      api_message_id: null,
      payload: { state }
    });

    if (error) {
      throw new Error(error.message || "Failed to write fallback workspace state");
    }
    return;
  }

  await pgPool.query(
    `insert into public.whatsapp_logs
    (direction, channel, to_number, contact_name, message_text, template_name, status, api_message_id, payload)
    values ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)`,
    [
      "system",
      STATE_FALLBACK_CHANNEL,
      STATE_FALLBACK_TO,
      null,
      null,
      null,
      "snapshot",
      null,
      JSON.stringify({ state })
    ]
  );
}

// Best effort init. If it fails, API responses will show an explicit error.
ensurePersistentStateTable().catch(() => {});

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function normalizeGoogleOauthSession(input) {
  const source = input && typeof input === "object" ? input : {};
  return {
    accessToken: String(source.accessToken || "").trim(),
    refreshToken: String(source.refreshToken || "").trim(),
    expiresAt: Number(source.expiresAt || 0) || 0,
    scope: String(source.scope || "").trim(),
    tokenType: String(source.tokenType || "Bearer").trim() || "Bearer"
  };
}

function applyGoogleOauthSession(input) {
  const next = normalizeGoogleOauthSession(input);
  googleOauthSession.accessToken = next.accessToken;
  googleOauthSession.refreshToken = next.refreshToken;
  googleOauthSession.expiresAt = next.expiresAt;
  googleOauthSession.scope = next.scope;
  googleOauthSession.tokenType = next.tokenType;
}

async function persistGoogleOauthSession() {
  const payload = {
    accessToken: googleOauthSession.accessToken,
    refreshToken: googleOauthSession.refreshToken,
    expiresAt: googleOauthSession.expiresAt,
    scope: googleOauthSession.scope,
    tokenType: googleOauthSession.tokenType
  };

  if (supabaseEnabled && supabase) {
    try {
      const { error } = await supabase
        .from("workspace_state")
        .upsert([{ key: "google_oauth_session", value: payload }], { onConflict: "key" });
      if (!error) {
        return;
      }
    } catch (error) {
      warnSoftError("google.oauth.persist.supabase", error);
    }
  }

  if (pgEnabled && pgPool) {
    await ensurePersistentStateTable();
    await pgPool.query(
      `insert into public.workspace_state (key, value, updated_at)
      values ($1, $2::jsonb, now())
      on conflict (key) do update set value = excluded.value, updated_at = now()`,
      ["google_oauth_session", JSON.stringify(payload)]
    );
  }
}

async function hydrateGoogleOauthSession() {
  if (googleOauthSessionHydrated) {
    return;
  }

  googleOauthSessionHydrated = true;

  if (googleOauthSession.accessToken || googleOauthSession.refreshToken) {
    return;
  }

  if (supabaseEnabled && supabase) {
    try {
      const { data, error } = await supabase
        .from("workspace_state")
        .select("value")
        .eq("key", "google_oauth_session")
        .limit(1)
        .maybeSingle();

      if (!error && data?.value) {
        applyGoogleOauthSession(data.value);
        return;
      }
    } catch (error) {
      warnSoftError("google.oauth.hydrate.supabase", error);
    }
  }

  if (pgEnabled && pgPool) {
    try {
      await ensurePersistentStateTable();
      const { rows } = await pgPool.query(
        `select value from public.workspace_state where key = $1 limit 1`,
        ["google_oauth_session"]
      );
      const value = Array.isArray(rows) && rows.length > 0 ? rows[0]?.value : null;
      if (value && typeof value === "object") {
        applyGoogleOauthSession(value);
      }
    } catch (error) {
      warnSoftError("google.oauth.hydrate.postgres", error);
    }
  }
}

function googleOauthStateSecret() {
  const secret = String(process.env.GOOGLE_OAUTH_STATE_SECRET || process.env.GOOGLE_OAUTH_CLIENT_SECRET || "").trim();
  if (!secret) {
    throw new Error("Missing state signing secret for Google OAuth");
  }
  return secret;
}

function createGoogleOauthState() {
  const issuedAt = String(Date.now());
  const nonce = randomBytes(16).toString("hex");
  const payload = `${issuedAt}.${nonce}`;
  const signature = createHmac("sha256", googleOauthStateSecret()).update(payload).digest("hex");
  return `${payload}.${signature}`;
}

function isGoogleOauthStateValid(state) {
  if (!state) return false;

  const parts = String(state).split(".");
  if (parts.length !== 3) return false;

  const [issuedAtRaw, nonce, signature] = parts;
  if (!/^\d+$/.test(issuedAtRaw)) return false;
  if (!/^[a-f0-9]{32}$/i.test(nonce)) return false;
  if (!/^[a-f0-9]{64}$/i.test(signature)) return false;

  const issuedAt = Number(issuedAtRaw);
  const ageMs = Date.now() - issuedAt;
  if (!Number.isFinite(issuedAt) || ageMs < 0 || ageMs > GOOGLE_OAUTH_STATE_TTL_MS) {
    return false;
  }

  const payload = `${issuedAtRaw}.${nonce}`;
  const expectedSignature = createHmac("sha256", googleOauthStateSecret()).update(payload).digest("hex");
  const provided = Buffer.from(signature, "utf8");
  const expected = Buffer.from(expectedSignature, "utf8");
  if (provided.length !== expected.length) return false;

  return timingSafeEqual(provided, expected);
}

function wantsHtmlResponse(req) {
  const format = String(req.query.format || "").trim().toLowerCase();
  if (format === "json") return false;
  if (format === "html") return true;
  const accept = String(req.headers.accept || "").toLowerCase();
  return accept.includes("text/html");
}

function sendOauthHtmlResponse(res, {
  ok,
  title,
  message,
  statusCode = 200
}) {
  const safeTitle = String(title || "Google OAuth").replace(/[<>]/g, "");
  const safeMessage = String(message || "").replace(/[<>]/g, "");
  const bg = ok ? "#ecfdf3" : "#fef2f2";
  const border = ok ? "#10b981" : "#ef4444";
  const text = ok ? "#065f46" : "#7f1d1d";

  const html = `<!doctype html>
<html lang="pt">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${safeTitle}</title>
    <style>
      :root { color-scheme: light; }
      body {
        margin: 0;
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
        background: #f8fafc;
        color: #0f172a;
        min-height: 100vh;
        display: grid;
        place-items: center;
        padding: 24px;
      }
      .card {
        width: min(560px, 100%);
        background: ${bg};
        border: 1px solid ${border};
        border-radius: 14px;
        padding: 20px 18px;
        box-shadow: 0 8px 30px rgba(15, 23, 42, 0.08);
      }
      h1 {
        margin: 0 0 10px;
        font-size: 20px;
        line-height: 1.3;
        color: ${text};
      }
      p {
        margin: 0;
        line-height: 1.5;
      }
      .hint {
        margin-top: 12px;
        font-size: 13px;
        opacity: 0.85;
      }
      .actions {
        margin-top: 16px;
      }
      button {
        border: 1px solid #cbd5e1;
        border-radius: 8px;
        background: #fff;
        color: #0f172a;
        font: inherit;
        padding: 8px 12px;
        cursor: pointer;
      }
    </style>
  </head>
  <body>
    <main class="card">
      <h1>${safeTitle}</h1>
      <p>${safeMessage}</p>
      <p class="hint">Esta janela pode fechar automaticamente.</p>
      <div class="actions">
        <button type="button" onclick="window.close()">Fechar janela</button>
      </div>
    </main>
    <script>
      try {
        if (window.opener && !window.opener.closed) {
          window.opener.postMessage({ type: "google_oauth_done", ok: ${ok ? "true" : "false"} }, "*");
        }
      } catch (error) {
        console.warn("[oauth-window-message]", error && error.message ? error.message : error);
      }
      setTimeout(() => {
        try { window.close(); } catch (error) { console.warn("[oauth-window-close]", error && error.message ? error.message : error); }
      }, 500);
    </script>
  </body>
</html>`;

  return res.status(statusCode).type("html").send(html);
}

function normalizeRecipient(input) {
  const digits = String(input ?? "").replace(/\D/g, "");
  if (!digits) return "";

  const withoutInternationalPrefix = digits.startsWith("00") ? digits.slice(2) : digits;

  // PT fallback: local mobile number (9 digits) -> prepend country code.
  if (withoutInternationalPrefix.length === 9 && withoutInternationalPrefix.startsWith("9")) {
    return `351${withoutInternationalPrefix}`;
  }

  return withoutInternationalPrefix;
}

function decodeHtmlEntities(input) {
  return String(input || "")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&nbsp;/g, " ");
}

function stripHtml(input) {
  return decodeHtmlEntities(String(input || "").replace(/<[^>]*>/g, " "))
    .replace(/\s+/g, " ")
    .trim();
}

function escapeHtml(input) {
  return String(input || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function normalizeDatatableTextCell(input) {
  const seen = new WeakSet();

  function visit(value) {
    if (value == null) return "";
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      return stripHtml(value);
    }
    if (Array.isArray(value)) {
      for (const item of value) {
        const normalized = visit(item);
        if (normalized) return normalized;
      }
      return "";
    }

    if (typeof value === "object") {
      if (seen.has(value)) return "";
      seen.add(value);

      const candidates = [
        value.display,
        value.label,
        value.text,
        value.name,
        value.title,
        value.value,
        value.html
      ];

      for (const candidate of candidates) {
        const normalized = visit(candidate);
        if (normalized) return normalized;
      }

      for (const nested of Object.values(value)) {
        const normalized = visit(nested);
        if (normalized) return normalized;
      }
    }

    return "";
  }

  return visit(input);
}

function splitSetCookieHeader(headerValue) {
  if (!headerValue) return [];
  return String(headerValue).split(/,(?=[^;,\s]+=)/g).map((cookie) => cookie.trim()).filter(Boolean);
}

function getSetCookieHeaders(response) {
  const direct = typeof response?.headers?.getSetCookie === "function"
    ? response.headers.getSetCookie()
    : null;

  if (Array.isArray(direct) && direct.length > 0) {
    return direct;
  }

  const single = response?.headers?.get?.("set-cookie") || "";
  return splitSetCookieHeader(single);
}

function updateCookieJar(cookieJar, setCookieHeaders) {
  const cookies = Array.isArray(setCookieHeaders)
    ? setCookieHeaders
    : splitSetCookieHeader(setCookieHeaders);

  for (const cookie of cookies) {
    const pair = cookie.split(";")[0] || "";
    const eqIndex = pair.indexOf("=");
    if (eqIndex <= 0) continue;
    const key = pair.slice(0, eqIndex).trim();
    const value = pair.slice(eqIndex + 1).trim();
    if (!key) continue;
    cookieJar.set(key, value);
  }
}

function cookieJarHeader(cookieJar) {
  return Array.from(cookieJar.entries())
    .map(([key, value]) => `${key}=${value}`)
    .join("; ");
}

function extractFirstNumber(input, fallback = "0") {
  const clean = stripHtml(input);
  const match = clean.match(/-?\d[\d\.,]*/);
  return match ? match[0] : fallback;
}

function parseTmsInfoBoxes(html) {
  const boxes = [];
  const boxRegex = /<span class="info-box-text">([\s\S]*?)<\/span>[\s\S]*?<span class="info-box-number">([\s\S]*?)<\/span>/g;
  let match;

  while ((match = boxRegex.exec(html)) !== null) {
    const label = stripHtml(match[1]);
    if (!label) continue;
    const value = extractFirstNumber(match[2]);
    const trendMatch = String(match[2]).match(/<small[^>]*>([\s\S]*?)<\/small>/i);
    const trend = trendMatch ? stripHtml(trendMatch[1]) : "";
    boxes.push({ label, value, trend });
  }

  return boxes;
}

function parseTmsServiceStatus(html) {
  const blockMatch = html.match(/<div class="nicescroll"[\s\S]*?<\/table>\s*<\/div>/i);
  if (!blockMatch) {
    return { rows: [], totals: null, highlights: {} };
  }

  const block = blockMatch[0];
  const rowRegex = /<tr>([\s\S]*?)<\/tr>/g;
  const rows = [];
  let rowMatch;

  while ((rowMatch = rowRegex.exec(block)) !== null) {
    const rowHtml = rowMatch[1];
    const serviceMatch = rowHtml.match(/<span[^>]*>([\s\S]*?)<\/span>/i);
    const service = serviceMatch ? stripHtml(serviceMatch[1]) : "";
    if (!service) continue;

    const values = [];
    const valueRegex = /stats-link-counter">\s*([\d]+)\s*<\/a>/g;
    let valueMatch;
    while ((valueMatch = valueRegex.exec(rowHtml)) !== null) {
      values.push(Number(valueMatch[1] || 0));
    }
    if (values.length < 6) continue;

    rows.push({
      service,
      pending: values[0],
      accepted: values[1],
      pickup: values[2],
      transport: values[3],
      delivered: values[4],
      incidence: values[5]
    });
  }

  const totalMatch = html.match(/<td class="bg-gray bold">TOTAL<\/td>[\s\S]*?<\/tr>/i);
  let totals = null;
  if (totalMatch) {
    const values = [];
    const valueRegex = /stats-link-counter">\s*([\d]+)\s*<\/a>/g;
    let valueMatch;
    while ((valueMatch = valueRegex.exec(totalMatch[0])) !== null) {
      values.push(Number(valueMatch[1] || 0));
    }
    if (values.length >= 6) {
      totals = {
        pending: values[0],
        accepted: values[1],
        pickup: values[2],
        transport: values[3],
        delivered: values[4],
        incidence: values[5]
      };
    }
  }

  const highlights = {};
  const highlightRegex = /stats-link-(accepted|pickup|transit|incidence)"><b>[\s\S]*?(\d+)\s*<\/b>/g;
  let highlightMatch;
  while ((highlightMatch = highlightRegex.exec(html)) !== null) {
    highlights[highlightMatch[1]] = Number(highlightMatch[2] || 0);
  }

  return { rows, totals, highlights };
}

function parseTmsPendingAcceptance(html) {
  const sectionMatch = html.match(/<th>Envios pendentes de aceitação<\/th>[\s\S]*?<\/tbody>/i);
  if (!sectionMatch) return [];

  const rows = [];
  const rowRegex = /<tr>\s*<td>([\s\S]*?)<\/td>\s*<td[^>]*>([\s\S]*?)<\/td>\s*<\/tr>/g;
  let match;
  while ((match = rowRegex.exec(sectionMatch[0])) !== null) {
    const customer = stripHtml(match[1]);
    const shipments = Number(extractFirstNumber(match[2], "0"));
    if (!customer) continue;
    rows.push({ customer, shipments });
  }
  return rows;
}

function parseIconBoolean(iconHtml) {
  return /fa-check-circle|text-green/i.test(String(iconHtml || ""));
}

function parseTmsIncidencesDatatable(payload) {
  const data = Array.isArray(payload?.data) ? payload.data : [];
  return data.map((row) => ({
    id: Number(row?.id || 0),
    name: stripHtml(row?.name || ""),
    shipment: parseIconBoolean(row?.is_shipment),
    pickup: parseIconBoolean(row?.is_pickup),
    appVisible: parseIconBoolean(row?.operator_visible),
    active: parseIconBoolean(row?.is_active),
    sort: Number(row?.sort || 0)
  }));
}

function extractIncidentReasonFromShipmentRow(row) {
  const lastIncidence = row?.last_incidence && typeof row.last_incidence === "object"
    ? row.last_incidence
    : null;

  if (lastIncidence) {
    const incidenceName = normalizeDatatableTextCell(
      lastIncidence?.incidence?.name ||
      lastIncidence?.incidence_name ||
      ""
    );
    const obs = normalizeDatatableTextCell(lastIncidence?.obs || "");

    if (obs && incidenceName) {
      return `${incidenceName}: ${obs}`.trim();
    }
    if (obs) {
      return obs;
    }
    if (incidenceName) {
      return incidenceName;
    }
  }

  const directCandidates = [
    row?.incident_reason,
    row?.incidence_reason,
    row?.reason,
    row?.reason_name,
    row?.reason_description,
    row?.status_reason,
    row?.incident_notes,
    row?.incidence
  ];

  for (const candidate of directCandidates) {
    const normalized = normalizeDatatableTextCell(candidate);
    if (normalized) {
      return normalized.replace(/^motivo\s*:\s*/i, "").trim();
    }
  }

  const htmlCandidates = [row?.status, row?.delivery_date, row?.last_incidence];
  for (const candidate of htmlCandidates) {
    const raw = String(candidate || "");
    if (!raw) continue;

    const motivoMatch = raw.match(/motivo\s*:\s*([^<\n\r"]+)/i);
    if (motivoMatch?.[1]) {
      return stripHtml(motivoMatch[1]).replace(/^motivo\s*:\s*/i, "").trim();
    }

    const titleMatch = raw.match(/title\s*=\s*"([^"]+)"/i);
    const titleText = titleMatch?.[1] ? stripHtml(titleMatch[1]) : "";
    if (/motivo\s*:/i.test(titleText)) {
      return titleText.replace(/^.*?motivo\s*:\s*/i, "").trim();
    }
  }

  return "";
}

function parseTmsIncidenceShipmentsDatatable(payload) {
  const data = Array.isArray(payload?.data) ? payload.data : [];
  return data.map((row) => {
    const chargeAmountRaw = String(row?.charge_price || row?.cod || "").trim();
    const chargeAmountNumber = Number.parseFloat(chargeAmountRaw.replace(",", "."));
    const hasChargeByAmount = Number.isFinite(chargeAmountNumber) && chargeAmountNumber > 0;
    const hasChargeByHtmlHint = /cobran[çc]a|€/i.test(String(row?.delivery_date || ""));
    const hasCharge = hasChargeByAmount || hasChargeByHtmlHint;

    return {
      parcelId: String(row?.tracking_code || "").trim() || stripHtml(row?.id || ""),
      providerTrackingCode: String(row?.provider_tracking_code || "").trim(),
      service: normalizeDatatableTextCell(
        row?.service ||
        row?.service_name ||
        row?.provider ||
        row?.provider_name ||
        row?.shipment_method ||
        row?.method ||
        row?.courier ||
        row?.operator ||
        row?.shipping_provider ||
        row?.shipping_provider_name ||
        row?.provider_code ||
        row?.delivery_provider ||
        row?.service_id ||
        row?.webservice_method ||
        ""
      ),
      sender: stripHtml(row?.sender_name || ""),
      recipient: stripHtml(row?.recipient_name || ""),
      finalClientPhone: String(row?.recipient_phone || "").trim(),
      pickupDate: normalizeDatatableTextCell(
        row?.pickup_date ||
        row?.collection_date ||
        row?.recolha_date ||
        row?.date_pickup ||
        row?.pickup_at ||
        row?.collected_at ||
        row?.created_at ||
        ""
      ),
      deliveryDate: normalizeDatatableTextCell(row?.delivery_date || row?.delivery_at || ""),
      status: normalizeDatatableTextCell(row?.status || row?.status_id || ""),
      incidence: normalizeDatatableTextCell(row?.last_incidence || ""),
      incidentReason: extractIncidentReasonFromShipmentRow(row),
      hasCharge,
      chargeAmount: hasChargeByAmount ? chargeAmountNumber.toFixed(2) : ""
    };
  });
}

function extractShipmentDateInfo(rawValue) {
  const raw = String(rawValue || "").trim();
  if (!raw) {
    return { key: "", ts: NaN };
  }

  const iso = raw.match(/(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
  if (iso) {
    const year = Number(iso[1]);
    const month = Number(iso[2]);
    const day = Number(iso[3]);
    const hour = Number(iso[4] || 0);
    const minute = Number(iso[5] || 0);
    const second = Number(iso[6] || 0);
    const key = `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    const ts = new Date(year, Math.max(0, month - 1), day, hour, minute, second).getTime();
    return { key, ts };
  }

  const dmy = raw.match(/(\d{1,2})[\/.](\d{1,2})[\/.](\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
  if (dmy) {
    const day = Number(dmy[1]);
    const month = Number(dmy[2]);
    const parsedYear = Number(dmy[3]);
    const year = parsedYear < 100 ? 2000 + parsedYear : parsedYear;
    const hour = Number(dmy[4] || 0);
    const minute = Number(dmy[5] || 0);
    const second = Number(dmy[6] || 0);
    const key = `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    const ts = new Date(year, Math.max(0, month - 1), day, hour, minute, second).getTime();
    return { key, ts };
  }

  return { key: "", ts: NaN };
}

function resolveShipmentPrimaryDateInfo(row) {
  const pickup = extractShipmentDateInfo(row?.pickupDate || "");
  if (pickup.key) {
    return pickup;
  }
  return extractShipmentDateInfo(row?.deliveryDate || "");
}

function isDateKeyWithinRange(dateKey, fromKey, toKey) {
  const key = String(dateKey || "").trim();
  if (!key) return false;
  if (fromKey && key < fromKey) return false;
  if (toKey && key > toKey) return false;
  return true;
}

function clampPercentage(value) {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, value));
}

function pct(numerator, denominator) {
  if (!Number.isFinite(denominator) || denominator <= 0) return 0;
  return clampPercentage((Number(numerator) / Number(denominator)) * 100);
}

function safeSegment(value, fallback = "N/D") {
  const raw = String(value || "").trim();
  if (!raw) return fallback;
  const normalized = raw.replace(/\s+/g, " ").trim();
  const byComma = normalized.split(",")[0] || normalized;
  const byDash = byComma.split("-")[0] || byComma;
  return String(byDash || fallback).trim() || fallback;
}

function buildRouteLabelFromRow(row) {
  const sender = safeSegment(row?.sender || "Origem", "Origem");
  const recipient = safeSegment(row?.recipient || "Destino", "Destino");
  return `${sender} -> ${recipient}`;
}

function buildShipmentIdentityKey(row) {
  const parcelId = String(row?.parcelId || "").trim();
  const tracking = String(row?.providerTrackingCode || "").trim();
  if (parcelId || tracking) return `${parcelId}|${tracking}`;
  return `${String(row?.recipient || "").trim()}|${String(row?.pickupDate || "").trim()}|${String(row?.deliveryDate || "").trim()}`;
}

function buildCttDashboardDataFromRows({ deliveredRows, inDistributionRows, inTransportRows, incidenceRows, fromKey, toKey }) {
  const combinedRows = [
    ...deliveredRows,
    ...inDistributionRows,
    ...inTransportRows,
    ...incidenceRows
  ];

  const filteredRows = combinedRows.filter((row) => {
    const dateInfo = resolveShipmentPrimaryDateInfo(row);
    if (!dateInfo.key) return false;
    return isDateKeyWithinRange(dateInfo.key, fromKey, toKey);
  });

  const deliveredFiltered = deliveredRows.filter((row) => {
    const dateInfo = resolveShipmentPrimaryDateInfo(row);
    return dateInfo.key && isDateKeyWithinRange(dateInfo.key, fromKey, toKey);
  });

  const inDistributionFiltered = inDistributionRows.filter((row) => {
    const dateInfo = resolveShipmentPrimaryDateInfo(row);
    return dateInfo.key && isDateKeyWithinRange(dateInfo.key, fromKey, toKey);
  });

  const inTransportFiltered = inTransportRows.filter((row) => {
    const dateInfo = resolveShipmentPrimaryDateInfo(row);
    return dateInfo.key && isDateKeyWithinRange(dateInfo.key, fromKey, toKey);
  });

  const incidenceFiltered = incidenceRows.filter((row) => {
    const dateInfo = resolveShipmentPrimaryDateInfo(row);
    return dateInfo.key && isDateKeyWithinRange(dateInfo.key, fromKey, toKey);
  });

  const uniqueShipmentKeys = new Set(filteredRows.map((row) => buildShipmentIdentityKey(row)).filter(Boolean));
  const totalEnvios = uniqueShipmentKeys.size;

  const routeAgg = new Map();
  for (const row of filteredRows) {
    const route = buildRouteLabelFromRow(row);
    const key = String(route || "N/D");
    const current = routeAgg.get(key) || { route: key, total: 0, delivered: 0, incidences: 0 };
    current.total += 1;
    if (deliveredFiltered.includes(row)) {
      current.delivered += 1;
    }
    if (incidenceFiltered.includes(row)) {
      current.incidences += 1;
    }
    routeAgg.set(key, current);
  }

  const topRoutes = Array.from(routeAgg.values())
    .sort((a, b) => b.total - a.total)
    .slice(0, 6);

  const weekdayOrder = ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sab"];
  const weekdayCounts = new Map(weekdayOrder.map((day) => [day, 0]));
  for (const row of filteredRows) {
    const dateInfo = resolveShipmentPrimaryDateInfo(row);
    if (!Number.isFinite(dateInfo.ts)) continue;
    const weekday = weekdayOrder[new Date(dateInfo.ts).getDay()] || "Seg";
    weekdayCounts.set(weekday, Number(weekdayCounts.get(weekday) || 0) + 1);
  }
  const trend = weekdayOrder.map((day) => ({ day, value: Number(weekdayCounts.get(day) || 0) }));

  const incidenceReasonAgg = new Map();
  for (const row of incidenceFiltered) {
    const rawReason = String(row?.incidentReason || row?.incidence || row?.status || "").trim();
    const reason = rawReason || "Sem detalhe";
    incidenceReasonAgg.set(reason, Number(incidenceReasonAgg.get(reason) || 0) + 1);
  }
  const incidenceBreakdown = Array.from(incidenceReasonAgg.entries())
    .map(([reason, total]) => ({ reason, total }))
    .sort((a, b) => b.total - a.total)
    .slice(0, 8);

  const heatmapHours = ["08h", "10h", "12h", "14h", "16h", "18h"];
  const heatmapHourBuckets = [8, 10, 12, 14, 16, 18];
  const heatmapRows = weekdayOrder.slice(1).map((day) => ({ day, values: [0, 0, 0, 0, 0, 0] }));
  const heatmapByDay = new Map(heatmapRows.map((row) => [row.day, row]));

  for (const row of filteredRows) {
    const dateInfo = resolveShipmentPrimaryDateInfo(row);
    if (!Number.isFinite(dateInfo.ts)) continue;
    const date = new Date(dateInfo.ts);
    const weekday = weekdayOrder[date.getDay()] || "Seg";
    if (weekday === "Dom") continue;
    const rowBucket = heatmapByDay.get(weekday);
    if (!rowBucket) continue;
    const hour = date.getHours();
    let nearestIndex = 0;
    let nearestDistance = Infinity;
    for (let i = 0; i < heatmapHourBuckets.length; i += 1) {
      const distance = Math.abs(hour - heatmapHourBuckets[i]);
      if (distance < nearestDistance) {
        nearestDistance = distance;
        nearestIndex = i;
      }
    }
    rowBucket.values[nearestIndex] += 1;
  }

  const regionalAgg = new Map();
  for (const row of filteredRows) {
    const region = safeSegment(row?.recipient || row?.sender || "N/D", "N/D");
    const current = regionalAgg.get(region) || { region, volume: 0, delivered: 0, incidences: 0 };
    current.volume += 1;
    if (deliveredFiltered.includes(row)) current.delivered += 1;
    if (incidenceFiltered.includes(row)) current.incidences += 1;
    regionalAgg.set(region, current);
  }

  const regionalPerformance = Array.from(regionalAgg.values())
    .map((row) => ({
      region: row.region,
      volume: row.volume,
      onTime: `${pct(row.delivered - row.incidences, row.volume).toFixed(0)}%`,
      incidences: row.incidences
    }))
    .sort((a, b) => b.volume - a.volume)
    .slice(0, 6);

  const deliveredWithIssue = deliveredFiltered.filter((row) => String(row?.incidentReason || row?.incidence || "").trim()).length;
  const deliveredWithoutIssue = Math.max(0, deliveredFiltered.length - deliveredWithIssue);
  const onTimePct = pct(deliveredWithoutIssue, deliveredFiltered.length);
  const firstAttemptPct = pct(deliveredWithoutIssue, deliveredFiltered.length);
  const reDeliveryPct = pct(deliveredWithIssue, deliveredFiltered.length);

  const delaysMinutes = [];
  for (const row of deliveredFiltered) {
    const pickupInfo = extractShipmentDateInfo(row?.pickupDate || "");
    const deliveryInfo = extractShipmentDateInfo(row?.deliveryDate || "");
    if (!Number.isFinite(pickupInfo.ts) || !Number.isFinite(deliveryInfo.ts)) continue;
    const diffMinutes = Math.round((deliveryInfo.ts - pickupInfo.ts) / (1000 * 60));
    if (Number.isFinite(diffMinutes) && diffMinutes >= 0) {
      delaysMinutes.push(diffMinutes);
    }
  }
  const avgDelayMinutes = delaysMinutes.length > 0
    ? Math.round(delaysMinutes.reduce((acc, value) => acc + value, 0) / delaysMinutes.length)
    : 0;
  const avgDelayHours = Math.floor(avgDelayMinutes / 60);
  const avgDelayMins = String(avgDelayMinutes % 60).padStart(2, "0");

  const kpis = [
    { label: "Total Envios", value: String(totalEnvios), delta: "" },
    { label: "Em Transporte", value: String(inTransportFiltered.length), delta: "" },
    { label: "Entregues", value: String(deliveredFiltered.length), delta: "" },
    { label: "Incidencias", value: String(incidenceFiltered.length), delta: "" }
  ];

  const sla = [
    { label: "On-time delivery", value: `${onTimePct.toFixed(1)}%`, hint: "real" },
    { label: "1a tentativa sucesso", value: `${firstAttemptPct.toFixed(1)}%`, hint: "real" },
    { label: "Atraso medio", value: `${avgDelayHours}h ${avgDelayMins}m`, hint: "real" },
    { label: "Reentrega", value: `${reDeliveryPct.toFixed(1)}%`, hint: "real" }
  ];

  const funnel = [
    { label: "Em transito", count: inTransportFiltered.length },
    { label: "Em distribuicao", count: inDistributionFiltered.length },
    { label: "Incidencias", count: incidenceFiltered.length },
    { label: "Entregue", count: deliveredFiltered.length }
  ];

  return {
    kpis,
    trend,
    topRoutes,
    sla,
    funnel,
    incidenceBreakdown,
    heatmap: {
      hours: heatmapHours,
      rows: heatmapRows
    },
    regionalPerformance,
    meta: {
      totalEnvios,
      inTransport: inTransportFiltered.length,
      inDistribution: inDistributionFiltered.length,
      delivered: deliveredFiltered.length,
      incidencias: incidenceFiltered.length
    }
  };
}

async function fetchTmsDeliveredShipmentsData({ page = 1, limit = 250 } = {}) {
  const enabled = String(process.env.TMS_ENABLED || "false").toLowerCase() === "true";
  if (!enabled) {
    throw new Error("TMS integration disabled. Set TMS_ENABLED=true.");
  }

  const baseUrl = String(process.env.TMS_BASE_URL || "").trim().replace(/\/$/, "");
  const email = String(process.env.TMS_ADMIN_EMAIL || "").trim();
  const password = String(process.env.TMS_ADMIN_PASSWORD || "");

  if (!baseUrl || !email || !password) {
    throw new Error("Missing TMS_BASE_URL, TMS_ADMIN_EMAIL or TMS_ADMIN_PASSWORD.");
  }

  const safePage = Number.isFinite(page) ? Math.max(1, Math.trunc(page)) : 1;
  const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(250, Math.trunc(limit))) : 250;

  const cookieJar = new Map();
  const loginUrl = `${baseUrl}/admin/login`;

  const loginPageRes = await fetch(loginUrl, { redirect: "manual" });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginPageRes));
  const loginHtml = await loginPageRes.text();
  const tokenMatch = loginHtml.match(/name="_token"\s+type="hidden"\s+value="([^"]+)"/i);
  const csrfToken = tokenMatch?.[1] || "";
  if (!csrfToken) {
    throw new Error("Could not extract TMS CSRF token.");
  }

  const loginBody = new URLSearchParams();
  loginBody.set("_token", csrfToken);
  loginBody.set("email", email);
  loginBody.set("password", password);
  loginBody.set("remember", "on");

  const loginSubmitRes = await fetch(loginUrl, {
    method: "POST",
    redirect: "manual",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Referer: loginUrl,
      Cookie: cookieJarHeader(cookieJar)
    },
    body: loginBody.toString()
  });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginSubmitRes));

  const shipmentsEndpoint = `${baseUrl}/admin/shipments/datatable`;
  async function fetchDeliveredDatatablePage(draw, start, length) {
    const requestBody = new URLSearchParams();
    requestBody.set("_token", csrfToken);
    requestBody.set("draw", String(draw));
    requestBody.set("start", String(Math.max(0, Math.trunc(start))));
    requestBody.set("length", String(Math.max(1, Math.trunc(length))));
    requestBody.set("filter", "1");
    requestBody.set("status", "5");

    const shipmentsRes = await fetch(shipmentsEndpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        Referer: `${baseUrl}/admin/shipments?filter=1&status=5`,
        Cookie: cookieJarHeader(cookieJar)
      },
      body: requestBody.toString(),
      redirect: "manual"
    });

    if (!shipmentsRes.ok) {
      throw new Error(`TMS delivered shipments request failed with status ${shipmentsRes.status}`);
    }

    return shipmentsRes.json().catch(() => ({}));
  }

  // Datatable comes oldest -> newest; compute an offset from the end so page 1 is newest first.
  const probePayload = await fetchDeliveredDatatablePage(1, 0, 1);
  const total = Number(probePayload?.recordsFiltered ?? probePayload?.recordsTotal ?? 0) || 0;

  const totalPages = Math.max(1, Math.ceil(total / safeLimit));
  const boundedPage = Math.min(safePage, totalPages);
  const sourceStart = Math.max(0, total - (boundedPage * safeLimit));
  const sourceEndExclusive = Math.max(0, total - ((boundedPage - 1) * safeLimit));
  const sourceLength = Math.max(1, sourceEndExclusive - sourceStart);

  const payload = await fetchDeliveredDatatablePage(boundedPage, sourceStart, sourceLength);
  const rows = parseTmsIncidenceShipmentsDatatable(payload).reverse();

  return {
    rows,
    meta: {
      page: boundedPage,
      limit: safeLimit,
      total,
      totalPages,
      fetchedAt: new Date().toISOString(),
      source: `${baseUrl}/admin/shipments?filter=1&status=5`
    }
  };
}

async function fetchTmsInDistributionShipmentsData({ page = 1, limit = 250 } = {}) {
  const enabled = String(process.env.TMS_ENABLED || "false").toLowerCase() === "true";
  if (!enabled) {
    throw new Error("TMS integration disabled. Set TMS_ENABLED=true.");
  }

  const baseUrl = String(process.env.TMS_BASE_URL || "").trim().replace(/\/$/, "");
  const email = String(process.env.TMS_ADMIN_EMAIL || "").trim();
  const password = String(process.env.TMS_ADMIN_PASSWORD || "");

  if (!baseUrl || !email || !password) {
    throw new Error("Missing TMS_BASE_URL, TMS_ADMIN_EMAIL or TMS_ADMIN_PASSWORD.");
  }

  const safePage = Number.isFinite(page) ? Math.max(1, Math.trunc(page)) : 1;
  const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(250, Math.trunc(limit))) : 250;

  const cookieJar = new Map();
  const loginUrl = `${baseUrl}/admin/login`;

  const loginPageRes = await fetch(loginUrl, { redirect: "manual" });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginPageRes));
  const loginHtml = await loginPageRes.text();
  const tokenMatch = loginHtml.match(/name="_token"\s+type="hidden"\s+value="([^"]+)"/i);
  const csrfToken = tokenMatch?.[1] || "";
  if (!csrfToken) {
    throw new Error("Could not extract TMS CSRF token.");
  }

  const loginBody = new URLSearchParams();
  loginBody.set("_token", csrfToken);
  loginBody.set("email", email);
  loginBody.set("password", password);
  loginBody.set("remember", "on");

  const loginSubmitRes = await fetch(loginUrl, {
    method: "POST",
    redirect: "manual",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Referer: loginUrl,
      Cookie: cookieJarHeader(cookieJar)
    },
    body: loginBody.toString()
  });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginSubmitRes));

  const shipmentsEndpoint = `${baseUrl}/admin/shipments/datatable`;
  async function fetchInDistributionDatatablePage(draw, start, length) {
    const requestBody = new URLSearchParams();
    requestBody.set("_token", csrfToken);
    requestBody.set("draw", String(draw));
    requestBody.set("start", String(Math.max(0, Math.trunc(start))));
    requestBody.set("length", String(Math.max(1, Math.trunc(length))));
    requestBody.set("filter", "1");
    requestBody.set("status", "4");

    const shipmentsRes = await fetch(shipmentsEndpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        Referer: `${baseUrl}/admin/shipments?filter=1&status=4`,
        Cookie: cookieJarHeader(cookieJar)
      },
      body: requestBody.toString(),
      redirect: "manual"
    });

    if (!shipmentsRes.ok) {
      throw new Error(`TMS in-distribution shipments request failed with status ${shipmentsRes.status}`);
    }

    return shipmentsRes.json().catch(() => ({}));
  }

  // Datatable comes oldest -> newest; compute an offset from the end so page 1 is newest first.
  const probePayload = await fetchInDistributionDatatablePage(1, 0, 1);
  const total = Number(probePayload?.recordsFiltered ?? probePayload?.recordsTotal ?? 0) || 0;

  const totalPages = Math.max(1, Math.ceil(total / safeLimit));
  const boundedPage = Math.min(safePage, totalPages);
  const sourceStart = Math.max(0, total - (boundedPage * safeLimit));
  const sourceEndExclusive = Math.max(0, total - ((boundedPage - 1) * safeLimit));
  const sourceLength = Math.max(1, sourceEndExclusive - sourceStart);

  const payload = await fetchInDistributionDatatablePage(boundedPage, sourceStart, sourceLength);
  const rows = parseTmsIncidenceShipmentsDatatable(payload).reverse();

  return {
    rows,
    meta: {
      page: boundedPage,
      limit: safeLimit,
      total,
      totalPages,
      fetchedAt: new Date().toISOString(),
      source: `${baseUrl}/admin/shipments?filter=1&status=4`
    }
  };
}

async function fetchTmsIncidenceShipmentsData({ page = 1, limit = 250 } = {}) {
  const enabled = String(process.env.TMS_ENABLED || "false").toLowerCase() === "true";
  if (!enabled) {
    throw new Error("TMS integration disabled. Set TMS_ENABLED=true.");
  }

  const baseUrl = String(process.env.TMS_BASE_URL || "").trim().replace(/\/$/, "");
  const email = String(process.env.TMS_ADMIN_EMAIL || "").trim();
  const password = String(process.env.TMS_ADMIN_PASSWORD || "");

  if (!baseUrl || !email || !password) {
    throw new Error("Missing TMS_BASE_URL, TMS_ADMIN_EMAIL or TMS_ADMIN_PASSWORD.");
  }

  const safePage = Number.isFinite(page) ? Math.max(1, Math.trunc(page)) : 1;
  const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(250, Math.trunc(limit))) : 250;

  const cookieJar = new Map();
  const loginUrl = `${baseUrl}/admin/login`;

  const loginPageRes = await fetch(loginUrl, { redirect: "manual" });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginPageRes));
  const loginHtml = await loginPageRes.text();
  const tokenMatch = loginHtml.match(/name="_token"\s+type="hidden"\s+value="([^"]+)"/i);
  const csrfToken = tokenMatch?.[1] || "";
  if (!csrfToken) {
    throw new Error("Could not extract TMS CSRF token.");
  }

  const loginBody = new URLSearchParams();
  loginBody.set("_token", csrfToken);
  loginBody.set("email", email);
  loginBody.set("password", password);
  loginBody.set("remember", "on");

  const loginSubmitRes = await fetch(loginUrl, {
    method: "POST",
    redirect: "manual",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Referer: loginUrl,
      Cookie: cookieJarHeader(cookieJar)
    },
    body: loginBody.toString()
  });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginSubmitRes));

  const shipmentsEndpoint = `${baseUrl}/admin/shipments/datatable`;
  async function fetchIncidenceDatatablePage(draw, start, length) {
    const requestBody = new URLSearchParams();
    requestBody.set("_token", csrfToken);
    requestBody.set("draw", String(draw));
    requestBody.set("start", String(Math.max(0, Math.trunc(start))));
    requestBody.set("length", String(Math.max(1, Math.trunc(length))));
    requestBody.set("filter", "1");
    requestBody.set("status", "9");

    const shipmentsRes = await fetch(shipmentsEndpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        Referer: `${baseUrl}/admin/shipments?filter=1&status=9`,
        Cookie: cookieJarHeader(cookieJar)
      },
      body: requestBody.toString(),
      redirect: "manual"
    });

    if (!shipmentsRes.ok) {
      throw new Error(`TMS incidence shipments request failed with status ${shipmentsRes.status}`);
    }

    return shipmentsRes.json().catch(() => ({}));
  }

  const probePayload = await fetchIncidenceDatatablePage(1, 0, 1);
  const total = Number(probePayload?.recordsFiltered ?? probePayload?.recordsTotal ?? 0) || 0;

  const totalPages = Math.max(1, Math.ceil(total / safeLimit));
  const boundedPage = Math.min(safePage, totalPages);
  const sourceStart = Math.max(0, total - (boundedPage * safeLimit));
  const sourceEndExclusive = Math.max(0, total - ((boundedPage - 1) * safeLimit));
  const sourceLength = Math.max(1, sourceEndExclusive - sourceStart);

  const payload = await fetchIncidenceDatatablePage(boundedPage, sourceStart, sourceLength);
  const rows = parseTmsIncidenceShipmentsDatatable(payload).reverse();

  return {
    rows,
    meta: {
      page: boundedPage,
      limit: safeLimit,
      total,
      totalPages,
      fetchedAt: new Date().toISOString(),
      source: `${baseUrl}/admin/shipments?filter=1&status=9`
    }
  };
}

async function fetchTmsInTransportShipmentsData({ page = 1, limit = 250 } = {}) {
  const enabled = String(process.env.TMS_ENABLED || "false").toLowerCase() === "true";
  if (!enabled) {
    throw new Error("TMS integration disabled. Set TMS_ENABLED=true.");
  }

  const baseUrl = String(process.env.TMS_BASE_URL || "").trim().replace(/\/$/, "");
  const email = String(process.env.TMS_ADMIN_EMAIL || "").trim();
  const password = String(process.env.TMS_ADMIN_PASSWORD || "");

  if (!baseUrl || !email || !password) {
    throw new Error("Missing TMS_BASE_URL, TMS_ADMIN_EMAIL or TMS_ADMIN_PASSWORD.");
  }

  const safePage = Number.isFinite(page) ? Math.max(1, Math.trunc(page)) : 1;
  const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(250, Math.trunc(limit))) : 250;

  const cookieJar = new Map();
  const loginUrl = `${baseUrl}/admin/login`;

  const loginPageRes = await fetch(loginUrl, { redirect: "manual" });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginPageRes));
  const loginHtml = await loginPageRes.text();
  const tokenMatch = loginHtml.match(/name="_token"\s+type="hidden"\s+value="([^"]+)"/i);
  const csrfToken = tokenMatch?.[1] || "";
  if (!csrfToken) {
    throw new Error("Could not extract TMS CSRF token.");
  }

  const loginBody = new URLSearchParams();
  loginBody.set("_token", csrfToken);
  loginBody.set("email", email);
  loginBody.set("password", password);
  loginBody.set("remember", "on");

  const loginSubmitRes = await fetch(loginUrl, {
    method: "POST",
    redirect: "manual",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Referer: loginUrl,
      Cookie: cookieJarHeader(cookieJar)
    },
    body: loginBody.toString()
  });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginSubmitRes));

  const transportStatus = "3";

  const shipmentsEndpoint = `${baseUrl}/admin/shipments/datatable`;
  async function fetchInTransportDatatablePage(draw, start, length) {
    const requestBody = new URLSearchParams();
    requestBody.set("_token", csrfToken);
    requestBody.set("draw", String(draw));
    requestBody.set("start", String(Math.max(0, Math.trunc(start))));
    requestBody.set("length", String(Math.max(1, Math.trunc(length))));
    requestBody.set("filter", "1");
    requestBody.set("status", transportStatus);

    const shipmentsRes = await fetch(shipmentsEndpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        Referer: `${baseUrl}/admin/shipments?filter=1&status=${encodeURIComponent(transportStatus)}`,
        Cookie: cookieJarHeader(cookieJar)
      },
      body: requestBody.toString(),
      redirect: "manual"
    });

    if (!shipmentsRes.ok) {
      throw new Error(`TMS in-transport shipments request failed with status ${shipmentsRes.status}`);
    }

    return shipmentsRes.json().catch(() => ({}));
  }

  // Datatable comes oldest -> newest; compute an offset from the end so page 1 is newest first.
  const probePayload = await fetchInTransportDatatablePage(1, 0, 1);
  const total = Number(probePayload?.recordsFiltered ?? probePayload?.recordsTotal ?? 0) || 0;

  const totalPages = Math.max(1, Math.ceil(total / safeLimit));
  const boundedPage = Math.min(safePage, totalPages);
  const sourceStart = Math.max(0, total - (boundedPage * safeLimit));
  const sourceEndExclusive = Math.max(0, total - ((boundedPage - 1) * safeLimit));
  const sourceLength = Math.max(1, sourceEndExclusive - sourceStart);

  const payload = await fetchInTransportDatatablePage(boundedPage, sourceStart, sourceLength);
  const rows = parseTmsIncidenceShipmentsDatatable(payload).reverse();

  return {
    rows,
    meta: {
      page: boundedPage,
      limit: safeLimit,
      total,
      totalPages,
      fetchedAt: new Date().toISOString(),
      source: `${baseUrl}/admin/shipments?filter=1&status=${encodeURIComponent(transportStatus)}`
    }
  };
}

function parseTmsStatusFromLink(html, linkClassName, fallbackStatus = "") {
  const escapedClass = String(linkClassName || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const regex = new RegExp(`${escapedClass}[\\s\\S]*?href=\\"([^\\"]+)\\"`, "i");
  const match = String(html || "").match(regex);
  const href = match?.[1] || "";
  const statusMatch = href.match(/[?&]status=([^&]+)/i);
  return statusMatch?.[1] || fallbackStatus;
}

async function fetchTmsDashboardData() {
  const enabled = String(process.env.TMS_ENABLED || "false").toLowerCase() === "true";
  if (!enabled) {
    throw new Error("TMS integration disabled. Set TMS_ENABLED=true.");
  }

  const baseUrl = String(process.env.TMS_BASE_URL || "").trim().replace(/\/$/, "");
  const email = String(process.env.TMS_ADMIN_EMAIL || "").trim();
  const password = String(process.env.TMS_ADMIN_PASSWORD || "");

  if (!baseUrl || !email || !password) {
    throw new Error("Missing TMS_BASE_URL, TMS_ADMIN_EMAIL or TMS_ADMIN_PASSWORD.");
  }

  const cookieJar = new Map();
  const loginUrl = `${baseUrl}/admin/login`;

  const loginPageRes = await fetch(loginUrl, { redirect: "manual" });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginPageRes));
  const loginHtml = await loginPageRes.text();
  const tokenMatch = loginHtml.match(/name="_token"\s+type="hidden"\s+value="([^"]+)"/i);
  const csrfToken = tokenMatch?.[1] || "";
  if (!csrfToken) {
    throw new Error("Could not extract TMS CSRF token.");
  }

  const loginBody = new URLSearchParams();
  loginBody.set("_token", csrfToken);
  loginBody.set("email", email);
  loginBody.set("password", password);
  loginBody.set("remember", "on");

  const loginSubmitRes = await fetch(loginUrl, {
    method: "POST",
    redirect: "manual",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Referer: loginUrl,
      Cookie: cookieJarHeader(cookieJar)
    },
    body: loginBody.toString()
  });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginSubmitRes));

  const dashboardUrl = `${baseUrl}/admin`;
  const dashboardRes = await fetch(dashboardUrl, {
    headers: {
      Cookie: cookieJarHeader(cookieJar)
    },
    redirect: "manual"
  });
  updateCookieJar(cookieJar, getSetCookieHeaders(dashboardRes));

  if (!dashboardRes.ok) {
    throw new Error(`TMS dashboard request failed with status ${dashboardRes.status}`);
  }

  const dashboardHtml = await dashboardRes.text();

  let incidences = [];
  try {
    const incidencesEndpoint = `${baseUrl}/admin/tracking/incidences/datatable`;
    const incidencesBody = new URLSearchParams();
    incidencesBody.set("_token", csrfToken);
    incidencesBody.set("draw", "1");
    incidencesBody.set("start", "0");
    incidencesBody.set("length", "100");

    const incidencesRes = await fetch(incidencesEndpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        Referer: `${baseUrl}/admin/tracking/incidences`,
        Cookie: cookieJarHeader(cookieJar)
      },
      body: incidencesBody.toString(),
      redirect: "manual"
    });

    if (incidencesRes.ok) {
      const incidencesJson = await incidencesRes.json().catch(() => ({}));
      incidences = parseTmsIncidencesDatatable(incidencesJson);
    }
  } catch {
    incidences = [];
  }

  let incidenceShipments = [];
  try {
    const incidenceShipmentsEndpoint = `${baseUrl}/admin/shipments/datatable`;
    const incidenceShipmentsBody = new URLSearchParams();
    incidenceShipmentsBody.set("_token", csrfToken);
    incidenceShipmentsBody.set("draw", "1");
    incidenceShipmentsBody.set("start", "0");
    incidenceShipmentsBody.set("length", "100");
    incidenceShipmentsBody.set("status", "5");

    const incidenceShipmentsRes = await fetch(incidenceShipmentsEndpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        Referer: `${baseUrl}/admin/shipments?status=5`,
        Cookie: cookieJarHeader(cookieJar)
      },
      body: incidenceShipmentsBody.toString(),
      redirect: "manual"
    });

    if (incidenceShipmentsRes.ok) {
      const incidenceShipmentsJson = await incidenceShipmentsRes.json().catch(() => ({}));
      incidenceShipments = parseTmsIncidenceShipmentsDatatable(incidenceShipmentsJson);
    }
  } catch {
    incidenceShipments = [];
  }

  let pudoShipments = [];
  try {
    const pickupStatus = parseTmsStatusFromLink(dashboardHtml, "stats-link-pickup", "4");
    const pudoShipmentsEndpoint = `${baseUrl}/admin/shipments/datatable`;
    const pudoShipmentsBody = new URLSearchParams();
    pudoShipmentsBody.set("_token", csrfToken);
    pudoShipmentsBody.set("draw", "1");
    pudoShipmentsBody.set("start", "0");
    pudoShipmentsBody.set("length", "100");
    pudoShipmentsBody.set("status", pickupStatus);

    const pudoShipmentsRes = await fetch(pudoShipmentsEndpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        Referer: `${baseUrl}/admin/shipments?status=${encodeURIComponent(pickupStatus)}`,
        Cookie: cookieJarHeader(cookieJar)
      },
      body: pudoShipmentsBody.toString(),
      redirect: "manual"
    });

    if (pudoShipmentsRes.ok) {
      const pudoShipmentsJson = await pudoShipmentsRes.json().catch(() => ({}));
      pudoShipments = parseTmsIncidenceShipmentsDatatable(pudoShipmentsJson);
    }
  } catch {
    pudoShipments = [];
  }

  return {
    meta: {
      fetchedAt: new Date().toISOString(),
      source: `${baseUrl}/admin`
    },
    infoBoxes: parseTmsInfoBoxes(dashboardHtml),
    serviceStatus: parseTmsServiceStatus(dashboardHtml),
    pendingAcceptance: parseTmsPendingAcceptance(dashboardHtml),
    incidences,
    incidenceShipments,
    pudoShipments
  };
}

function isAutoSmsFallbackEnabled() {
  const rawValue = String(process.env.AUTO_SMS_FALLBACK_ENABLED || "true").trim().toLowerCase();
  return !["0", "false", "no", "off"].includes(rawValue);
}

function isSmsSendingEnabled() {
  const rawValue = String(process.env.SMS_SENDING_ENABLED || "true").trim().toLowerCase();
  return !["0", "false", "no", "off"].includes(rawValue);
}

function isIncidenciaSmsFallbackEnabled() {
  const rawValue = String(process.env.AUTO_SMS_FALLBACK_INCIDENCIA_ENABLED || "true").trim().toLowerCase();
  return !["0", "false", "no", "off"].includes(rawValue);
}

function isIncidenciaMessageType(value) {
  const normalized = String(value || "")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .toLowerCase();
  return normalized.includes("incid");
}

const SMS_FALLBACK_TEMPLATE_BODIES = {
  notificaoc_de_incidencia:
    "Ola {{1}}, houve um atraso/problema na entrega da sua encomenda no {{2}}. Encomenda da loja {{3}}. Estamos a trabalhar para resolver o problema o mais rapidamente possivel. Lamentamos qualquer inconveniente. Enviaremos o estado atualizado da entrega assim que possivel.",
  notificacao_de_incidencia:
    "Ola {{1}}, houve um atraso/problema na entrega da sua encomenda no {{2}}. Encomenda da loja {{3}}. Estamos a trabalhar para resolver o problema o mais rapidamente possivel. Lamentamos qualquer inconveniente. Enviaremos o estado atualizado da entrega assim que possivel.",
  notificacao_auto_incidencia:
    "Ola {{1}}, houve um atraso/problema na entrega da sua encomenda no {{2}}. Encomenda da loja {{3}}. Estamos a trabalhar para resolver o problema o mais rapidamente possivel. Lamentamos qualquer inconveniente. Enviaremos o estado atualizado da entrega assim que possivel."
};

function renderTemplateBodyWithVariables(templateBody, bodyVariables = []) {
  const safeBody = String(templateBody || "").trim();
  if (!safeBody) return "";

  return safeBody
    .replace(/{{\s*(\d+)\s*}}/g, (_match, indexText) => {
      const idx = Number(indexText);
      if (!Number.isFinite(idx) || idx < 1) return "";
      return String(bodyVariables[idx - 1] ?? "").trim();
    })
    .replace(/\s+/g, " ")
    .trim();
}

function buildTemplateFallbackText({ templateName, bodyVariables = [], buttonUrlVariable = "" }) {
  const cleanTemplateName = String(templateName || "").trim();
  const cleanVars = Array.isArray(bodyVariables)
    ? bodyVariables.map((value) => String(value ?? "").trim()).filter(Boolean)
    : [];
  const cleanButtonVar = String(buttonUrlVariable || "").trim();

  const templateKey = cleanTemplateName.toLowerCase();
  const knownTemplateBody = SMS_FALLBACK_TEMPLATE_BODIES[templateKey] || "";
  const renderedKnownBody = renderTemplateBodyWithVariables(knownTemplateBody, cleanVars);

  if (renderedKnownBody) {
    return cleanButtonVar ? `${renderedKnownBody} Link: ${cleanButtonVar}`.trim() : renderedKnownBody;
  }

  const parts = [`Template ${cleanTemplateName}`];

  if (cleanVars.length > 0) {
    parts.push(cleanVars.join(" | "));
  }
  if (cleanButtonVar) {
    parts.push(`Link: ${cleanButtonVar}`);
  }

  return parts.join("\n").trim();
}

async function sendClickSendSms({
  to,
  message,
  source = "manual",
  templateName = "",
  waStatus = "",
  waResponse = null
}) {
  if (!isSmsSendingEnabled()) {
    return {
      attempted: false,
      status: "skipped_globally_disabled",
      reason: "SMS_SENDING_ENABLED=false"
    };
  }

  const normalizedTo = normalizeRecipient(to);
  const cleanMessage = String(message || "").trim();

  if (!normalizedTo || !cleanMessage) {
    return {
      attempted: false,
      status: "skipped_invalid_input",
      reason: "missing_to_or_message"
    };
  }

  const clickSendUsername = String(process.env.CLICKSEND_USERNAME || "").trim();
  const clickSendApiKey = String(process.env.CLICKSEND_API_KEY || "").trim();
  const clickSendFrom = String(process.env.CLICKSEND_FROM || "Linke").trim();

  if (!clickSendUsername || !clickSendApiKey) {
    return {
      attempted: false,
      status: "skipped_not_configured",
      reason: "missing_clicksend_credentials"
    };
  }

  try {
    const endpoint = "https://rest.clicksend.com/v3/sms/send";
    const basicToken = Buffer.from(`${clickSendUsername}:${clickSendApiKey}`).toString("base64");
    const payload = {
      messages: [
        {
          source: "javascript",
          from: clickSendFrom,
          body: cleanMessage,
          to: normalizedTo
        }
      ]
    };

    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        Authorization: `Basic ${basicToken}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    });

    const responseBody = await response.json().catch(() => ({}));

    const supabaseWarning = await safeSupabaseLog(() =>
      createSupabaseLogRow({
        direction: "out",
        channel: "sms",
        to: normalizedTo,
        messageText: cleanMessage,
        templateName,
        status: response.ok ? "sent" : `failed_${response.status}`,
        apiMessageId: String(responseBody?.data?.messages?.[0]?.message_id || ""),
        payload: {
          source,
          waStatus,
          waResponse,
          clicksendResponse: responseBody
        }
      })
    );

    return {
      attempted: true,
      status: response.ok ? "sent" : `failed_${response.status}`,
      source,
      to: normalizedTo,
      responseStatus: response.status,
      responseBody,
      _supabaseWarning: supabaseWarning || undefined
    };
  } catch (error) {
    return {
      attempted: true,
      status: "failed_exception",
      source,
      to: normalizedTo,
      error: error instanceof Error ? error.message : "Unknown error"
    };
  }
}

async function maybeSendAutomaticSmsFallback({
  to,
  message,
  source,
  templateName = "",
  waStatus,
  waResponse
}) {
  if (!isAutoSmsFallbackEnabled()) {
    return {
      attempted: false,
      status: "skipped_auto_disabled",
      reason: "AUTO_SMS_FALLBACK_ENABLED=false"
    };
  }

  return sendClickSendSms({
    to,
    message,
    source,
    templateName,
    waStatus,
    waResponse
  });
}

function notionPropName(name, fallback) {
  return process.env[name] || fallback;
}

function richText(text) {
  return [{ type: "text", text: { content: String(text ?? "") } }];
}

function titleText(text) {
  return [{ type: "text", text: { content: String(text ?? "") } }];
}

function normalizeNotionFieldName(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .toLowerCase();
}

function pickNotionPropertyName(properties, aliases, allowedTypes = []) {
  const entries = Object.entries(properties || {});
  if (!entries.length) return "";

  const normalizedAliases = aliases.map((alias) => normalizeNotionFieldName(alias));

  for (const [name, prop] of entries) {
    const propType = String(prop?.type || "");
    if (allowedTypes.length > 0 && !allowedTypes.includes(propType)) continue;
    const normalizedName = normalizeNotionFieldName(name);
    if (normalizedAliases.some((alias) => normalizedName.includes(alias))) {
      return name;
    }
  }

  return "";
}

function notionPropertyToText(property) {
  if (!property || typeof property !== "object") {
    return "";
  }

  const type = String(property.type || "");

  if (type === "title") {
    return (property.title || []).map((item) => item?.plain_text || "").join("").trim();
  }

  if (type === "rich_text") {
    return (property.rich_text || []).map((item) => item?.plain_text || "").join("").trim();
  }

  if (type === "number") {
    return property.number === null || property.number === undefined ? "" : String(property.number);
  }

  if (type === "select") {
    return String(property.select?.name || "").trim();
  }

  if (type === "status") {
    return String(property.status?.name || "").trim();
  }

  if (type === "date") {
    return String(property.date?.start || "").trim();
  }

  if (type === "checkbox") {
    return property.checkbox ? "Yes" : "No";
  }

  if (type === "phone_number") {
    return String(property.phone_number || "").trim();
  }

  if (type === "email") {
    return String(property.email || "").trim();
  }

  if (type === "url") {
    return String(property.url || "").trim();
  }

  if (type === "multi_select") {
    return (property.multi_select || []).map((item) => item?.name || "").filter(Boolean).join(", ");
  }

  if (type === "formula") {
    const formula = property.formula || {};
    if (formula.type === "string") return String(formula.string || "").trim();
    if (formula.type === "number") return formula.number === null || formula.number === undefined ? "" : String(formula.number);
    if (formula.type === "boolean") return formula.boolean ? "Yes" : "No";
    if (formula.type === "date") return String(formula.date?.start || "").trim();
  }

  return "";
}

function firstNotionTitleText(properties) {
  const entries = Object.entries(properties || {});
  for (const [, prop] of entries) {
    if (String(prop?.type || "") === "title") {
      const value = notionPropertyToText(prop);
      if (value) return value;
    }
  }
  return "";
}

function normalizeConsumiveisRow(page, columns = []) {
  const properties = page?.properties && typeof page.properties === "object" ? page.properties : {};

  const nameProp = pickNotionPropertyName(
    properties,
    ["name", "nome", "produto", "item", "consumivel", "consumivel", "descricao", "descricao"],
    ["title", "rich_text"]
  );
  const clientProp = pickNotionPropertyName(
    properties,
    ["cliente", "client", "destinatario", "recipient", "company", "empresa"],
    ["title", "rich_text", "select", "status", "phone_number", "email"]
  );
  const quantityProp = pickNotionPropertyName(
    properties,
    ["quantidade", "qtd", "qty", "amount", "units", "unidades"],
    ["number", "rich_text", "formula", "rollup", "title"]
  );
  const statusProp = pickNotionPropertyName(
    properties,
    ["status", "estado", "situacao", "situacao"],
    ["status", "select", "checkbox", "formula", "rich_text", "title"]
  );
  const dateProp = pickNotionPropertyName(
    properties,
    ["data", "date", "envio", "sent", "entrega", "created"],
    ["date", "created_time", "last_edited_time", "formula", "rich_text", "title"]
  );
  const notesProp = pickNotionPropertyName(
    properties,
    ["nota", "notes", "observ", "comment", "coment"],
    ["rich_text", "title", "formula"]
  );

  const name = notionPropertyToText(properties[nameProp]) || firstNotionTitleText(properties) || "-";
  const client = notionPropertyToText(properties[clientProp]) || "-";
  const quantity = notionPropertyToText(properties[quantityProp]) || "-";
  const status = notionPropertyToText(properties[statusProp]) || "-";
  const dateRaw = notionPropertyToText(properties[dateProp]) || page?.created_time || "";
  const date = dateRaw ? new Date(dateRaw).toLocaleString("pt-PT") : "-";
  const notes = notionPropertyToText(properties[notesProp]) || "-";

  const fields = {};
  const propertyEntries = Object.entries(properties || {});
  for (const [propertyName, propertyValue] of propertyEntries) {
    const textValue = notionPropertyToText(propertyValue);
    fields[propertyName] = textValue || "-";
  }

  for (const column of columns) {
    if (!Object.prototype.hasOwnProperty.call(fields, column)) {
      fields[column] = "-";
    }
  }

  return {
    id: String(page?.id || ""),
    name,
    client,
    quantity,
    status,
    date,
    notes,
    fields,
    url: String(page?.url || ""),
    lastEditedAt: String(page?.last_edited_time || "")
  };
}

async function queryConsumiveisRows(databaseId, limit) {
  const dbInfo = await notionConsumiveis.databases.retrieve({ database_id: databaseId });
  const columns = Object.keys(dbInfo?.properties || {});

  let hasMore = true;
  let nextCursor = undefined;
  const rows = [];

  while (hasMore && rows.length < limit) {
    const pageSize = Math.min(100, limit - rows.length);
    const query = {
      database_id: databaseId,
      page_size: pageSize,
      sorts: [{ timestamp: "last_edited_time", direction: "descending" }]
    };

    if (nextCursor) {
      query.start_cursor = nextCursor;
    }

    const response = await notionConsumiveis.databases.query(query);
    const results = Array.isArray(response?.results) ? response.results : [];
    rows.push(...results.map((page) => normalizeConsumiveisRow(page, columns)));

    hasMore = Boolean(response?.has_more);
    nextCursor = response?.next_cursor || undefined;
  }

  return { rows, hasMore, columns };
}

async function resolveConsumiveisDatabaseIdFromPage() {
  if (!notionConsumiveis || !notionConsumiveisPageId) {
    return "";
  }

  try {
    const children = await notionConsumiveis.blocks.children.list({
      block_id: notionConsumiveisPageId,
      page_size: 100
    });

    const blocks = Array.isArray(children?.results) ? children.results : [];
    const childDatabase = blocks.find((block) => String(block?.type || "") === "child_database");
    return childDatabase?.id ? String(childDatabase.id) : "";
  } catch {
    return "";
  }
}

async function queryFeedbackRows(databaseId, limit) {
  const dbInfo = await notionFeedback.databases.retrieve({ database_id: databaseId });
  const columns = Object.keys(dbInfo?.properties || {});

  let hasMore = true;
  let nextCursor = undefined;
  const rows = [];

  while (hasMore && rows.length < limit) {
    const pageSize = Math.min(100, limit - rows.length);
    const query = {
      database_id: databaseId,
      page_size: pageSize,
      sorts: [{ timestamp: "last_edited_time", direction: "descending" }]
    };

    if (nextCursor) {
      query.start_cursor = nextCursor;
    }

    const response = await notionFeedback.databases.query(query);
    const results = Array.isArray(response?.results) ? response.results : [];
    rows.push(...results.map((page) => normalizeConsumiveisRow(page, columns)));

    hasMore = Boolean(response?.has_more);
    nextCursor = response?.next_cursor || undefined;
  }

  return { rows, hasMore, columns };
}

async function resolveFeedbackDatabaseIdFromPage() {
  if (!notionFeedback || !notionFeedbackPageId) {
    return "";
  }

  try {
    const children = await notionFeedback.blocks.children.list({
      block_id: notionFeedbackPageId,
      page_size: 100
    });

    const blocks = Array.isArray(children?.results) ? children.results : [];
    const childDatabase = blocks.find((block) => String(block?.type || "") === "child_database");
    return childDatabase?.id ? String(childDatabase.id) : "";
  } catch {
    return "";
  }
}

function notionErrorDetails(error) {
  if (!(error && typeof error === "object")) {
    return "Unknown error";
  }

  const message = error instanceof Error ? error.message : "Unknown error";
  const code = typeof error.code === "string" ? error.code : "";
  const status = Number(error.status || 0) || 0;
  const requestId =
    typeof error.request_id === "string"
      ? error.request_id
      : typeof error.requestId === "string"
        ? error.requestId
        : "";

  const parts = [message];
  if (code) parts.push(`code=${code}`);
  if (status > 0) parts.push(`status=${status}`);
  if (requestId) parts.push(`request_id=${requestId}`);

  return parts.join(" | ");
}

function parseBooleanLike(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return ["1", "true", "yes", "sim", "on"].includes(normalized);
}

function normalizeConsumiveisCreateInput(rawInput) {
  const input = rawInput && typeof rawInput === "object" ? rawInput : {};
  return {
    clientName: String(input.clientName || "").trim(),
    dateSent: String(input.dateSent || "").trim(),
    tabela: String(input.tabela || "").trim(),
    tipoCliente: String(input.tipoCliente || "").trim(),
    texto: String(input.texto || "").trim(),
    texto1: String(input.texto1 || "").trim(),
    text: String(input.text || "").trim(),
    texto2: String(input.texto2 || "").trim()
  };
}

function normalizeFeedbackCreateInput(rawInput) {
  const input = rawInput && typeof rawInput === "object" ? rawInput : {};
  return {
    shopName: String(input.shopName || input.clientName || "").trim(),
    feedbackUrl: String(input.feedbackUrl || input.link || "").trim(),
    referencia: String(input.referencia || input.reference || "").trim(),
    whatsappTemplate: String(input.whatsappTemplate || input.template || "").trim(),
    codServico: String(input.codServico || input.codServ || input.serviceCode || "").trim(),
    destinatario: String(input.destinatario || input.recipient || "").trim(),
    contactoDestinatario: String(input.contactoDestinatario || input.contacto || input.phone || "").trim(),
    trkSecundario: String(input.trkSecundario || input.trk || input.tracking || "").trim(),
    dataEntrega: String(input.dataEntrega || input.deliveryDate || "").trim(),
    sentDate: String(input.sentDate || input.dateSent || "").trim(),
    status: String(input.status || input.estado || "").trim(),
    whatsappFollowUpSms: String(
      input.whatsappFollowUpSms || input.whatsapp_follow_up_sms || input.followUpSms || ""
    ).trim()
  };
}

function buildFeedbackCreateProperties(databaseProperties, input) {
  const properties = {};

  const shopNameProp = pickNotionPropertyName(
    databaseProperties,
    ["nome cliente", "client name", "cliente", "loja", "store", "destinatario", "recipient"],
    ["title", "rich_text", "select", "status"]
  );
  const feedbackUrlProp = pickNotionPropertyName(
    databaseProperties,
    ["feedback url", "feedback link", "survey url", "survey link", "url", "link"],
    ["url", "rich_text", "title"]
  );
  const referenciaProp = pickNotionPropertyName(
    databaseProperties,
    ["referencia", "reference", "ref"],
    ["rich_text", "title", "select", "status", "number"]
  );
  const whatsappTemplateProp = pickNotionPropertyName(
    databaseProperties,
    ["whatsapp template", "template whatsapp", "template"],
    ["rich_text", "title", "select", "status"]
  );
  const codServicoProp = pickNotionPropertyName(
    databaseProperties,
    ["cod. serviço", "cod. servico", "cod serviço", "cod servico", "service code"],
    ["rich_text", "title", "select", "status", "number"]
  );
  const destinatarioProp = pickNotionPropertyName(
    databaseProperties,
    ["destinatário", "destinatario", "recipient"],
    ["rich_text", "title", "select", "status"]
  );
  const contactoDestinatarioProp = pickNotionPropertyName(
    databaseProperties,
    ["contacto destinatário", "contacto destinatario", "contacto", "phone", "client phone"],
    ["phone_number", "rich_text", "title", "select", "status"]
  );
  const trkSecundarioProp = pickNotionPropertyName(
    databaseProperties,
    ["trk secundário", "trk secundario", "trk", "tracking", "tracking number"],
    ["rich_text", "title", "select", "status"]
  );
  const dataEntregaProp = pickNotionPropertyName(
    databaseProperties,
    ["data entrega", "delivery date", "entrega"],
    ["date", "rich_text", "title"]
  );
  const sentDateProp = pickNotionPropertyName(
    databaseProperties,
    ["sent date", "date sent", "data envio", "data de envio"],
    ["date", "rich_text", "title"]
  );
  const statusProp = pickNotionPropertyName(
    databaseProperties,
    ["status", "estado", "situacao", "situação"],
    ["status", "select", "rich_text", "title"]
  );
  const whatsappFollowUpSmsProp = pickNotionPropertyName(
    databaseProperties,
    [
      "whatsapp follow-up sms",
      "whatsapp follow up sms",
      "follow-up sms",
      "follow up sms"
    ],
    ["status", "select", "rich_text", "title", "checkbox"]
  );

  const assignValue = (propName, value) => {
    if (!propName || !databaseProperties[propName]) {
      return;
    }

    const propType = String(databaseProperties[propName]?.type || "");
    if (value === undefined || value === null || String(value).trim() === "") {
      return;
    }

    const cleanValue = String(value).trim();

    if (propType === "title") {
      properties[propName] = { title: titleText(cleanValue) };
      return;
    }
    if (propType === "rich_text") {
      properties[propName] = { rich_text: richText(cleanValue) };
      return;
    }
    if (propType === "url") {
      properties[propName] = { url: cleanValue };
      return;
    }
    if (propType === "phone_number") {
      properties[propName] = { phone_number: cleanValue };
      return;
    }
    if (propType === "date") {
      properties[propName] = { date: { start: cleanValue } };
      return;
    }
    if (propType === "number") {
      const parsed = Number(cleanValue);
      if (Number.isFinite(parsed)) properties[propName] = { number: parsed };
      return;
    }
    if (propType === "select") {
      properties[propName] = { select: { name: cleanValue } };
      return;
    }
    if (propType === "status") {
      properties[propName] = { status: { name: cleanValue } };
      return;
    }
    if (propType === "checkbox") {
      properties[propName] = { checkbox: parseBooleanLike(cleanValue) };
    }
  };

  assignValue(shopNameProp, input.shopName);
  assignValue(feedbackUrlProp, input.feedbackUrl);
  assignValue(referenciaProp, input.referencia);
  assignValue(whatsappTemplateProp, input.whatsappTemplate);
  assignValue(codServicoProp, input.codServico);
  assignValue(destinatarioProp, input.destinatario);
  assignValue(contactoDestinatarioProp, input.contactoDestinatario);
  assignValue(trkSecundarioProp, input.trkSecundario);
  assignValue(dataEntregaProp, input.dataEntrega);
  assignValue(sentDateProp, input.sentDate);
  assignValue(statusProp, input.status);
  assignValue(whatsappFollowUpSmsProp, input.whatsappFollowUpSms);

  // Fallback for databases with different property names.
  if (!Object.keys(properties).length) {
    const fallbackTitleProp = Object.entries(databaseProperties).find(([, prop]) => String(prop?.type || "") === "title")?.[0] || "";
    const fallbackUrlProp = Object.entries(databaseProperties).find(([, prop]) => String(prop?.type || "") === "url")?.[0] || "";
    const fallbackRichTextProp = Object.entries(databaseProperties).find(([, prop]) => String(prop?.type || "") === "rich_text")?.[0] || "";

    if (fallbackTitleProp) {
      assignValue(fallbackTitleProp, input.shopName || input.referencia || "Feedback Survey");
    }
    if (fallbackUrlProp) {
      assignValue(fallbackUrlProp, input.feedbackUrl);
    } else if (fallbackRichTextProp) {
      assignValue(fallbackRichTextProp, input.feedbackUrl);
    }
  }

  return properties;
}

function buildConsumiveisCreateProperties(databaseProperties, input) {
  const properties = {};

  const clientNameProp = pickNotionPropertyName(databaseProperties, ["client name", "cliente", "nome", "name"], ["title", "rich_text"]);
  const tabelaProp = pickNotionPropertyName(databaseProperties, ["tabela", "table"], ["rich_text", "title", "select", "status"]);
  const tipoClienteProp = pickNotionPropertyName(databaseProperties, ["tipo de cliente", "tipo cliente", "tipo", "cliente tipo"], ["rich_text", "title", "select", "status"]);
  const textoProp = pickNotionPropertyName(databaseProperties, ["texto"], ["rich_text", "title"]);
  const texto1Prop = pickNotionPropertyName(databaseProperties, ["texto 1"], ["rich_text", "title"]);
  const textProp = pickNotionPropertyName(databaseProperties, ["text"], ["rich_text", "title"]);
  const texto2Prop = pickNotionPropertyName(databaseProperties, ["texto 2"], ["rich_text", "title"]);
  const dateSentProps = Object.entries(databaseProperties)
    .filter(([name, prop]) => {
      const normalizedName = normalizeNotionFieldName(name);
      const propType = String(prop?.type || "");
      return normalizedName.startsWith("date sent") && propType === "date";
    })
    .map(([name]) => name);

  const dateSentValue = input.dateSent || new Date().toISOString().slice(0, 10);

  const assignValue = (propName, value) => {
    if (!propName || !databaseProperties[propName]) {
      return;
    }

    const propType = String(databaseProperties[propName]?.type || "");
    if (value === undefined || value === null || String(value).trim() === "") {
      return;
    }

    const cleanValue = String(value).trim();

    if (propType === "title") {
      properties[propName] = { title: titleText(cleanValue) };
      return;
    }
    if (propType === "rich_text") {
      properties[propName] = { rich_text: richText(cleanValue) };
      return;
    }
    if (propType === "date") {
      properties[propName] = { date: { start: cleanValue } };
      return;
    }
    if (propType === "number") {
      const parsed = Number(cleanValue);
      if (Number.isFinite(parsed)) properties[propName] = { number: parsed };
      return;
    }
    if (propType === "select") {
      properties[propName] = { select: { name: cleanValue } };
      return;
    }
    if (propType === "status") {
      properties[propName] = { status: { name: cleanValue } };
      return;
    }
    if (propType === "checkbox") {
      properties[propName] = { checkbox: parseBooleanLike(cleanValue) };
      return;
    }
    if (propType === "email") {
      properties[propName] = { email: cleanValue };
      return;
    }
    if (propType === "url") {
      properties[propName] = { url: cleanValue };
      return;
    }
    if (propType === "phone_number") {
      properties[propName] = { phone_number: cleanValue };
      return;
    }
    if (propType === "multi_select") {
      const names = cleanValue
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean)
        .map((name) => ({ name }));
      if (names.length > 0) {
        properties[propName] = { multi_select: names };
      }
    }
  };

  assignValue(clientNameProp, input.clientName);
  assignValue(tabelaProp, input.tabela);
  assignValue(tipoClienteProp, input.tipoCliente);
  assignValue(textoProp, input.texto);
  assignValue(texto1Prop, input.texto1);
  assignValue(textProp, input.text);
  assignValue(texto2Prop, input.texto2);
  for (const propName of dateSentProps) {
    assignValue(propName, dateSentValue);
  }

  return properties;
}

function humanizeUsername(username) {
  return String(username || "")
    .split("_")
    .filter(Boolean)
    .map((chunk) => chunk.charAt(0).toUpperCase() + chunk.slice(1))
    .join(" ");
}

function getWorkspaceAuthUsers() {
  return [1, 2, 3]
    .map((index) => ({
      username: String(process.env[`AUTH_USER_${index}_USERNAME`] || "").trim(),
      password: String(process.env[`AUTH_USER_${index}_PASSWORD`] || ""),
      displayName: String(process.env[`AUTH_USER_${index}_DISPLAY_NAME`] || "").trim()
    }))
    .filter((user) => user.username && user.password)
    .map((user) => ({
      ...user,
      displayName: user.displayName || humanizeUsername(user.username)
    }));
}

async function safeNotionLog(logFn) {
  if (!notionEnabled) {
    return null;
  }

  try {
    await logFn();
    return null;
  } catch (error) {
    return error instanceof Error ? error.message : "Notion logging failed";
  }
}

async function safeNotionTrackerLog(logFn) {
  if (!notionTrackerEnabled || !notionTracker) {
    return null;
  }

  try {
    await logFn();
    return null;
  } catch (error) {
    return error instanceof Error ? error.message : "Notion tracker logging failed";
  }
}

function trackerPropName(name, fallback) {
  return process.env[name] || fallback;
}

function inferTrackerMessageType(templateName, contextType) {
  const normalizedContext = String(contextType || "").trim().toLowerCase();
  if (normalizedContext.includes("incident") || normalizedContext.includes("incid")) {
    return "Incident";
  }
  if (normalizedContext.includes("pick")) {
    return "Pick Up Point";
  }

  const normalizedTemplate = String(templateName || "").trim().toLowerCase();
  if (normalizedTemplate === "order_pick_no_ctt") {
    return "Incident";
  }
  if (normalizedTemplate === "order_pick_up_1") {
    return "Pick Up Point";
  }

  return "";
}

async function createNotionTrackerRow({
  to,
  templateName,
  bodyVariables,
  status,
  trackerContext,
  rawResponse
}) {
  if (!notionTrackerEnabled || !notionTracker) {
    return null;
  }

  const messageType = inferTrackerMessageType(templateName, trackerContext?.messageType);
  if (!messageType) {
    return null;
  }

  const propClientName = trackerPropName("NOTION_TRACKER_PROP_CLIENT_NAME", "Client Name");
  const propMessage = trackerPropName("NOTION_TRACKER_PROP_MESSAGE", "Mensagem");
  const propClientPhone = trackerPropName("NOTION_TRACKER_PROP_CLIENT_PHONE", "Client Phone");
  const propParcelId = trackerPropName("NOTION_TRACKER_PROP_PARCEL_ID", "Parcel ID");
  const propMessageType = trackerPropName("NOTION_TRACKER_PROP_MESSAGE_TYPE", "Message Type");
  const propDateSent = trackerPropName("NOTION_TRACKER_PROP_DATE_SENT", "Date Sent");
  const propSmsClicksend = trackerPropName("NOTION_TRACKER_PROP_SMS_CLICKSEND", "sms Clicksend");
  const propStatus = trackerPropName("NOTION_TRACKER_PROP_STATUS", "Status");
  const propMessageTitle = trackerPropName("NOTION_TRACKER_PROP_MESSAGE_TITLE", "Message Title");
  const propResponseReceived = trackerPropName("NOTION_TRACKER_PROP_RESPONSE_RECEIVED", "Response Received");
  const propFollowUpRequired = trackerPropName("NOTION_TRACKER_PROP_FOLLOW_UP_REQUIRED", "Follow-up Required");
  const propReminderText = trackerPropName("NOTION_TRACKER_PROP_REMINDER_TEXT", "Reminder Text");
  const propNotes = trackerPropName("NOTION_TRACKER_PROP_NOTES", "Notes");

  const clientName = String(trackerContext?.clientName || bodyVariables?.[0] || "").trim();
  const parcelId = String(trackerContext?.parcelId || bodyVariables?.[1] || "").trim();
  const notes = String(trackerContext?.notes || "").trim();
  const prettyDate = new Date().toLocaleDateString("pt-PT", {
    day: "numeric",
    month: "long",
    year: "numeric"
  });

  const trackerProperties = {
    [propClientName]: { rich_text: richText(clientName) },
    [propMessage]: { rich_text: richText(`Template ${templateName} | Vars: ${(bodyVariables || []).join(" | ")}`) },
    [propClientPhone]: { phone_number: String(to || "").trim() || null },
    [propParcelId]: { rich_text: richText(parcelId) },
    [propMessageType]: { select: { name: messageType } },
    [propDateSent]: { date: { start: new Date().toISOString() } },
    [propSmsClicksend]: { rich_text: richText("No") },
    [propStatus]: { status: { name: status && String(status).startsWith("failed_") ? "Failed" : "In Progress" } },
    [propMessageTitle]: { title: richText("Whatsapp Template") },
    [propResponseReceived]: { checkbox: false },
    [propFollowUpRequired]: { checkbox: false },
    [propReminderText]: { rich_text: richText("") },
    [propNotes]: { rich_text: richText(notes) }
  };

  // Avoid duplicate tracker entries for the same pickup parcel: update existing row when possible.
  if (parcelId) {
    try {
      const existing = await notionTracker.databases.query({
        database_id: notionTrackerDatabaseId,
        filter: {
          and: [
            {
              property: propParcelId,
              rich_text: {
                equals: parcelId
              }
            },
            {
              property: propMessageType,
              select: {
                equals: messageType
              }
            }
          ]
        },
        page_size: 1
      });

      const existingPageId = existing?.results?.[0]?.id;
      if (existingPageId) {
        await notionTracker.pages.update({
          page_id: existingPageId,
          properties: trackerProperties
        });
        return;
      }
    } catch {
      // If lookup/update fails due to schema mismatch, fallback to create flow below.
    }
  }

  await notionTracker.pages.create({
    parent: { database_id: notionTrackerDatabaseId },
    properties: trackerProperties,
    children: [
      {
        object: "block",
        type: "paragraph",
        paragraph: {
          rich_text: richText(`Raw response: ${JSON.stringify(rawResponse ?? {})}`)
        }
      }
    ]
  });
}

async function safeSupabaseLog(logFn) {
  if ((!supabaseEnabled || !supabase) && (!pgEnabled || !pgPool)) {
    return null;
  }

  try {
    await logFn();
    return null;
  } catch (error) {
    return error instanceof Error ? error.message : "Supabase logging failed";
  }
}

async function createSupabaseLogRow({
  direction = "out",
  channel,
  to,
  contactName,
  messageText,
  templateName,
  status,
  apiMessageId,
  payload
}) {
  if ((!supabaseEnabled || !supabase) && (!pgEnabled || !pgPool)) {
    return null;
  }

  const row = {
    direction,
    channel,
    to_number: String(to || "").trim(),
    contact_name: contactName ? String(contactName) : null,
    message_text: messageText ? String(messageText) : null,
    template_name: templateName ? String(templateName) : null,
    status: status ? String(status) : null,
    api_message_id: apiMessageId ? String(apiMessageId) : null,
    payload: payload && typeof payload === "object" ? payload : {}
  };

  if (supabaseEnabled && supabase) {
    const { error } = await supabase.from("whatsapp_logs").insert(row);
    if (error) {
      throw new Error(error.message || "Failed to insert Supabase log row");
    }
    return;
  }

  if (pgEnabled && pgPool) {
    await pgPool.query(
      `insert into public.whatsapp_logs
      (direction, channel, to_number, contact_name, message_text, template_name, status, api_message_id, payload)
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)`,
      [
        row.direction,
        row.channel,
        row.to_number,
        row.contact_name,
        row.message_text,
        row.template_name,
        row.status,
        row.api_message_id,
        JSON.stringify(row.payload || {})
      ]
    );
  }
}

async function findSupabaseLogByApiMessageId(messageId) {
  const normalized = String(messageId || "").trim();
  if (!normalized) {
    return null;
  }

  if (supabaseEnabled && supabase) {
    const { data, error } = await supabase
      .from("whatsapp_logs")
      .select("id")
      .eq("api_message_id", normalized)
      .limit(1);

    if (error) {
      throw new Error(error.message || "Failed to query Supabase logs by message id");
    }

    return Array.isArray(data) && data.length > 0 ? data[0] : null;
  }

  if (pgEnabled && pgPool) {
    const { rows } = await pgPool.query(
      `select id from public.whatsapp_logs where api_message_id = $1 limit 1`,
      [normalized]
    );
    return Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
  }

  return null;
}

async function findSupabaseLogPayloadByApiMessageId(messageId) {
  const normalized = String(messageId || "").trim();
  if (!normalized) {
    return null;
  }

  if (supabaseEnabled && supabase) {
    const { data, error } = await supabase
      .from("whatsapp_logs")
      .select("payload")
      .eq("api_message_id", normalized)
      .order("id", { ascending: false })
      .limit(1);

    if (error) {
      throw new Error(error.message || "Failed to query Supabase log payload by message id");
    }

    if (Array.isArray(data) && data.length > 0) {
      return data[0]?.payload || null;
    }

    return null;
  }

  if (pgEnabled && pgPool) {
    const { rows } = await pgPool.query(
      `select payload
       from public.whatsapp_logs
       where api_message_id = $1
       order by id desc
       limit 1`,
      [normalized]
    );
    if (Array.isArray(rows) && rows.length > 0) {
      return rows[0]?.payload || null;
    }
  }

  return null;
}

async function updateSupabaseLogStatusByMessageId({ messageId, status, payload }) {
  const normalizedMessageId = String(messageId || "").trim();
  const normalizedStatus = String(status || "").trim();
  if (!normalizedMessageId || !normalizedStatus) {
    return;
  }

  const nextPayload = payload && typeof payload === "object" ? payload : {};

  if (supabaseEnabled && supabase) {
    const { error } = await supabase
      .from("whatsapp_logs")
      .update({
        status: normalizedStatus,
        payload: nextPayload
      })
      .eq("api_message_id", normalizedMessageId);

    if (error) {
      throw new Error(error.message || "Failed to update Supabase log status");
    }
    return;
  }

  if (pgEnabled && pgPool) {
    await pgPool.query(
      `update public.whatsapp_logs
      set status = $2,
          payload = coalesce(payload, '{}'::jsonb) || $3::jsonb
      where api_message_id = $1`,
      [normalizedMessageId, normalizedStatus, JSON.stringify(nextPayload)]
    );
  }
}

async function forwardWebhookToBotpress(payload) {
  if (!botpressWebhookRelayUrl) {
    return;
  }

  const headers = { "Content-Type": "application/json" };
  if (botpressApiKey) {
    // Some Botpress deployments validate Authorization and some use x-botpress-api-key.
    headers.Authorization = `Bearer ${botpressApiKey}`;
    headers["x-botpress-api-key"] = botpressApiKey;
  }

  try {
    await fetch(botpressWebhookRelayUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(8000)
    });
  } catch {
    // Best effort relay; do not fail webhook handling.
  }
}

function readBotpressTextPayload(input) {
  if (!input || typeof input !== "object") {
    return "";
  }

  const directText = String(input.text || input.message || input.content || "").trim();
  if (directText) {
    return directText;
  }

  const payloadText = String(input?.payload?.text || input?.payload?.message || "").trim();
  if (payloadText) {
    return payloadText;
  }

  const messages = Array.isArray(input?.messages) ? input.messages : [];
  for (const message of messages) {
    const text = String(message?.text || message?.message || message?.payload?.text || "").trim();
    if (text) {
      return text;
    }
  }

  return "";
}

function readBotpressRecipient(input) {
  if (!input || typeof input !== "object") {
    return "";
  }

  const candidates = [
    input.to,
    input.phone,
    input.userPhone,
    input.wa_id,
    input?.recipient?.phone,
    input?.recipient?.to,
    input?.payload?.to,
    input?.payload?.phone,
    input?.conversation?.phone
  ];

  for (const candidate of candidates) {
    const value = String(candidate || "").trim();
    if (value) {
      return value;
    }
  }

  return "";
}

function isBotpressRequestAuthorized(req) {
  if (!botpressApiKey) {
    return true;
  }

  const authHeader = String(req.headers.authorization || "").trim();
  const xApiKey = String(req.headers["x-botpress-api-key"] || "").trim();
  const queryToken = String(req.query?.token || "").trim();

  return (
    authHeader === `Bearer ${botpressApiKey}` ||
    xApiKey === botpressApiKey ||
    queryToken === botpressApiKey
  );
}

async function createNotionMessageRow({ to, text, status, messageId, rawResponse }) {
  if (!notionEnabled) {
    return null;
  }

  const databaseId = requiredEnv("NOTION_DATABASE_ID");
  const titleProp = notionPropName("NOTION_PROP_TITLE", "Name");
  const messageIdProp = notionPropName("NOTION_PROP_MESSAGE_ID", "Message ID");
  const toProp = notionPropName("NOTION_PROP_TO", "To");
  const textProp = notionPropName("NOTION_PROP_TEXT", "Text");
  const statusProp = notionPropName("NOTION_PROP_STATUS", "Status");
  const updatedAtProp = notionPropName("NOTION_PROP_UPDATED_AT", "Updated At");

  const safeTo = to || "Unknown recipient";
  const safeMessageId = messageId || "pending";
  const safeStatus = status || "queued";

  return notion.pages.create({
    parent: { database_id: databaseId },
    properties: {
      [titleProp]: { title: titleText(`WA ${safeTo} ${safeStatus}`) },
      [messageIdProp]: { rich_text: richText(safeMessageId) },
      [toProp]: { rich_text: richText(safeTo) },
      [textProp]: { rich_text: richText(text || "") },
      [statusProp]: { rich_text: richText(safeStatus) },
      [updatedAtProp]: { date: { start: new Date().toISOString() } }
    },
    children: [
      {
        object: "block",
        type: "paragraph",
        paragraph: {
          rich_text: richText(`Raw response: ${JSON.stringify(rawResponse ?? {})}`)
        }
      }
    ]
  });
}

function inboundMessageText(message) {
  const type = String(message?.type || "unknown");

  if (type === "text") {
    return String(message?.text?.body || "").trim() || "[text]";
  }

  if (type === "button") {
    return `[button] ${String(message?.button?.text || "").trim()}`.trim();
  }

  if (type === "interactive") {
    const listReply = String(message?.interactive?.list_reply?.title || "").trim();
    const buttonReply = String(message?.interactive?.button_reply?.title || "").trim();
    const reply = listReply || buttonReply;
    return reply ? `[interactive] ${reply}` : "[interactive]";
  }

  if (type === "image") {
    return "[image]";
  }

  if (type === "video") {
    return "[video]";
  }

  if (type === "audio") {
    return "[audio]";
  }

  if (type === "document") {
    return "[document]";
  }

  if (type === "sticker") {
    return "[sticker]";
  }

  return `[${type}]`;
}

function inboundMessageMediaInfo(message) {
  const type = String(message?.type || "").trim().toLowerCase();
  if (!type) {
    return { mediaType: "", mediaId: "" };
  }

  const mediaNode = message?.[type] || null;
  const mediaId = String(mediaNode?.id || "").trim();
  return {
    mediaType: type,
    mediaId
  };
}

function sanitizeBlobPathSegment(value, fallback = "unknown") {
  const clean = String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9_-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");

  if (!clean) {
    return fallback;
  }

  return clean.slice(0, 80);
}

function toBase64Url(input) {
  return Buffer.from(String(input || ""), "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function fromBase64Url(input) {
  const normalized = String(input || "")
    .replace(/-/g, "+")
    .replace(/_/g, "/");
  const padLength = normalized.length % 4;
  const padded = padLength === 0 ? normalized : normalized + "=".repeat(4 - padLength);
  return Buffer.from(padded, "base64").toString("utf8");
}

function signMediaBlobToken(payload) {
  if (!mediaBlobUrlSignSecret) {
    throw new Error("MEDIA_BLOB_URL_SIGN_SECRET or CRON_SECRET is required");
  }

  const payloadJson = JSON.stringify(payload);
  const encodedPayload = toBase64Url(payloadJson);
  const signature = createHmac("sha256", mediaBlobUrlSignSecret).update(encodedPayload).digest("hex");
  return `${encodedPayload}.${signature}`;
}

function verifyMediaBlobToken(token) {
  const raw = String(token || "").trim();
  const dotIndex = raw.lastIndexOf(".");
  if (!raw || dotIndex <= 0) {
    throw new Error("Invalid token");
  }

  const encodedPayload = raw.slice(0, dotIndex);
  const signature = raw.slice(dotIndex + 1);
  if (!encodedPayload || !signature) {
    throw new Error("Invalid token");
  }

  const expectedSignature = createHmac("sha256", mediaBlobUrlSignSecret).update(encodedPayload).digest("hex");
  if (!/^[0-9a-f]+$/i.test(signature) || signature.length !== expectedSignature.length) {
    throw new Error("Invalid token signature");
  }
  const isValidSignature = timingSafeEqual(Buffer.from(signature, "hex"), Buffer.from(expectedSignature, "hex"));
  if (!isValidSignature) {
    throw new Error("Invalid token signature");
  }

  let payload;
  try {
    payload = JSON.parse(fromBase64Url(encodedPayload));
  } catch {
    throw new Error("Invalid token payload");
  }

  const blobPath = String(payload?.p || "").trim();
  const expiresAt = Number(payload?.e || 0);
  const accessRaw = String(payload?.a || "").trim().toLowerCase();
  const access = accessRaw === "public" ? "public" : "private";

  if (!blobPath || !Number.isFinite(expiresAt) || expiresAt <= 0) {
    throw new Error("Invalid token payload");
  }

  return { blobPath, expiresAt, access };
}

function parseBlobPathFromUrl(input) {
  const raw = String(input || "").trim();
  if (!raw) return "";

  try {
    const parsed = new URL(raw);
    return String(parsed.pathname || "").replace(/^\/+/, "").trim();
  } catch {
    return "";
  }
}

function getExternalBaseUrl(req) {
  const forwardedProto = String(req.headers["x-forwarded-proto"] || "").split(",")[0].trim();
  const forwardedHost = String(req.headers["x-forwarded-host"] || "").split(",")[0].trim();
  const host = forwardedHost || String(req.headers.host || "").trim();
  const proto = forwardedProto || req.protocol || "https";

  if (!host) {
    return "";
  }

  return `${proto}://${host}`;
}

function readBlobReferenceFromPayload(payload) {
  const mediaStorage = payload?.media?.storage || null;
  if (!mediaStorage || typeof mediaStorage !== "object") {
    return { blobPath: "", blobUrl: "", blobAccess: "" };
  }

  const blobPath = String(mediaStorage?.blobPath || "").trim();
  const blobUrl = String(mediaStorage?.blobUrl || "").trim();
  const blobAccess = String(mediaStorage?.blobAccess || "").trim().toLowerCase();
  return { blobPath, blobUrl, blobAccess };
}

function extensionFromMimeType(mimeType) {
  const mime = String(mimeType || "").trim().toLowerCase();
  if (!mime) return "bin";

  const map = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "image/gif": "gif",
    "video/mp4": "mp4",
    "video/3gpp": "3gp",
    "audio/mpeg": "mp3",
    "audio/mp4": "m4a",
    "audio/ogg": "ogg",
    "audio/aac": "aac",
    "application/pdf": "pdf"
  };

  if (map[mime]) {
    return map[mime];
  }

  const subtype = mime.split("/")[1] || "";
  const cleanSubtype = subtype.split(";")[0].replace(/[^a-z0-9.+-]/g, "");
  if (!cleanSubtype) {
    return "bin";
  }

  if (cleanSubtype.includes("+")) {
    return cleanSubtype.split("+").pop() || "bin";
  }

  return cleanSubtype;
}

async function fetchWhatsappMediaBinary(mediaId) {
  const normalizedMediaId = String(mediaId || "").trim();
  if (!normalizedMediaId) {
    throw new Error("Missing media id");
  }

  const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
  const token = requiredEnv("WHATSAPP_ACCESS_TOKEN");

  const metaResponse = await fetch(`https://graph.facebook.com/${apiVersion}/${encodeURIComponent(normalizedMediaId)}`, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`
    },
    signal: AbortSignal.timeout(12000)
  });

  const metaBody = await metaResponse.json().catch(() => ({}));
  if (!metaResponse.ok || !metaBody?.url) {
    throw new Error(`Failed to resolve media URL (${metaResponse.status})`);
  }

  const binaryResponse = await fetch(String(metaBody.url), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`
    },
    signal: AbortSignal.timeout(20000)
  });

  if (!binaryResponse.ok) {
    throw new Error(`Failed to fetch media binary (${binaryResponse.status})`);
  }

  const arrayBuffer = await binaryResponse.arrayBuffer();
  const buffer = Buffer.from(arrayBuffer);
  const mimeType = String(binaryResponse.headers.get("content-type") || metaBody?.mime_type || "application/octet-stream");
  const sha256 = createHash("sha256").update(buffer).digest("hex");

  return {
    buffer,
    mimeType,
    sha256,
    sizeBytes: buffer.length,
    meta: metaBody
  };
}

async function uploadInboundMediaToBlob({ from, messageId, mediaType, mediaId }) {
  if (!blobMediaUploadEnabled) {
    return { uploaded: false, reason: "blob_disabled" };
  }

  const normalizedMediaId = String(mediaId || "").trim();
  if (!normalizedMediaId) {
    return { uploaded: false, reason: "missing_media_id" };
  }

  const mediaBinary = await fetchWhatsappMediaBinary(normalizedMediaId);
  const now = new Date();
  const year = String(now.getUTCFullYear());
  const month = String(now.getUTCMonth() + 1).padStart(2, "0");
  const day = String(now.getUTCDate()).padStart(2, "0");
  const toSegment = sanitizeBlobPathSegment(from, "unknown-contact");
  const messageSegment = sanitizeBlobPathSegment(messageId || normalizedMediaId, normalizedMediaId.slice(0, 40));
  const mediaSegment = sanitizeBlobPathSegment(mediaType, "media");
  const extension = extensionFromMimeType(mediaBinary.mimeType);
  const blobPath = `whatsapp/inbound/${year}/${month}/${day}/${toSegment}/${messageSegment}-${mediaSegment}.${extension}`;

  const blob = await put(blobPath, mediaBinary.buffer, {
    access: blobMediaAccess,
    contentType: mediaBinary.mimeType,
    token: blobReadWriteToken
  });

  return {
    uploaded: true,
    mediaType: String(mediaType || "").trim(),
    mediaId: normalizedMediaId,
    mimeType: mediaBinary.mimeType,
    sizeBytes: mediaBinary.sizeBytes,
    sha256: mediaBinary.sha256,
    blobUrl: String(blob?.url || ""),
    blobPath: String(blob?.pathname || blobPath),
    blobAccess: blobMediaAccess
  };
}

async function logInboundMessage({ from, message, contact }) {
  const inboundMessageId = String(message?.id || "").trim();
  if (!inboundMessageId) {
    return null;
  }

  let existingSharedLog = null;
  try {
    existingSharedLog = await findSupabaseLogByApiMessageId(inboundMessageId);
  } catch {
    existingSharedLog = null;
  }

  if (!existingSharedLog) {
    const contactName = String(contact?.profile?.name || "").trim();
    const fromWaId = String(from || "").trim();
    const summaryText = inboundMessageText(message);
    const mediaInfo = inboundMessageMediaInfo(message);
    let mediaStorage = null;

    if (mediaInfo.mediaId) {
      try {
        mediaStorage = await uploadInboundMediaToBlob({
          from: fromWaId,
          messageId: inboundMessageId,
          mediaType: mediaInfo.mediaType,
          mediaId: mediaInfo.mediaId
        });
      } catch (error) {
        mediaStorage = {
          uploaded: false,
          reason: "blob_upload_failed",
          error: error instanceof Error ? error.message : "Unknown error"
        };
      }
    }

    await safeSupabaseLog(() =>
      createSupabaseLogRow({
        direction: "in",
        channel: "chat",
        to: fromWaId,
        contactName,
        messageText: summaryText,
        status: "received",
        apiMessageId: inboundMessageId,
        payload: {
          direction: "inbound",
          contact,
          message,
          media: {
            type: mediaInfo.mediaType || null,
            id: mediaInfo.mediaId || null,
            storage: mediaStorage
          }
        }
      })
    );
  }

  if (!notionEnabled) {
    return inboundMessageId;
  }

  const existing = await findRowByMessageId(inboundMessageId);
  if (existing) {
    return existing.id;
  }

  const contactName = String(contact?.profile?.name || "").trim();
  const fromWaId = String(from || "").trim();
  const summaryText = inboundMessageText(message);
  const fullText = contactName
    ? `[INBOUND] ${contactName} (${fromWaId}): ${summaryText}`
    : `[INBOUND] ${fromWaId}: ${summaryText}`;

  await createNotionMessageRow({
    to: fromWaId,
    text: fullText,
    status: "received_inbound",
    messageId: inboundMessageId,
    rawResponse: {
      direction: "inbound",
      contact,
      message
    }
  });

  return inboundMessageId;
}

async function findRowByMessageId(messageId) {
  if (!notionEnabled) {
    return null;
  }

  const databaseId = requiredEnv("NOTION_DATABASE_ID");
  const messageIdProp = notionPropName("NOTION_PROP_MESSAGE_ID", "Message ID");

  const query = await notion.databases.query({
    database_id: databaseId,
    filter: {
      property: messageIdProp,
      rich_text: { equals: messageId }
    },
    page_size: 1
  });

  return query.results[0] || null;
}

async function updateNotionMessageStatus({ messageId, status, rawStatus }) {
  if (!notionEnabled) {
    return null;
  }

  const page = await findRowByMessageId(messageId);
  if (!page) {
    return null;
  }

  const statusProp = notionPropName("NOTION_PROP_STATUS", "Status");
  const updatedAtProp = notionPropName("NOTION_PROP_UPDATED_AT", "Updated At");

  await notion.pages.update({
    page_id: page.id,
    properties: {
      [statusProp]: { rich_text: richText(status) },
      [updatedAtProp]: { date: { start: new Date().toISOString() } }
    }
  });

  await notion.blocks.children.append({
    block_id: page.id,
    children: [
      {
        object: "block",
        type: "paragraph",
        paragraph: {
          rich_text: richText(`Status update: ${JSON.stringify(rawStatus ?? {})}`)
        }
      }
    ]
  });

  return page.id;
}

app.get("/api/events", (req, res) => {
  // Long-lived SSE connections are expensive on Vercel serverless.
  // The frontend already polls logs; disable SSE on Vercel to reduce CPU usage.
  if (process.env.VERCEL) {
    return res.status(204).end();
  }

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();
  sseClients.add(res);
  const keepalive = setInterval(() => {
    try {
      res.write(": keepalive\n\n");
    } catch (error) {
      warnSoftError("sse.keepalive.write", error, { route: "/api/events" });
    }
  }, 25000);
  req.on("close", () => { sseClients.delete(res); clearInterval(keepalive); });
});

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    service: "linke-cloud-backend",
    message: "API is running",
    health: "/health"
  });
});

app.get("/api/google/oauth/start", (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const clientId = requiredEnv("GOOGLE_OAUTH_CLIENT_ID");
    const redirectUri = requiredEnv("GOOGLE_OAUTH_REDIRECT_URI");
    const authBase = String(process.env.GOOGLE_OAUTH_AUTH_URI || "https://accounts.google.com/o/oauth2/v2/auth").trim();
    const scope = String(process.env.GOOGLE_OAUTH_SCOPE || "openid email profile https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/gmail.send").trim();
    const loginHint = String(process.env.GOOGLE_OAUTH_LOGIN_HINT || "").trim();
    const hostedDomain = String(process.env.GOOGLE_OAUTH_HD || "").trim();
    const state = createGoogleOauthState();

    const url = new URL(authBase);
    url.searchParams.set("client_id", clientId);
    url.searchParams.set("redirect_uri", redirectUri);
    url.searchParams.set("response_type", "code");
    url.searchParams.set("scope", scope);
    url.searchParams.set("access_type", "offline");
    url.searchParams.set("prompt", "select_account consent");
    if (loginHint) {
      url.searchParams.set("login_hint", loginHint);
    }
    if (hostedDomain) {
      url.searchParams.set("hd", hostedDomain);
    }
    url.searchParams.set("state", state);

    const mode = String(req.query.mode || "json").toLowerCase();
    if (mode === "redirect") {
      return res.redirect(url.toString());
    }

    return res.json({ ok: true, url: url.toString(), state });
  } catch (error) {
    warnSoftError("google.oauth.start", error, { route: "/api/google/oauth/start", requestId });
    return res.status(500).json({
      error: "Failed to build Google OAuth URL",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/google/oauth/callback", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const htmlResponse = wantsHtmlResponse(req);
    const code = String(req.query.code || "").trim();
    const state = String(req.query.state || "").trim();

    if (!code) {
      if (htmlResponse) {
        return sendOauthHtmlResponse(res, {
          ok: false,
          title: "Ligacao Google incompleta",
          message: "Falta o codigo de autorizacao. Inicia novamente a ligacao Google.",
          statusCode: 400
        });
      }
      return res.status(400).json({ error: "Missing 'code' query parameter." });
    }
    if (!isGoogleOauthStateValid(state)) {
      if (htmlResponse) {
        return sendOauthHtmlResponse(res, {
          ok: false,
          title: "Sessao OAuth expirada",
          message: "O pedido expirou. Clica em Ligar Google novamente para gerar uma nova sessao.",
          statusCode: 400
        });
      }
      return res.status(400).json({ error: "Invalid or expired OAuth state." });
    }

    const clientId = requiredEnv("GOOGLE_OAUTH_CLIENT_ID");
    const clientSecret = requiredEnv("GOOGLE_OAUTH_CLIENT_SECRET");
    const redirectUri = requiredEnv("GOOGLE_OAUTH_REDIRECT_URI");
    const tokenUri = String(process.env.GOOGLE_OAUTH_TOKEN_URI || "https://oauth2.googleapis.com/token").trim();

    const tokenBody = new URLSearchParams();
    tokenBody.set("code", code);
    tokenBody.set("client_id", clientId);
    tokenBody.set("client_secret", clientSecret);
    tokenBody.set("redirect_uri", redirectUri);
    tokenBody.set("grant_type", "authorization_code");

    const tokenRes = await fetch(tokenUri, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: tokenBody.toString()
    });

    const tokenData = await tokenRes.json().catch(() => ({}));
    if (!tokenRes.ok) {
      const isInvalidGrant = String(tokenData?.error || "").toLowerCase() === "invalid_grant";
      if (isInvalidGrant && googleOauthSession.accessToken && htmlResponse) {
        return sendOauthHtmlResponse(res, {
          ok: true,
          title: "Conta Google ja ligada",
          message: "Este codigo ja foi usado. A tua conta ja esta conectada, podes voltar a app.",
          statusCode: 200
        });
      }

      if (htmlResponse) {
        return sendOauthHtmlResponse(res, {
          ok: false,
          title: "Falha na ligacao Google",
          message: isInvalidGrant
            ? "O codigo de autorizacao ja foi usado ou expirou. Inicia novamente a ligacao Google."
            : "Nao foi possivel concluir a ligacao Google. Tenta novamente.",
          statusCode: tokenRes.status || 400
        });
      }

      return res.status(tokenRes.status).json({
        error: "Failed to exchange Google OAuth code",
        details: tokenData
      });
    }

    const allowedEmail = String(process.env.GOOGLE_OAUTH_ALLOWED_EMAIL || "").trim().toLowerCase();
    if (allowedEmail) {
      const idToken = String(tokenData?.id_token || "").trim();
      const jwtPayload = idToken.split(".")[1] || "";
      let tokenEmail = "";
      try {
        const normalized = jwtPayload.replace(/-/g, "+").replace(/_/g, "/");
        const decoded = JSON.parse(Buffer.from(normalized, "base64").toString("utf8"));
        tokenEmail = String(decoded?.email || "").trim().toLowerCase();
      } catch {
        tokenEmail = "";
      }

      if (!tokenEmail || tokenEmail !== allowedEmail) {
        if (htmlResponse) {
          return sendOauthHtmlResponse(res, {
            ok: false,
            title: "Conta Google errada",
            message: `Liga com a conta ${allowedEmail}.`,
            statusCode: 403
          });
        }
        return res.status(403).json({
          error: "Wrong Google account connected",
          details: `Please connect with ${allowedEmail}`
        });
      }
    }

    googleOauthSession.accessToken = String(tokenData?.access_token || "").trim();
    if (tokenData?.refresh_token) {
      googleOauthSession.refreshToken = String(tokenData.refresh_token || "").trim();
    }
    googleOauthSession.scope = String(tokenData?.scope || "").trim();
    googleOauthSession.tokenType = String(tokenData?.token_type || "Bearer").trim() || "Bearer";
    const expiresIn = Number(tokenData?.expires_in || 0) || 0;
    googleOauthSession.expiresAt = Date.now() + (expiresIn > 0 ? expiresIn * 1000 : 0);
    await persistGoogleOauthSession();

    if (htmlResponse) {
      return sendOauthHtmlResponse(res, {
        ok: true,
        title: "Google ligado com sucesso",
        message: "Autorizacao concluida. Ja podes voltar a app e enviar emails.",
        statusCode: 200
      });
    }

    return res.json({
      ok: true,
      token_type: tokenData?.token_type || "Bearer",
      scope: tokenData?.scope || "",
      expires_in: tokenData?.expires_in || 0,
      access_token: tokenData?.access_token || "",
      refresh_token: tokenData?.refresh_token || ""
    });
  } catch (error) {
    warnSoftError("google.oauth.callback", error, { route: "/api/google/oauth/callback", requestId });
    if (wantsHtmlResponse(req)) {
      return sendOauthHtmlResponse(res, {
        ok: false,
        title: "Erro no callback Google",
        message: error instanceof Error ? error.message : "Erro inesperado no OAuth Google.",
        statusCode: 500
      });
    }

    return res.status(500).json({
      error: "Google OAuth callback failed",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/google/oauth/status", (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  return (async () => {
    await hydrateGoogleOauthSession();
    const connected = Boolean(googleOauthSession.accessToken);
    return res.json({
      ok: true,
      connected,
      scope: googleOauthSession.scope || "",
      expires_at: googleOauthSession.expiresAt || 0
    });
  })().catch((error) => {
    warnSoftError("google.oauth.status", error, { route: "/api/google/oauth/status", requestId });
    return res.status(500).json({
      error: "Google OAuth status failed",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  });
});

async function ensureGoogleAccessToken() {
  await hydrateGoogleOauthSession();

  if (!googleOauthSession.accessToken) {
    throw new Error("Google account not connected. Authorize first at /api/google/oauth/start?mode=redirect");
  }

  const stillValid = googleOauthSession.expiresAt > Date.now() + 60_000;
  if (stillValid) {
    return googleOauthSession.accessToken;
  }

  if (!googleOauthSession.refreshToken) {
    return googleOauthSession.accessToken;
  }

  const clientId = requiredEnv("GOOGLE_OAUTH_CLIENT_ID");
  const clientSecret = requiredEnv("GOOGLE_OAUTH_CLIENT_SECRET");
  const tokenUri = String(process.env.GOOGLE_OAUTH_TOKEN_URI || "https://oauth2.googleapis.com/token").trim();

  const refreshBody = new URLSearchParams();
  refreshBody.set("client_id", clientId);
  refreshBody.set("client_secret", clientSecret);
  refreshBody.set("refresh_token", googleOauthSession.refreshToken);
  refreshBody.set("grant_type", "refresh_token");

  const refreshRes = await fetch(tokenUri, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: refreshBody.toString()
  });

  const refreshData = await refreshRes.json().catch(() => ({}));
  if (!refreshRes.ok || !refreshData?.access_token) {
    throw new Error(`Failed to refresh Google token (${refreshRes.status})`);
  }

  googleOauthSession.accessToken = String(refreshData.access_token || "").trim();
  googleOauthSession.tokenType = String(refreshData.token_type || "Bearer").trim() || "Bearer";
  const expiresIn = Number(refreshData.expires_in || 0) || 0;
  googleOauthSession.expiresAt = Date.now() + (expiresIn > 0 ? expiresIn * 1000 : 0);
  await persistGoogleOauthSession();

  return googleOauthSession.accessToken;
}

function sanitizeMimeHeaderValue(value) {
  return String(value || "").replace(/[\r\n]+/g, " ").trim();
}

function encodeMimeHeaderUtf8(value) {
  const cleanValue = sanitizeMimeHeaderValue(value);
  if (!cleanValue) return "";
  if (/^[\x00-\x7F]*$/.test(cleanValue)) return cleanValue;
  return `=?UTF-8?B?${Buffer.from(cleanValue, "utf8").toString("base64")}?=`;
}

function encodeMimeBodyBase64Utf8(value) {
  const base64 = Buffer.from(String(value ?? ""), "utf8").toString("base64");
  const chunks = base64.match(/.{1,76}/g);
  return chunks ? chunks.join("\r\n") : "";
}

function fixCommonMojibake(value) {
  const text = String(value ?? "");
  if (!text) return "";

  // Heuristic for UTF-8 bytes previously decoded as latin1 (e.g., "negÃ³cio").
  const looksBroken = /(?:Ã.|Â.|â.|ð)/.test(text);
  if (!looksBroken) {
    return text;
  }

  const repaired = Buffer.from(text, "latin1").toString("utf8");
  const hasReplacementChar = repaired.includes("\uFFFD");
  return hasReplacementChar ? text : repaired;
}

app.post("/api/google/email/send", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const to = sanitizeMimeHeaderValue(req.body?.to || "");
    const subject = sanitizeMimeHeaderValue(fixCommonMojibake(req.body?.subject || ""));
    const body = fixCommonMojibake(String(req.body?.body || "").trim());
    const htmlBodyInput = fixCommonMojibake(String(req.body?.htmlBody || "").trim());
    const requestedHtml = Boolean(req.body?.sendAsHtml);

    if (!to) {
      return res.status(400).json({ error: "Field 'to' is required." });
    }

    const accessToken = await ensureGoogleAccessToken();

    const effectiveHtmlBody = htmlBodyInput || body;
    const hasHtmlTags = /<\/?[a-z][\s\S]*>/i.test(effectiveHtmlBody);
    const sendAsHtml = requestedHtml || hasHtmlTags;
    const normalizedHtml = hasHtmlTags
      ? effectiveHtmlBody
      : escapeHtml(effectiveHtmlBody).replace(/\r?\n/g, "<br>");
    const plainBodyFromHtml = stripHtml(normalizedHtml) || "(mensagem vazia)";
    const plainBody = body || plainBodyFromHtml || "(mensagem vazia)";
    const encodedSubject = encodeMimeHeaderUtf8(subject || "(sem assunto)");
    const encodedPlainBody = encodeMimeBodyBase64Utf8(plainBody);
    const encodedHtmlBody = encodeMimeBodyBase64Utf8(normalizedHtml || "<p>(mensagem vazia)</p>");

    const mimeLines = [
      `To: ${to}`,
      `Subject: ${encodedSubject}`,
      "MIME-Version: 1.0"
    ];

    if (sendAsHtml) {
      const boundary = `mixed_${Date.now()}_${Math.random().toString(16).slice(2)}`;
      mimeLines.push(`Content-Type: multipart/alternative; boundary=\"${boundary}\"`);
      mimeLines.push("");
      mimeLines.push(`--${boundary}`);
      mimeLines.push("Content-Type: text/plain; charset=UTF-8");
      mimeLines.push("Content-Transfer-Encoding: base64");
      mimeLines.push("");
      mimeLines.push(encodedPlainBody);
      mimeLines.push("");
      mimeLines.push(`--${boundary}`);
      mimeLines.push("Content-Type: text/html; charset=UTF-8");
      mimeLines.push("Content-Transfer-Encoding: base64");
      mimeLines.push("");
      mimeLines.push(encodedHtmlBody);
      mimeLines.push("");
      mimeLines.push(`--${boundary}--`);
    } else {
      mimeLines.push("Content-Type: text/plain; charset=UTF-8");
      mimeLines.push("Content-Transfer-Encoding: base64");
      mimeLines.push("");
      mimeLines.push(encodedPlainBody);
    }

    const raw = Buffer.from(mimeLines.join("\r\n"), "utf8")
      .toString("base64")
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/g, "");

    const sendRes = await fetch("https://gmail.googleapis.com/gmail/v1/users/me/messages/send", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ raw })
    });

    const sendData = await sendRes.json().catch(() => ({}));
    if (!sendRes.ok) {
      return res.status(sendRes.status).json({ error: "Failed to send email", details: sendData });
    }

    return res.json({ ok: true, data: sendData });
  } catch (error) {
    warnSoftError("google.email.send", error, { route: "/api/google/email/send", requestId });
    return res.status(500).json({
      error: "Google email send failed",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

function readGmailHeader(headers, headerName) {
  const normalizedTarget = String(headerName || "").trim().toLowerCase();
  const list = Array.isArray(headers) ? headers : [];
  const found = list.find((item) => String(item?.name || "").trim().toLowerCase() === normalizedTarget);
  return String(found?.value || "").trim();
}

app.get("/api/google/email/inbox", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const requested = Number(req.query.maxResults || 20);
    const maxResults = Number.isFinite(requested)
      ? Math.max(1, Math.min(50, Math.floor(requested)))
      : 20;

    const accessToken = await ensureGoogleAccessToken();

    const listUrl = new URL("https://gmail.googleapis.com/gmail/v1/users/me/messages");
    listUrl.searchParams.set("maxResults", String(maxResults));
    listUrl.searchParams.set("labelIds", "INBOX");

    const listRes = await fetch(listUrl.toString(), {
      method: "GET",
      headers: { Authorization: `Bearer ${accessToken}` }
    });

    const listData = await listRes.json().catch(() => ({}));
    if (!listRes.ok) {
      return res.status(listRes.status).json({ error: "Failed to fetch inbox list", details: listData });
    }

    const messages = Array.isArray(listData?.messages) ? listData.messages : [];
    if (messages.length === 0) {
      return res.json({ ok: true, data: [] });
    }

    const detailed = await Promise.all(
      messages.map(async (item) => {
        const messageId = String(item?.id || "").trim();
        if (!messageId) return null;

        const detailUrl = new URL(`https://gmail.googleapis.com/gmail/v1/users/me/messages/${messageId}`);
        detailUrl.searchParams.set("format", "metadata");
        detailUrl.searchParams.set("metadataHeaders", "Subject");
        detailUrl.searchParams.set("metadataHeaders", "From");
        detailUrl.searchParams.set("metadataHeaders", "Date");

        const detailRes = await fetch(detailUrl.toString(), {
          method: "GET",
          headers: { Authorization: `Bearer ${accessToken}` }
        });

        const detailData = await detailRes.json().catch(() => ({}));
        if (!detailRes.ok) {
          return {
            id: messageId,
            error: `Failed to load message (${detailRes.status})`
          };
        }

        const headers = detailData?.payload?.headers;
        const internalDateMs = Number(detailData?.internalDate || 0) || 0;

        return {
          id: String(detailData?.id || messageId),
          threadId: String(detailData?.threadId || ""),
          from: readGmailHeader(headers, "From") || "(sem remetente)",
          subject: readGmailHeader(headers, "Subject") || "(sem assunto)",
          date: readGmailHeader(headers, "Date") || "",
          snippet: String(detailData?.snippet || "").trim(),
          unread: Array.isArray(detailData?.labelIds) ? detailData.labelIds.includes("UNREAD") : false,
          internalDate: internalDateMs
        };
      })
    );

    const rows = detailed
      .filter(Boolean)
      .sort((a, b) => (Number(b?.internalDate || 0) - Number(a?.internalDate || 0)));

    return res.json({ ok: true, data: rows });
  } catch (error) {
    warnSoftError("google.email.inbox", error, { route: "/api/google/email/inbox", requestId });
    return res.status(500).json({
      error: "Google inbox fetch failed",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/consumiveis", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  if (!notionConsumiveisEnabled || !notionConsumiveis) {
    return res.status(503).json({
      error: "Consumiveis Notion integration is not configured.",
      details: "Set NOTION_CONSUMIVEIS_API_KEY and NOTION_CONSUMIVEIS_DATABASE_ID or NOTION_CONSUMIVEIS_PAGE_ID"
    });
  }

  try {
    const requestedLimit = Number(req.query.limit || 100);
    const limit = Number.isFinite(requestedLimit)
      ? Math.max(1, Math.min(200, Math.floor(requestedLimit)))
      : 100;

      const pageDerivedDatabaseId = await resolveConsumiveisDatabaseIdFromPage();
      let resolvedDatabaseId = notionConsumiveisDatabaseId || pageDerivedDatabaseId;
      let pageFallbackUsed = !notionConsumiveisDatabaseId && Boolean(pageDerivedDatabaseId);

    if (!resolvedDatabaseId) {
      return res.status(404).json({
        error: "Could not resolve consumiveis database from page.",
        details: "Ensure NOTION_CONSUMIVEIS_DATABASE_ID is valid or page contains a child database and is shared with integration."
      });
    }

    let rows = [];
    let hasMore = false;
    let columns = [];

    try {
      const result = await queryConsumiveisRows(resolvedDatabaseId, limit);
      rows = result.rows;
      hasMore = result.hasMore;
      columns = result.columns;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      const canFallbackToPageDb = Boolean(pageDerivedDatabaseId) && !pageFallbackUsed;

      if (!(canFallbackToPageDb && /could not find database with id/i.test(message))) {
        throw error;
      }

      resolvedDatabaseId = pageDerivedDatabaseId;
      pageFallbackUsed = true;
      const fallbackResult = await queryConsumiveisRows(resolvedDatabaseId, limit);
      rows = fallbackResult.rows;
      hasMore = fallbackResult.hasMore;
      columns = fallbackResult.columns;
    }

    return res.json({
      data: rows,
      meta: {
        fetchedAt: new Date().toISOString(),
        count: rows.length,
        hasMore,
        columns
      }
    });
  } catch (error) {
    warnSoftError("consumiveis.list", error, { route: "/api/consumiveis", requestId });
    return res.status(500).json({
      error: "Failed to fetch consumiveis from Notion.",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/feedback-tracker", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  if (!notionFeedbackEnabled || !notionFeedback) {
    return res.status(503).json({
      error: "Feedback Tracker Notion integration is not configured.",
      details: "Set NOTION_FEEDBACK_API_KEY and NOTION_FEEDBACK_DATABASE_ID or NOTION_FEEDBACK_PAGE_ID"
    });
  }

  try {
    const requestedLimit = Number(req.query.limit || 100);
    const limit = Number.isFinite(requestedLimit)
      ? Math.max(1, Math.min(200, Math.floor(requestedLimit)))
      : 100;

    const pageDerivedDatabaseId = await resolveFeedbackDatabaseIdFromPage();
    let resolvedDatabaseId = notionFeedbackDatabaseId || pageDerivedDatabaseId;
    const initialDatabaseId = resolvedDatabaseId;
    let pageFallbackUsed = !notionFeedbackDatabaseId && Boolean(pageDerivedDatabaseId);

    if (!resolvedDatabaseId) {
      return res.status(404).json({
        error: "Could not resolve feedback tracker database from page.",
        details: "Ensure NOTION_FEEDBACK_DATABASE_ID is valid or page contains a child database and is shared with integration."
      });
    }

    let rows = [];
    let hasMore = false;
    let columns = [];

    try {
      const result = await queryFeedbackRows(resolvedDatabaseId, limit);
      rows = result.rows;
      hasMore = result.hasMore;
      columns = result.columns;
    } catch (error) {
      const canFallbackToPageDb =
        Boolean(pageDerivedDatabaseId) && !pageFallbackUsed && pageDerivedDatabaseId !== resolvedDatabaseId;

      if (canFallbackToPageDb) {
        resolvedDatabaseId = pageDerivedDatabaseId;
        pageFallbackUsed = true;
        const fallbackResult = await queryFeedbackRows(resolvedDatabaseId, limit);
        rows = fallbackResult.rows;
        hasMore = fallbackResult.hasMore;
        columns = fallbackResult.columns;
      } else {
        throw error;
      }
    }

    return res.json({
      data: rows,
      meta: {
        fetchedAt: new Date().toISOString(),
        count: rows.length,
        hasMore,
        columns,
        databaseId: resolvedDatabaseId,
        fallbackFromDatabaseId: pageFallbackUsed ? initialDatabaseId : ""
      }
    });
  } catch (error) {
    warnSoftError("feedback_tracker.list", error, { route: "/api/feedback-tracker", requestId });
    return res.status(500).json({
      error: "Failed to fetch feedback tracker from Notion.",
      details: notionErrorDetails(error)
    });
  }
});

app.post("/api/feedback-tracker", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  if (!notionFeedbackEnabled || !notionFeedback) {
    return res.status(503).json({
      error: "Feedback Tracker Notion integration is not configured.",
      details: "Set NOTION_FEEDBACK_API_KEY and NOTION_FEEDBACK_DATABASE_ID or NOTION_FEEDBACK_PAGE_ID"
    });
  }

  try {
    const input = normalizeFeedbackCreateInput(req.body);
    if (!input.shopName) {
      return res.status(400).json({ error: "Field 'shopName' is required." });
    }
    if (!input.feedbackUrl) {
      return res.status(400).json({ error: "Field 'feedbackUrl' is required." });
    }

    const pageDerivedDatabaseId = await resolveFeedbackDatabaseIdFromPage();
    let resolvedDatabaseId = notionFeedbackDatabaseId || pageDerivedDatabaseId;
    const initialDatabaseId = resolvedDatabaseId;
    let pageFallbackUsed = !notionFeedbackDatabaseId && Boolean(pageDerivedDatabaseId);

    if (!resolvedDatabaseId) {
      return res.status(404).json({
        error: "Could not resolve feedback tracker database from page.",
        details: "Ensure NOTION_FEEDBACK_DATABASE_ID is valid or page contains a child database and is shared with integration."
      });
    }

    let databaseInfo;
    try {
      databaseInfo = await notionFeedback.databases.retrieve({ database_id: resolvedDatabaseId });
    } catch (error) {
      const canFallbackToPageDb =
        Boolean(pageDerivedDatabaseId) && !pageFallbackUsed && pageDerivedDatabaseId !== resolvedDatabaseId;

      if (!canFallbackToPageDb) {
        throw error;
      }

      resolvedDatabaseId = pageDerivedDatabaseId;
      pageFallbackUsed = true;
      databaseInfo = await notionFeedback.databases.retrieve({ database_id: resolvedDatabaseId });
    }

    const databaseProperties = databaseInfo?.properties || {};
    const notionProperties = buildFeedbackCreateProperties(databaseProperties, input);

    if (!Object.keys(notionProperties).length) {
      return res.status(400).json({
        error: "No writable properties matched the feedback tracker database schema.",
        details: "Check property names and property types in Notion database."
      });
    }

    const created = await notionFeedback.pages.create({
      parent: { database_id: resolvedDatabaseId },
      properties: notionProperties
    });

    const columns = Object.keys(databaseProperties);
    return res.status(201).json({
      data: normalizeConsumiveisRow(created, columns),
      meta: {
        createdAt: new Date().toISOString(),
        databaseId: resolvedDatabaseId,
        fallbackFromDatabaseId: pageFallbackUsed ? initialDatabaseId : ""
      }
    });
  } catch (error) {
    warnSoftError("feedback_tracker.create", error, { route: "/api/feedback-tracker", requestId });
    return res.status(500).json({
      error: "Failed to create feedback tracker row in Notion.",
      details: notionErrorDetails(error)
    });
  }
});

app.post("/api/consumiveis", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  if (!notionConsumiveisEnabled || !notionConsumiveis) {
    return res.status(503).json({
      error: "Consumiveis Notion integration is not configured.",
      details: "Set NOTION_CONSUMIVEIS_API_KEY and NOTION_CONSUMIVEIS_DATABASE_ID or NOTION_CONSUMIVEIS_PAGE_ID"
    });
  }

  try {
    const input = normalizeConsumiveisCreateInput(req.body);
    if (!input.clientName) {
      return res.status(400).json({ error: "Field 'clientName' is required." });
    }

    const pageDerivedDatabaseId = await resolveConsumiveisDatabaseIdFromPage();
    let resolvedDatabaseId = notionConsumiveisDatabaseId || pageDerivedDatabaseId;
    let pageFallbackUsed = !notionConsumiveisDatabaseId && Boolean(pageDerivedDatabaseId);

    if (!resolvedDatabaseId) {
      return res.status(404).json({
        error: "Could not resolve consumiveis database from page.",
        details: "Ensure NOTION_CONSUMIVEIS_DATABASE_ID is valid or page contains a child database and is shared with integration."
      });
    }

    let databaseInfo;
    try {
      databaseInfo = await notionConsumiveis.databases.retrieve({ database_id: resolvedDatabaseId });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      const canFallbackToPageDb = Boolean(pageDerivedDatabaseId) && !pageFallbackUsed;

      if (!(canFallbackToPageDb && /could not find database with id/i.test(message))) {
        throw error;
      }

      resolvedDatabaseId = pageDerivedDatabaseId;
      pageFallbackUsed = true;
      databaseInfo = await notionConsumiveis.databases.retrieve({ database_id: resolvedDatabaseId });
    }

    const databaseProperties = databaseInfo?.properties || {};
    const notionProperties = buildConsumiveisCreateProperties(databaseProperties, input);

    if (!Object.keys(notionProperties).length) {
      return res.status(400).json({
        error: "No writable properties matched the consumiveis database schema.",
        details: "Check property names and property types in Notion database."
      });
    }

    const created = await notionConsumiveis.pages.create({
      parent: { database_id: resolvedDatabaseId },
      properties: notionProperties
    });

    const columns = Object.keys(databaseProperties);
    return res.status(201).json({
      data: normalizeConsumiveisRow(created, columns),
      meta: {
        createdAt: new Date().toISOString(),
        databaseId: resolvedDatabaseId
      }
    });
  } catch (error) {
    warnSoftError("consumiveis.create", error, { route: "/api/consumiveis", requestId });
    return res.status(500).json({
      error: "Failed to create consumiveis row in Notion.",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.post("/api/messages/send", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const to = normalizeRecipient(req.body?.to || "");
    const text = String(req.body?.text || "").trim();

    if (!to || !text) {
      return res.status(400).json({ error: "Fields 'to' and 'text' are required." });
    }

    const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
    const phoneNumberId = requiredEnv("WHATSAPP_PHONE_NUMBER_ID");
    const token = requiredEnv("WHATSAPP_ACCESS_TOKEN");

    const endpoint = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}/messages`;
    const payload = {
      messaging_product: "whatsapp",
      recipient_type: "individual",
      to,
      type: "text",
      text: { body: text }
    };

    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    });

    const responseBody = await response.json();
    const messageId = responseBody?.messages?.[0]?.id || "";

    const notionWarning = await safeNotionLog(() =>
      createNotionMessageRow({
        to,
        text,
        status: response.ok ? "accepted" : `failed_${response.status}`,
        messageId,
        rawResponse: responseBody
      })
    );

    const supabaseWarning = await safeSupabaseLog(() =>
      createSupabaseLogRow({
        direction: "out",
        channel: "text",
        to,
        messageText: text,
        status: response.ok ? "accepted" : `failed_${response.status}`,
        apiMessageId: messageId,
        payload: responseBody
      })
    );

    const smsFallback = !response.ok
      ? await maybeSendAutomaticSmsFallback({
          to,
          message: text,
          source: "wa_text_failure",
          waStatus: `failed_${response.status}`,
          waResponse: responseBody
        })
      : null;

    const finalBody =
      notionWarning && typeof responseBody === "object" && responseBody !== null
        ? { ...responseBody, _notionWarning: notionWarning }
        : responseBody;

    if (supabaseWarning && typeof finalBody === "object" && finalBody !== null) {
      finalBody._supabaseWarning = supabaseWarning;
    }

    if (smsFallback && typeof finalBody === "object" && finalBody !== null) {
      finalBody._smsFallback = smsFallback;
    }

    return res.status(response.ok ? 200 : response.status).json(finalBody);
  } catch (error) {
    warnSoftError("messages.send", error, { route: "/api/messages/send", requestId });
    return res.status(500).json({
      error: "Failed to send message",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.post("/api/messages/status", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  if (!notionEnabled) {
    return res.json({ success: true, skipped: "notion_disabled" });
  }

  try {
    const messageId = String(req.body?.messageId || "").trim();
    const status = String(req.body?.status || "").trim();

    if (!messageId || !status) {
      return res.status(400).json({ error: "Fields 'messageId' and 'status' are required." });
    }

    const pageId = await updateNotionMessageStatus({ messageId, status, rawStatus: req.body });

    if (!pageId) {
      return res.status(404).json({ error: "Message row not found in Notion." });
    }

    return res.json({ success: true, pageId });
  } catch (error) {
    warnSoftError("messages.status", error, { route: "/api/messages/status", requestId });
    return res.status(500).json({
      error: "Failed to update status",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.post("/api/media/upload", upload.single("file"), async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    if (!req.file) {
      return res.status(400).json({ error: "Field 'file' is required as multipart/form-data." });
    }

    const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
    const phoneNumberId = requiredEnv("WHATSAPP_PHONE_NUMBER_ID");
    const token = requiredEnv("WHATSAPP_ACCESS_TOKEN");

    const endpoint = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}/media`;
    const multipart = new FormData();
    const blob = new Blob([req.file.buffer], {
      type: req.file.mimetype || "application/octet-stream"
    });

    multipart.append("messaging_product", "whatsapp");
    multipart.append("file", blob, req.file.originalname || "upload.bin");

    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`
      },
      body: multipart
    });

    const responseBody = await response.json();
    const mediaId = responseBody?.id || "";

    const notionWarning =
      process.env.NOTION_DATABASE_ID && process.env.NOTION_API_KEY
        ? await safeNotionLog(() =>
            createNotionMessageRow({
              to: "media-upload",
              text: `${req.file.originalname || "unknown"} (${req.file.mimetype || "unknown"})`,
              status: response.ok ? "media_uploaded" : `media_failed_${response.status}`,
              messageId: mediaId,
              rawResponse: responseBody
            })
          )
        : null;

    const finalBody =
      notionWarning && typeof responseBody === "object" && responseBody !== null
        ? { ...responseBody, _notionWarning: notionWarning }
        : responseBody;

    return res.status(response.ok ? 200 : response.status).json(finalBody);
  } catch (error) {
    warnSoftError("media.upload", error, { route: "/api/media/upload", requestId });
    return res.status(500).json({
      error: "Failed to upload media",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.post("/api/templates/send-return-to-sender", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const to = normalizeRecipient(req.body?.to || "");
    const customerName = String(req.body?.customerName || "").trim();
    const shipmentCode = String(req.body?.shipmentCode || "").trim();
    const pickupDate = String(req.body?.pickupDate || "").trim();

    if (!to || !customerName || !shipmentCode || !pickupDate) {
      return res.status(400).json({
        error: "Fields 'to', 'customerName', 'shipmentCode', and 'pickupDate' are required."
      });
    }

    const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
    const phoneNumberId = requiredEnv("WHATSAPP_PHONE_NUMBER_ID");
    const token = requiredEnv("WHATSAPP_ACCESS_TOKEN");
    const templateLanguage = process.env.WHATSAPP_TEMPLATE_LANGUAGE || "pt_PT";

    const endpoint = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}/messages`;
    const payload = {
      messaging_product: "whatsapp",
      recipient_type: "individual",
      to,
      type: "template",
      template: {
        name: "entrega_de_volta_ao_remetente",
        language: {
          code: templateLanguage
        },
        components: [
          {
            type: "body",
            parameters: [
              { type: "text", text: customerName },
              { type: "text", text: shipmentCode },
              { type: "text", text: pickupDate }
            ]
          }
        ]
      }
    };

    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    });

    const responseBody = await response.json();
    const messageId = responseBody?.messages?.[0]?.id || "";

    const notionWarning = await safeNotionLog(() =>
      createNotionMessageRow({
        to,
        text: `Template entrega_de_volta_ao_remetente | ${customerName} | ${shipmentCode} | ${pickupDate}`,
        status: response.ok ? "accepted" : `failed_${response.status}`,
        messageId,
        rawResponse: responseBody
      })
    );

    const smsFallbackMessage = buildTemplateFallbackText({
      templateName: "entrega_de_volta_ao_remetente",
      bodyVariables: [customerName, shipmentCode, pickupDate]
    });

    const smsFallback = !response.ok
      ? await maybeSendAutomaticSmsFallback({
          to,
          message: smsFallbackMessage,
          source: "wa_template_failure",
          templateName: "entrega_de_volta_ao_remetente",
          waStatus: `failed_${response.status}`,
          waResponse: responseBody
        })
      : null;

    const finalBody =
      notionWarning && typeof responseBody === "object" && responseBody !== null
        ? { ...responseBody, _notionWarning: notionWarning }
        : responseBody;

    if (smsFallback && typeof finalBody === "object" && finalBody !== null) {
      finalBody._smsFallback = smsFallback;
    }

    return res.status(response.ok ? 200 : response.status).json(finalBody);
  } catch (error) {
    warnSoftError("templates.send_return_to_sender", error, { route: "/api/templates/send-return-to-sender", requestId });
    return res.status(500).json({
      error: "Failed to send template message",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

async function sendGenericTemplateMessage({
  to,
  templateName,
  languageCode,
  bodyVariables = [],
  buttonUrlVariable = "",
  trackerContext = null
}) {
  try {
    const normalizedTo = normalizeRecipient(to || "");
    const cleanTemplateName = String(templateName || "").trim();
    const cleanLanguageCode = String(languageCode || process.env.WHATSAPP_TEMPLATE_LANGUAGE || "pt_PT").trim() || "pt_PT";
    const cleanBodyVariables = Array.isArray(bodyVariables)
      ? bodyVariables.map((value) => String(value ?? "").trim()).filter(Boolean)
      : [];
    const cleanButtonUrlVariable = String(buttonUrlVariable || "").trim();
    const cleanTrackerContext = trackerContext && typeof trackerContext === "object"
      ? {
          clientName: String(trackerContext.clientName || "").trim(),
          parcelId: String(trackerContext.parcelId || "").trim(),
          messageType: String(trackerContext.messageType || "").trim(),
          notes: String(trackerContext.notes || "").trim()
        }
      : null;

    if (!normalizedTo || !cleanTemplateName) {
      return {
        ok: false,
        status: 400,
        finalBody: {
          error: "Fields 'to' and 'templateName' are required."
        }
      };
    }

    const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
    const phoneNumberId = requiredEnv("WHATSAPP_PHONE_NUMBER_ID");
    const token = requiredEnv("WHATSAPP_ACCESS_TOKEN");

    const endpoint = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}/messages`;
    const components = [];

    if (cleanBodyVariables.length > 0) {
      components.push({
        type: "body",
        parameters: cleanBodyVariables.map((text) => ({ type: "text", text }))
      });
    }

    if (cleanButtonUrlVariable) {
      components.push({
        type: "button",
        sub_type: "url",
        index: "0",
        parameters: [{ type: "text", text: cleanButtonUrlVariable }]
      });
    }

    const templatePayload = {
      name: cleanTemplateName,
      language: {
        code: cleanLanguageCode
      }
    };

    if (components.length > 0) {
      templatePayload.components = components;
    }

    const payload = {
      messaging_product: "whatsapp",
      recipient_type: "individual",
      to: normalizedTo,
      type: "template",
      template: templatePayload
    };

    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    });

    const responseBody = await response.json();
    const messageId = responseBody?.messages?.[0]?.id || "";

    const notionWarning = await safeNotionLog(() =>
      createNotionMessageRow({
        to: normalizedTo,
        text: `Template ${cleanTemplateName} | Vars: ${cleanBodyVariables.join(" | ")}`,
        status: response.ok ? "accepted" : `failed_${response.status}`,
        messageId,
        rawResponse: responseBody
      })
    );

    const notionTrackerWarning = await safeNotionTrackerLog(() =>
      createNotionTrackerRow({
        to: normalizedTo,
        templateName: cleanTemplateName,
        bodyVariables: cleanBodyVariables,
        status: response.ok ? "accepted" : `failed_${response.status}`,
        trackerContext: cleanTrackerContext,
        rawResponse: responseBody
      })
    );

    const logPayload = responseBody && typeof responseBody === "object"
      ? {
          ...responseBody,
          trackerContext: cleanTrackerContext || null
        }
      : {
          response: responseBody,
          trackerContext: cleanTrackerContext || null
        };

    const supabaseWarning = await safeSupabaseLog(() =>
      createSupabaseLogRow({
        direction: "out",
        channel: "template",
        to: normalizedTo,
        messageText: cleanBodyVariables.join(" | "),
        templateName: cleanTemplateName,
        status: response.ok ? "accepted" : `failed_${response.status}`,
        apiMessageId: messageId,
        payload: logPayload
      })
    );

    const smsFallbackMessage = buildTemplateFallbackText({
      templateName: cleanTemplateName,
      bodyVariables: cleanBodyVariables,
      buttonUrlVariable: cleanButtonUrlVariable
    });

    const smsFallbackDisabledForIncidencia =
      isIncidenciaMessageType(cleanTrackerContext?.messageType) &&
      !isIncidenciaSmsFallbackEnabled();

    const smsFallback = !response.ok && !smsFallbackDisabledForIncidencia
      ? await maybeSendAutomaticSmsFallback({
          to: normalizedTo,
          message: smsFallbackMessage,
          source: "wa_template_failure",
          templateName: cleanTemplateName,
          waStatus: `failed_${response.status}`,
          waResponse: responseBody
        })
      : (!response.ok && smsFallbackDisabledForIncidencia
        ? {
            attempted: false,
            status: "skipped_for_incidencia",
            reason: "sms_fallback_disabled_for_incidencia"
          }
        : null);

    const finalBody =
      notionWarning && typeof responseBody === "object" && responseBody !== null
        ? { ...responseBody, _notionWarning: notionWarning }
        : responseBody;

    if (supabaseWarning && typeof finalBody === "object" && finalBody !== null) {
      finalBody._supabaseWarning = supabaseWarning;
    }

    if (notionTrackerWarning && typeof finalBody === "object" && finalBody !== null) {
      finalBody._notionTrackerWarning = notionTrackerWarning;
    }

    if (smsFallback && typeof finalBody === "object" && finalBody !== null) {
      finalBody._smsFallback = smsFallback;
    }

    return {
      ok: response.ok,
      status: response.ok ? 200 : response.status,
      finalBody
    };
  } catch (error) {
    return {
      ok: false,
      status: 500,
      finalBody: {
        error: "Failed to send generic template message",
        details: error instanceof Error ? error.message : "Unknown error"
      }
    };
  }
}

app.post("/api/templates/send-generic", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  const result = await sendGenericTemplateMessage({
    to: req.body?.to,
    templateName: req.body?.templateName,
    languageCode: req.body?.languageCode,
    bodyVariables: req.body?.bodyVariables,
    buttonUrlVariable: req.body?.buttonUrlVariable,
    trackerContext: req.body?.trackerContext
  });

  if (result.status >= 500) {
    warnSoftError("templates.send_generic", result.finalBody?.details || result.finalBody?.error || "unknown_error", {
      route: "/api/templates/send-generic",
      requestId
    });
  }

  return res.status(result.status).json(result.finalBody);
});

app.post("/api/sms/clicksend", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const to = normalizeRecipient(req.body?.to || "");
    const message = String(req.body?.message || "").trim();

    if (!to || !message) {
      return res.status(400).json({ error: "Fields 'to' and 'message' are required." });
    }

    const result = await sendClickSendSms({
      to,
      message,
      source: "manual"
    });

    if (!result.attempted && result.status === "skipped_not_configured") {
      return res.status(400).json({
        error: "ClickSend is not configured",
        details: "Missing CLICKSEND_USERNAME or CLICKSEND_API_KEY"
      });
    }

    if (result.status === "failed_exception") {
      warnSoftError("sms.clicksend.failed_exception", result.error || "unknown_error", {
        route: "/api/sms/clicksend",
        requestId
      });
      return res.status(500).json({
        error: "Failed to send SMS via ClickSend",
        details: result.error || "Unknown error"
      });
    }

    return res.status(result.status === "sent" ? 200 : result.responseStatus || 500).json({
      ...(result.responseBody && typeof result.responseBody === "object" ? result.responseBody : {}),
      _smsMeta: {
        status: result.status,
        source: result.source,
        to: result.to
      },
      ...(result._supabaseWarning ? { _supabaseWarning: result._supabaseWarning } : {})
    });
  } catch (error) {
    warnSoftError("sms.clicksend", error, { route: "/api/sms/clicksend", requestId });
    return res.status(500).json({
      error: "Failed to send SMS via ClickSend",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.post("/api/templates/send-feedback-request", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const to = normalizeRecipient(req.body?.to || "");
    const customerName = String(req.body?.customerName || "").trim();
    const storeName = String(req.body?.storeName || "").trim();
    const templateName = String(
      req.body?.templateName || process.env.WHATSAPP_FEEDBACK_TEMPLATE_NAME || "feedback_request_template"
    ).trim();
    const templateLanguage = String(
      req.body?.languageCode || process.env.WHATSAPP_TEMPLATE_LANGUAGE || "pt_PT"
    ).trim();

    if (!to || !customerName || !storeName) {
      return res.status(400).json({
        error: "Fields 'to', 'customerName', and 'storeName' are required."
      });
    }

    const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
    const phoneNumberId = requiredEnv("WHATSAPP_PHONE_NUMBER_ID");
    const token = requiredEnv("WHATSAPP_ACCESS_TOKEN");

    const endpoint = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}/messages`;
    const payload = {
      messaging_product: "whatsapp",
      recipient_type: "individual",
      to,
      type: "template",
      template: {
        name: templateName,
        language: {
          code: templateLanguage
        },
        components: [
          {
            type: "body",
            parameters: [
              { type: "text", text: customerName },
              { type: "text", text: storeName }
            ]
          }
        ]
      }
    };

    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    });

    const responseBody = await response.json();
    const messageId = responseBody?.messages?.[0]?.id || "";

    const notionWarning = await safeNotionLog(() =>
      createNotionMessageRow({
        to,
        text: `Template ${templateName} | ${customerName} | ${storeName}`,
        status: response.ok ? "accepted" : `failed_${response.status}`,
        messageId,
        rawResponse: responseBody
      })
    );

    const smsFallbackMessage = buildTemplateFallbackText({
      templateName,
      bodyVariables: [customerName, storeName]
    });

    const smsFallback = !response.ok
      ? await maybeSendAutomaticSmsFallback({
          to,
          message: smsFallbackMessage,
          source: "wa_template_failure",
          templateName,
          waStatus: `failed_${response.status}`,
          waResponse: responseBody
        })
      : null;

    const finalBody =
      notionWarning && typeof responseBody === "object" && responseBody !== null
        ? { ...responseBody, _notionWarning: notionWarning }
        : responseBody;

    if (smsFallback && typeof finalBody === "object" && finalBody !== null) {
      finalBody._smsFallback = smsFallback;
    }

    return res.status(response.ok ? 200 : response.status).json(finalBody);
  } catch (error) {
    warnSoftError("templates.send_feedback_request", error, { route: "/api/templates/send-feedback-request", requestId });
    return res.status(500).json({
      error: "Failed to send feedback request template",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/templates", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
    const token = requiredEnv("WHATSAPP_ACCESS_TOKEN");
    const phoneNumberId = String(req.query.phoneNumberId || process.env.WHATSAPP_PHONE_NUMBER_ID || "").trim();
    const providedWabaId = String(
      req.query.wabaId || process.env.WHATSAPP_BUSINESS_ACCOUNT_ID || ""
    ).trim();
    const limit = String(req.query.limit || "50").trim();
    const fetchAll = String(req.query.fetchAll || "true").toLowerCase() !== "false";

    if (!phoneNumberId && !providedWabaId) {
      return res.status(400).json({
        error:
          "Provide 'phoneNumberId' or 'wabaId' query param (or env WHATSAPP_PHONE_NUMBER_ID / WHATSAPP_BUSINESS_ACCOUNT_ID)."
      });
    }

    let wabaId = providedWabaId;
    let phoneLookupBody = null;

    if (!wabaId && phoneNumberId) {
      const phoneLookupUrl = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}?fields=whatsapp_business_account`;
      const phoneLookupResponse = await fetch(phoneLookupUrl, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${token}`
        }
      });

      phoneLookupBody = await phoneLookupResponse.json();

      if (phoneLookupResponse.ok) {
        wabaId = String(phoneLookupBody?.whatsapp_business_account?.id || "").trim();
      }
    }

    if (!wabaId) {
      return res.status(400).json({
        error:
          "Could not resolve WABA ID from phoneNumberId. Provide 'wabaId' query param or set WHATSAPP_BUSINESS_ACCOUNT_ID.",
        phoneNumberId,
        phoneLookup: phoneLookupBody
      });
    }

    const baseTemplatesUrl = `https://graph.facebook.com/${apiVersion}/${wabaId}/message_templates?limit=${encodeURIComponent(limit)}&fields=id,name,language,status,category,quality_score,components`;

    if (!fetchAll) {
      const templatesResponse = await fetch(baseTemplatesUrl, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${token}`
        }
      });

      const templatesBody = await templatesResponse.json();

      if (!templatesResponse.ok) {
        return res.status(templatesResponse.status).json(templatesBody);
      }

      return res.json({
        phoneNumberId,
        wabaId,
        fetchedAllPages: false,
        ...templatesBody
      });
    }

    const templates = [];
    let nextUrl = baseTemplatesUrl;
    let pagesFetched = 0;
    const maxPages = 50;

    while (nextUrl && pagesFetched < maxPages) {
      const pageResponse = await fetch(nextUrl, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${token}`
        }
      });

      const pageBody = await pageResponse.json();

      if (!pageResponse.ok) {
        return res.status(pageResponse.status).json(pageBody);
      }

      const pageData = Array.isArray(pageBody?.data) ? pageBody.data : [];
      templates.push(...pageData);
      nextUrl = pageBody?.paging?.next || "";
      pagesFetched += 1;
    }

    return res.json({
      phoneNumberId,
      wabaId,
      fetchedAllPages: !nextUrl,
      pagesFetched,
      data: templates,
      paging: nextUrl ? { next: nextUrl } : undefined
    });
  } catch (error) {
    warnSoftError("templates.list", error, { route: "/api/templates", requestId });
    return res.status(500).json({
      error: "Failed to fetch templates",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

function handleWebhookVerify(req, res) {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === process.env.WHATSAPP_WEBHOOK_VERIFY_TOKEN) {
    return res.status(200).send(challenge);
  }

  return res.sendStatus(403);
}

app.get("/webhook", handleWebhookVerify);
app.get("/api/webhook", handleWebhookVerify);

async function handleWebhookEvent(req, res) {
  // Acknowledge immediately so Meta doesn't retry
  res.sendStatus(200);
  void forwardWebhookToBotpress(req.body);

  try {
    const entry = req.body?.entry || [];
    const changes = entry.flatMap((item) => item?.changes || []);

    for (const change of changes) {
      const value = change?.value || {};
      if (String(change?.field || "") === "calls") {
        recentCallEvents.unshift({
          at: new Date().toISOString(),
          field: "calls",
          value
        });
        if (recentCallEvents.length > MAX_CALL_EVENTS) {
          recentCallEvents.length = MAX_CALL_EVENTS;
        }

        broadcastSSE("call_event", {
          at: new Date().toISOString(),
          value
        });
      }
      const statuses = value?.statuses || [];
      const messages = value?.messages || [];
      const contacts = value?.contacts || [];

      for (const statusEvent of statuses) {
        const messageId = statusEvent?.id;
        const status = statusEvent?.status;

        if (messageId && status) {
          // Always broadcast delivery ticks to SSE clients
          broadcastSSE("status", { messageId: String(messageId), status: String(status) });

          await safeSupabaseLog(() =>
            updateSupabaseLogStatusByMessageId({
              messageId: String(messageId),
              status: String(status),
              payload: { statusEvent }
            })
          );

          if (notionEnabled) {
            await updateNotionMessageStatus({
              messageId: String(messageId),
              status: String(status),
              rawStatus: statusEvent
            });
          }
        }
      }

      for (const inboundMessage of messages) {
        const from = String(inboundMessage?.from || "").trim();
        if (!from) continue;
        const contact = contacts.find((item) => String(item?.wa_id || "") === from) || null;
        const inboundMessageId = String(inboundMessage?.id || "").trim();
        const summaryText = inboundMessageText(inboundMessage);
        const mediaInfo = inboundMessageMediaInfo(inboundMessage);

        await logInboundMessage({ from, message: inboundMessage, contact });

        broadcastSSE("inbound", {
          messageId: inboundMessageId,
          from,
          contactName: String(contact?.profile?.name || "").trim(),
          text: summaryText,
          mediaType: mediaInfo.mediaType || undefined,
          mediaId: mediaInfo.mediaId || undefined,
          status: "received"
        });
      }
    }
  } catch (_error) {
    // response already sent
  }
}

app.post("/webhook", handleWebhookEvent);
app.post("/api/webhook", handleWebhookEvent);

app.get("/api/calls/events", (_req, res) => {
  return res.json({ data: recentCallEvents });
});

app.post("/api/botpress/events", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    if (!isBotpressRequestAuthorized(req)) {
      return res.status(401).json({ error: "Unauthorized Botpress event" });
    }

    const to = normalizeRecipient(readBotpressRecipient(req.body));
    const text = readBotpressTextPayload(req.body);
    const messageId = String(req.body?.messageId || req.body?.id || "").trim();

    if (!to || !text) {
      return res.status(400).json({ error: "Fields 'to' and 'text' are required in Botpress event payload" });
    }

    await safeSupabaseLog(() =>
      createSupabaseLogRow({
        direction: "out",
        channel: "chat",
        to,
        messageText: text,
        status: "sent",
        apiMessageId: messageId || null,
        payload: {
          source: "botpress",
          event: req.body
        }
      })
    );

    broadcastSSE("bot_outbound", {
      to,
      text,
      messageId: messageId || undefined,
      status: "sent"
    });

    return res.json({ ok: true });
  } catch (error) {
    warnSoftError("botpress.events", error, { route: "/api/botpress/events", requestId });
    return res.status(500).json({
      error: "Failed to ingest Botpress event",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

// ── Scheduling endpoints ──────────────────────────────────────────────────
function isCronAuthorized(req) {
  const secret = String(process.env.CRON_SECRET || "").trim();
  if (!secret) {
    // In production/serverless, fail closed if CRON_SECRET is missing.
    // For local development, allow running cron endpoints without a secret.
    return !process.env.VERCEL;
  }

  const authHeader = String(req.headers.authorization || "").trim();
  if (!authHeader.toLowerCase().startsWith("bearer ")) return false;
  const token = authHeader.slice("Bearer ".length).trim();
  return token === secret;
}

function isAdminPerfAuthorized(req) {
  const secret = String(process.env.ADMIN_PERF_SECRET || process.env.CRON_SECRET || "").trim();
  if (!secret) {
    return !process.env.VERCEL;
  }

  const authHeader = String(req.headers.authorization || "").trim();
  if (authHeader.toLowerCase().startsWith("bearer ")) {
    const token = authHeader.slice("Bearer ".length).trim();
    return token === secret;
  }

  const headerSecret = String(req.headers["x-admin-perf-secret"] || "").trim();
  return headerSecret === secret;
}

app.get("/api/admin/perf", (req, res) => {
  if (!isAdminPerfAuthorized(req)) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const requestedLimit = Number(req.query?.limit || 20);
  const limit = Number.isFinite(requestedLimit)
    ? Math.max(1, Math.min(200, Math.trunc(requestedLimit)))
    : 20;

  const source = String(req.query?.source || "memory").trim().toLowerCase();
  if (source === "db") {
    return (async () => {
      if (!isRequestPerfDbEnabled()) {
        return res.status(400).json({ error: "DB perf source is not enabled" });
      }

      await ensureRequestPerfEventsTable();
      const totalsResult = await pgPool.query(
        `select
          count(*)::int as requests_tracked,
          count(*) filter (where status_code >= 500)::int as errors_tracked,
          count(distinct (method || ' ' || route))::int as routes_tracked
        from public.request_perf_events`
      );

      const rowsResult = await pgPool.query(
        `select
          method,
          route,
          count(*)::int as count,
          round(avg(duration_ms)::numeric, 2) as avg_duration_ms,
          max(duration_ms)::int as max_duration_ms,
          count(*) filter (where status_code between 200 and 299)::int as status2xx,
          count(*) filter (where status_code between 400 and 499)::int as status4xx,
          count(*) filter (where status_code >= 500)::int as status5xx,
          count(*) filter (where status_code >= 500)::int as error_count,
          max(created_at) as last_seen_at
        from public.request_perf_events
        group by method, route`
      );

      const recentResult = await pgPool.query(
        `select request_id, method, route, status_code, duration_ms, backend_tags, created_at
        from public.request_perf_events
        order by created_at desc
        limit $1`,
        [limit]
      );

      const rows = (rowsResult.rows || []).map((item) => ({
        key: `${item.method} ${item.route}`,
        method: item.method,
        route: item.route,
        count: Number(item.count || 0),
        avgDurationMs: Number(item.avg_duration_ms || 0),
        maxDurationMs: Number(item.max_duration_ms || 0),
        status2xx: Number(item.status2xx || 0),
        status4xx: Number(item.status4xx || 0),
        status5xx: Number(item.status5xx || 0),
        errorCount: Number(item.error_count || 0),
        failureRate: Number(item.count || 0) > 0
          ? Number((Number(item.error_count || 0) / Number(item.count || 0)).toFixed(4))
          : 0,
        lastSeenAt: item.last_seen_at
      }));

      const topSlowByAvg = [...rows]
        .sort((a, b) => b.avgDurationMs - a.avgDurationMs)
        .slice(0, limit);
      const topSlowByMax = [...rows]
        .sort((a, b) => b.maxDurationMs - a.maxDurationMs)
        .slice(0, limit);
      const topFailing = [...rows]
        .sort((a, b) => b.errorCount - a.errorCount || b.failureRate - a.failureRate)
        .slice(0, limit);

      return res.json({
        ok: true,
        source: "db",
        data: {
          totals: {
            routesTracked: Number(totalsResult.rows?.[0]?.routes_tracked || 0),
            traceSamples: Number(recentResult.rows?.length || 0),
            requestsTracked: Number(totalsResult.rows?.[0]?.requests_tracked || 0),
            errorsTracked: Number(totalsResult.rows?.[0]?.errors_tracked || 0)
          },
          topSlowByAvg,
          topSlowByMax,
          topFailing,
          recentTraces: (recentResult.rows || []).map((item) => ({
            requestId: item.request_id,
            method: item.method,
            route: item.route,
            statusCode: Number(item.status_code || 0),
            durationMs: Number(item.duration_ms || 0),
            backendTags: Array.isArray(item.backend_tags) ? item.backend_tags : [],
            at: item.created_at
          }))
        },
        meta: {
          generatedAt: new Date().toISOString(),
          limit
        }
      });
    })().catch((error) => {
      return res.status(500).json({
        error: "Failed to load admin performance dashboard",
        details: error instanceof Error ? error.message : "Unknown error"
      });
    });
  }

  const rows = Array.from(requestPerfStats.values()).map((item) => {
    const avgDurationMs = item.count > 0 ? item.totalDurationMs / item.count : 0;
    return {
      ...item,
      avgDurationMs: Number(avgDurationMs.toFixed(2)),
      failureRate: item.count > 0 ? Number((item.errorCount / item.count).toFixed(4)) : 0
    };
  });

  const topSlowByAvg = [...rows]
    .sort((a, b) => b.avgDurationMs - a.avgDurationMs)
    .slice(0, limit);
  const topSlowByMax = [...rows]
    .sort((a, b) => b.maxDurationMs - a.maxDurationMs)
    .slice(0, limit);
  const topFailing = [...rows]
    .sort((a, b) => b.errorCount - a.errorCount || b.failureRate - a.failureRate)
    .slice(0, limit);

  return res.json({
    ok: true,
    source: "memory",
    data: {
      totals: {
        routesTracked: rows.length,
        traceSamples: requestTraceSamples.length,
        requestsTracked: rows.reduce((sum, row) => sum + row.count, 0),
        errorsTracked: rows.reduce((sum, row) => sum + row.errorCount, 0)
      },
      topSlowByAvg,
      topSlowByMax,
      topFailing,
      recentTraces: requestTraceSamples.slice(Math.max(0, requestTraceSamples.length - limit))
    },
    meta: {
      generatedAt: new Date().toISOString(),
      limit
    }
  });
});

function isOutboundJobQueueEnabled() {
  const enabled = ["1", "true", "yes", "on"].includes(
    String(process.env.OUTBOUND_JOB_QUEUE_ENABLED || "").trim().toLowerCase()
  );
  return enabled && pgEnabled && !!pgPool;
}

async function ensureOutboundMessageJobsTable() {
  if (!pgEnabled || !pgPool) return;
  await pgPool.query(`
    create table if not exists public.outbound_message_jobs (
      id bigserial primary key,
      to_number text not null,
      template_name text not null,
      language_code text not null default 'pt_PT',
      body_variables jsonb not null default '[]'::jsonb,
      scheduled_at timestamptz not null,
      status text not null default 'pending',
      attempts integer not null default 0,
      max_attempts integer not null default 3,
      worker_id text,
      locked_at timestamptz,
      last_error text,
      last_response jsonb,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
  `);
  await pgPool.query(`
    create index if not exists idx_outbound_jobs_pending_schedule
    on public.outbound_message_jobs (status, scheduled_at);
  `);
}

async function enqueueOutboundMessageJob({ to, templateName, languageCode, bodyVariables, scheduledAt, maxAttempts = 3 }) {
  await ensureOutboundMessageJobsTable();
  const cleanBodyVariables = Array.isArray(bodyVariables)
    ? bodyVariables.map((v) => String(v ?? "").trim()).filter(Boolean)
    : [];
  const safeMaxAttempts = Number.isFinite(Number(maxAttempts))
    ? Math.max(1, Math.min(10, Math.trunc(Number(maxAttempts))))
    : 3;

  const { rows } = await pgPool.query(
    `insert into public.outbound_message_jobs
    (to_number, template_name, language_code, body_variables, scheduled_at, max_attempts, status, created_at, updated_at)
    values ($1, $2, $3, $4::jsonb, $5::timestamptz, $6, 'pending', now(), now())
    returning id, to_number, template_name, language_code, body_variables, scheduled_at, status, attempts, max_attempts, created_at, updated_at`,
    [to, templateName, languageCode || "pt_PT", JSON.stringify(cleanBodyVariables), scheduledAt, safeMaxAttempts]
  );

  return rows[0] || null;
}

async function claimOutboundMessageJobs(limit, workerId) {
  await ensureOutboundMessageJobsTable();
  const safeLimit = Number.isFinite(Number(limit))
    ? Math.max(1, Math.min(200, Math.trunc(Number(limit))))
    : 20;

  const { rows } = await pgPool.query(
    `with claimed as (
      select id
      from public.outbound_message_jobs
      where status = 'pending'
        and scheduled_at <= now()
        and attempts < max_attempts
      order by scheduled_at asc, id asc
      limit $1
      for update skip locked
    )
    update public.outbound_message_jobs as j
    set status = 'processing',
        attempts = j.attempts + 1,
        worker_id = $2,
        locked_at = now(),
        updated_at = now()
    from claimed
    where j.id = claimed.id
    returning j.id, j.to_number, j.template_name, j.language_code, j.body_variables, j.scheduled_at, j.status, j.attempts, j.max_attempts`,
    [safeLimit, workerId]
  );

  return rows || [];
}

async function updateOutboundMessageJobResult(job, result) {
  const isSuccess = Boolean(result?.ok);
  const attempts = Number(job?.attempts || 0);
  const maxAttempts = Number(job?.max_attempts || 3);
  const hasRetriesLeft = attempts < maxAttempts;
  const nextStatus = isSuccess ? "sent" : (hasRetriesLeft ? "pending" : "failed");
  const lastError = isSuccess
    ? null
    : String(result?.finalBody?.details || result?.finalBody?.error || "unknown_error").slice(0, 500);

  await pgPool.query(
    `update public.outbound_message_jobs
    set status = $2,
        last_error = $3,
        last_response = $4::jsonb,
        locked_at = null,
        worker_id = null,
        updated_at = now()
    where id = $1`,
    [
      job.id,
      nextStatus,
      lastError,
      JSON.stringify(result?.finalBody && typeof result.finalBody === "object" ? result.finalBody : { value: result?.finalBody ?? null })
    ]
  );

  return { nextStatus, hasRetriesLeft };
}

async function getOutboundMessageJobsPendingCount() {
  await ensureOutboundMessageJobsTable();
  const { rows } = await pgPool.query(
    `select count(*)::int as count
    from public.outbound_message_jobs
    where status = 'pending'`
  );
  return Number(rows?.[0]?.count || 0);
}

async function listOutboundMessageJobs(limit = 200) {
  await ensureOutboundMessageJobsTable();
  const safeLimit = Number.isFinite(Number(limit))
    ? Math.max(1, Math.min(500, Math.trunc(Number(limit))))
    : 200;
  const { rows } = await pgPool.query(
    `select id, to_number, template_name, language_code, body_variables, scheduled_at, status, attempts, max_attempts, last_error, created_at, updated_at
    from public.outbound_message_jobs
    order by scheduled_at asc, id asc
    limit $1`,
    [safeLimit]
  );
  return rows || [];
}

async function processOutboundMessageQueue() {
  const claimLimitRaw = Number(process.env.OUTBOUND_JOB_QUEUE_CLAIM_LIMIT || 20);
  const claimLimit = Number.isFinite(claimLimitRaw)
    ? Math.max(1, Math.min(200, Math.trunc(claimLimitRaw)))
    : 20;
  const concurrencyRaw = Number(process.env.SCHEDULED_MESSAGES_CONCURRENCY || 5);
  const concurrency = Number.isFinite(concurrencyRaw)
    ? Math.max(1, Math.min(20, Math.trunc(concurrencyRaw)))
    : 5;
  const workerId = `worker-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const claimed = await claimOutboundMessageJobs(claimLimit, workerId);

  let processed = 0;
  let sent = 0;
  let failed = 0;
  let requeued = 0;

  for (let i = 0; i < claimed.length; i += concurrency) {
    const chunk = claimed.slice(i, i + concurrency);
    await Promise.allSettled(
      chunk.map(async (job) => {
        processed += 1;
        const result = await sendGenericTemplateMessage({
          to: job.to_number,
          templateName: job.template_name,
          languageCode: job.language_code || "pt_PT",
          bodyVariables: Array.isArray(job.body_variables) ? job.body_variables : []
        });

        const update = await updateOutboundMessageJobResult(job, result);
        if (result.ok) {
          sent += 1;
        } else {
          failed += 1;
          if (update.nextStatus === "pending") {
            requeued += 1;
          }
        }
      })
    );
  }

  return { processed, sent, failed, requeued };
}

app.post("/api/messages/schedule", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const to = normalizeRecipient(req.body?.to || "");
    const templateName = String(req.body?.templateName || "").trim();
    const languageCode = String(req.body?.languageCode || process.env.WHATSAPP_TEMPLATE_LANGUAGE || "pt_PT").trim();
    const bodyVariables = Array.isArray(req.body?.bodyVariables)
      ? req.body.bodyVariables.map((v) => String(v ?? "").trim()).filter(Boolean)
      : [];
    const scheduledAt = String(req.body?.scheduledAt || "").trim();

    if (!to || !templateName || !scheduledAt) {
      return res.status(400).json({ error: "Fields 'to', 'templateName', and 'scheduledAt' are required." });
    }
    const when = new Date(scheduledAt);
    if (isNaN(when.getTime())) {
      return res.status(400).json({ error: "Invalid scheduledAt value." });
    }

    if (isOutboundJobQueueEnabled()) {
      const maxAttemptsRaw = Number(req.body?.maxAttempts || process.env.OUTBOUND_JOB_QUEUE_MAX_ATTEMPTS || 3);
      const job = await enqueueOutboundMessageJob({
        to,
        templateName,
        languageCode,
        bodyVariables,
        scheduledAt,
        maxAttempts: maxAttemptsRaw
      });

      return res.json({
        id: `job-${job?.id}`,
        queueId: job?.id,
        to: job?.to_number,
        templateName: job?.template_name,
        languageCode: job?.language_code,
        bodyVariables: Array.isArray(job?.body_variables) ? job.body_variables : [],
        scheduledAt: job?.scheduled_at,
        status: job?.status,
        attempts: job?.attempts,
        maxAttempts: job?.max_attempts,
        createdAt: job?.created_at,
        queue: "postgres"
      });
    }

    const id = `sched-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    const item = { id, to, templateName, languageCode, bodyVariables, scheduledAt, status: "pending", createdAt: new Date().toISOString() };
    scheduledMessages.push(item);
    return res.json(item);
  } catch (error) {
    warnSoftError("messages.schedule", error, { route: "/api/messages/schedule", requestId });
    return res.status(500).json({ error: "Failed to schedule message", details: error instanceof Error ? error.message : "Unknown error" });
  }
});

app.get("/api/messages/scheduled", async (_req, res) => {
  try {
    if (isOutboundJobQueueEnabled()) {
      const data = await listOutboundMessageJobs(200);
      return res.json({ data, queue: "postgres" });
    }

    return res.json({ data: scheduledMessages, queue: "memory" });
  } catch (error) {
    return res.status(500).json({
      error: "Failed to list scheduled messages",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.post("/api/messages/process-scheduled", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "Unauthorized cron trigger" });
  }

  try {
    if (isOutboundJobQueueEnabled()) {
      const stats = await processOutboundMessageQueue();
      const pending = await getOutboundMessageJobsPendingCount();
      return res.json({
        ok: true,
        queue: "postgres",
        ...stats,
        pending,
        at: new Date().toISOString()
      });
    }

    const stats = await processScheduledMessages();
    return res.json({
      ok: true,
      queue: "memory",
      ...stats,
      pending: scheduledMessages.filter((item) => item.status === "pending").length,
      at: new Date().toISOString()
    });
  } catch (error) {
    warnSoftError("cron.process_scheduled", error, { route: "/api/messages/process-scheduled", requestId });
    return res.status(500).json({
      error: "Failed to process scheduled messages",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/cron/auto-notificacao-envio", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "Unauthorized cron trigger" });
  }

  const forceRaw = String(req.query?.force || "").trim().toLowerCase();
  const forceRun = ["1", "true", "yes", "on"].includes(forceRaw);

  const enabledRaw = String(process.env.AUTO_NOTIFICACAO_ENVIO_ENABLED || "true").trim().toLowerCase();
  const enabled = !["0", "false", "no", "off"].includes(enabledRaw);
  if (!enabled) {
    return res.json({ ok: true, skipped: true, reason: "AUTO_NOTIFICACAO_ENVIO_ENABLED=false" });
  }

  const parts = getLisbonClockParts();
  if (!forceRun && !shouldRunAutoNotificacaoEnvioAtClock(parts)) {
    return res.json({
      ok: true,
      skipped: true,
      reason: "outside_schedule_window",
      lisbonClock: {
        dateKey: parts.dateKey,
        weekday: parts.weekday,
        hour: parts.hour,
        minute: parts.minute
      }
    });
  }

  await hydrateAutoNotificacaoEnvioState();

  if (!forceRun && autoNotificacaoEnvioLastRunDateKey === parts.dateKey) {
    return res.json({ ok: true, skipped: true, reason: "already_ran_today", dateKey: parts.dateKey });
  }

  if (autoNotificacaoEnvioRunning) {
    return res.json({ ok: true, skipped: true, reason: "already_running" });
  }

  try {
    autoNotificacaoEnvioRunning = true;
    const summary = await runAutoNotificacaoEnvioForInDistribution();
    autoNotificacaoEnvioLastRunDateKey = parts.dateKey;
    await persistAutoNotificacaoEnvioState();
    return res.json({ ok: true, triggeredBy: forceRun ? "manual_force" : "cron", ...summary, at: new Date().toISOString() });
  } catch (error) {
    warnSoftError("cron.auto_notificacao_envio", error, { route: "/api/cron/auto-notificacao-envio", requestId });
    return res.status(500).json({
      error: "Failed to run auto notificacao envio cron",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  } finally {
    autoNotificacaoEnvioRunning = false;
  }
});

app.get("/api/cron/auto-notificacao-envio-em-transporte", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "Unauthorized cron trigger" });
  }

  const forceRaw = String(req.query?.force || "").trim().toLowerCase();
  const forceRun = ["1", "true", "yes", "on"].includes(forceRaw);

  const enabledRaw = String(process.env.AUTO_NOTIFICACAO_ENVIO_TRANSPORTE_ENABLED || "true").trim().toLowerCase();
  const enabled = !["0", "false", "no", "off"].includes(enabledRaw);
  if (!enabled) {
    return res.json({ ok: true, skipped: true, reason: "AUTO_NOTIFICACAO_ENVIO_TRANSPORTE_ENABLED=false" });
  }

  const parts = getLisbonClockParts();
  if (!forceRun && !shouldRunAutoNotificacaoEnvioTransporteAtClock(parts)) {
    return res.json({
      ok: true,
      skipped: true,
      reason: "outside_schedule_window",
      lisbonClock: {
        dateKey: parts.dateKey,
        weekday: parts.weekday,
        hour: parts.hour,
        minute: parts.minute
      }
    });
  }

  await hydrateAutoNotificacaoEnvioState();

  if (!forceRun && autoNotificacaoEnvioTransporteLastRunDateKey === parts.dateKey) {
    return res.json({ ok: true, skipped: true, reason: "already_ran_today", dateKey: parts.dateKey });
  }

  if (autoNotificacaoEnvioTransporteRunning) {
    return res.json({ ok: true, skipped: true, reason: "already_running" });
  }

  try {
    autoNotificacaoEnvioTransporteRunning = true;
    const summary = await runAutoNotificacaoEnvioForInTransport();
    autoNotificacaoEnvioTransporteLastRunDateKey = parts.dateKey;
    await persistAutoNotificacaoEnvioState();
    return res.json({ ok: true, triggeredBy: forceRun ? "manual_force" : "cron", ...summary, at: new Date().toISOString() });
  } catch (error) {
    warnSoftError("cron.auto_notificacao_envio_transporte", error, { route: "/api/cron/auto-notificacao-envio-em-transporte", requestId });
    return res.status(500).json({
      error: "Failed to run auto notificacao envio em transporte cron",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  } finally {
    autoNotificacaoEnvioTransporteRunning = false;
  }
});

app.get("/api/cron/auto-notificacao-incidencia", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "Unauthorized cron trigger" });
  }

  const forceRaw = String(req.query?.force || "").trim().toLowerCase();
  const forceRun = ["1", "true", "yes", "on"].includes(forceRaw);

  const result = await maybeRunAutoNotificacaoIncidenciaSchedule({ forceRun });
  if (!result || result.ok === false) {
    warnSoftError("cron.auto_notificacao_incidencia", result?.details || result?.error || "unknown_cron_failure", {
      route: "/api/cron/auto-notificacao-incidencia",
      requestId,
      forceRun
    });
    return res.status(500).json({
      error: result?.error || "Failed to run auto notificacao incidencia cron",
      details: result?.details || "Unknown error"
    });
  }

  return res.json({
    triggeredBy: forceRun ? "manual_force" : "cron",
    ...result,
    at: new Date().toISOString()
  });
});

app.get("/api/cron/auto-notificacao-envio/dry-run", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "Unauthorized cron trigger" });
  }

  try {
    const previewHourRaw = req.query?.hour;
    const previewMinuteRaw = req.query?.minute;
    const previewHour = Number.isFinite(Number(previewHourRaw)) ? Number(previewHourRaw) : Number(process.env.AUTO_NOTIFICACAO_ENVIO_HOUR || 9);
    const previewMinute = Number.isFinite(Number(previewMinuteRaw)) ? Number(previewMinuteRaw) : Number(process.env.AUTO_NOTIFICACAO_ENVIO_MINUTE || 0);

    const now = getLisbonClockParts();
    const wouldRunAtPreviewTime = shouldRunAutoNotificacaoEnvioAtClock({
      ...now,
      hour: previewHour,
      minute: previewMinute
    });

    const summary = await buildAutoNotificacaoEnvioDryRunSummary({
      limit: req.query?.limit,
      maxPages: req.query?.maxPages,
      sampleSize: req.query?.sampleSize
    });

    return res.json({
      ok: true,
      dryRun: true,
      message: "No messages were sent.",
      previewClock: {
        timezone: "Europe/Lisbon",
        weekday: now.weekday,
        hour: previewHour,
        minute: previewMinute,
        wouldRunAtPreviewTime
      },
      ...summary,
      at: new Date().toISOString()
    });
  } catch (error) {
    warnSoftError("cron.auto_notificacao_envio_dry_run", error, { route: "/api/cron/auto-notificacao-envio/dry-run", requestId });
    return res.status(500).json({
      error: "Failed to run auto notificacao envio dry-run",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

// ── Send media message (upload + send in one call) ─────────────────────────
app.post("/api/messages/send-media", upload.single("file"), async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const to = normalizeRecipient(req.body?.to || "");
    if (!req.file || !to) {
      return res.status(400).json({ error: "Fields 'file' (multipart) and 'to' are required." });
    }

    const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
    const phoneNumberId = requiredEnv("WHATSAPP_PHONE_NUMBER_ID");
    const token = requiredEnv("WHATSAPP_ACCESS_TOKEN");

    // Step 1 – upload media
    const uploadMultipart = new FormData();
    const blob = new Blob([req.file.buffer], { type: req.file.mimetype || "application/octet-stream" });
    uploadMultipart.append("messaging_product", "whatsapp");
    uploadMultipart.append("file", blob, req.file.originalname || "upload.bin");

    const uploadRes = await fetch(`https://graph.facebook.com/${apiVersion}/${phoneNumberId}/media`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
      body: uploadMultipart
    });
    if (!uploadRes.ok) {
      const err = await uploadRes.json();
      return res.status(uploadRes.status).json({ error: "Media upload failed", details: err });
    }
    const uploadBody = await uploadRes.json();
    const mediaId = String(uploadBody?.id || "").trim();
    if (!mediaId) {
      warnSoftError("messages.send_media.upload_no_id", "media_upload_returned_no_id", {
        route: "/api/messages/send-media",
        requestId
      });
      return res.status(500).json({ error: "Media upload returned no ID" });
    }

    // Step 2 – determine media type
    const mime = req.file.mimetype || "";
    let mediaType = "document";
    if (mime.startsWith("image/")) mediaType = "image";
    else if (mime.startsWith("video/")) mediaType = "video";
    else if (mime.startsWith("audio/")) mediaType = "audio";

    // Step 3 – send message
    const sendPayload = {
      messaging_product: "whatsapp",
      recipient_type: "individual",
      to,
      type: mediaType,
      [mediaType]: { id: mediaId, filename: mediaType === "document" ? (req.file.originalname || "file") : undefined }
    };
    const sendRes = await fetch(`https://graph.facebook.com/${apiVersion}/${phoneNumberId}/messages`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify(sendPayload)
    });
    const sendBody = await sendRes.json();
    const messageId = sendBody?.messages?.[0]?.id || "";
    const supabaseWarning = await safeSupabaseLog(() =>
      createSupabaseLogRow({
        direction: "out",
        channel: "media",
        to,
        messageText: req.file?.originalname || "[media]",
        status: sendRes.ok ? "accepted" : `failed_${sendRes.status}`,
        apiMessageId: messageId,
        payload: sendBody
      })
    );
    if (supabaseWarning && typeof sendBody === "object" && sendBody !== null) {
      sendBody._supabaseWarning = supabaseWarning;
    }
    return res.status(sendRes.ok ? 200 : sendRes.status).json(sendBody);
  } catch (error) {
    warnSoftError("messages.send_media", error, { route: "/api/messages/send-media", requestId });
    return res.status(500).json({ error: "Failed to send media", details: error instanceof Error ? error.message : "Unknown error" });
  }
});

app.get("/api/media/:mediaId", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const mediaId = String(req.params?.mediaId || "").trim();
    if (!mediaId) {
      return res.status(400).json({ error: "Missing media id" });
    }
    const mediaBinary = await fetchWhatsappMediaBinary(mediaId);

    res.setHeader("Content-Type", mediaBinary.mimeType);
    res.setHeader("Cache-Control", "private, max-age=300");
    return res.send(mediaBinary.buffer);
  } catch (error) {
    warnSoftError("media.proxy", error, { route: "/api/media/:mediaId", requestId });
    return res.status(500).json({
      error: "Failed to proxy media",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.post("/api/media/access-url", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    if (!blobReadWriteToken) {
      return res.status(400).json({ error: "BLOB_READ_WRITE_TOKEN is not configured" });
    }
    if (!mediaBlobUrlSignSecret) {
      return res.status(400).json({ error: "MEDIA_BLOB_URL_SIGN_SECRET (or CRON_SECRET) is not configured" });
    }

    const messageId = String(req.body?.messageId || "").trim();
    const explicitBlobPath = String(req.body?.blobPath || "").trim();
    const explicitBlobUrl = String(req.body?.blobUrl || "").trim();
    const explicitAccessRaw = String(req.body?.access || "").trim().toLowerCase();
    const explicitAccess = explicitAccessRaw === "public" ? "public" : "private";

    let blobPath = explicitBlobPath;
    let blobUrl = explicitBlobUrl;
    let access = explicitAccess;

    if (!blobPath && blobUrl) {
      blobPath = parseBlobPathFromUrl(blobUrl);
    }

    if (!blobPath && messageId) {
      const payload = await findSupabaseLogPayloadByApiMessageId(messageId);
      const fromPayload = readBlobReferenceFromPayload(payload || {});
      blobPath = fromPayload.blobPath || parseBlobPathFromUrl(fromPayload.blobUrl);
      blobUrl = blobUrl || fromPayload.blobUrl;
      if (fromPayload.blobAccess === "public" || fromPayload.blobAccess === "private") {
        access = fromPayload.blobAccess;
      }
    }

    if (!blobPath) {
      return res.status(400).json({ error: "Missing blobPath/blobUrl or resolvable messageId" });
    }

    const requestedTtlSeconds = Number(req.body?.ttlSeconds);
    const ttlSeconds = Number.isFinite(requestedTtlSeconds)
      ? Math.max(60, Math.min(24 * 60 * 60, Math.trunc(requestedTtlSeconds)))
      : mediaBlobUrlDefaultTtlSeconds;
    const expiresAt = Date.now() + (ttlSeconds * 1000);

    const token = signMediaBlobToken({ p: blobPath, e: expiresAt, a: access });
    const baseUrl = getExternalBaseUrl(req);
    const relativePath = `/api/media/access/${encodeURIComponent(token)}`;
    const accessUrl = baseUrl ? `${baseUrl}${relativePath}` : relativePath;

    return res.json({
      ok: true,
      accessUrl,
      blobPath,
      blobUrl: blobUrl || null,
      access,
      expiresAt,
      expiresAtIso: new Date(expiresAt).toISOString(),
      ttlSeconds
    });
  } catch (error) {
    warnSoftError("media.access_url", error, { route: "/api/media/access-url", requestId });
    return res.status(500).json({
      error: "Failed to create media access URL",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/media/access/:token", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    if (!blobReadWriteToken) {
      return res.status(400).json({ error: "BLOB_READ_WRITE_TOKEN is not configured" });
    }
    if (!mediaBlobUrlSignSecret) {
      return res.status(400).json({ error: "MEDIA_BLOB_URL_SIGN_SECRET (or CRON_SECRET) is not configured" });
    }

    let verified;
    try {
      verified = verifyMediaBlobToken(req.params?.token || "");
    } catch (error) {
      return res.status(401).json({
        error: "Invalid media access token",
        details: error instanceof Error ? error.message : "Unknown error"
      });
    }

    if (Date.now() > verified.expiresAt) {
      return res.status(410).json({ error: "Media access token expired" });
    }

    const blobResult = await get(verified.blobPath, {
      access: verified.access,
      token: blobReadWriteToken
    });

    if (!blobResult || blobResult.statusCode !== 200 || !blobResult.stream) {
      return res.status(404).json({ error: "Media blob not found" });
    }

    const arrayBuffer = await new Response(blobResult.stream).arrayBuffer();
    const contentType = String(blobResult.blob?.contentType || "application/octet-stream");
    const contentDisposition = String(blobResult.blob?.contentDisposition || "").trim();

    res.setHeader("Content-Type", contentType);
    res.setHeader("Cache-Control", "private, max-age=60");
    if (contentDisposition) {
      res.setHeader("Content-Disposition", contentDisposition);
    }

    return res.send(Buffer.from(arrayBuffer));
  } catch (error) {
    warnSoftError("media.access_token", error, { route: "/api/media/access/:token", requestId });
    return res.status(500).json({
      error: "Failed to fetch blob media",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/logs", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");
  recordRequestSpan(res, "route.start", Date.now(), { route: "/api/logs" });

  if ((!supabaseEnabled || !supabase) && (!pgEnabled || !pgPool)) {
    addRequestBackendTag(res, "fallback");
    return res.json({ data: [], warning: "supabase_not_configured" });
  }

  try {
    const rawLimit = String(req.query?.limit || "100").trim().toLowerCase();
    const fetchAll = rawLimit === "all" || rawLimit === "unlimited";
    const maxFetchAllRowsRaw = Number(process.env.LOGS_FETCH_ALL_MAX_ROWS || 1000);
    const maxFetchAllRows = Number.isFinite(maxFetchAllRowsRaw)
      ? Math.max(100, Math.min(10000, Math.trunc(maxFetchAllRowsRaw)))
      : 1000;
    const requestedLimit = Number(rawLimit);
    const limit = fetchAll
      ? null
      : (Number.isFinite(requestedLimit) ? Math.max(Math.trunc(requestedLimit), 1) : 100);

    const cacheKey = `${supabaseEnabled ? "supabase" : "pg"}:${fetchAll ? "all" : String(limit)}`;
    const now = Date.now();
    const ifNoneMatch = String(req.headers["if-none-match"] || "").trim();

    if (
      logsResponseCache.payload &&
      logsResponseCache.key === cacheKey &&
      logsResponseCache.expiresAt > now
    ) {
      if (ifNoneMatch && ifNoneMatch === logsResponseCache.etag) {
        return res.status(304).end();
      }
      res.setHeader("Cache-Control", "public, max-age=2, s-maxage=5, stale-while-revalidate=20");
      if (logsResponseCache.etag) {
        res.setHeader("ETag", logsResponseCache.etag);
      }
      return res.json(logsResponseCache.payload);
    }

    if (supabaseEnabled && supabase) {
      addRequestBackendTag(res, "supabase");
      let rows = [];

      if (fetchAll) {
        // Use a conservative page size so this works even when Supabase API max rows is configured to 100.
        const batchSize = 100;
        let offset = 0;
        const fetchAllStart = Date.now();
        while (true) {
          if (rows.length >= maxFetchAllRows) {
            rows = rows.slice(0, maxFetchAllRows);
            break;
          }

          const { data, error } = await supabase
            .from("whatsapp_logs")
            .select("id,created_at,direction,channel,to_number,contact_name,message_text,template_name,status,api_message_id,payload")
            .neq("channel", STATE_FALLBACK_CHANNEL)
            .order("created_at", { ascending: false })
            .range(offset, offset + batchSize - 1);

          if (error) {
            recordRequestSpan(res, "logs.supabase.fetch_all", fetchAllStart, { backend: "supabase", fetchAll: true }, error);
            warnSoftError("logs.fetch.supabase_all", error, { route: "/api/logs", requestId, fetchAll: true });
            return res.status(500).json({ error: "Failed to fetch logs", details: error.message });
          }

          const chunk = Array.isArray(data) ? data : [];
          if (chunk.length === 0) {
            break;
          }

          rows.push(...chunk);
          offset += chunk.length;

          if (chunk.length < batchSize) {
            break;
          }
        }
        recordRequestSpan(res, "logs.supabase.fetch_all", fetchAllStart, { backend: "supabase", fetchAll: true });
      } else {
        const { data, error } = await runWithRequestSpan(res, "logs.supabase.fetch_limited", { backend: "supabase", fetchAll: false }, () =>
          supabase
            .from("whatsapp_logs")
            .select("id,created_at,direction,channel,to_number,contact_name,message_text,template_name,status,api_message_id,payload")
            .neq("channel", STATE_FALLBACK_CHANNEL)
            .order("created_at", { ascending: false })
            .limit(limit)
        );

        if (error) {
          warnSoftError("logs.fetch.supabase_limited", error, { route: "/api/logs", requestId, fetchAll: false });
          return res.status(500).json({ error: "Failed to fetch logs", details: error.message });
        }

        rows = Array.isArray(data) ? data : [];
      }

      const payload = { data: rows };
      const etag = `"${createHash("sha1").update(JSON.stringify(payload)).digest("hex")}"`;

      logsResponseCache.key = cacheKey;
      logsResponseCache.payload = payload;
      logsResponseCache.etag = etag;
      logsResponseCache.expiresAt = now + 5000;

      if (ifNoneMatch && ifNoneMatch === etag) {
        return res.status(304).end();
      }
      res.setHeader("Cache-Control", "public, max-age=2, s-maxage=5, stale-while-revalidate=20");
      res.setHeader("ETag", etag);
      return res.json(payload);
    }

    addRequestBackendTag(res, "pg");
    const pgResult = fetchAll
      ? await runWithRequestSpan(res, "logs.pg.fetch_all", { backend: "pg", fetchAll: true }, () =>
        pgPool.query(
          `select id, created_at, direction, channel, to_number, contact_name, message_text, template_name, status, api_message_id, payload
          from public.whatsapp_logs
          where channel is distinct from $1
          order by created_at desc
          limit $2`,
          [STATE_FALLBACK_CHANNEL, maxFetchAllRows]
        )
      )
      : await runWithRequestSpan(res, "logs.pg.fetch_limited", { backend: "pg", fetchAll: false }, () =>
        pgPool.query(
          `select id, created_at, direction, channel, to_number, contact_name, message_text, template_name, status, api_message_id, payload
          from public.whatsapp_logs
          where channel is distinct from $2
          order by created_at desc
          limit $1`,
          [limit, STATE_FALLBACK_CHANNEL]
        )
      );

    const payload = { data: pgResult.rows || [] };
    const etag = `"${createHash("sha1").update(JSON.stringify(payload)).digest("hex")}"`;

    logsResponseCache.key = cacheKey;
    logsResponseCache.payload = payload;
    logsResponseCache.etag = etag;
    logsResponseCache.expiresAt = now + 5000;

    if (ifNoneMatch && ifNoneMatch === etag) {
      return res.status(304).end();
    }
    res.setHeader("Cache-Control", "public, max-age=2, s-maxage=5, stale-while-revalidate=20");
    res.setHeader("ETag", etag);
    return res.json(payload);
  } catch (error) {
    warnSoftError("logs.fetch.unhandled", error, { route: "/api/logs", requestId });
    return res.status(500).json({
      error: "Failed to fetch logs",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/tms/dashboard", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const data = await fetchTmsDashboardData();
    return res.json({ ok: true, data });
  } catch (error) {
    warnSoftError("tms.dashboard", error, { route: "/api/tms/dashboard", requestId });
    return res.status(500).json({
      error: "Failed to fetch TMS dashboard data",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

async function fetchTmsCustomersData({ page = 1, limit = 100, search = "" } = {}) {
  const enabled = String(process.env.TMS_ENABLED || "false").toLowerCase() === "true";
  if (!enabled) throw new Error("TMS integration disabled. Set TMS_ENABLED=true.");

  const baseUrl = String(process.env.TMS_BASE_URL || "").trim().replace(/\/$/, "");
  const email = String(process.env.TMS_ADMIN_EMAIL || "").trim();
  const password = String(process.env.TMS_ADMIN_PASSWORD || "");
  if (!baseUrl || !email || !password) throw new Error("Missing TMS_BASE_URL, TMS_ADMIN_EMAIL or TMS_ADMIN_PASSWORD.");

  const safePage = Math.max(1, Math.trunc(Number(page) || 1));
  const safeLimit = Math.max(1, Math.min(500, Math.trunc(Number(limit) || 100)));

  const cookieJar = new Map();
  const loginUrl = `${baseUrl}/admin/login`;

  const loginPageRes = await fetch(loginUrl, { redirect: "manual" });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginPageRes));
  const loginHtml = await loginPageRes.text();
  const tokenMatch = loginHtml.match(/name="_token"\s+type="hidden"\s+value="([^"]+)"/i);
  const csrfToken = tokenMatch?.[1] || "";
  if (!csrfToken) throw new Error("Could not extract CSRF token.");

  const loginBody = new URLSearchParams();
  loginBody.set("_token", csrfToken);
  loginBody.set("email", email);
  loginBody.set("password", password);
  loginBody.set("remember", "on");

  const loginSubmitRes = await fetch(loginUrl, {
    method: "POST",
    redirect: "manual",
    headers: { "Content-Type": "application/x-www-form-urlencoded", Referer: loginUrl, Cookie: cookieJarHeader(cookieJar) },
    body: loginBody.toString()
  });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginSubmitRes));

  const start = (safePage - 1) * safeLimit;
  const datatableBody = new URLSearchParams();
  datatableBody.set("_token", csrfToken);
  datatableBody.set("draw", String(safePage));
  datatableBody.set("start", String(start));
  datatableBody.set("length", String(safeLimit));
  if (search) datatableBody.set("search[value]", search);

  const customersRes = await fetch(`${baseUrl}/admin/customers/datatable`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      "X-Requested-With": "XMLHttpRequest",
      Referer: `${baseUrl}/admin/customers`,
      Cookie: cookieJarHeader(cookieJar)
    },
    body: datatableBody.toString(),
    redirect: "manual"
  });

  if (!customersRes.ok) throw new Error(`Customers datatable failed: ${customersRes.status}`);
  const payload = await customersRes.json().catch(() => ({}));

  const data = Array.isArray(payload?.data) ? payload.data : [];
  const total = Number(payload?.recordsFiltered ?? payload?.recordsTotal ?? data.length) || 0;

  const rows = data.map((row) => ({
    id: Number(row?.id || 0),
    name: stripHtml(row?.name || row?.company_name || ""),
    email: String(row?.email || "").trim(),
    phone: decodeHtmlEntities(String(row?.phone || row?.phone_number || ""))
      .replace(/<br\s*\/?>/gi, " / ")
      .replace(/<[^>]*>/g, " ")
      .replace(/\s+/g, " ")
      .trim(),
    nif: String(row?.nif || row?.vat || row?.tax_id || "").trim(),
    address: stripHtml(row?.address || row?.street || ""),
    city: stripHtml(row?.city || ""),
    country: stripHtml(row?.country || ""),
    active: parseIconBoolean(row?.is_active) || String(row?.active || row?.status || "").toLowerCase() === "1" || String(row?.active || "").toLowerCase() === "true" || String(row?.is_active || "").toLowerCase() === "1",
    createdAt: String(row?.created_at || "").trim(),
    lastShipment: stripHtml(
      row?.last_shipment_at ||
      row?.last_shipping_date ||
      row?.last_shipment_date ||
      row?.last_shipment ||
      row?.last_send ||
      row?.updated_at ||
      ""
    ),
    shipments: Number(row?.shipments_count || row?.total_shipments || 0) || 0,
    url: `${baseUrl}/admin/customers/${row?.id || ""}`
  }));

  return {
    rows,
    meta: { page: safePage, limit: safeLimit, total, totalPages: Math.max(1, Math.ceil(total / safeLimit)) }
  };
}

async function createTmsAuthenticatedContext() {
  const enabled = String(process.env.TMS_ENABLED || "false").toLowerCase() === "true";
  if (!enabled) throw new Error("TMS integration disabled. Set TMS_ENABLED=true.");

  const baseUrl = String(process.env.TMS_BASE_URL || "").trim().replace(/\/$/, "");
  const email = String(process.env.TMS_ADMIN_EMAIL || "").trim();
  const password = String(process.env.TMS_ADMIN_PASSWORD || "");
  if (!baseUrl || !email || !password) throw new Error("Missing TMS_BASE_URL, TMS_ADMIN_EMAIL or TMS_ADMIN_PASSWORD.");

  const cookieJar = new Map();
  const loginUrl = `${baseUrl}/admin/login`;

  const loginPageRes = await fetch(loginUrl, { redirect: "manual" });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginPageRes));
  const loginHtml = await loginPageRes.text();
  const tokenMatch = loginHtml.match(/name="_token"\s+type="hidden"\s+value="([^"]+)"/i);
  const csrfToken = tokenMatch?.[1] || "";
  if (!csrfToken) throw new Error("Could not extract CSRF token.");

  const loginBody = new URLSearchParams();
  loginBody.set("_token", csrfToken);
  loginBody.set("email", email);
  loginBody.set("password", password);
  loginBody.set("remember", "on");

  const loginSubmitRes = await fetch(loginUrl, {
    method: "POST",
    redirect: "manual",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Referer: loginUrl,
      Cookie: cookieJarHeader(cookieJar)
    },
    body: loginBody.toString()
  });
  updateCookieJar(cookieJar, getSetCookieHeaders(loginSubmitRes));

  return { baseUrl, csrfToken, cookieJar };
}

async function fetchTmsWebservicesDatatable({ type = "shipping", page = 1, limit = 20 } = {}) {
  const safeType = String(type || "shipping").trim().toLowerCase();
  const safePage = Math.max(1, Math.trunc(Number(page) || 1));
  const safeLimit = Math.max(1, Math.min(200, Math.trunc(Number(limit) || 20)));
  const start = (safePage - 1) * safeLimit;

  const { baseUrl, csrfToken, cookieJar } = await createTmsAuthenticatedContext();

  const endpointByType = {
    shipping: "/admin/webservices/datatable",
    telematic: "/admin/webservices/datatable",
    sms: "/admin/webservices/datatable",
    maps: "/admin/webservices/datatable",
    tolls: "/admin/webservices/datatable",
    ai: "/admin/webservices/datatable",
    credit_insurers: "/admin/webservices/datatable",
    ecommerce: "/admin/webservices/ecommerce/datatable",
    payments: "/admin/webservices/payments/datatable"
  };

  const endpointPath = endpointByType[safeType] || endpointByType.shipping;
  const body = new URLSearchParams();
  body.set("_token", csrfToken);
  body.set("draw", String(safePage));
  body.set("start", String(start));
  body.set("length", String(safeLimit));
  if (endpointPath === "/admin/webservices/datatable") {
    body.set("type", safeType);
  }

  const res = await fetch(`${baseUrl}${endpointPath}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      "X-Requested-With": "XMLHttpRequest",
      Referer: `${baseUrl}/admin/webservices`,
      Cookie: cookieJarHeader(cookieJar)
    },
    body: body.toString(),
    redirect: "manual"
  });

  if (!res.ok) {
    throw new Error(`TMS webservices datatable failed (${safeType}): ${res.status}`);
  }

  const payload = await res.json().catch(() => ({}));
  const rows = Array.isArray(payload?.data) ? payload.data : [];
  const total = Number(payload?.recordsFiltered ?? payload?.recordsTotal ?? rows.length) || 0;

  return {
    endpoint: `${baseUrl}${endpointPath}`,
    type: safeType,
    rows,
    meta: {
      page: safePage,
      limit: safeLimit,
      total,
      totalPages: Math.max(1, Math.ceil(total / safeLimit))
    }
  };
}

function parseIntegrationHeaders(prefix) {
  const headers = {};

  const headersJsonRaw = String(process.env[`${prefix}_HEADERS_JSON`] || "").trim();
  if (headersJsonRaw) {
    try {
      const parsed = JSON.parse(headersJsonRaw);
      if (parsed && typeof parsed === "object") {
        for (const [key, value] of Object.entries(parsed)) {
          if (!key) continue;
          if (value === undefined || value === null) continue;
          headers[String(key)] = String(value);
        }
      }
    } catch {
      // Ignore malformed JSON and fallback to explicit env headers.
    }
  }

  const apiKey = String(process.env[`${prefix}_API_KEY`] || "").trim();
  const apiKeyHeader = String(process.env[`${prefix}_API_KEY_HEADER`] || "x-api-key").trim();
  if (apiKey) {
    headers[apiKeyHeader] = apiKey;
  }

  const userId = String(process.env[`${prefix}_USER_ID`] || "").trim();
  const userIdHeader = String(process.env[`${prefix}_USER_ID_HEADER`] || "x-user-id").trim();
  if (userId) {
    headers[userIdHeader] = userId;
  }

  const bearerToken = String(process.env[`${prefix}_BEARER_TOKEN`] || "").trim();
  if (bearerToken && !headers.Authorization) {
    headers.Authorization = `Bearer ${bearerToken}`;
  }

  return headers;
}

function resolveIntegrationServiceConfig(service) {
  const safeService = String(service || "").trim().toLowerCase();
  if (!safeService) {
    throw new Error("Service name is required.");
  }

  const prefix = safeService.toUpperCase().replace(/[^A-Z0-9]+/g, "_");
  const baseUrl = String(process.env[`${prefix}_BASE_URL`] || "").trim().replace(/\/$/, "");
  const endpointPath = String(process.env[`${prefix}_ENDPOINT_PATH`] || "").trim();
  const timeoutMsRaw = Number(process.env[`${prefix}_TIMEOUT_MS`] || 25000);
  const timeoutMs = Number.isFinite(timeoutMsRaw)
    ? Math.max(1000, Math.min(120000, Math.trunc(timeoutMsRaw)))
    : 25000;

  if (!baseUrl || !endpointPath) {
    throw new Error(`Missing ${prefix}_BASE_URL or ${prefix}_ENDPOINT_PATH.`);
  }

  return {
    service: safeService,
    prefix,
    baseUrl,
    endpointPath,
    timeoutMs,
    headers: parseIntegrationHeaders(prefix)
  };
}

async function fetchIntegrationServiceData({ service, query = {} }) {
  const config = resolveIntegrationServiceConfig(service);
  const queryEntries = Object.entries(query || {}).filter(([, value]) => value !== undefined && value !== null && value !== "");
  const queryString = new URLSearchParams(queryEntries.map(([key, value]) => [String(key), String(value)])).toString();
  const url = `${config.baseUrl}${config.endpointPath}${queryString ? `?${queryString}` : ""}`;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), config.timeoutMs);

  let upstreamRes;
  try {
    upstreamRes = await fetch(url, {
      method: "GET",
      headers: {
        Accept: "application/json, text/plain, */*",
        ...config.headers
      },
      signal: controller.signal
    });
  } finally {
    clearTimeout(timeoutId);
  }

  const contentType = String(upstreamRes.headers.get("content-type") || "").toLowerCase();
  const textBody = await upstreamRes.text();

  let parsedBody = null;
  if (contentType.includes("application/json")) {
    try {
      parsedBody = JSON.parse(textBody);
    } catch {
      parsedBody = null;
    }
  }

  return {
    ok: upstreamRes.ok,
    status: upstreamRes.status,
    service: config.service,
    upstream: {
      baseUrl: config.baseUrl,
      endpointPath: config.endpointPath,
      url,
      contentType
    },
    data: parsedBody ?? textBody,
    preview: textBody.slice(0, 1200)
  };
}

app.get("/api/integrations/:service/live", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const service = String(req.params?.service || "").trim();
    const payload = await fetchIntegrationServiceData({
      service,
      query: req.query || {}
    });

    const statusCode = payload.ok ? 200 : 502;
    return res.status(statusCode).json({
      ok: payload.ok,
      service: payload.service,
      status: payload.status,
      upstream: payload.upstream,
      data: payload.data,
      preview: payload.preview
    });
  } catch (error) {
    warnSoftError("integrations.live", error, {
      route: "/api/integrations/:service/live",
      requestId,
      service: String(req.params?.service || "").trim()
    });
    return res.status(500).json({
      error: "Failed to fetch integration service",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/ctt/live", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const payload = await fetchIntegrationServiceData({
      service: "ctt",
      query: req.query || {}
    });

    const statusCode = payload.ok ? 200 : 502;
    return res.status(statusCode).json({
      ok: payload.ok,
      service: payload.service,
      status: payload.status,
      upstream: payload.upstream,
      data: payload.data,
      preview: payload.preview
    });
  } catch (error) {
    warnSoftError("ctt.live", error, { route: "/api/ctt/live", requestId });
    return res.status(500).json({
      error: "Failed to fetch CTT live data",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/tms/webservices/discovery", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const type = String(req.query?.type || "shipping").trim().toLowerCase();
    const page = Number(req.query?.page || 1);
    const limit = Number(req.query?.limit || 20);
    const data = await fetchTmsWebservicesDatatable({ type, page, limit });

    const cttRows = data.rows.filter((row) => JSON.stringify(row || {}).toLowerCase().includes("ctt"));

    return res.json({
      ok: true,
      discovered: {
        shippingLike: "/admin/webservices/datatable",
        ecommerce: "/admin/webservices/ecommerce/datatable",
        payments: "/admin/webservices/payments/datatable"
      },
      data: {
        endpoint: data.endpoint,
        type: data.type,
        rows: data.rows,
        cttRows,
        meta: data.meta
      }
    });
  } catch (error) {
    warnSoftError("tms.webservices_discovery", error, { route: "/api/tms/webservices/discovery", requestId });
    return res.status(500).json({
      error: "Failed to discover TMS webservices endpoints",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/customers", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const page = Number(req.query?.page || 1);
    const limit = Number(req.query?.limit || 100);
    const search = String(req.query?.search || "").trim();
    const data = await fetchTmsCustomersData({ page, limit, search });
    return res.json({ ok: true, data: data.rows, meta: data.meta });
  } catch (error) {
    warnSoftError("tms.customers", error, { route: "/api/customers", requestId });
    return res.status(500).json({
      error: "Failed to fetch customers",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/tms/delivered", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const requestedPage = Number(req.query?.page || 1);
    const requestedLimit = Number(req.query?.limit || 250);

    const data = await fetchTmsDeliveredShipmentsData({
      page: requestedPage,
      limit: requestedLimit
    });

    return res.json({ ok: true, data: data.rows, meta: data.meta });
  } catch (error) {
    warnSoftError("tms.delivered", error, { route: "/api/tms/delivered", requestId });
    return res.status(500).json({
      error: "Failed to fetch delivered TMS shipments",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/tms/in-distribution", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const requestedPage = Number(req.query?.page || 1);
    const requestedLimit = Number(req.query?.limit || 250);

    const data = await fetchTmsInDistributionShipmentsData({
      page: requestedPage,
      limit: requestedLimit
    });

    return res.json({ ok: true, data: data.rows, meta: data.meta });
  } catch (error) {
    warnSoftError("tms.in_distribution", error, { route: "/api/tms/in-distribution", requestId });
    return res.status(500).json({
      error: "Failed to fetch in-distribution TMS shipments",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/tms/incidencias", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const requestedPage = Number(req.query?.page || 1);
    const requestedLimit = Number(req.query?.limit || 250);

    const data = await fetchTmsIncidenceShipmentsData({
      page: requestedPage,
      limit: requestedLimit
    });

    return res.json({ ok: true, data: data.rows, meta: data.meta });
  } catch (error) {
    warnSoftError("tms.incidencias", error, { route: "/api/tms/incidencias", requestId });
    return res.status(500).json({
      error: "Failed to fetch incidence TMS shipments",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/tms/in-transport", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const requestedPage = Number(req.query?.page || 1);
    const requestedLimit = Number(req.query?.limit || 250);

    const data = await fetchTmsInTransportShipmentsData({
      page: requestedPage,
      limit: requestedLimit
    });

    return res.json({ ok: true, data: data.rows, meta: data.meta });
  } catch (error) {
    warnSoftError("tms.in_transport", error, { route: "/api/tms/in-transport", requestId });
    return res.status(500).json({
      error: "Failed to fetch in-transport TMS shipments",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/tms/em-transporte", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const requestedPage = Number(req.query?.page || 1);
    const requestedLimit = Number(req.query?.limit || 250);

    const data = await fetchTmsInTransportShipmentsData({
      page: requestedPage,
      limit: requestedLimit
    });

    return res.json({ ok: true, data: data.rows, meta: data.meta });
  } catch (error) {
    warnSoftError("tms.em_transporte", error, { route: "/api/tms/em-transporte", requestId });
    return res.status(500).json({
      error: "Failed to fetch in-transport TMS shipments",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

function resolveCttDataSource() {
  const raw = String(process.env.CTT_DATA_SOURCE || "tms").trim().toLowerCase();
  return raw === "ctt-api" ? "ctt-api" : "tms";
}

function parseStatusText(value) {
  return String(value || "").trim().toLowerCase();
}

function toCttRowFromUnknown(input) {
  if (!input || typeof input !== "object") return null;

  const parcelId = String(
    input.parcelId || input.parcel_id || input.shipmentId || input.shipment_id || input.id || ""
  ).trim();
  const providerTrackingCode = String(
    input.providerTrackingCode || input.tracking || input.trackingCode || input.tracking_code || input.code || ""
  ).trim();

  const status = String(input.status || input.state || input.shipment_status || "").trim();
  const sender = String(input.sender || input.from || input.sender_name || "").trim();
  const recipient = String(input.recipient || input.to || input.recipient_name || input.customer || "").trim();
  const pickupDate = String(input.pickupDate || input.pickup_date || input.created_at || input.date || "").trim();
  const deliveryDate = String(input.deliveryDate || input.delivery_date || input.delivered_at || "").trim();
  const incidence = String(input.incidence || input.incidencia || input.occurrence || "").trim();
  const incidentReason = String(input.incidentReason || input.reason || input.error || "").trim();

  if (!parcelId && !providerTrackingCode) return null;

  return {
    parcelId: parcelId || providerTrackingCode,
    providerTrackingCode,
    service: String(input.service || input.method || "CTT").trim() || "CTT",
    sender,
    recipient,
    finalClientPhone: String(input.finalClientPhone || input.phone || input.mobile || "").trim(),
    pickupDate,
    deliveryDate,
    status,
    incidence,
    incidentReason,
    hasCharge: false,
    chargeAmount: ""
  };
}

function extractCttRowsFromLivePayload(payload) {
  const candidates = [];
  if (Array.isArray(payload)) {
    candidates.push(...payload);
  } else if (payload && typeof payload === "object") {
    const knownArrays = [
      payload.data,
      payload.shipments,
      payload.items,
      payload.results,
      payload.rows
    ];
    for (const maybe of knownArrays) {
      if (Array.isArray(maybe)) candidates.push(...maybe);
    }
  }

  return candidates
    .map((item) => toCttRowFromUnknown(item))
    .filter(Boolean);
}

async function fetchCttRowsFromDirectApi(query = {}) {
  const live = await fetchIntegrationServiceData({
    service: "ctt",
    query
  });

  if (!live.ok) {
    throw new Error(`CTT API request failed with status ${live.status}`);
  }

  const rows = extractCttRowsFromLivePayload(live.data);
  if (rows.length === 0) {
    throw new Error("CTT API responded but no shipment rows could be mapped yet.");
  }

  const deliveredRows = [];
  const inDistributionRows = [];
  const inTransportRows = [];
  const incidenceRows = [];

  for (const row of rows) {
    const status = parseStatusText(row.status);
    const hasIncidence = !!String(row.incidence || row.incidentReason || "").trim();

    if (hasIncidence || status.includes("incid") || status.includes("erro")) {
      incidenceRows.push(row);
    }
    if (status.includes("entreg")) {
      deliveredRows.push(row);
      continue;
    }
    if (status.includes("distribu")) {
      inDistributionRows.push(row);
      continue;
    }
    if (status.includes("transport") || status.includes("expedi") || status.includes("transit")) {
      inTransportRows.push(row);
      continue;
    }
    inTransportRows.push(row);
  }

  return {
    deliveredRows,
    inDistributionRows,
    inTransportRows,
    incidenceRows,
    totalRows: rows.length
  };
}

app.get("/api/ctt/dashboard", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const fromRaw = String(req.query?.from || "").trim();
    const toRaw = String(req.query?.to || "").trim();
    const fromKey = fromRaw.match(/^\d{4}-\d{2}-\d{2}$/) ? fromRaw : "";
    const toKey = toRaw.match(/^\d{4}-\d{2}-\d{2}$/) ? toRaw : "";
    const requestedMaxPages = Number(req.query?.maxPages || 4);
    const safeMaxPages = Number.isFinite(requestedMaxPages) ? Math.max(1, Math.min(20, Math.trunc(requestedMaxPages))) : 4;

    const source = resolveCttDataSource();

    let deliveredRows;
    let inDistributionRows;
    let inTransportRows;
    let incidenceRows;

    if (source === "ctt-api") {
      const direct = await fetchCttRowsFromDirectApi(req.query || {});
      deliveredRows = direct.deliveredRows;
      inDistributionRows = direct.inDistributionRows;
      inTransportRows = direct.inTransportRows;
      incidenceRows = direct.incidenceRows;
    } else {
      [deliveredRows, inDistributionRows, inTransportRows, incidenceRows] = await Promise.all([
        fetchAllTmsDeliveredShipmentsData({ limit: 120, maxPages: safeMaxPages }),
        fetchAllTmsInDistributionShipmentsData({ limit: 120, maxPages: safeMaxPages }),
        fetchAllTmsInTransportShipmentsData({ limit: 120, maxPages: safeMaxPages }),
        fetchAllTmsIncidenceShipmentsData({ limit: 120, maxPages: safeMaxPages })
      ]);
    }

    const dashboard = buildCttDashboardDataFromRows({
      deliveredRows,
      inDistributionRows,
      inTransportRows,
      incidenceRows,
      fromKey,
      toKey
    });

    return res.json({
      ok: true,
      data: dashboard,
      meta: {
        source,
        from: fromKey || null,
        to: toKey || null,
        maxPages: safeMaxPages,
        fetchedAt: new Date().toISOString()
      }
    });
  } catch (error) {
    warnSoftError("ctt.dashboard", error, { route: "/api/ctt/dashboard", requestId });
    return res.status(500).json({
      error: "Failed to fetch CTT dashboard",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/ctt/risk-shipments", async (req, res) => {
  try {
    const fromRaw = String(req.query?.from || "").trim();
    const toRaw = String(req.query?.to || "").trim();
    const fromKey = fromRaw.match(/^\d{4}-\d{2}-\d{2}$/) ? fromRaw : "";
    const toKey = toRaw.match(/^\d{4}-\d{2}-\d{2}$/) ? toRaw : "";
    const requestedLimit = Number(req.query?.limit || 20);
    const requestedMaxPages = Number(req.query?.maxPages || 4);
    const requestedMinHours = Number(req.query?.minHours || 8);
    const safeLimit = Number.isFinite(requestedLimit) ? Math.max(1, Math.min(100, Math.trunc(requestedLimit))) : 20;
    const safeMaxPages = Number.isFinite(requestedMaxPages) ? Math.max(1, Math.min(20, Math.trunc(requestedMaxPages))) : 4;
    const safeMinHours = Number.isFinite(requestedMinHours) ? Math.max(1, Math.min(240, Math.trunc(requestedMinHours))) : 8;

    const source = resolveCttDataSource();

    let inTransportRows;
    let incidenceRows;

    if (source === "ctt-api") {
      const direct = await fetchCttRowsFromDirectApi(req.query || {});
      inTransportRows = direct.inTransportRows;
      incidenceRows = direct.incidenceRows;
    } else {
      [inTransportRows, incidenceRows] = await Promise.all([
        fetchAllTmsInTransportShipmentsData({ limit: 120, maxPages: safeMaxPages }),
        fetchAllTmsIncidenceShipmentsData({ limit: 120, maxPages: safeMaxPages })
      ]);
    }

    const now = Date.now();
    const byKey = new Map();

    const upsertRisk = (row, options = {}) => {
      const parcelId = String(row?.parcelId || "").trim();
      const tracking = String(row?.providerTrackingCode || "").trim();
      const identityKey = `${parcelId}|${tracking}`.trim();
      if (!identityKey || identityKey === "|") return;

      const dateInfo = resolveShipmentPrimaryDateInfo(row);
      if (fromKey || toKey) {
        if (!isDateKeyWithinRange(dateInfo.key, fromKey, toKey)) {
          return;
        }
      }

      const fallbackHours = Number(options.fallbackHours || 0);
      const computedHours = Number.isFinite(dateInfo.ts)
        ? Math.max(1, Math.round((now - dateInfo.ts) / (1000 * 60 * 60)))
        : fallbackHours;

      const risk = String(options.risk || "Medio");
      const riskWeight = risk === "Alto" ? 2 : 1;
      const hoursWithoutUpdate = Math.max(1, computedHours);

      if (hoursWithoutUpdate < safeMinHours && risk !== "Alto") {
        return;
      }

      const next = {
        tracking: tracking || parcelId,
        customer: String(row?.recipient || row?.sender || "").trim() || "Cliente",
        route: `${String(row?.sender || "Origem").trim()} -> ${String(row?.recipient || "Destino").trim()}`,
        risk,
        riskWeight,
        hoursWithoutUpdate,
        status: String(row?.status || "").trim(),
        incidentReason: String(row?.incidentReason || row?.incidence || "").trim(),
        parcelId,
        providerTrackingCode: tracking,
        shipmentDateKey: dateInfo.key,
        pickupDate: String(row?.pickupDate || "").trim(),
        deliveryDate: String(row?.deliveryDate || "").trim()
      };

      const previous = byKey.get(identityKey);
      if (!previous) {
        byKey.set(identityKey, next);
        return;
      }

      if (next.riskWeight > previous.riskWeight || (next.riskWeight === previous.riskWeight && next.hoursWithoutUpdate > previous.hoursWithoutUpdate)) {
        byKey.set(identityKey, next);
      }
    };

    for (const row of incidenceRows) {
      upsertRisk(row, { risk: "Alto", fallbackHours: 24 });
    }

    for (const row of inTransportRows) {
      const dateInfo = resolveShipmentPrimaryDateInfo(row);
      const transportHours = Number.isFinite(dateInfo.ts)
        ? Math.max(1, Math.round((now - dateInfo.ts) / (1000 * 60 * 60)))
        : 12;
      upsertRisk(row, {
        risk: transportHours >= 24 ? "Alto" : "Medio",
        fallbackHours: 12
      });
    }

    const items = Array.from(byKey.values())
      .sort((a, b) => (b.riskWeight - a.riskWeight) || (b.hoursWithoutUpdate - a.hoursWithoutUpdate))
      .slice(0, safeLimit)
      .map(({ riskWeight, ...item }) => item);

    return res.json({
      ok: true,
      data: items,
      meta: {
        source,
        from: fromKey || null,
        to: toKey || null,
        limit: safeLimit,
        maxPages: safeMaxPages,
        minHours: safeMinHours,
        transportFetched: inTransportRows.length,
        incidenceFetched: incidenceRows.length,
        totalRiskCandidates: byKey.size,
        fetchedAt: new Date().toISOString()
      }
    });
  } catch (error) {
    return res.status(500).json({
      error: "Failed to fetch CTT risk shipments",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.post("/api/auth/login", async (req, res) => {
  const username = String(req.body?.username || "").trim();
  const password = String(req.body?.password || "");

  if (!username || !password) {
    return res.status(400).json({ error: "Missing username or password" });
  }

  const users = getWorkspaceAuthUsers();
  if (users.length === 0) {
    return res.status(500).json({ error: "No workspace users configured" });
  }

  const matched = users.find((user) => user.username === username && user.password === password);
  if (!matched) {
    return res.status(401).json({ error: "Credenciais inválidas" });
  }

  return res.json({
    ok: true,
    user: {
      username: matched.username,
      displayName: matched.displayName
    }
  });
});

app.get("/api/state", async (req, res) => {
  const defaults = defaultWorkspaceState();
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");
  recordRequestSpan(res, "route.start", Date.now(), { route: "/api/state" });

  if ((!supabaseEnabled || !supabase) && (!pgEnabled || !pgPool)) {
    addRequestBackendTag(res, "fallback");
    return res.json({ data: defaults, warning: "persistent_state_not_configured" });
  }

  try {
    if (supabaseEnabled && supabase) {
      addRequestBackendTag(res, "supabase");
      try {
        const { data, error } = await runWithRequestSpan(res, "state.supabase.fetch_keys", { backend: "supabase" }, () =>
          supabase
            .from("workspace_state")
            .select("key,value")
            .in("key", PERSISTENCE_KEYS)
        );

        if (!error) {
          const next = { ...defaults };
          for (const row of data || []) {
            if (row?.key && Object.prototype.hasOwnProperty.call(next, row.key)) {
              next[row.key] = row.value;
            }
          }

          try {
            const contacts = await runWithRequestSpan(res, "state.contacts.fetch_after_supabase", { backend: "supabase" }, () =>
              loadContactsFromDedicatedTable()
            );
            if (Object.keys(contacts).length > 0) {
              next.contacts = contacts;
            }
          } catch (error) {
            warnSoftError("state.get.contacts_supabase", error, { route: "/api/state", requestId });
          }

          return res.json({ data: next });
        }
      } catch (error) {
        warnSoftError("state.get.supabase", error, { route: "/api/state", requestId });
      }

      try {
        addRequestBackendTag(res, "fallback");
        const snapshot = await getWorkspaceStateFromLogsFallback();
        if (snapshot) {
          recordRequestSpan(res, "state.logs_fallback.fetch", Date.now(), { backend: "fallback", source: "whatsapp_logs" });
          return res.json({ data: snapshot, warning: "workspace_state_table_unavailable_using_log_fallback" });
        }
      } catch (error) {
        warnSoftError("state.get.logs_fallback", error, { route: "/api/state", requestId });
      }
    }

    if (pgEnabled && pgPool) {
      addRequestBackendTag(res, "pg");
      await runWithRequestSpan(res, "state.pg.ensure_table", { backend: "pg" }, () => ensurePersistentStateTable());
      const { rows } = await runWithRequestSpan(res, "state.pg.fetch_keys", { backend: "pg" }, () =>
        pgPool.query(
          `select key, value from public.workspace_state where key = any($1::text[])`,
          [PERSISTENCE_KEYS]
        )
      );
      const next = { ...defaults };
      for (const row of rows || []) {
        if (row?.key && Object.prototype.hasOwnProperty.call(next, row.key)) {
          next[row.key] = row.value;
        }
      }

      try {
        const contacts = await runWithRequestSpan(res, "state.contacts.fetch_after_pg", { backend: "pg" }, () =>
          loadContactsFromDedicatedTable()
        );
        if (Object.keys(contacts).length > 0) {
          next.contacts = contacts;
        }
      } catch (error) {
        warnSoftError("state.get.contacts_postgres", error, { route: "/api/state", requestId });
      }

      return res.json({ data: next });
    }

    return res.status(500).json({ error: "Failed to fetch persistent state", details: "No reachable state backend" });
  } catch (error) {
    return res.json({
      data: defaults,
      warning: error instanceof Error ? error.message : "persistent_state_unavailable"
    });
  }
});

app.post("/api/state", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");
  recordRequestSpan(res, "route.start", Date.now(), { route: "/api/state", method: "POST" });

  if ((!supabaseEnabled || !supabase) && (!pgEnabled || !pgPool)) {
    addRequestBackendTag(res, "fallback");
    return res.status(400).json({ error: "persistent_state_not_configured" });
  }

  const body = req.body && typeof req.body === "object" ? req.body : {};
  const updates = PERSISTENCE_KEYS
    .filter((key) => Object.prototype.hasOwnProperty.call(body, key))
    .map((key) => ({ key, value: body[key] }));
  const contactsUpdate = updates.find((item) => item.key === "contacts");

  if (updates.length === 0) {
    return res.status(400).json({ error: "No valid state keys supplied" });
  }

  try {
    if (supabaseEnabled && supabase) {
      addRequestBackendTag(res, "supabase");
      try {
        const payload = updates.map((item) => ({ key: item.key, value: item.value ?? null }));
        const { error } = await runWithRequestSpan(res, "state.supabase.upsert", { backend: "supabase" }, () =>
          supabase
            .from("workspace_state")
            .upsert(payload, { onConflict: "key" })
        );

        if (!error) {
          if (contactsUpdate) {
            try {
              await runWithRequestSpan(res, "state.contacts.sync_after_supabase", { backend: "supabase" }, () =>
                syncContactsToDedicatedTable(contactsUpdate.value)
              );
            } catch (error) {
              warnSoftError("state.post.contacts_supabase", error, { route: "/api/state", requestId });
            }
          }
          return res.json({ ok: true, updated: updates.length, via: "supabase" });
        }
      } catch (error) {
        warnSoftError("state.post.supabase", error, { route: "/api/state", requestId });
      }

      try {
        addRequestBackendTag(res, "fallback");
        const existing = (await getWorkspaceStateFromLogsFallback()) || defaultWorkspaceState();
        const next = { ...existing };
        for (const item of updates) {
          next[item.key] = item.value ?? null;
        }
        await runWithRequestSpan(res, "state.logs_fallback.write", { backend: "fallback" }, () =>
          writeWorkspaceStateToLogsFallback(next)
        );

        if (contactsUpdate) {
          try {
            await runWithRequestSpan(res, "state.contacts.sync_after_logs_fallback", { backend: "fallback" }, () =>
              syncContactsToDedicatedTable(contactsUpdate.value)
            );
          } catch (error) {
            warnSoftError("state.post.contacts_logs_fallback", error, { route: "/api/state", requestId });
          }
        }

        return res.json({ ok: true, updated: updates.length, via: "supabase_logs_fallback", warning: "workspace_state_table_unavailable_using_log_fallback" });
      } catch (error) {
        warnSoftError("state.post.logs_fallback", error, { route: "/api/state", requestId });
      }
    }

    if (pgEnabled && pgPool) {
      addRequestBackendTag(res, "pg");
      await runWithRequestSpan(res, "state.pg.ensure_table", { backend: "pg" }, () => ensurePersistentStateTable());
      const placeholders = [];
      const values = [];
      for (let index = 0; index < updates.length; index += 1) {
        const keyParam = index * 2 + 1;
        const valueParam = index * 2 + 2;
        placeholders.push(`($${keyParam}, $${valueParam}::jsonb, now())`);
        values.push(updates[index].key, JSON.stringify(updates[index].value ?? null));
      }

      await runWithRequestSpan(res, "state.pg.bulk_upsert", { backend: "pg" }, () =>
        pgPool.query(
          `insert into public.workspace_state (key, value, updated_at)
          values ${placeholders.join(", ")}
          on conflict (key) do update set value = excluded.value, updated_at = now()`,
          values
        )
      );

      if (contactsUpdate) {
        try {
          await runWithRequestSpan(res, "state.contacts.sync_after_pg", { backend: "pg" }, () =>
            syncContactsToDedicatedTable(contactsUpdate.value)
          );
        } catch (error) {
          warnSoftError("state.post.contacts_postgres", error, { route: "/api/state", requestId });
        }
      }

      return res.json({ ok: true, updated: updates.length, via: "postgres" });
    }

    return res.status(500).json({ error: "Failed to persist state", details: "No reachable state backend" });
  } catch (error) {
    return res.status(500).json({
      error: "Failed to persist state",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

// ── Radio stream proxy (CORS bypass) ──────────────────────────────────────
// Uses native http/https.request to handle ICY/SHOUTcast streams that
// node's fetch API cannot handle. Only used for custom user-supplied URLs.
const BLOCKED_HOSTS = /^(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.|::1$)/i;

app.get("/api/radio/proxy", (req, res) => {
  const { url } = req.query;
  if (!url || typeof url !== "string") {
    return res.status(400).json({ error: "Missing url parameter" });
  }
  if (!/^https?:\/\//i.test(url)) {
    return res.status(400).json({ error: "Only http/https URLs are allowed" });
  }
  let parsed;
  try {
    parsed = new URL(url);
    if (BLOCKED_HOSTS.test(parsed.hostname)) {
      return res.status(400).json({ error: "Blocked URL" });
    }
  } catch {
    return res.status(400).json({ error: "Invalid URL" });
  }

  const transport = parsed.protocol === "https:" ? https : http;
  const options = {
    hostname: parsed.hostname,
    port: parsed.port || (parsed.protocol === "https:" ? 443 : 80),
    path: parsed.pathname + parsed.search,
    method: "GET",
    headers: {
      "User-Agent": "Mozilla/5.0 (compatible; LinkeRadioPT/1.0)",
      "Icy-MetaData": "0",
      "Accept": "audio/mpeg, audio/ogg, audio/aac, audio/*, */*"
    },
    timeout: 12000
  };

  const proxyReq = transport.request(options, (proxyRes) => {
    const status = proxyRes.statusCode || 0;
    // Redirect handling disabled: return an explicit error instead of recursive proxying.
    if ((status === 301 || status === 302 || status === 307 || status === 308) && proxyRes.headers.location) {
      proxyReq.destroy();
      return res.status(502).json({
        error: "Upstream redirect not supported",
        location: String(proxyRes.headers.location || "")
      });
    }
    if (status < 200 || status >= 400) {
      if (!res.headersSent) res.status(502).json({ error: `Upstream returned ${status}` });
      return;
    }
    const contentType = proxyRes.headers["content-type"] || "audio/mpeg";
    res.setHeader("Content-Type", contentType);
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Access-Control-Allow-Origin", "*");
    proxyRes.pipe(res);
    req.on("close", () => proxyReq.destroy());
  });

  proxyReq.on("timeout", () => {
    proxyReq.destroy();
    if (!res.headersSent) res.status(504).json({ error: "Stream connection timed out" });
  });

  proxyReq.on("error", (err) => {
    if (!res.headersSent) res.status(502).json({ error: "Failed to connect to stream", details: err.message });
  });

  proxyReq.end();
});

// ── WhatsApp Calling API routes ────────────────────────────────────────────

app.get("/api/calls/permissions", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const userWaId = String(req.query.user_wa_id || "").trim();
    if (!userWaId) {
      return res.status(400).json({ error: "Query param 'user_wa_id' is required." });
    }

    const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
    const phoneNumberId = requiredEnv("WHATSAPP_PHONE_NUMBER_ID");
    const token = requiredEnv("WHATSAPP_ACCESS_TOKEN");

    const url = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}/call_permissions?user_wa_id=${encodeURIComponent(userWaId)}`;
    const apiRes = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` }
    });

    const data = await apiRes.json();
    return res.status(apiRes.status).json(data);
  } catch (error) {
    warnSoftError("calls.permissions", error, { route: "/api/calls/permissions", requestId });
    return res.status(500).json({ error: "Failed to check call permissions", details: error instanceof Error ? error.message : "Unknown error" });
  }
});

app.post("/api/calls/request-permission", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const userWaId = String(req.body?.user_wa_id || "").trim();
    if (!userWaId) {
      return res.status(400).json({ error: "Field 'user_wa_id' is required." });
    }

    const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
    const phoneNumberId = requiredEnv("WHATSAPP_PHONE_NUMBER_ID");
    const token = requiredEnv("WHATSAPP_ACCESS_TOKEN");

    const url = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}/call_permissions`;
    const payload = {
      messaging_product: "whatsapp",
      user_wa_id: userWaId,
      action: "send_call_permission_request"
    };

    const apiRes = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    });

    const data = await apiRes.json();
    return res.status(apiRes.status).json(data);
  } catch (error) {
    warnSoftError("calls.request_permission", error, { route: "/api/calls/request-permission", requestId });
    return res.status(500).json({ error: "Failed to request call permission", details: error instanceof Error ? error.message : "Unknown error" });
  }
});

app.post("/api/calls/manage", async (req, res) => {
  const requestId = String(res.locals?.requestId || req.headers["x-request-id"] || "");

  try {
    const { action, to, call_id, session, biz_opaque_callback_data } = req.body || {};
    if (!action) {
      return res.status(400).json({ error: "Field 'action' is required." });
    }

    const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
    const phoneNumberId = requiredEnv("WHATSAPP_PHONE_NUMBER_ID");
    const token = requiredEnv("WHATSAPP_ACCESS_TOKEN");

    let payload;
    if (action === "terminate") {
      if (!call_id) return res.status(400).json({ error: "Field 'call_id' is required for terminate." });
      payload = { messaging_product: "whatsapp", call_id: String(call_id), action: "terminate" };
    } else {
      if (!to) return res.status(400).json({ error: "Field 'to' is required for this action." });
      payload = { messaging_product: "whatsapp", to: String(to), action: String(action) };
      if (biz_opaque_callback_data) {
        payload.biz_opaque_callback_data = String(biz_opaque_callback_data);
      }
      if (action === "connect") {
        if (!session || typeof session !== "object") {
          return res.status(400).json({ error: "Field 'session' is required for connect action." });
        }
        const sdpType = String(session?.sdp_type || "").trim();
        const sdp = String(session?.sdp || "").trim();
        if (!sdpType || !sdp) {
          return res.status(400).json({ error: "session.sdp_type and session.sdp are required for connect action." });
        }
        payload.session = {
          sdp_type: sdpType,
          sdp
        };
      }
    }

    const url = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}/calls`;
    const apiRes = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    });

    const data = await apiRes.json();
    return res.status(apiRes.status).json(data);
  } catch (error) {
    warnSoftError("calls.manage", error, { route: "/api/calls/manage", requestId });
    return res.status(500).json({ error: "Failed to manage call", details: error instanceof Error ? error.message : "Unknown error" });
  }
});

app.use((error, _req, res, next) => {
  if (error?.type === "entity.too.large" || Number(error?.status) === 413) {
    return res.status(413).json({
      error: "Payload too large",
      details: "Email demasiado grande. Reduz o conteúdo e tenta novamente."
    });
  }

  return next(error);
});

if (!process.env.VERCEL) {
  app.listen(port, () => {
    console.log(`Backend running on http://localhost:${port}`);
  });
}

export default app;
