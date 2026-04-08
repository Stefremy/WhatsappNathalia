import "dotenv/config";
import express from "express";
import cors from "cors";
import multer from "multer";
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

function broadcastSSE(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const client of sseClients) {
    try { client.write(payload); } catch {}
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
      } catch {}
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
      } catch {}
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
      } catch {}
    }

    const raw = await readFile(autoNotificacaoIncidenciaStateFile, "utf8");
    const parsed = JSON.parse(raw || "{}") || {};
    applyAutoNotificacaoIncidenciaState(parsed);
  } catch {
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
    } catch {}
  }

  if (supabaseEnabled && supabase) {
    try {
      const { error } = await supabase
        .from("workspace_state")
        .upsert([{ key: "auto_notificacao_incidencia_state", value: payload }], { onConflict: "key" });
      if (!error) {
        return;
      }
    } catch {}
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
    } catch {}
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

  for (const item of scheduledMessages) {
    if (item.status !== "pending") continue;
    if (new Date(item.scheduledAt).getTime() > now) continue;
    processed += 1;
    item.status = "sending";
    try {
      const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
      const phoneNumberId = process.env.WHATSAPP_PHONE_NUMBER_ID || "";
      const token = process.env.WHATSAPP_ACCESS_TOKEN || "";
      if (!phoneNumberId || !token) { item.status = "failed"; continue; }
      const endpoint = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}/messages`;
      const components = [];
      if (item.bodyVariables && item.bodyVariables.length > 0) {
        components.push({
          type: "body",
          parameters: item.bodyVariables.map((text) => ({ type: "text", text }))
        });
      }
      const templatePayload = { name: item.templateName, language: { code: item.languageCode || "pt_PT" } };
      if (components.length > 0) templatePayload.components = components;
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
        body: JSON.stringify({ messaging_product: "whatsapp", recipient_type: "individual", to: item.to, type: "template", template: templatePayload })
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
    } catch {
      item.status = "failed";
      failed += 1;
    }
    broadcastSSE("scheduled_sent", { id: item.id, status: item.status });
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
      } catch {}
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
      } catch {}
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
      } catch {}
    }

    const raw = await readFile(autoNotificacaoEnvioStateFile, "utf8");
    const parsed = JSON.parse(raw || "{}") || {};

    autoNotificacaoEnvioLastRunDateKey = String(parsed.envioLastRunDateKey || "").trim();
    autoNotificacaoEnvioTransporteLastRunDateKey = String(parsed.transporteLastRunDateKey || "").trim();
  } catch {
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
    } catch {}
  }

  if (supabaseEnabled && supabase) {
    try {
      const { error } = await supabase
        .from("workspace_state")
        .upsert([{ key: "auto_notificacao_envio_state", value: payload }], { onConflict: "key" });
      if (!error) {
        return;
      }
    } catch {}
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
    } catch {}
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

    // Refresh source data each cycle to detect newly appeared incidence rows.
    const rows = await fetchAllTmsIncidenceShipmentsData({ limit: 250, maxPages: 40 });
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

async function runAutoNotificacaoEnvioForInDistribution() {
  const templateName = String(process.env.AUTO_NOTIFICACAO_ENVIO_TEMPLATE || "notificacao_de_envio").trim();
  const languageCode = String(process.env.AUTO_NOTIFICACAO_ENVIO_LANGUAGE || "pt_PT").trim() || "pt_PT";

  // Refresh source data first (equivalent to clicking "Atualizar em distribuicao").
  const rows = await fetchAllTmsInDistributionShipmentsData({ limit: 250, maxPages: 40 });

  let processed = 0;
  let sent = 0;
  let failed = 0;

  for (const row of rows) {
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
    fetchedRows: rows.length,
    templateName,
    languageCode
  };
}

async function runAutoNotificacaoEnvioForInTransport() {
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

  // Refresh source data first (equivalent to clicking "Atualizar em transporte").
  const rows = await fetchAllTmsInTransportShipmentsData({ limit: 250, maxPages: 40 });

  let processed = 0;
  let sent = 0;
  let failed = 0;
  let skippedMaritimoIlhas = 0;

  for (const row of rows) {
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
    skippedMaritimoIlhas,
    fetchedRows: rows.length,
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
  const targetMinute = Number(process.env.AUTO_NOTIFICACAO_ENVIO_MINUTE || 0);
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
    } catch {}
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
    } catch {}
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
    } catch {}
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
      } catch {}
      setTimeout(() => {
        try { window.close(); } catch {}
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
          message
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
  const keepalive = setInterval(() => { try { res.write(": keepalive\n\n"); } catch {} }, 25000);
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
    return res.status(500).json({
      error: "Failed to build Google OAuth URL",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/google/oauth/callback", async (req, res) => {
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

app.get("/api/google/oauth/status", (_req, res) => {
  return (async () => {
    await hydrateGoogleOauthSession();
    const connected = Boolean(googleOauthSession.accessToken);
    return res.json({
      ok: true,
      connected,
      scope: googleOauthSession.scope || "",
      expires_at: googleOauthSession.expiresAt || 0
    });
  })().catch((error) =>
    res.status(500).json({
      error: "Google OAuth status failed",
      details: error instanceof Error ? error.message : "Unknown error"
    })
  );
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
    return res.status(500).json({
      error: "Google inbox fetch failed",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/consumiveis", async (req, res) => {
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
    return res.status(500).json({
      error: "Failed to fetch consumiveis from Notion.",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/feedback-tracker", async (req, res) => {
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
    return res.status(500).json({
      error: "Failed to fetch feedback tracker from Notion.",
      details: notionErrorDetails(error)
    });
  }
});

app.post("/api/feedback-tracker", async (req, res) => {
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
    return res.status(500).json({
      error: "Failed to create feedback tracker row in Notion.",
      details: notionErrorDetails(error)
    });
  }
});

app.post("/api/consumiveis", async (req, res) => {
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
    return res.status(500).json({
      error: "Failed to create consumiveis row in Notion.",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.post("/api/messages/send", async (req, res) => {
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
    return res.status(500).json({
      error: "Failed to send message",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.post("/api/messages/status", async (req, res) => {
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
    return res.status(500).json({
      error: "Failed to update status",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.post("/api/media/upload", upload.single("file"), async (req, res) => {
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
    return res.status(500).json({
      error: "Failed to upload media",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.post("/api/templates/send-return-to-sender", async (req, res) => {
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
  const result = await sendGenericTemplateMessage({
    to: req.body?.to,
    templateName: req.body?.templateName,
    languageCode: req.body?.languageCode,
    bodyVariables: req.body?.bodyVariables,
    buttonUrlVariable: req.body?.buttonUrlVariable,
    trackerContext: req.body?.trackerContext
  });

  return res.status(result.status).json(result.finalBody);
});

app.post("/api/sms/clicksend", async (req, res) => {
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
    return res.status(500).json({
      error: "Failed to send SMS via ClickSend",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.post("/api/templates/send-feedback-request", async (req, res) => {
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
    return res.status(500).json({
      error: "Failed to send feedback request template",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/templates", async (req, res) => {
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

app.post("/api/messages/schedule", async (req, res) => {
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

    const id = `sched-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    const item = { id, to, templateName, languageCode, bodyVariables, scheduledAt, status: "pending", createdAt: new Date().toISOString() };
    scheduledMessages.push(item);
    return res.json(item);
  } catch (error) {
    return res.status(500).json({ error: "Failed to schedule message", details: error instanceof Error ? error.message : "Unknown error" });
  }
});

app.get("/api/messages/scheduled", (_req, res) => {
  return res.json({ data: scheduledMessages });
});

app.post("/api/messages/process-scheduled", async (req, res) => {
  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "Unauthorized cron trigger" });
  }

  try {
    const stats = await processScheduledMessages();
    return res.json({
      ok: true,
      ...stats,
      pending: scheduledMessages.filter((item) => item.status === "pending").length,
      at: new Date().toISOString()
    });
  } catch (error) {
    return res.status(500).json({
      error: "Failed to process scheduled messages",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/cron/auto-notificacao-envio", async (req, res) => {
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
    return res.status(500).json({
      error: "Failed to run auto notificacao envio cron",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  } finally {
    autoNotificacaoEnvioRunning = false;
  }
});

app.get("/api/cron/auto-notificacao-envio-em-transporte", async (req, res) => {
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
    return res.status(500).json({
      error: "Failed to run auto notificacao envio em transporte cron",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  } finally {
    autoNotificacaoEnvioTransporteRunning = false;
  }
});

app.get("/api/cron/auto-notificacao-incidencia", async (req, res) => {
  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "Unauthorized cron trigger" });
  }

  const forceRaw = String(req.query?.force || "").trim().toLowerCase();
  const forceRun = ["1", "true", "yes", "on"].includes(forceRaw);

  const result = await maybeRunAutoNotificacaoIncidenciaSchedule({ forceRun });
  if (!result || result.ok === false) {
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
    return res.status(500).json({
      error: "Failed to run auto notificacao envio dry-run",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

// ── Send media message (upload + send in one call) ─────────────────────────
app.post("/api/messages/send-media", upload.single("file"), async (req, res) => {
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
    if (!mediaId) return res.status(500).json({ error: "Media upload returned no ID" });

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
    return res.status(500).json({ error: "Failed to send media", details: error instanceof Error ? error.message : "Unknown error" });
  }
});

app.get("/api/media/:mediaId", async (req, res) => {
  try {
    const mediaId = String(req.params?.mediaId || "").trim();
    if (!mediaId) {
      return res.status(400).json({ error: "Missing media id" });
    }

    const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
    const token = requiredEnv("WHATSAPP_ACCESS_TOKEN");

    const metaResponse = await fetch(`https://graph.facebook.com/${apiVersion}/${encodeURIComponent(mediaId)}`, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`
      }
    });

    const metaBody = await metaResponse.json().catch(() => ({}));
    if (!metaResponse.ok || !metaBody?.url) {
      return res.status(metaResponse.ok ? 404 : metaResponse.status).json({
        error: "Failed to resolve media URL",
        details: metaBody
      });
    }

    const binaryResponse = await fetch(String(metaBody.url), {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`
      }
    });

    if (!binaryResponse.ok) {
      const details = await binaryResponse.text().catch(() => "");
      return res.status(binaryResponse.status).json({ error: "Failed to fetch media binary", details });
    }

    const mimeType = String(binaryResponse.headers.get("content-type") || metaBody?.mime_type || "application/octet-stream");
    const arrayBuffer = await binaryResponse.arrayBuffer();

    res.setHeader("Content-Type", mimeType);
    res.setHeader("Cache-Control", "private, max-age=300");
    return res.send(Buffer.from(arrayBuffer));
  } catch (error) {
    return res.status(500).json({
      error: "Failed to proxy media",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/logs", async (req, res) => {
  if ((!supabaseEnabled || !supabase) && (!pgEnabled || !pgPool)) {
    return res.json({ data: [], warning: "supabase_not_configured" });
  }

  try {
    const rawLimit = String(req.query?.limit || "100").trim().toLowerCase();
    const fetchAll = rawLimit === "all" || rawLimit === "unlimited";
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
      let rows = [];

      if (fetchAll) {
        // Use a conservative page size so this works even when Supabase API max rows is configured to 100.
        const batchSize = 100;
        let offset = 0;

        while (true) {
          const { data, error } = await supabase
            .from("whatsapp_logs")
            .select("id,created_at,direction,channel,to_number,contact_name,message_text,template_name,status,api_message_id,payload")
            .neq("channel", STATE_FALLBACK_CHANNEL)
            .order("created_at", { ascending: false })
            .range(offset, offset + batchSize - 1);

          if (error) {
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
      } else {
        const { data, error } = await supabase
          .from("whatsapp_logs")
          .select("id,created_at,direction,channel,to_number,contact_name,message_text,template_name,status,api_message_id,payload")
          .neq("channel", STATE_FALLBACK_CHANNEL)
          .order("created_at", { ascending: false })
          .limit(limit);

        if (error) {
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

    const pgResult = fetchAll
      ? await pgPool.query(
        `select id, created_at, direction, channel, to_number, contact_name, message_text, template_name, status, api_message_id, payload
        from public.whatsapp_logs
        where channel is distinct from $1
        order by created_at desc`,
        [STATE_FALLBACK_CHANNEL]
      )
      : await pgPool.query(
        `select id, created_at, direction, channel, to_number, contact_name, message_text, template_name, status, api_message_id, payload
        from public.whatsapp_logs
        where channel is distinct from $2
        order by created_at desc
        limit $1`,
        [limit, STATE_FALLBACK_CHANNEL]
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
    return res.status(500).json({
      error: "Failed to fetch logs",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/tms/dashboard", async (_req, res) => {
  try {
    const data = await fetchTmsDashboardData();
    return res.json({ ok: true, data });
  } catch (error) {
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

app.get("/api/customers", async (req, res) => {
  try {
    const page = Number(req.query?.page || 1);
    const limit = Number(req.query?.limit || 100);
    const search = String(req.query?.search || "").trim();
    const data = await fetchTmsCustomersData({ page, limit, search });
    return res.json({ ok: true, data: data.rows, meta: data.meta });
  } catch (error) {
    return res.status(500).json({
      error: "Failed to fetch customers",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/tms/delivered", async (req, res) => {
  try {
    const requestedPage = Number(req.query?.page || 1);
    const requestedLimit = Number(req.query?.limit || 250);

    const data = await fetchTmsDeliveredShipmentsData({
      page: requestedPage,
      limit: requestedLimit
    });

    return res.json({ ok: true, data: data.rows, meta: data.meta });
  } catch (error) {
    return res.status(500).json({
      error: "Failed to fetch delivered TMS shipments",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/tms/in-distribution", async (req, res) => {
  try {
    const requestedPage = Number(req.query?.page || 1);
    const requestedLimit = Number(req.query?.limit || 250);

    const data = await fetchTmsInDistributionShipmentsData({
      page: requestedPage,
      limit: requestedLimit
    });

    return res.json({ ok: true, data: data.rows, meta: data.meta });
  } catch (error) {
    return res.status(500).json({
      error: "Failed to fetch in-distribution TMS shipments",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/tms/incidencias", async (req, res) => {
  try {
    const requestedPage = Number(req.query?.page || 1);
    const requestedLimit = Number(req.query?.limit || 250);

    const data = await fetchTmsIncidenceShipmentsData({
      page: requestedPage,
      limit: requestedLimit
    });

    return res.json({ ok: true, data: data.rows, meta: data.meta });
  } catch (error) {
    return res.status(500).json({
      error: "Failed to fetch incidence TMS shipments",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/tms/in-transport", async (req, res) => {
  try {
    const requestedPage = Number(req.query?.page || 1);
    const requestedLimit = Number(req.query?.limit || 250);

    const data = await fetchTmsInTransportShipmentsData({
      page: requestedPage,
      limit: requestedLimit
    });

    return res.json({ ok: true, data: data.rows, meta: data.meta });
  } catch (error) {
    return res.status(500).json({
      error: "Failed to fetch in-transport TMS shipments",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.get("/api/tms/em-transporte", async (req, res) => {
  try {
    const requestedPage = Number(req.query?.page || 1);
    const requestedLimit = Number(req.query?.limit || 250);

    const data = await fetchTmsInTransportShipmentsData({
      page: requestedPage,
      limit: requestedLimit
    });

    return res.json({ ok: true, data: data.rows, meta: data.meta });
  } catch (error) {
    return res.status(500).json({
      error: "Failed to fetch in-transport TMS shipments",
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

app.get("/api/state", async (_req, res) => {
  const defaults = defaultWorkspaceState();

  if ((!supabaseEnabled || !supabase) && (!pgEnabled || !pgPool)) {
    return res.json({ data: defaults, warning: "persistent_state_not_configured" });
  }

  try {
    if (supabaseEnabled && supabase) {
      try {
        const { data, error } = await supabase
          .from("workspace_state")
          .select("key,value")
          .in("key", PERSISTENCE_KEYS);

        if (!error) {
          const next = { ...defaults };
          for (const row of data || []) {
            if (row?.key && Object.prototype.hasOwnProperty.call(next, row.key)) {
              next[row.key] = row.value;
            }
          }

          try {
            const contacts = await loadContactsFromDedicatedTable();
            if (Object.keys(contacts).length > 0) {
              next.contacts = contacts;
            }
          } catch {}

          return res.json({ data: next });
        }
      } catch {}

      try {
        const snapshot = await getWorkspaceStateFromLogsFallback();
        if (snapshot) {
          return res.json({ data: snapshot, warning: "workspace_state_table_unavailable_using_log_fallback" });
        }
      } catch {}
    }

    if (pgEnabled && pgPool) {
      await ensurePersistentStateTable();
      const { rows } = await pgPool.query(
        `select key, value from public.workspace_state where key = any($1::text[])`,
        [PERSISTENCE_KEYS]
      );
      const next = { ...defaults };
      for (const row of rows || []) {
        if (row?.key && Object.prototype.hasOwnProperty.call(next, row.key)) {
          next[row.key] = row.value;
        }
      }

      try {
        const contacts = await loadContactsFromDedicatedTable();
        if (Object.keys(contacts).length > 0) {
          next.contacts = contacts;
        }
      } catch {}

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
  if ((!supabaseEnabled || !supabase) && (!pgEnabled || !pgPool)) {
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
      try {
        const payload = updates.map((item) => ({ key: item.key, value: item.value ?? null }));
        const { error } = await supabase
          .from("workspace_state")
          .upsert(payload, { onConflict: "key" });

        if (!error) {
          if (contactsUpdate) {
            try {
              await syncContactsToDedicatedTable(contactsUpdate.value);
            } catch {}
          }
          return res.json({ ok: true, updated: updates.length, via: "supabase" });
        }
      } catch {}

      try {
        const existing = (await getWorkspaceStateFromLogsFallback()) || defaultWorkspaceState();
        const next = { ...existing };
        for (const item of updates) {
          next[item.key] = item.value ?? null;
        }
        await writeWorkspaceStateToLogsFallback(next);

        if (contactsUpdate) {
          try {
            await syncContactsToDedicatedTable(contactsUpdate.value);
          } catch {}
        }

        return res.json({ ok: true, updated: updates.length, via: "supabase_logs_fallback", warning: "workspace_state_table_unavailable_using_log_fallback" });
      } catch {}
    }

    if (pgEnabled && pgPool) {
      await ensurePersistentStateTable();
      for (const item of updates) {
        await pgPool.query(
          `insert into public.workspace_state (key, value, updated_at)
          values ($1, $2::jsonb, now())
          on conflict (key) do update set value = excluded.value, updated_at = now()`,
          [item.key, JSON.stringify(item.value ?? null)]
        );
      }

      if (contactsUpdate) {
        try {
          await syncContactsToDedicatedTable(contactsUpdate.value);
        } catch {}
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
    // Follow simple redirects
    if ((status === 301 || status === 302 || status === 307 || status === 308) && proxyRes.headers.location) {
      proxyReq.destroy();
      req.query.url = proxyRes.headers.location;
      return app._router.handle(req, res, () => {});
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
    return res.status(500).json({ error: "Failed to check call permissions", details: error instanceof Error ? error.message : "Unknown error" });
  }
});

app.post("/api/calls/request-permission", async (req, res) => {
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
    return res.status(500).json({ error: "Failed to request call permission", details: error instanceof Error ? error.message : "Unknown error" });
  }
});

app.post("/api/calls/manage", async (req, res) => {
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
