import "dotenv/config";
import express from "express";
import cors from "cors";
import multer from "multer";
import { Client as NotionClient } from "@notionhq/client";
import { createClient } from "@supabase/supabase-js";
import { Pool } from "pg";
import https from "https";
import http from "http";

const app = express();
const port = Number(process.env.PORT || 3001);

app.use(cors());
app.use(express.json());
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 }
});

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

async function processScheduledMessages() {
  const now = Date.now();
  for (const item of scheduledMessages) {
    if (item.status !== "pending") continue;
    if (new Date(item.scheduledAt).getTime() > now) continue;
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
    } catch {
      item.status = "failed";
    }
    broadcastSSE("scheduled_sent", { id: item.id, status: item.status });
  }
}

setInterval(processScheduledMessages, 15000);

const notion = new NotionClient({ auth: process.env.NOTION_API_KEY });
const notionEnabled = String(process.env.NOTION_ENABLED || "false").toLowerCase() === "true";
const notionTrackerApiKey = String(process.env.NOTION_TRACKER_API_KEY || process.env.NOTION_API_KEY || "").trim();
const notionTracker = notionTrackerApiKey ? new NotionClient({ auth: notionTrackerApiKey }) : null;
const notionTrackerDatabaseId = String(process.env.NOTION_TRACKER_DATABASE_ID || "").trim();
const notionTrackerEnabled = Boolean(notionTracker && notionTrackerDatabaseId);
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
  "calendar_events"
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
    calendar_events: []
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

function normalizeRecipient(input) {
  return String(input ?? "").replace(/\D/g, "");
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
      status: normalizeDatatableTextCell(row?.status || row?.status_id || ""),
      incidence: normalizeDatatableTextCell(row?.last_incidence || ""),
      hasCharge,
      chargeAmount: hasChargeByAmount ? chargeAmountNumber.toFixed(2) : ""
    };
  });
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
    incidenceShipmentsBody.set("status", "9");

    const incidenceShipmentsRes = await fetch(incidenceShipmentsEndpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        Referer: `${baseUrl}/admin/shipments?status=9`,
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

function buildTemplateFallbackText({ templateName, bodyVariables = [], buttonUrlVariable = "" }) {
  const cleanTemplateName = String(templateName || "").trim();
  const cleanVars = Array.isArray(bodyVariables)
    ? bodyVariables.map((value) => String(value ?? "").trim()).filter(Boolean)
    : [];
  const cleanButtonVar = String(buttonUrlVariable || "").trim();
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

  await notionTracker.pages.create({
    parent: { database_id: notionTrackerDatabaseId },
    properties: {
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

app.post("/api/templates/send-generic", async (req, res) => {
  try {
    const to = normalizeRecipient(req.body?.to || "");
    const templateName = String(req.body?.templateName || "").trim();
    const languageCode = String(
      req.body?.languageCode || process.env.WHATSAPP_TEMPLATE_LANGUAGE || "pt_PT"
    ).trim();

    const bodyVariablesInput = Array.isArray(req.body?.bodyVariables)
      ? req.body.bodyVariables
      : [];
    const bodyVariables = bodyVariablesInput
      .map((value) => String(value ?? "").trim())
      .filter(Boolean);

    const buttonUrlVariable = String(req.body?.buttonUrlVariable || "").trim();
    const trackerContext = req.body?.trackerContext && typeof req.body.trackerContext === "object"
      ? {
          clientName: String(req.body.trackerContext.clientName || "").trim(),
          parcelId: String(req.body.trackerContext.parcelId || "").trim(),
          messageType: String(req.body.trackerContext.messageType || "").trim(),
          notes: String(req.body.trackerContext.notes || "").trim()
        }
      : null;

    if (!to || !templateName) {
      return res.status(400).json({
        error: "Fields 'to' and 'templateName' are required."
      });
    }

    const apiVersion = process.env.WHATSAPP_API_VERSION || "v23.0";
    const phoneNumberId = requiredEnv("WHATSAPP_PHONE_NUMBER_ID");
    const token = requiredEnv("WHATSAPP_ACCESS_TOKEN");

    const endpoint = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}/messages`;
    const components = [];

    if (bodyVariables.length > 0) {
      components.push({
        type: "body",
        parameters: bodyVariables.map((text) => ({ type: "text", text }))
      });
    }

    if (buttonUrlVariable) {
      components.push({
        type: "button",
        sub_type: "url",
        index: "0",
        parameters: [{ type: "text", text: buttonUrlVariable }]
      });
    }

    const templatePayload = {
      name: templateName,
      language: {
        code: languageCode
      }
    };

    if (components.length > 0) {
      templatePayload.components = components;
    }

    const payload = {
      messaging_product: "whatsapp",
      recipient_type: "individual",
      to,
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
        to,
        text: `Template ${templateName} | Vars: ${bodyVariables.join(" | ")}`,
        status: response.ok ? "accepted" : `failed_${response.status}`,
        messageId,
        rawResponse: responseBody
      })
    );

    const notionTrackerWarning = await safeNotionTrackerLog(() =>
      createNotionTrackerRow({
        to,
        templateName,
        bodyVariables,
        status: response.ok ? "accepted" : `failed_${response.status}`,
        trackerContext,
        rawResponse: responseBody
      })
    );

    const supabaseWarning = await safeSupabaseLog(() =>
      createSupabaseLogRow({
        direction: "out",
        channel: "template",
        to,
        messageText: bodyVariables.join(" | "),
        templateName,
        status: response.ok ? "accepted" : `failed_${response.status}`,
        apiMessageId: messageId,
        payload: responseBody
      })
    );

    const smsFallbackMessage = buildTemplateFallbackText({
      templateName,
      bodyVariables,
      buttonUrlVariable
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

    if (supabaseWarning && typeof finalBody === "object" && finalBody !== null) {
      finalBody._supabaseWarning = supabaseWarning;
    }

    if (notionTrackerWarning && typeof finalBody === "object" && finalBody !== null) {
      finalBody._notionTrackerWarning = notionTrackerWarning;
    }

    if (smsFallback && typeof finalBody === "object" && finalBody !== null) {
      finalBody._smsFallback = smsFallback;
    }

    return res.status(response.ok ? 200 : response.status).json(finalBody);
  } catch (error) {
    return res.status(500).json({
      error: "Failed to send generic template message",
      details: error instanceof Error ? error.message : "Unknown error"
    });
  }
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
    const requestedLimit = Number(req.query?.limit || 100);
    const limit = Number.isFinite(requestedLimit)
      ? Math.min(Math.max(Math.trunc(requestedLimit), 1), 500)
      : 100;

    if (supabaseEnabled && supabase) {
      const { data, error } = await supabase
        .from("whatsapp_logs")
        .select("id,created_at,direction,channel,to_number,contact_name,message_text,template_name,status,api_message_id,payload")
        .neq("channel", STATE_FALLBACK_CHANNEL)
        .order("created_at", { ascending: false })
        .limit(limit);

      if (error) {
        return res.status(500).json({ error: "Failed to fetch logs", details: error.message });
      }

      return res.json({ data: Array.isArray(data) ? data : [] });
    }

    const { rows } = await pgPool.query(
      `select id, created_at, direction, channel, to_number, contact_name, message_text, template_name, status, api_message_id, payload
      from public.whatsapp_logs
      where channel is distinct from $2
      order by created_at desc
      limit $1`,
      [limit, STATE_FALLBACK_CHANNEL]
    );
    return res.json({ data: rows || [] });
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

if (!process.env.VERCEL) {
  app.listen(port, () => {
    console.log(`Backend running on http://localhost:${port}`);
  });
}

export default app;
