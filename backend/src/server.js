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

  return `[${type}]`;
}

async function logInboundMessage({ from, message, contact }) {
  if (!notionEnabled) {
    return null;
  }

  const inboundMessageId = String(message?.id || "").trim();
  if (!inboundMessageId) {
    return null;
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

    const finalBody =
      notionWarning && typeof responseBody === "object" && responseBody !== null
        ? { ...responseBody, _notionWarning: notionWarning }
        : responseBody;

    if (supabaseWarning && typeof finalBody === "object" && finalBody !== null) {
      finalBody._supabaseWarning = supabaseWarning;
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

    const finalBody =
      notionWarning && typeof responseBody === "object" && responseBody !== null
        ? { ...responseBody, _notionWarning: notionWarning }
        : responseBody;

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

    const finalBody =
      notionWarning && typeof responseBody === "object" && responseBody !== null
        ? { ...responseBody, _notionWarning: notionWarning }
        : responseBody;

    if (supabaseWarning && typeof finalBody === "object" && finalBody !== null) {
      finalBody._supabaseWarning = supabaseWarning;
    }

    return res.status(response.ok ? 200 : response.status).json(finalBody);
  } catch (error) {
    return res.status(500).json({
      error: "Failed to send generic template message",
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

    const finalBody =
      notionWarning && typeof responseBody === "object" && responseBody !== null
        ? { ...responseBody, _notionWarning: notionWarning }
        : responseBody;

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

app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === process.env.WHATSAPP_WEBHOOK_VERIFY_TOKEN) {
    return res.status(200).send(challenge);
  }

  return res.sendStatus(403);
});

app.post("/webhook", async (req, res) => {
  // Acknowledge immediately so Meta doesn't retry
  res.sendStatus(200);

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

          if (notionEnabled) {
            await updateNotionMessageStatus({
              messageId: String(messageId),
              status: String(status),
              rawStatus: statusEvent
            });
          }
        }
      }

      if (notionEnabled) {
        for (const inboundMessage of messages) {
          const from = String(inboundMessage?.from || "").trim();
          if (!from) continue;
          const contact = contacts.find((item) => String(item?.wa_id || "") === from) || null;
          await logInboundMessage({ from, message: inboundMessage, contact });
        }
      }
    }
  } catch (_error) {
    // response already sent
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
        .select("id,created_at,direction,channel,to_number,contact_name,message_text,template_name,status,api_message_id")
        .neq("channel", STATE_FALLBACK_CHANNEL)
        .order("created_at", { ascending: false })
        .limit(limit);

      if (error) {
        return res.status(500).json({ error: "Failed to fetch logs", details: error.message });
      }

      return res.json({ data: Array.isArray(data) ? data : [] });
    }

    const { rows } = await pgPool.query(
      `select id, created_at, direction, channel, to_number, contact_name, message_text, template_name, status, api_message_id
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
