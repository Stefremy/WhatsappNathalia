import "dotenv/config";
import express from "express";
import cors from "cors";
import multer from "multer";
import { Client as NotionClient } from "@notionhq/client";

const app = express();
const port = Number(process.env.PORT || 3001);

app.use(cors());
app.use(express.json());
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 }
});

const notion = new NotionClient({ auth: process.env.NOTION_API_KEY });
const notionEnabled = String(process.env.NOTION_ENABLED || "false").toLowerCase() === "true";

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

    const finalBody =
      notionWarning && typeof responseBody === "object" && responseBody !== null
        ? { ...responseBody, _notionWarning: notionWarning }
        : responseBody;

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

    const payload = {
      messaging_product: "whatsapp",
      recipient_type: "individual",
      to,
      type: "template",
      template: {
        name: templateName,
        language: {
          code: languageCode
        },
        components
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
        text: `Template ${templateName} | Vars: ${bodyVariables.join(" | ")}`,
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
    const limit = String(req.query.limit || "50").trim();
    const fetchAll = String(req.query.fetchAll || "true").toLowerCase() !== "false";

    if (!phoneNumberId) {
      return res.status(400).json({
        error: "Query param 'phoneNumberId' (or env WHATSAPP_PHONE_NUMBER_ID) is required."
      });
    }

    const phoneLookupUrl = `https://graph.facebook.com/${apiVersion}/${phoneNumberId}?fields=whatsapp_business_account`;
    const phoneLookupResponse = await fetch(phoneLookupUrl, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`
      }
    });

    const phoneLookupBody = await phoneLookupResponse.json();

    if (!phoneLookupResponse.ok) {
      return res.status(phoneLookupResponse.status).json(phoneLookupBody);
    }

    const wabaId = phoneLookupBody?.whatsapp_business_account?.id;

    if (!wabaId) {
      return res.status(404).json({
        error: "Could not resolve WhatsApp Business Account for this phone number ID.",
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
  if (!notionEnabled) {
    return res.sendStatus(200);
  }

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
          await updateNotionMessageStatus({
            messageId: String(messageId),
            status: String(status),
            rawStatus: statusEvent
          });
        }
      }

      for (const inboundMessage of messages) {
        const from = String(inboundMessage?.from || "").trim();
        if (!from) {
          continue;
        }

        const contact = contacts.find((item) => String(item?.wa_id || "") === from) || null;
        await logInboundMessage({ from, message: inboundMessage, contact });
      }
    }

    return res.sendStatus(200);
  } catch (_error) {
    return res.sendStatus(500);
  }
});

app.listen(port, () => {
  console.log(`Backend running on http://localhost:${port}`);
});
