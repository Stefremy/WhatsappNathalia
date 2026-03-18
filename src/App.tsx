import { FormEvent, useEffect, useMemo, useState } from "react";

const docsFacts = [
  { label: "Version", value: "v23.0" },
  { label: "Method", value: "POST" },
  { label: "Endpoint", value: "/{Version}/{Phone-Number-ID}/messages" },
  { label: "Auth", value: "Bearer token" }
];

type ConversationMessage = {
  id: string;
  direction: "in" | "out";
  text: string;
  time: string;
  apiMessageId?: string;
  deliveryStatus?: "sent" | "delivered" | "read" | "failed";
};

type ScheduledItem = {
  id: string;
  to: string;
  templateName: string;
  languageCode: string;
  bodyVariables: string[];
  scheduledAt: string;
  status: string;
};

type ConversationContact = {
  id: string;
  name: string;
  phone: string;
  lastAt: string;
  unread: number;
  messages: ConversationMessage[];
};

type MetaTemplate = {
  id?: string;
  name: string;
  language?: string;
  status?: string;
  components?: Array<{ type?: string; text?: string }>;
};

type TemplateHistoryItem = {
  id: string;
  to: string;
  templateName: string;
  previewText: string;
  time: string;
  status: string;
};

const initialConversations: ConversationContact[] = [];

function digitsOnly(value: string) {
  return String(value || "").replace(/\D/g, "");
}

function contactDisplayName(phone: string) {
  const normalized = digitsOnly(phone);
  return normalized ? `Contact ${normalized}` : "Unknown contact";
}

function resolveContactName(phone: string, savedContacts: Record<string, string>) {
  const digits = digitsOnly(phone);
  return savedContacts[digits] || contactDisplayName(digits);
}

function deliveryTickMark(status?: ConversationMessage["deliveryStatus"]) {
  if (status === "read") return " ✓✓";
  if (status === "delivered") return " ✓✓";
  if (status === "sent") return " ✓";
  if (status === "failed") return " ✗";
  return "";
}

function nowLabel() {
  return new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function extractBodyTemplateText(template: MetaTemplate | null) {
  if (!template?.components) {
    return "";
  }

  const body = template.components.find((component) => String(component?.type || "").toLowerCase() === "body");
  return String(body?.text || "");
}

function fillTemplateBody(templateText: string, values: string[]) {
  return templateText.replace(/\{\{(\d+)\}\}/g, (_match, index) => {
    const idx = Number(index) - 1;
    return values[idx] || `{{${index}}}`;
  });
}

function extractPlaceholderIndexes(input: string) {
  const matches = [...String(input || "").matchAll(/\{\{(\d+)\}\}/g)];
  return [...new Set(matches.map((match) => Number(match[1])).filter(Number.isFinite))].sort(
    (a, b) => a - b
  );
}

function templateNeedsUrlButtonVariable(template: MetaTemplate | null) {
  const components = Array.isArray(template?.components) ? template.components : [];
  const serialized = JSON.stringify(components);
  const hasUrlButton = /"type"\s*:\s*"?url"?/i.test(serialized) || /"sub_type"\s*:\s*"?url"?/i.test(serialized);
  const hasUrlPlaceholder = /url[^\n]*\{\{\d+\}\}/i.test(serialized);
  return hasUrlButton && hasUrlPlaceholder;
}

function App() {
  const [apiVersion, setApiVersion] = useState(
    import.meta.env.VITE_WHATSAPP_API_VERSION ?? "v23.0"
  );
  const [phoneNumberId, setPhoneNumberId] = useState(
    import.meta.env.VITE_WHATSAPP_PHONE_NUMBER_ID ?? "configured in backend"
  );
  const [wabaId, setWabaId] = useState(import.meta.env.VITE_WHATSAPP_BUSINESS_ACCOUNT_ID ?? "");
  const backendBaseUrl = (import.meta.env.VITE_BACKEND_BASE_URL?.trim() || "").replace(/\/$/, "");
  const apiUrl = (path: string) => (backendBaseUrl ? `${backendBaseUrl}${path}` : path);

  async function parseResponse(response: Response) {
    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      return response.json();
    }

    const text = await response.text();
    return { raw: text || "Empty response" };
  }
  const [toNumber, setToNumber] = useState(import.meta.env.VITE_DEFAULT_TO_NUMBER ?? "");
  const [messageText, setMessageText] = useState("Hello from Linke Cloud API frontend.");
  const [loading, setLoading] = useState(false);
  const [statusText, setStatusText] = useState("Idle");
  const [responseText, setResponseText] = useState("No request sent yet.");
  const [mediaFile, setMediaFile] = useState<File | null>(null);
  const [mediaLoading, setMediaLoading] = useState(false);
  const [mediaStatusText, setMediaStatusText] = useState("Idle");
  const [mediaResponseText, setMediaResponseText] = useState("No upload sent yet.");
  const [genericTo, setGenericTo] = useState(import.meta.env.VITE_DEFAULT_TO_NUMBER ?? "");
  const [genericTemplateName, setGenericTemplateName] = useState(
    import.meta.env.VITE_DEFAULT_TEMPLATE_NAME ?? "order_pickup_ctt"
  );
  const [genericLanguage, setGenericLanguage] = useState("pt_PT");
  const [genericBodyVars, setGenericBodyVars] = useState<Record<number, string>>({});
  const [genericButtonUrlVariable, setGenericButtonUrlVariable] = useState("");
  const [genericLoading, setGenericLoading] = useState(false);
  const [genericStatus, setGenericStatus] = useState("Idle");
  const [genericResponse, setGenericResponse] = useState("No generic template request sent yet.");
  const [templateHistory, setTemplateHistory] = useState<TemplateHistoryItem[]>([]);
  const [metaTemplates, setMetaTemplates] = useState<MetaTemplate[]>([]);
  const [metaTemplatesLoading, setMetaTemplatesLoading] = useState(false);
  const [metaTemplatesStatus, setMetaTemplatesStatus] = useState("Not loaded");
  const [conversations, setConversations] = useState<ConversationContact[]>(() => {
    try { return JSON.parse(localStorage.getItem("wa_conversations") || "[]") as ConversationContact[]; } catch { return []; }
  });
  const [activeConversationId, setActiveConversationId] = useState(() => {
    try {
      const c = JSON.parse(localStorage.getItem("wa_conversations") || "[]") as ConversationContact[];
      return c[0]?.id || "";
    } catch { return ""; }
  });
  const [emojiOpen, setEmojiOpen] = useState(false);

  // Feature 1: Contact book
  const [savedContacts, setSavedContacts] = useState<Record<string, string>>(() => {
    try { return JSON.parse(localStorage.getItem("wa_contacts") || "{}"); } catch { return {}; }
  });
  const [contactBookOpen, setContactBookOpen] = useState(false);
  const [newContactPhone, setNewContactPhone] = useState("");
  const [newContactName, setNewContactName] = useState("");

  // Feature 2: Template var presets
  const [templatePresets, setTemplatePresets] = useState<Record<string, Record<number, string>>>(() => {
    try { return JSON.parse(localStorage.getItem("wa_template_presets") || "{}"); } catch { return {}; }
  });

  // Feature 3: Bulk send
  const [bulkCsv, setBulkCsv] = useState("");
  const [bulkRows, setBulkRows] = useState<Array<{ phone: string; status: string }>>([]);
  const [bulkProgress, setBulkProgress] = useState({ sent: 0, total: 0 });
  const [bulkRunning, setBulkRunning] = useState(false);

  // Feature 4: Message scheduling
  const [scheduleAt, setScheduleAt] = useState("");
  const [useSchedule, setUseSchedule] = useState(false);
  const [scheduledItems, setScheduledItems] = useState<ScheduledItem[]>([]);

  // Feature 5: Delivery ticks via SSE
  const [messageStatuses, setMessageStatuses] = useState<Record<string, string>>({});

  // Feature 9: Sidebar search
  const [contactSearch, setContactSearch] = useState("");

  // Feature 10: Media in composer
  const [composeMedia, setComposeMedia] = useState<File | null>(null);
  const [composeMediaLoading, setComposeMediaLoading] = useState(false);

  const activeConversation = useMemo(
    () => conversations.find((item) => item.id === activeConversationId) || null,
    [activeConversationId, conversations]
  );

  const selectedMetaTemplate = useMemo(
    () => metaTemplates.find((template) => template.name === genericTemplateName) || null,
    [genericTemplateName, metaTemplates]
  );

  const selectedTemplateBody = useMemo(
    () => extractBodyTemplateText(selectedMetaTemplate),
    [selectedMetaTemplate]
  );

  const requiredBodyIndexes = useMemo(
    () => extractPlaceholderIndexes(selectedTemplateBody),
    [selectedTemplateBody]
  );

  const requiredBodyVarCount = requiredBodyIndexes.length;
  const needsUrlButtonVariable = useMemo(
    () => templateNeedsUrlButtonVariable(selectedMetaTemplate),
    [selectedMetaTemplate]
  );

  const previewBodyVars = useMemo(() => {
    const maxIndex = requiredBodyIndexes.length > 0 ? Math.max(...requiredBodyIndexes) : 0;
    const values = Array.from({ length: maxIndex }, () => "");
    requiredBodyIndexes.forEach((index) => {
      values[index - 1] = String(genericBodyVars[index] || "").trim();
    });
    return values;
  }, [genericBodyVars, requiredBodyIndexes]);

  const selectedTemplatePreview = useMemo(
    () => fillTemplateBody(selectedTemplateBody, previewBodyVars),
    [selectedTemplateBody, previewBodyVars]
  );

  useEffect(() => {
    setGenericBodyVars((current) => {
      const next: Record<number, string> = {};
      requiredBodyIndexes.forEach((index) => {
        next[index] = current[index] || "";
      });
      return next;
    });
  }, [requiredBodyIndexes]);

  function addEmoji(emoji: string) {
    setMessageText((current) => `${current}${emoji}`);
    setEmojiOpen(false);
  }

  // Feature 1: Contact book
  function saveContact() {
    const digits = digitsOnly(newContactPhone);
    if (!digits || !newContactName.trim()) return;
    setSavedContacts((prev) => ({ ...prev, [digits]: newContactName.trim() }));
    setConversations((prev) =>
      prev.map((c) =>
        digitsOnly(c.phone) === digits ? { ...c, name: newContactName.trim() } : c
      )
    );
    setNewContactPhone("");
    setNewContactName("");
  }

  function removeContact(digits: string) {
    setSavedContacts((prev) => {
      const { [digits]: _removed, ...rest } = prev;
      return rest;
    });
  }

  // Feature 2: Template var presets
  function savePreset() {
    if (!genericTemplateName || requiredBodyVarCount === 0) return;
    setTemplatePresets((prev) => ({ ...prev, [genericTemplateName]: { ...genericBodyVars } }));
  }

  function loadPreset() {
    const preset = templatePresets[genericTemplateName];
    if (preset) setGenericBodyVars({ ...preset });
  }

  // Feature 3: Bulk send
  async function runBulkSend() {
    if (bulkRunning || !bulkCsv.trim()) return;
    const parsed = bulkCsv
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean)
      .map((line) => {
        const cols = line.split(",").map((c) => c.trim());
        return { phone: digitsOnly(cols[0]), vars: cols.slice(1).filter(Boolean) };
      })
      .filter((r) => r.phone.length >= 7);
    if (!parsed.length) return;
    setBulkRunning(true);
    setBulkProgress({ sent: 0, total: parsed.length });
    setBulkRows([]);
    for (let i = 0; i < parsed.length; i++) {
      const { phone, vars } = parsed[i];
      try {
        const res = await fetch(apiUrl("/api/templates/send-generic"), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            to: phone,
            templateName: genericTemplateName,
            languageCode: genericLanguage,
            bodyVariables: vars.length ? vars : previewBodyVars
          })
        });
        setBulkRows((prev) => [...prev, { phone, status: res.ok ? "sent ✓" : `failed_${res.status}` }]);
      } catch {
        setBulkRows((prev) => [...prev, { phone, status: "network_error" }]);
      }
      setBulkProgress({ sent: i + 1, total: parsed.length });
    }
    setBulkRunning(false);
  }

  // Feature 10: Media send in composer
  async function sendMediaMessage() {
    if (!composeMedia || !toNumber.trim()) return;
    setComposeMediaLoading(true);
    setStatusText("Sending media...");
    try {
      const formData = new FormData();
      formData.append("file", composeMedia);
      formData.append("to", toNumber);
      const response = await fetch(apiUrl("/api/messages/send-media"), {
        method: "POST",
        body: formData
      });
      const data = await parseResponse(response);
      const sentTime = nowLabel();
      const mediaLabel = `[📎 ${composeMedia.name}]`;
      const apiMsgId = String(data?.messages?.[0]?.id || "").trim();
      const targetPhone = digitsOnly(toNumber.trim());
      setConversations((current) => {
        const existing = current.find(
          (c) => c.id === activeConversationId || digitsOnly(c.phone) === targetPhone
        );
        const msg: ConversationMessage = {
          id: `m-${Date.now()}`,
          direction: "out",
          text: mediaLabel,
          time: sentTime,
          apiMessageId: apiMsgId || undefined,
          deliveryStatus: apiMsgId ? "sent" : undefined
        };
        if (existing) {
          return current
            .map((c) => c.id !== existing.id ? c : { ...c, lastAt: sentTime, messages: [...c.messages, msg] })
            .sort((a, b) => (a.id === existing.id ? -1 : b.id === existing.id ? 1 : 0));
        }
        const created: ConversationContact = {
          id: `c-${Date.now()}`,
          name: resolveContactName(targetPhone, savedContacts),
          phone: targetPhone,
          unread: 0,
          lastAt: sentTime,
          messages: [msg]
        };
        setActiveConversationId(created.id);
        return [created, ...current];
      });
      setStatusText(response.ok ? "Media sent" : `Failed (${response.status})`);
      if (response.ok) setComposeMedia(null);
    } catch {
      setStatusText("Media send failed");
    } finally {
      setComposeMediaLoading(false);
    }
  }

  async function fetchMetaTemplates() {
    if (!phoneNumberId.trim()) {
      setMetaTemplatesStatus("Phone number ID is required to fetch templates");
      return;
    }

    setMetaTemplatesLoading(true);
    setMetaTemplatesStatus("Loading templates...");

    try {
      const query = new URLSearchParams({
        fetchAll: "true",
        limit: "100"
      });

      if (phoneNumberId.trim()) {
        query.set("phoneNumberId", phoneNumberId.trim());
      }

      if (wabaId.trim()) {
        query.set("wabaId", wabaId.trim());
      }

      const response = await fetch(apiUrl(`/api/templates?${query.toString()}`));
      const data = await parseResponse(response);

      if (!response.ok) {
        setMetaTemplatesStatus(`Failed to load templates (${response.status})`);
        return;
      }

      const rows = Array.isArray(data?.data) ? data.data : [];
      const approved = rows
        .filter((item) => String(item?.status || "").toUpperCase() === "APPROVED")
        .map((item) => ({
          id: String(item?.id || ""),
          name: String(item?.name || ""),
          language: String(item?.language || "pt_PT"),
          status: String(item?.status || ""),
          components: Array.isArray(item?.components) ? item.components : []
        }))
        .filter((item) => item.name);

      setMetaTemplates(approved);
      setMetaTemplatesStatus(
        approved.length > 0 ? `Loaded ${approved.length} approved templates` : "No approved templates found"
      );

      if (approved.length > 0 && !approved.some((item) => item.name === genericTemplateName)) {
        setGenericTemplateName(approved[0].name);
        setGenericLanguage(approved[0].language || "pt_PT");
      }
    } catch (error) {
      setMetaTemplatesStatus(error instanceof Error ? error.message : "Failed to load templates");
    } finally {
      setMetaTemplatesLoading(false);
    }
  }

  useEffect(() => {
    fetchMetaTemplates();
  }, [phoneNumberId, wabaId]);

  // Persist conversations
  useEffect(() => {
    try { localStorage.setItem("wa_conversations", JSON.stringify(conversations)); } catch {}
  }, [conversations]);

  // Persist saved contacts
  useEffect(() => {
    try { localStorage.setItem("wa_contacts", JSON.stringify(savedContacts)); } catch {}
  }, [savedContacts]);

  // Persist template presets
  useEffect(() => {
    try { localStorage.setItem("wa_template_presets", JSON.stringify(templatePresets)); } catch {}
  }, [templatePresets]);

  // SSE – delivery status ticks & scheduled_sent events
  useEffect(() => {
    const url = apiUrl("/api/events");
    const evtSource = new EventSource(url);
    evtSource.addEventListener("status", (e) => {
      try {
        const { messageId, status } = JSON.parse((e as MessageEvent).data);
        if (messageId && status) {
          setMessageStatuses((prev) => ({ ...prev, [messageId]: status }));
          setConversations((prev) =>
            prev.map((c) => ({
              ...c,
              messages: c.messages.map((m) =>
                m.apiMessageId === messageId
                  ? { ...m, deliveryStatus: status as ConversationMessage["deliveryStatus"] }
                  : m
              )
            }))
          );
        }
      } catch {}
    });
    evtSource.addEventListener("scheduled_sent", (e) => {
      try {
        const data = JSON.parse((e as MessageEvent).data);
        setScheduledItems((prev) =>
          prev.map((item) => item.id === data.id ? { ...item, status: data.status } : item)
        );
      } catch {}
    });
    return () => evtSource.close();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Load scheduled items from backend on mount
  useEffect(() => {
    fetch(apiUrl("/api/messages/scheduled"))
      .then((r) => r.json())
      .then((d) => { if (Array.isArray(d?.data)) setScheduledItems(d.data); })
      .catch(() => {});
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const filteredConversations = useMemo(() => {
    const q = contactSearch.trim().toLowerCase();
    if (!q) return conversations;
    return conversations.filter(
      (c) =>
        resolveContactName(c.phone, savedContacts).toLowerCase().includes(q) ||
        c.phone.includes(contactSearch.trim())
    );
  }, [conversations, contactSearch, savedContacts]);

  const endpoint = useMemo(() => {
    const cleanVersion = apiVersion.trim() || "v23.0";
    const cleanPhoneId = phoneNumberId.trim() || "configured in backend";
    return `https://graph.facebook.com/${cleanVersion}/${cleanPhoneId}/messages`;
  }, [apiVersion, phoneNumberId]);

  const payload = useMemo(
    () => ({
      messaging_product: "whatsapp",
      recipient_type: "individual",
      to: toNumber,
      type: "text",
      text: {
        body: messageText
      }
    }),
    [messageText, toNumber]
  );

  async function sendMessage(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!toNumber.trim() || !messageText.trim()) {
      setStatusText("Missing required fields");
      setResponseText("Recipient number and message are required.");
      return;
    }

    setLoading(true);
    setStatusText("Sending...");

    try {
      const response = await fetch(apiUrl("/api/messages/send"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          to: toNumber,
          text: messageText
        })
      });

      const data = await parseResponse(response);
      const sentTime = nowLabel();
      const resolvedWaId = String(data?.contacts?.[0]?.wa_id || "").trim();
      const targetPhone = digitsOnly(resolvedWaId || toNumber.trim());
      const targetName = resolveContactName(targetPhone, savedContacts);
      const apiMsgId = String(data?.messages?.[0]?.id || "").trim();
      let nextActiveConversationId = activeConversationId;

      setConversations((current) => {
        const matchingById = current.find((item) => item.id === activeConversationId);
        const matching =
          matchingById || current.find((item) => digitsOnly(item.phone) === digitsOnly(targetPhone));

        const nextMessage: ConversationMessage = {
          id: `m-${Date.now()}`,
          direction: "out",
          text: messageText,
          time: sentTime,
          apiMessageId: apiMsgId || undefined,
          deliveryStatus: apiMsgId ? "sent" : undefined
        };

        if (matching) {
          return current
            .map((item) => {
              if (item.id !== matching.id) {
                return item;
              }

              return {
                ...item,
                name: targetName,
                phone: targetPhone,
                lastAt: sentTime,
                messages: [...item.messages, nextMessage]
              };
            })
            .sort((a, b) => (a.id === matching.id ? -1 : b.id === matching.id ? 1 : 0));
        }

        const created: ConversationContact = {
          id: `c-${Date.now()}`,
          name: targetName,
          phone: targetPhone,
          unread: 0,
          lastAt: sentTime,
          messages: [nextMessage]
        };

        nextActiveConversationId = created.id;
        return [created, ...current];
      });

      if (nextActiveConversationId) {
        setActiveConversationId(nextActiveConversationId);
      }

      setStatusText(response.ok ? "Accepted by API (delivery pending)" : `Failed (${response.status})`);
      setResponseText(JSON.stringify(data, null, 2));
      if (response.ok) {
        setMessageText("");
      }
    } catch (error) {
      setStatusText("Backend unreachable");
      setResponseText(
        error instanceof Error
          ? `${error.message}\n\nVerify backend is running and reachable at ${backendBaseUrl || "same-origin /api"}.`
          : `Unknown error\n\nVerify backend is running and reachable at ${backendBaseUrl || "same-origin /api"}.`
      );
    } finally {
      setLoading(false);
    }
  }

  const curlCommand = [
    `curl -X POST \"${apiUrl("/api/messages/send")}\"`,
    '  -H "Content-Type: application/json"',
    `  -d '${JSON.stringify({ to: toNumber, text: messageText })}'`
  ].join("\n");

  async function uploadMedia(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!mediaFile) {
      setMediaStatusText("Missing file");
      setMediaResponseText("Choose a file first.");
      return;
    }

    setMediaLoading(true);
    setMediaStatusText("Uploading...");

    try {
      const formData = new FormData();
      formData.append("file", mediaFile);
      formData.append("messaging_product", "whatsapp");

      const response = await fetch(apiUrl("/api/media/upload"), {
        method: "POST",
        body: formData
      });

      const data = await response.json();
      setMediaStatusText(response.ok ? "Media uploaded" : `Failed (${response.status})`);
      setMediaResponseText(JSON.stringify(data, null, 2));
    } catch (error) {
      setMediaStatusText("Network error");
      setMediaResponseText(error instanceof Error ? error.message : "Unknown error");
    } finally {
      setMediaLoading(false);
    }
  }

  const mediaCurlCommand = [
    `curl -X POST "${apiUrl("/api/media/upload")}"`,
    '  -H "Content-Type: multipart/form-data"',
    '  -F "file=@/path/to/file.jpg"',
    '  -F "messaging_product=whatsapp"'
  ].join("\n");

  async function sendGenericTemplate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    const missingIndexes = requiredBodyIndexes.filter(
      (index) => !String(genericBodyVars[index] || "").trim()
    );

    const variables = previewBodyVars;

    if (!genericTo.trim() || !genericTemplateName.trim()) {
      setGenericStatus("Missing required fields");
      setGenericResponse("Recipient and template name are required.");
      return;
    }

    if (missingIndexes.length > 0) {
      setGenericStatus("Missing template variables");
      setGenericResponse(
        `Fill required variables for indexes: ${missingIndexes.join(", ")}.`
      );
      return;
    }

    if (needsUrlButtonVariable && !genericButtonUrlVariable.trim()) {
      setGenericStatus("Missing URL button variable");
      setGenericResponse("This template includes a dynamic URL button and requires button URL variable.");
      return;
    }

    // Feature 4: Schedule mode
    if (useSchedule) {
      if (!scheduleAt) {
        setGenericStatus("Please select a date/time to schedule");
        return;
      }
      try {
        const response = await fetch(apiUrl("/api/messages/schedule"), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            to: genericTo,
            templateName: genericTemplateName,
            languageCode: genericLanguage,
            bodyVariables: variables,
            scheduledAt: scheduleAt
          })
        });
        const data = await response.json();
        if (response.ok) {
          setScheduledItems((prev) => [data as ScheduledItem, ...prev]);
          setScheduleAt("");
          setUseSchedule(false);
          setGenericStatus("Scheduled ✓");
        } else {
          setGenericStatus(`Schedule failed: ${data?.error || response.status}`);
        }
      } catch {
        setGenericStatus("Schedule request failed");
      }
      return;
    }

    setGenericLoading(true);
    setGenericStatus("Sending...");

    const historyPreview = selectedTemplatePreview || selectedTemplateBody || `Template ${genericTemplateName}`;

    try {
      const response = await fetch(apiUrl("/api/templates/send-generic"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          to: genericTo,
          templateName: genericTemplateName,
          languageCode: genericLanguage,
          bodyVariables: variables,
          buttonUrlVariable: needsUrlButtonVariable ? genericButtonUrlVariable.trim() : ""
        })
      });

      const data = await response.json();
      setGenericStatus(response.ok ? "Template accepted" : `Failed (${response.status})`);
      setGenericResponse(JSON.stringify(data, null, 2));

      setTemplateHistory((current) => [
        {
          id: `h-${Date.now()}`,
          to: genericTo,
          templateName: genericTemplateName,
          previewText: historyPreview,
          time: nowLabel(),
          status: response.ok ? "accepted" : `failed_${response.status}`
        },
        ...current
      ]);
    } catch (error) {
      setGenericStatus("Network error");
      setGenericResponse(error instanceof Error ? error.message : "Unknown error");

      setTemplateHistory((current) => [
        {
          id: `h-${Date.now()}`,
          to: genericTo,
          templateName: genericTemplateName,
          previewText: historyPreview,
          time: nowLabel(),
          status: "network_error"
        },
        ...current
      ]);
    } finally {
      setGenericLoading(false);
    }
  }

  return (
    <div className="page">
      <header className="hero">
        <div className="badge">www.linke.pt</div>
        <h1>Own your cloud API frontend without Make bottlenecks.</h1>
        <p>
          Linke gives your business a direct and resilient frontend to present, sell,
          and operate API products with confidence.
        </p>
        <div className="hero-actions">
          <a href="#contact" className="btn btn-primary">
            Start with Linke
          </a>
          <a href="#plans" className="btn btn-secondary">
            View plans
          </a>
        </div>
      </header>

      <section className="panel" id="api-console">
        <h2>WhatsApp Cloud API Message Console</h2>
        <p>
          Based on official docs: <strong>POST /{`{Version}`}/{`{Phone-Number-ID}`}/messages</strong>
          with Bearer authentication and JSON payload, relayed by your backend for secure
          Notion logging.
        </p>

        <div className="wa-console">
          <aside className="wa-sidebar">
            <div className="wa-search">
              <input
                placeholder="Search contacts..."
                value={contactSearch}
                onChange={(e) => setContactSearch(e.target.value)}
              />
            </div>

            <div className="wa-contact-list">
              {conversations.length === 0 ? (
                <div className="wa-empty-contacts">
                  No conversations yet. Send a message to create contact history.
                </div>
              ) : null}

              {filteredConversations.map((contact) => {
                const last = contact.messages[contact.messages.length - 1];
                const isActive = contact.id === activeConversationId;
                const displayName = resolveContactName(contact.phone, savedContacts);

                return (
                  <button
                    key={contact.id}
                    type="button"
                    className={`wa-contact ${isActive ? "active" : ""}`}
                    onClick={() => {
                      setActiveConversationId(contact.id);
                      setToNumber(contact.phone);
                    }}
                  >
                    <span className="wa-contact-avatar">{displayName.slice(0, 2).toUpperCase()}</span>
                    <span className="wa-contact-meta">
                      <strong>{displayName}</strong>
                      <small>{last?.text || "No messages yet"}</small>
                    </span>
                    <span className="wa-contact-right">
                      <small>{contact.lastAt}</small>
                      {contact.unread > 0 ? <b>{contact.unread}</b> : null}
                    </span>
                  </button>
                );
              })}
            </div>

            <div className="wa-contact-book-bar">
              <button
                type="button"
                className="wa-contact-book-toggle"
                onClick={() => setContactBookOpen((o) => !o)}
              >
                👤 {contactBookOpen ? "Hide contacts" : "Manage contacts"}
              </button>
            </div>
            {contactBookOpen ? (
              <div className="wa-contact-book">
                <div className="wa-contact-book-add">
                  <input
                    placeholder="Phone (digits)"
                    value={newContactPhone}
                    onChange={(e) => setNewContactPhone(e.target.value)}
                  />
                  <input
                    placeholder="Name"
                    value={newContactName}
                    onChange={(e) => setNewContactName(e.target.value)}
                    onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); saveContact(); } }}
                  />
                  <button type="button" className="wa-contact-book-save" onClick={saveContact}>Save</button>
                </div>
                {Object.keys(savedContacts).length > 0 ? (
                  <div className="wa-contact-book-list">
                    {Object.entries(savedContacts).map(([digits, name]) => (
                      <div key={digits} className="wa-contact-book-row">
                        <span>{digits}</span>
                        <strong>{name}</strong>
                        <button type="button" className="wa-contact-book-del" onClick={() => removeContact(digits)}>×</button>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="wa-empty-contacts">No saved contacts yet.</p>
                )}
              </div>
            ) : null}
          </aside>

          <div className="wa-phone">
            <header className="wa-phone-header">
              <div className="wa-avatar">LN</div>
              <div>
                <strong>{activeConversation ? resolveContactName(activeConversation.phone, savedContacts) : "Live customer chat"}</strong>
                <small>{toNumber || activeConversation?.phone || "No recipient"}</small>
              </div>
              <span className={`wa-live-status ${loading ? "busy" : ""}`}>{statusText}</span>
            </header>

            <main className="wa-thread">
              {(activeConversation?.messages || []).map((message) => (
                <article key={message.id} className={`wa-msg ${message.direction === "in" ? "in" : "out"}`}>
                  <p>{message.text}</p>
                  <time>
                    {message.time}
                    {message.direction === "out" ? (
                      <span className={`wa-tick${message.deliveryStatus === "read" ? " wa-tick-read" : message.deliveryStatus === "delivered" ? " wa-tick-delivered" : ""}`}>
                        {deliveryTickMark(message.deliveryStatus)}
                      </span>
                    ) : null}
                  </time>
                </article>
              ))}
              {loading && messageText ? (
                <article className="wa-msg out composing">
                  <p>{messageText}</p>
                  <time>{loading ? "sending" : "draft"}</time>
                </article>
              ) : null}
            </main>

            <form className="wa-compose" onSubmit={sendMessage}>
              <label>
                Recipient Number (E.164)
                <input
                  value={toNumber}
                  onChange={(event) => setToNumber(event.target.value)}
                  placeholder="3519XXXXXXXX"
                />
              </label>
              <label>
                Message
                <div className="wa-message-bar">
                  <div className="wa-emoji-wrap">
                    <button
                      type="button"
                      className="wa-emoji"
                      onClick={() => setEmojiOpen((open) => !open)}
                      aria-label="Open emoji picker"
                      title="Open emoji picker"
                    >
                      🙂
                    </button>
                    {emojiOpen ? (
                      <div className="wa-emoji-picker" role="menu" aria-label="Emoji picker">
                        {"😀 😅 😂 😊 😉 😍 😎 🙌 👍 👌 ✅ 🎉 ❤️".split(" ").map((emoji) => (
                          <button
                            key={emoji}
                            type="button"
                            className="wa-emoji-item"
                            onClick={() => addEmoji(emoji)}
                          >
                            {emoji}
                          </button>
                        ))}
                      </div>
                    ) : null}
                  </div>
                  <textarea
                    value={messageText}
                    onChange={(event) => setMessageText(event.target.value)}
                    rows={2}
                    placeholder="Write your response..."
                  />
                  <label className="wa-attach-btn" title="Attach file">
                    📎
                    <input
                      type="file"
                      className="wa-attach-input"
                      accept="image/*,video/*,audio/*,.pdf,.doc,.docx"
                      onChange={(e) => setComposeMedia(e.target.files?.[0] || null)}
                    />
                  </label>
                </div>
              </label>
              {composeMedia ? (
                <div className="wa-compose-file-bar">
                  <span className="wa-compose-file-name">📎 {composeMedia.name}</span>
                  <button
                    type="button"
                    className="wa-send"
                    onClick={sendMediaMessage}
                    disabled={composeMediaLoading}
                  >
                    {composeMediaLoading ? "Sending..." : "Send File"}
                  </button>
                  <button type="button" className="wa-attach-clear" onClick={() => setComposeMedia(null)}>×</button>
                </div>
              ) : null}
              <button className="wa-send" type="submit" disabled={loading}>
                {loading ? "Sending..." : "Send Text Message"}
              </button>
            </form>

            <details className="wa-details">
              <summary>API Details</summary>

              <div className="facts wa-facts">
                {docsFacts.map((fact) => (
                  <div key={fact.label} className="fact">
                    <span>{fact.label}</span>
                    <strong>{fact.value}</strong>
                  </div>
                ))}
              </div>

              <form className="api-form wa-config">
                <label>
                  API Version
                  <input
                    value={apiVersion}
                    onChange={(event) => setApiVersion(event.target.value)}
                    placeholder="v23.0"
                  />
                </label>

                <label>
                  Phone Number ID
                  <input
                    value={phoneNumberId}
                    onChange={(event) => setPhoneNumberId(event.target.value)}
                    placeholder="configured in backend"
                  />
                </label>
              </form>

              <div className="code-grid wa-code-grid">
                <article className="card code-block">
                  <h3>Resolved Endpoint</h3>
                  <pre>{endpoint}</pre>
                </article>
                <article className="card code-block">
                  <h3>Backend Relay</h3>
                  <pre>{apiUrl("/api/messages/send")}</pre>
                </article>
                <article className="card code-block">
                  <h3>Request Payload</h3>
                  <pre>{JSON.stringify(payload, null, 2)}</pre>
                </article>
                <article className="card code-block">
                  <h3>cURL Example</h3>
                  <pre>{curlCommand}</pre>
                </article>
                <article className="card code-block">
                  <h3>API Response</h3>
                  <pre>{responseText}</pre>
                </article>
              </div>
            </details>
          </div>
        </div>
      </section>

      <section className="panel" id="media-console">
        <h2>WhatsApp Cloud API Media Upload Console</h2>
        <p>
          Based on official docs: <strong>POST /{`{Version}`}/{`{Phone-Number-ID}`}/media</strong>
          using multipart form-data. This returns a media <strong>id</strong> that you can use in
          media messages.
        </p>

        <form className="api-form" onSubmit={uploadMedia}>
          <label>
            Select Media File
            <input
              type="file"
              onChange={(event) => setMediaFile(event.target.files?.[0] || null)}
              accept="image/*,video/*,audio/*,.pdf,.doc,.docx,.txt,.webp"
            />
          </label>

          <div className="api-actions">
            <button className="btn btn-primary" type="submit" disabled={mediaLoading}>
              {mediaLoading ? "Uploading..." : "Upload Media"}
            </button>
            <span className="status">Status: {mediaStatusText}</span>
          </div>
        </form>

        <div className="code-grid">
          <article className="card code-block">
            <h3>Graph Upload Endpoint</h3>
            <pre>{`https://graph.facebook.com/${apiVersion}/${phoneNumberId}/media`}</pre>
          </article>
          <article className="card code-block">
            <h3>Backend Relay</h3>
            <pre>{apiUrl("/api/media/upload")}</pre>
          </article>
          <article className="card code-block">
            <h3>cURL Example</h3>
            <pre>{mediaCurlCommand}</pre>
          </article>
          <article className="card code-block">
            <h3>Upload Response</h3>
            <pre>{mediaResponseText}</pre>
          </article>
        </div>
      </section>

      <section className="panel" id="generic-template-console">
        <h2>Template Notifications</h2>
        <p>
          Pick an approved template from your Meta account, fill variables, preview the message,
          and send the notification.
        </p>

        <div className="template-toolbar">
          <input
            value={wabaId}
            onChange={(event) => setWabaId(event.target.value)}
            placeholder="Optional: paste WABA ID"
          />
          <button
            className="btn btn-secondary"
            type="button"
            onClick={fetchMetaTemplates}
            disabled={metaTemplatesLoading}
          >
            {metaTemplatesLoading ? "Loading..." : "Refresh Templates"}
          </button>
          <span className="status">{metaTemplatesStatus}</span>
        </div>

        <form className="api-form" onSubmit={sendGenericTemplate}>
          <label>
            Recipient Number (E.164)
            <input
              value={genericTo}
              onChange={(event) => setGenericTo(event.target.value)}
              placeholder="+351912858229"
            />
          </label>

          <label>
            Template (Approved)
            <select
              value={genericTemplateName}
              onChange={(event) => {
                const chosen = metaTemplates.find((item) => item.name === event.target.value) || null;
                setGenericTemplateName(event.target.value);
                if (chosen?.language) {
                  setGenericLanguage(chosen.language);
                }
              }}
            >
              {metaTemplates.length === 0 ? <option value="">No templates loaded</option> : null}
              {metaTemplates.map((template) => (
                <option key={template.id || template.name} value={template.name}>
                  {template.name} ({template.language || "pt_PT"})
                </option>
              ))}
            </select>
          </label>

          <label>
            Language Code
            <input
              value={genericLanguage}
              onChange={(event) => setGenericLanguage(event.target.value)}
              placeholder="pt_PT"
            />
          </label>

          <span className="status">
            Required body vars: {requiredBodyVarCount} {requiredBodyVarCount > 0 ? `(indexes: ${requiredBodyIndexes.join(", ")})` : ""}
          </span>

          {requiredBodyVarCount > 0 ? (
            <div className="template-var-grid">
              {requiredBodyIndexes.map((index) => (
                <label key={index}>
                  Variable {`{{${index}}}`}
                  <input
                    value={genericBodyVars[index] || ""}
                    onChange={(event) =>
                      setGenericBodyVars((current) => ({
                        ...current,
                        [index]: event.target.value
                      }))
                    }
                    placeholder={`Value for {{${index}}}`}
                  />
                </label>
              ))}
            </div>
          ) : null}

          {requiredBodyVarCount > 0 ? (
            <div className="preset-bar">
              <button type="button" className="btn btn-secondary" onClick={savePreset}>
                💾 Save preset
              </button>
              {templatePresets[genericTemplateName] ? (
                <button type="button" className="btn btn-secondary" onClick={loadPreset}>
                  📂 Load preset
                </button>
              ) : null}
            </div>
          ) : null}

          <article className="template-chat-box">
            <header>
              <strong>Template Text Box</strong>
              <span>{genericTemplateName || "No template selected"}</span>
            </header>
            <div className="template-thread">
              <article className="wa-msg in">
                <p>Template selected: {genericTemplateName || "-"}</p>
                <time>{genericLanguage || "pt_PT"}</time>
              </article>
              <article className="wa-msg out">
                <p>{selectedTemplatePreview || selectedTemplateBody || "No body text in selected template"}</p>
                <time>preview</time>
              </article>
            </div>
          </article>

          {needsUrlButtonVariable ? (
            <label>
              URL Button Variable (required)
              <input
                value={genericButtonUrlVariable}
                onChange={(event) => setGenericButtonUrlVariable(event.target.value)}
                placeholder="dynamic URL variable"
              />
            </label>
          ) : null}

          <label className="schedule-toggle">
            <input
              type="checkbox"
              checked={useSchedule}
              onChange={(e) => setUseSchedule(e.target.checked)}
            />
            Schedule for later
          </label>
          {useSchedule ? (
            <label>
              Send at
              <input
                type="datetime-local"
                value={scheduleAt}
                onChange={(e) => setScheduleAt(e.target.value)}
              />
            </label>
          ) : null}

          <div className="api-actions">
            <button
              className={useSchedule ? "btn btn-secondary" : "btn btn-primary"}
              type="submit"
              disabled={genericLoading || (useSchedule && !scheduleAt)}
            >
              {genericLoading ? "Sending..." : useSchedule ? "⏰ Schedule Message" : "Send Template Notification"}
            </button>
            <span className="status">Status: {genericStatus}</span>
          </div>
        </form>

        <div className="code-grid">
          <article className="card code-block">
            <h3>Backend Relay</h3>
            <pre>{apiUrl("/api/templates/send-generic")}</pre>
          </article>
          <article className="card code-block">
            <h3>Template Response</h3>
            <pre>{genericResponse}</pre>
          </article>
        </div>

        <section className="template-history">
          <h3>Sent Template History</h3>
          {templateHistory.length === 0 ? (
            <p>No template notifications sent yet.</p>
          ) : (
            <div className="template-history-list">
              {templateHistory.map((item) => (
                <article key={item.id} className="template-history-item">
                  <header>
                    <strong>{item.templateName}</strong>
                    <span>{item.time}</span>
                  </header>
                  <p>To: {item.to}</p>
                  <p>{item.previewText}</p>
                  <span className="status">Status: {item.status}</span>
                </article>
              ))}
            </div>
          )}
        </section>

        {scheduledItems.length > 0 ? (
          <section className="template-history">
            <h3>⏰ Scheduled Messages</h3>
            <div className="template-history-list">
              {scheduledItems.map((item) => (
                <article key={item.id} className="template-history-item">
                  <header>
                    <strong>{item.templateName}</strong>
                    <span>{item.scheduledAt ? new Date(item.scheduledAt).toLocaleString() : ""}</span>
                  </header>
                  <p>To: {item.to}</p>
                  <span className={`status ${item.status === "sent" ? "status-ok" : item.status === "failed" ? "status-err" : ""}`}>
                    Status: {item.status}
                  </span>
                </article>
              ))}
            </div>
          </section>
        ) : null}

        <details className="bulk-send-section">
          <summary>📋 Bulk Send (CSV)</summary>
          <div className="bulk-send-body">
            <p>One recipient per line: <code>phone,var1,var2,...</code> — uses current template &amp; language.</p>
            <textarea
              className="bulk-csv"
              rows={5}
              placeholder={`351912858229,João,Loja A\n351910000001,Maria,Loja B`}
              value={bulkCsv}
              onChange={(e) => setBulkCsv(e.target.value)}
            />
            <div className="api-actions">
              <button
                type="button"
                className="btn btn-primary"
                onClick={runBulkSend}
                disabled={bulkRunning || !bulkCsv.trim()}
              >
                {bulkRunning
                  ? `Sending ${bulkProgress.sent}/${bulkProgress.total}...`
                  : "Send to All"}
              </button>
              {bulkProgress.total > 0 && !bulkRunning ? (
                <span className="status">{bulkProgress.sent}/{bulkProgress.total} sent</span>
              ) : null}
            </div>
            {bulkRows.length > 0 ? (
              <div className="bulk-results">
                {bulkRows.map((row, i) => (
                  <div
                    key={i}
                    className={`bulk-result-row ${row.status.startsWith("sent") ? "ok" : "err"}`}
                  >
                    <span>{row.phone}</span>
                    <span>{row.status}</span>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </details>
      </section>

      <section className="panel cta" id="contact">
        <h2>Ready to replace fragile automations?</h2>
        <p>Deploy a frontend that your team fully controls and your clients can trust.</p>
        <a className="btn btn-primary" href="mailto:hello@linke.pt">
          Contact hello@linke.pt
        </a>
      </section>
    </div>
  );
}

export default App;
