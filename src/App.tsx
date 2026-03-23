import { FormEvent, useEffect, useMemo, useRef, useState } from "react";

const docsFacts = [
  { label: "Version", value: "v23.0" },
  { label: "Method", value: "POST" },
  { label: "Endpoint", value: "/{Version}/{Phone-Number-ID}/messages" },
  { label: "Auth", value: "Bearer token" }
];

const quickApps = [
  {
    name: "Tracking Linke",
    description: "Acompanhar envios e estado operacional em tempo real.",
    url: "https://portal.linke.pt/en/tracking",
    icon: "https://portal.linke.pt/assets/img/logo/logo.svg"
  },
  {
    name: "CTT Tracking",
    description: "Consulta de envios CTT para apoio ao acompanhamento operacional.",
    url: "https://www.ctt.pt/particulares/",
    icon: "https://www.ctt.pt/application/themes/images/logo-ctt.svg"
  },
  {
    name: "Notion Linke Space",
    description: "Base de conhecimento, processos e notas da equipa.",
    url: "https://www.notion.so/75be28fa59874502895a9700549329a1",
    icon: "https://www.insightplatforms.com/wp-content/uploads/2023/10/Notion-Logo-Square-Insight-Platforms.png"
  },
  {
    name: "Portal Linke TMS",
    description: "Gestão de shipments e operação logística diária.",
    url: "https://portal.linke.pt/admin/shipments",
    icon: "https://portal.linke.pt/assets/img/logo/logo.svg"
  },
  {
    name: "ClickSend SMS",
    description: "Acesso rápido ao sistema ClickSend para envios SMS.",
    url: "https://integrations.clicksend.com",
    icon: "https://integrations.clicksend.com/_nuxt/sinch-clicksend-logo.6K63Np5E.svg"
  }
];

const PUDO_ALLOWED_NOTIFICATION_TEMPLATES = new Set([
  "order_pick_no_ctt (en_US)",
  "order_pick_up_1 (pt_PT)"
]);

const PUDO_ALLOWED_NOTIFICATION_TEMPLATE_NAMES = new Set([
  "order_pick_no_ctt",
  "order_pick_up_1"
]);

const FEEDBACK_COLUMNS_ORDER = [
  "Cod. Serviço",
  "Nome Cliente",
  "Destinatário",
  "Contacto Destinatário",
  "TRK Secundário",
  "Data Entrega",
  "Status",
  "Sent Date",
  "Pedido 5*",
  "Respondeu?",
  "Feedback URL",
  "Customer Satisfaction",
  "whatsapp subject",
  "Referência",
  "WhatsApp Template"
];

const FEEDBACK_COLUMN_ALIASES: Record<string, string[]> = {
  "Cod. Serviço": ["Cod. Servi�o", "Cod. Servico"],
  "Destinatário": ["Destinat�rio"],
  "Contacto Destinatário": ["Contacto Destinat�rio"],
  "Referência": ["Refer�ncia"]
};

type ConversationMessage = {
  id: string;
  direction: "in" | "out";
  text: string;
  time: string;
  apiMessageId?: string;
  mediaType?: string;
  mediaUrl?: string;
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

type TeamReminder = {
  id: string;
  phone: string;
  note: string;
  dueAt: string;
  done: boolean;
};

type PersonalNote = {
  id: string;
  title: string;
  content: string;
  color: string;
  createdAt: string;
};

type SharedLogItem = {
  id: number;
  created_at: string;
  direction?: string;
  channel?: string;
  to_number?: string;
  contact_name?: string | null;
  message_text?: string | null;
  template_name?: string | null;
  status?: string | null;
  api_message_id?: string | null;
  payload?: Record<string, unknown> | null;
};

type ConsumivelItem = {
  id: string;
  fields: Record<string, string>;
  url?: string;
};

type CalendarEvent = {
  id: string;
  date: string;
  title: string;
  time: string;
};

type GenericTemplateTrackerContext = {
  clientName?: string;
  parcelId?: string;
  messageType?: string;
  notes?: string;
};

type TmsInfoBox = {
  label: string;
  value: string;
  trend?: string;
};

type TmsServiceRow = {
  service: string;
  pending: number;
  accepted: number;
  pickup: number;
  transport: number;
  delivered: number;
  incidence: number;
  incidenceOngoing?: number;
};

type TmsPendingAcceptanceRow = {
  customer: string;
  shipments: number;
};

type TmsIncidenceRow = {
  id: number;
  name: string;
  shipment: boolean;
  pickup: boolean;
  appVisible: boolean;
  active: boolean;
  sort: number;
};

type TmsDashboardData = {
  meta?: {
    fetchedAt?: string;
    source?: string;
  };
  infoBoxes: TmsInfoBox[];
  serviceStatus: {
    rows: TmsServiceRow[];
    totals?: Omit<TmsServiceRow, "service"> | null;
    highlights?: Record<string, number>;
  };
  pendingAcceptance: TmsPendingAcceptanceRow[];
  incidences: TmsIncidenceRow[];
  incidenceShipments: Array<{
    parcelId: string;
    providerTrackingCode?: string;
    service?: string;
    sender: string;
    recipient: string;
    finalClientPhone: string;
    status?: string;
    incidence?: string;
    hasCharge?: boolean;
    chargeAmount?: string;
  }>;
  pudoShipments?: Array<{
    parcelId: string;
    providerTrackingCode?: string;
    service?: string;
    sender: string;
    recipient: string;
    finalClientPhone: string;
    status?: string;
    incidence?: string;
    hasCharge?: boolean;
    chargeAmount?: string;
  }>;
};

type TmsDeliveredShipment = {
  parcelId: string;
  providerTrackingCode?: string;
  service?: string;
  sender: string;
  recipient: string;
  finalClientPhone: string;
  pickupDate?: string;
  deliveryDate?: string;
  status?: string;
  incidence?: string;
};

type PudoNotificationState = Record<
  string,
  {
    firstSeenAt: string;
    notifiedAt?: string;
  }
>;

type AuthUser = {
  username: string;
  displayName: string;
};

function digitsOnly(value: string) {
  return String(value || "").replace(/\D/g, "");
}

function normalizePhoneForPudoMatch(value: string) {
  const digits = digitsOnly(value);
  if (!digits) return "";

  const withoutInternationalPrefix = digits.startsWith("00") ? digits.slice(2) : digits;
  if (withoutInternationalPrefix.length > 9) {
    return withoutInternationalPrefix.slice(-9);
  }

  return withoutInternationalPrefix;
}

function getPudoShipmentKey(item: {
  parcelId: string;
  providerTrackingCode?: string;
  finalClientPhone: string;
  recipient: string;
}) {
  const parcel = String(item.parcelId || "").trim();
  const tracking = String(item.providerTrackingCode || "").trim();
  const phone = normalizePhoneForPudoMatch(item.finalClientPhone || "");
  const recipient = String(item.recipient || "").trim().toLowerCase();
  return parcel || tracking || `${recipient}:${phone}`;
}

function isAllowedPudoNotificationTemplate(templateName?: string | null, languageCode?: string | null) {
  const name = String(templateName || "").trim();
  if (!name) return false;

  const normalizedName = name.toLowerCase();

  // Some logs only store template name without language.
  if (PUDO_ALLOWED_NOTIFICATION_TEMPLATE_NAMES.has(normalizedName)) {
    return true;
  }

  const normalizedAllowedFull = Array.from(PUDO_ALLOWED_NOTIFICATION_TEMPLATES).map((item) => item.toLowerCase());
  if (normalizedAllowedFull.includes(normalizedName)) {
    return true;
  }

  const language = String(languageCode || "").trim();
  if (!language) return false;

  return normalizedAllowedFull.includes(`${name} (${language})`.toLowerCase());
}

function isPudoTrackerContext(context?: GenericTemplateTrackerContext | null) {
  const messageType = String(context?.messageType || "").trim().toLowerCase();
  return messageType.includes("pick") || messageType.includes("pudo");
}

function contactDisplayName(phone: string) {
  const normalized = digitsOnly(phone);
  return normalized ? `Contacto ${normalized}` : "Contacto desconhecido";
}

function humanizeUsername(username: string) {
  return String(username || "")
    .split("_")
    .filter(Boolean)
    .map((chunk) => chunk.charAt(0).toUpperCase() + chunk.slice(1))
    .join(" ");
}

function SidebarIcon({ name }: { name: "overview" | "chat" | "logs" | "upload" | "templates" | "notes" }) {
  switch (name) {
    case "overview":
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M4 11.5 12 5l8 6.5V20a1 1 0 0 1-1 1h-4.5v-5h-5v5H5a1 1 0 0 1-1-1z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    case "chat":
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M6 18.5 3.5 20V6.5A2.5 2.5 0 0 1 6 4h12a2.5 2.5 0 0 1 2.5 2.5v8A2.5 2.5 0 0 1 18 17H8z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    case "logs":
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M6 7.5h12M6 12h12M6 16.5h8" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    case "upload":
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M12 15V5m0 0 3.5 3.5M12 5 8.5 8.5M5 15.5V18a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2v-2.5" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    case "templates":
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M12 3.75a4 4 0 0 1 4 4v.55a3.7 3.7 0 0 0 1.05 2.6l.42.43a1.8 1.8 0 0 1-1.28 3.07H7.8a1.8 1.8 0 0 1-1.28-3.07l.42-.43A3.7 3.7 0 0 0 8 8.3v-.55a4 4 0 0 1 4-4Zm1.9 12.65a2 2 0 0 1-3.8 0" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    case "notes":
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M7 3.75v2.5M17 3.75v2.5M4.75 9h14.5M6.5 6.25h11A1.75 1.75 0 0 1 19.25 8v10.25A1.75 1.75 0 0 1 17.5 20h-11a1.75 1.75 0 0 1-1.75-1.75V8A1.75 1.75 0 0 1 6.5 6.25Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    default:
      return null;
  }
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

function statusTone(status: string, channelOrType?: string) {
  const normalized = String(status || "").toLowerCase();
  const context = String(channelOrType || "").toLowerCase();
  const isSmsContext = context.includes("sms") || context.includes("clicksend");

  if (normalized.includes("sent") || normalized.includes("accept")) {
    if (isSmsContext) {
      return "sent";
    }
    return "ok";
  }
  if (normalized.includes("read") || normalized.includes("delivered")) {
    return "ok";
  }
  if (normalized.includes("fail") || normalized.includes("error") || normalized.includes("reject") || normalized.includes("invalid") || normalized.includes("undeliver")) {
    return "err";
  }
  return "neutral";
}

function extractParcelCode(input: string) {
  const text = String(input || "").toUpperCase();
  const match = text.match(/\b([A-Z]{2}\d{8,}[A-Z]{2}|[A-Z]{2}\d{4,}[A-Z]{2}|TESTE)\b/);
  return match?.[1] || "-";
}

function formatMessageType(channel?: string) {
  const normalized = String(channel || "").toLowerCase();
  if (normalized === "template") return "Template";
  if (normalized === "sms") return "SMS";
  if (normalized === "media") return "Media";
  return "Text";
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

function normalizeFeedbackColumnKey(value: string) {
  return String(value || "")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/�/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "")
    .trim();
}

function parseFeedbackEntregaTimestamp(value: string) {
  const raw = String(value || "").trim();
  if (!raw || raw === "-") return Number.NaN;

  const parsed = new Date(raw).getTime();
  if (Number.isFinite(parsed)) return parsed;

  const match = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:,\s*(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (!match) return Number.NaN;

  const day = Number(match[1]);
  const month = Number(match[2]) - 1;
  const year = Number(match[3]);
  const hour = Number(match[4] || 0);
  const minute = Number(match[5] || 0);
  const second = Number(match[6] || 0);
  return new Date(year, month, day, hour, minute, second).getTime();
}

function parseDeliveredEntregaTimestamp(value: string) {
  const raw = String(value || "").trim();
  if (!raw || raw === "-") return Number.NaN;

  const directParsed = new Date(raw).getTime();
  if (Number.isFinite(directParsed)) return directParsed;

  // Accept repeated ISO-like fragments from the portal (e.g. "2025-03-14 2025-03-15")
  // and keep the most recent one so sorting can reliably be latest -> oldest.
  const matches = [
    ...raw.matchAll(/(\d{4})-(\d{1,2})-(\d{1,2})(?:[,\sT]+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/g),
    ...raw.matchAll(/(\d{1,2})[\/.](\d{1,2})[\/.](\d{2,4})(?:[,\sT]+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/g)
  ];
  if (matches.length === 0) return Number.NaN;

  let latestTs = Number.NaN;
  for (const match of matches) {
    let day = Number.NaN;
    let month = Number.NaN;
    let rawYear = Number.NaN;

    if (match[0].includes("-") && match[1]?.length === 4) {
      rawYear = Number(match[1]);
      month = Number(match[2]) - 1;
      day = Number(match[3]);
    } else {
      day = Number(match[1]);
      month = Number(match[2]) - 1;
      rawYear = Number(match[3]);
    }

    const year = rawYear < 100 ? 2000 + rawYear : rawYear;
    const hour = Number(match[4] || 0);
    const minute = Number(match[5] || 0);
    const second = Number(match[6] || 0);

    if (!Number.isFinite(day) || !Number.isFinite(month) || !Number.isFinite(year)) {
      continue;
    }

    const timestamp = new Date(year, month, day, hour, minute, second).getTime();

    if (Number.isFinite(timestamp) && (!Number.isFinite(latestTs) || timestamp > latestTs)) {
      latestTs = timestamp;
    }
  }

  return latestTs;
}

function parseDeliveredSortTimestamp(row: TmsDeliveredShipment) {
  const pickupTs = parseDeliveredEntregaTimestamp(row.pickupDate || "");
  if (Number.isFinite(pickupTs)) return pickupTs;
  return parseDeliveredEntregaTimestamp(row.deliveryDate || "");
}

function extractPlaceholderIndexes(input: string) {
  const matches = [...String(input || "").matchAll(/\{\{(\d+)\}\}/g)];
  return [...new Set(matches.map((match) => Number(match[1])).filter(Number.isFinite))].sort(
    (a, b) => a - b
  );
}

function extractInboundMediaFromLog(log: SharedLogItem) {
  const payload = log?.payload && typeof log.payload === "object" ? log.payload : null;
  const message = payload && typeof payload.message === "object" ? payload.message as Record<string, unknown> : null;
  if (!message) {
    return { mediaType: "", mediaId: "" };
  }

  const mediaType = String(message.type || "").trim().toLowerCase();
  if (!mediaType) {
    return { mediaType: "", mediaId: "" };
  }

  const mediaNode = message[mediaType] && typeof message[mediaType] === "object"
    ? message[mediaType] as Record<string, unknown>
    : null;

  return {
    mediaType,
    mediaId: String(mediaNode?.id || "").trim()
  };
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
  const [authUser, setAuthUser] = useState<AuthUser | null>(() => {
    try {
      const raw = localStorage.getItem("wa_auth_user");
      return raw ? JSON.parse(raw) as AuthUser : null;
    } catch {
      return null;
    }
  });
  const [loginUsername, setLoginUsername] = useState("");
  const [loginPassword, setLoginPassword] = useState("");
  const [loginLoading, setLoginLoading] = useState(false);
  const [loginError, setLoginError] = useState("");

  async function parseResponse(response: Response) {
    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      return response.json();
    }

    const text = await response.text();
    return { raw: text || "Empty response" };
  }

  async function handleLogin(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!loginUsername.trim() || !loginPassword) {
      setLoginError("Introduz utilizador e password.");
      return;
    }

    setLoginLoading(true);
    setLoginError("");

    try {
      const response = await fetch(apiUrl("/api/auth/login"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          username: loginUsername.trim(),
          password: loginPassword
        })
      });

      const data = await parseResponse(response);
      if (!response.ok || !data?.user) {
        setLoginError(String(data?.error || `Falha no login (${response.status})`));
        return;
      }

      const nextUser: AuthUser = {
        username: String(data.user.username || loginUsername.trim()),
        displayName: String(data.user.displayName || humanizeUsername(loginUsername.trim()))
      };
      setAuthUser(nextUser);
      try { localStorage.setItem("wa_auth_user", JSON.stringify(nextUser)); } catch {}
      setLoginPassword("");
    } catch {
      setLoginError("Não foi possível ligar ao backend.");
    } finally {
      setLoginLoading(false);
    }
  }

  function handleLogout() {
    setAuthUser(null);
    setLoginPassword("");
    try { localStorage.removeItem("wa_auth_user"); } catch {}
  }
  const [toNumber, setToNumber] = useState(import.meta.env.VITE_DEFAULT_TO_NUMBER ?? "");
  const [messageText, setMessageText] = useState("Olá! Esta é uma mensagem enviada pelo workspace da equipa.");
  const [loading, setLoading] = useState(false);
  const [statusText, setStatusText] = useState("Inativo");
  const [responseText, setResponseText] = useState("Ainda não foi enviado nenhum pedido.");
  const [mediaFile, setMediaFile] = useState<File | null>(null);
  const [mediaLoading, setMediaLoading] = useState(false);
  const [mediaConsoleExpanded, setMediaConsoleExpanded] = useState(false);
  const [templateConsoleExpanded, setTemplateConsoleExpanded] = useState(false);
  const [mediaStatusText, setMediaStatusText] = useState("Inativo");
  const [mediaResponseText, setMediaResponseText] = useState("Ainda não foi enviado nenhum ficheiro.");
  const [genericTo, setGenericTo] = useState(import.meta.env.VITE_DEFAULT_TO_NUMBER ?? "");
  const [genericTemplateName, setGenericTemplateName] = useState(
    import.meta.env.VITE_DEFAULT_TEMPLATE_NAME ?? "order_pickup_ctt"
  );
  const [genericLanguage, setGenericLanguage] = useState("pt_PT");
  const [genericBodyVars, setGenericBodyVars] = useState<Record<number, string>>({});
  const [genericButtonUrlVariable, setGenericButtonUrlVariable] = useState("");
  const [genericTrackerContext, setGenericTrackerContext] = useState<GenericTemplateTrackerContext | null>(null);
  const [genericLoading, setGenericLoading] = useState(false);
  const [genericStatus, setGenericStatus] = useState("Inativo");
  const [genericResponse, setGenericResponse] = useState("Ainda não foi enviado nenhum template.");
  const [smsTo, setSmsTo] = useState(import.meta.env.VITE_DEFAULT_TO_NUMBER ?? "");
  const [smsText, setSmsText] = useState("");
  const [smsLoading, setSmsLoading] = useState(false);
  const [smsStatus, setSmsStatus] = useState("Inativo");
  const [smsResponse, setSmsResponse] = useState("Ainda não foi enviado nenhum SMS.");
  const [templateHistory, setTemplateHistory] = useState<TemplateHistoryItem[]>(() => {
    try { return JSON.parse(localStorage.getItem("wa_template_history") || "[]") as TemplateHistoryItem[]; } catch { return []; }
  });
  const [metaTemplates, setMetaTemplates] = useState<MetaTemplate[]>([]);
  const [metaTemplatesLoading, setMetaTemplatesLoading] = useState(false);
  const [metaTemplatesStatus, setMetaTemplatesStatus] = useState("Por carregar");
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
  const [historySearch, setHistorySearch] = useState("");
  const [historyDate, setHistoryDate] = useState("");
  const [cloudStateReady, setCloudStateReady] = useState(false);
  const [sharedLogs, setSharedLogs] = useState<SharedLogItem[]>([]);
  const [sharedLogsLoading, setSharedLogsLoading] = useState(false);
  const [sharedLogsError, setSharedLogsError] = useState("");
  const [consumiveisRows, setConsumiveisRows] = useState<ConsumivelItem[]>([]);
  const [consumiveisColumns, setConsumiveisColumns] = useState<string[]>([]);
  const [consumiveisLoading, setConsumiveisLoading] = useState(false);
  const [consumiveisSaving, setConsumiveisSaving] = useState(false);
  const [consumiveisError, setConsumiveisError] = useState("");
  const [feedbackRows, setFeedbackRows] = useState<ConsumivelItem[]>([]);
  const [feedbackColumns, setFeedbackColumns] = useState<string[]>([]);
  const [feedbackLoading, setFeedbackLoading] = useState(false);
  const [feedbackError, setFeedbackError] = useState("");
  const [feedbackPage, setFeedbackPage] = useState(1);
  const [deliveredRows, setDeliveredRows] = useState<TmsDeliveredShipment[]>([]);
  const [deliveredLoading, setDeliveredLoading] = useState(false);
  const [deliveredError, setDeliveredError] = useState("");
  const [deliveredPage, setDeliveredPage] = useState(1);
  const [deliveredTotal, setDeliveredTotal] = useState(0);
  const [deliveredYearFilter, setDeliveredYearFilter] = useState<string>("all");
  const [deliveredSearchQuery, setDeliveredSearchQuery] = useState("");
  const [consumiveisForm, setConsumiveisForm] = useState({
    clientName: "",
    dateSent: "",
    tabela: "",
    tipoCliente: "",
    texto: "",
    texto1: "",
    text: "",
    texto2: ""
  });
  const [activeView, setActiveView] = useState<"workspace" | "tracker" | "consumiveis" | "feedback">("workspace");
  const [trackerSearchField, setTrackerSearchField] = useState<"all" | "clientName" | "clientPhone" | "parcelId" | "messageTitle" | "status">("all");
  const [trackerSearchQuery, setTrackerSearchQuery] = useState("");
  const [trackerPage, setTrackerPage] = useState(1);
  const [trackerPageSize, setTrackerPageSize] = useState(15);
  const [tmsDashboard, setTmsDashboard] = useState<TmsDashboardData | null>(null);
  const [tmsLoading, setTmsLoading] = useState(false);
  const [tmsError, setTmsError] = useState("");
  const [showIncidencesPanel, setShowIncidencesPanel] = useState(false);
  const [showIncidenceDetails, setShowIncidenceDetails] = useState(false);
  const [showPudoDetails, setShowPudoDetails] = useState(false);
  const [incidenceDetailsPage, setIncidenceDetailsPage] = useState(1);
  const [pudoDetailsPage, setPudoDetailsPage] = useState(1);
  const [pudoNotifications, setPudoNotifications] = useState<PudoNotificationState>(() => {
    try {
      return JSON.parse(localStorage.getItem("wa_pudo_notifications") || "{}") as PudoNotificationState;
    } catch {
      return {};
    }
  });

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

  // Feature 9: Sidebar search
  const [contactSearch, setContactSearch] = useState("");

  // Feature 10: Media in composer
  const [composeMedia, setComposeMedia] = useState<File | null>(null);
  const [composeMediaLoading, setComposeMediaLoading] = useState(false);
  const trackerTemplateToInputRef = useRef<HTMLInputElement | null>(null);
  const sharedLogsInFlightRef = useRef(false);
  const sharedLogsSignatureRef = useRef("");

  const [contactNotes, setContactNotes] = useState<Record<string, string>>(() => {
    try { return JSON.parse(localStorage.getItem("wa_contact_notes") || "{}"); } catch { return {}; }
  });
  const [newReminderNote, setNewReminderNote] = useState("");
  const [newReminderAt, setNewReminderAt] = useState("");
  const [teamReminders, setTeamReminders] = useState<TeamReminder[]>(() => {
    try { return JSON.parse(localStorage.getItem("wa_team_reminders") || "[]") as TeamReminder[]; } catch { return []; }
  });

  // Caderno pessoal
  const [personalNotes, setPersonalNotes] = useState<PersonalNote[]>(() => {
    try { return JSON.parse(localStorage.getItem("wa_personal_notes") || "[]") as PersonalNote[]; } catch { return []; }
  });
  const [noteTitle, setNoteTitle] = useState("");
  const [noteContent, setNoteContent] = useState("");
  const [noteColor, setNoteColor] = useState("amarelo");
  const [editingNoteId, setEditingNoteId] = useState<string | null>(null);

  // Calendário pessoal
  const [calendarEvents, setCalendarEvents] = useState<CalendarEvent[]>(() => {
    try { return JSON.parse(localStorage.getItem("wa_calendar_events") || "[]") as CalendarEvent[]; } catch { return []; }
  });
  const _today = new Date();
  const [calMonth, setCalMonth] = useState(_today.getMonth());
  const [calYear, setCalYear] = useState(_today.getFullYear());
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [newEventTitle, setNewEventTitle] = useState("");
  const [newEventTime, setNewEventTime] = useState("");
  const [calcDisplay, setCalcDisplay] = useState("0");
  const [calcStored, setCalcStored] = useState<number | null>(null);
  const [calcOperator, setCalcOperator] = useState<"+" | "-" | "*" | "/" | null>(null);
  const [calcWaitingNext, setCalcWaitingNext] = useState(false);

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

  const pudoShipments = useMemo(
    () => (Array.isArray(tmsDashboard?.pudoShipments) ? tmsDashboard.pudoShipments : []),
    [tmsDashboard]
  );

  const detailsPageSize = 25;
  const feedbackPageSize = 100;
  const deliveredPageSize = 250;

  const filteredSortedFeedbackRows = useMemo(() => {
    const withEntregaDate = feedbackRows.filter((row) =>
      Number.isFinite(parseFeedbackEntregaTimestamp(row.fields["Data Entrega"] || ""))
    );

    return [...withEntregaDate].sort((a, b) => {
      const bTs = parseFeedbackEntregaTimestamp(b.fields["Data Entrega"] || "");
      const aTs = parseFeedbackEntregaTimestamp(a.fields["Data Entrega"] || "");
      return bTs - aTs;
    });
  }, [feedbackRows]);

  const feedbackTotalPages = useMemo(
    () => Math.max(1, Math.ceil(filteredSortedFeedbackRows.length / feedbackPageSize)),
    [filteredSortedFeedbackRows.length]
  );

  const paginatedFeedbackRows = useMemo(() => {
    const start = (feedbackPage - 1) * feedbackPageSize;
    return filteredSortedFeedbackRows.slice(start, start + feedbackPageSize);
  }, [feedbackPage, filteredSortedFeedbackRows]);

  const deliveredTotalPages = useMemo(
    () => Math.max(1, Math.ceil((deliveredTotal || deliveredRows.length) / deliveredPageSize)),
    [deliveredRows.length, deliveredTotal]
  );

  const deliveredAvailableYears = useMemo(() => {
    const years = new Set<string>();

    for (const row of deliveredRows) {
      const timestamp = parseDeliveredSortTimestamp(row);
      if (!Number.isFinite(timestamp)) continue;
      years.add(String(new Date(timestamp).getFullYear()));
    }

    return [...years].sort((a, b) => Number(b) - Number(a));
  }, [deliveredRows]);

  const filteredDeliveredRows = useMemo(() => {
    const query = deliveredSearchQuery.trim().toLowerCase();

    return deliveredRows.filter((row) => {
      const timestamp = parseDeliveredSortTimestamp(row);
      const matchesYear = deliveredYearFilter === "all"
        ? true
        : Number.isFinite(timestamp) && String(new Date(timestamp).getFullYear()) === deliveredYearFilter;

      if (!matchesYear) return false;
      if (!query) return true;

      const haystack = [
        row.parcelId,
        row.providerTrackingCode,
        row.pickupDate,
        row.deliveryDate,
        row.sender,
        row.recipient,
        row.finalClientPhone,
        row.status
      ]
        .map((value) => String(value || "").toLowerCase())
        .join(" ");

      return haystack.includes(query);
    });
  }, [deliveredRows, deliveredSearchQuery, deliveredYearFilter]);

  const sortedDeliveredRows = useMemo(() => {
    return [...filteredDeliveredRows].sort((a, b) => {
      const aTs = parseDeliveredSortTimestamp(a);
      const bTs = parseDeliveredSortTimestamp(b);

      if (Number.isFinite(aTs) && Number.isFinite(bTs)) {
        return bTs - aTs;
      }
      if (Number.isFinite(aTs)) return -1;
      if (Number.isFinite(bTs)) return 1;

      return String(a.parcelId || "").localeCompare(String(b.parcelId || ""));
    });
  }, [filteredDeliveredRows]);

  const incidenceShipments = useMemo(
    () => (Array.isArray(tmsDashboard?.incidenceShipments) ? tmsDashboard.incidenceShipments : []),
    [tmsDashboard]
  );

  const incidenceDetailsTotalPages = useMemo(
    () => Math.max(1, Math.ceil(incidenceShipments.length / detailsPageSize)),
    [incidenceShipments.length]
  );

  const paginatedIncidenceShipments = useMemo(() => {
    const start = (incidenceDetailsPage - 1) * detailsPageSize;
    return incidenceShipments.slice(start, start + detailsPageSize);
  }, [incidenceDetailsPage, incidenceShipments]);

  const pudoDetailsTotalPages = useMemo(
    () => Math.max(1, Math.ceil(pudoShipments.length / detailsPageSize)),
    [pudoShipments.length]
  );

  const paginatedPudoShipments = useMemo(() => {
    const start = (pudoDetailsPage - 1) * detailsPageSize;
    return pudoShipments.slice(start, start + detailsPageSize);
  }, [pudoDetailsPage, pudoShipments]);

  const pudoOverdueCount = useMemo(() => {
    const oneDayMs = 24 * 60 * 60 * 1000;
    const now = Date.now();
    return pudoShipments.reduce((count, item) => {
      const key = getPudoShipmentKey(item);
      const status = pudoNotifications[key];
      if (!status?.firstSeenAt || status.notifiedAt) return count;
      const firstSeenTs = new Date(status.firstSeenAt).getTime();
      if (!Number.isFinite(firstSeenTs)) return count;
      return now - firstSeenTs >= oneDayMs ? count + 1 : count;
    }, 0);
  }, [pudoNotifications, pudoShipments]);

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

  const activeContactPhone = useMemo(
    () => digitsOnly(activeConversation?.phone || toNumber || ""),
    [activeConversation?.phone, toNumber]
  );

  const activeContactNote = useMemo(
    () => (activeContactPhone ? String(contactNotes[activeContactPhone] || "") : ""),
    [activeContactPhone, contactNotes]
  );

  const activeContactReminders = useMemo(
    () =>
      teamReminders
        .filter((item) => !item.done && digitsOnly(item.phone) === activeContactPhone)
        .sort((a, b) => new Date(a.dueAt).getTime() - new Date(b.dueAt).getTime()),
    [activeContactPhone, teamReminders]
  );

  const calendarDays = useMemo(() => {
    const firstDow = new Date(calYear, calMonth, 1).getDay(); // 0=Sun
    const daysInMonth = new Date(calYear, calMonth + 1, 0).getDate();
    const startOffset = (firstDow + 6) % 7; // shift to Mon-first
    const cells: Array<{ day: number; dateStr: string } | null> = [];
    for (let i = 0; i < startOffset; i++) cells.push(null);
    for (let d = 1; d <= daysInMonth; d++) {
      cells.push({
        day: d,
        dateStr: `${calYear}-${String(calMonth + 1).padStart(2, "0")}-${String(d).padStart(2, "0")}`
      });
    }
    return cells;
  }, [calMonth, calYear]);

  const todayStr = useMemo(() => {
    return `${_today.getFullYear()}-${String(_today.getMonth() + 1).padStart(2, "0")}-${String(_today.getDate()).padStart(2, "0")}`;
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const eventsForSelectedDate = useMemo(
    () =>
      selectedDate
        ? calendarEvents
            .filter((e) => e.date === selectedDate)
            .sort((a, b) => a.time.localeCompare(b.time))
        : [],
    [selectedDate, calendarEvents]
  );

  const calcExpression = useMemo(() => {
    if (calcStored === null || calcOperator === null) {
      return "";
    }
    return `${formatCalcValue(calcStored)} ${calcOperator}`;
  }, [calcOperator, calcStored]);

  useEffect(() => {
    setGenericBodyVars((current) => {
      const next: Record<number, string> = {};
      requiredBodyIndexes.forEach((index) => {
        next[index] = current[index] || "";
      });
      return next;
    });
  }, [requiredBodyIndexes]);

  useEffect(() => {
    setIncidenceDetailsPage(1);
  }, [incidenceShipments.length, showIncidenceDetails]);

  useEffect(() => {
    if (incidenceDetailsPage > incidenceDetailsTotalPages) {
      setIncidenceDetailsPage(incidenceDetailsTotalPages);
    }
  }, [incidenceDetailsPage, incidenceDetailsTotalPages]);

  useEffect(() => {
    setPudoDetailsPage(1);
  }, [pudoShipments.length, showPudoDetails]);

  useEffect(() => {
    if (pudoDetailsPage > pudoDetailsTotalPages) {
      setPudoDetailsPage(pudoDetailsTotalPages);
    }
  }, [pudoDetailsPage, pudoDetailsTotalPages]);

  useEffect(() => {
    try {
      localStorage.setItem("wa_pudo_notifications", JSON.stringify(pudoNotifications));
    } catch {}
  }, [pudoNotifications]);

  useEffect(() => {
    if (pudoShipments.length === 0) {
      return;
    }

    setPudoNotifications((prev) => {
      const nowIso = new Date().toISOString();
      let changed = false;
      const next: PudoNotificationState = { ...prev };

      for (const item of pudoShipments) {
        const key = getPudoShipmentKey(item);
        if (!key) continue;
        if (!next[key]) {
          next[key] = { firstSeenAt: nowIso };
          changed = true;
        }
      }

      return changed ? next : prev;
    });
  }, [pudoShipments]);

  useEffect(() => {
    if (pudoShipments.length === 0 || sharedLogs.length === 0) {
      return;
    }

    const outgoingWhatsappLogs = sharedLogs
      .filter((item) => String(item.direction || "").toLowerCase() === "out")
      .filter((item) => String(item.channel || "").toLowerCase() === "template")
      .filter((item) => isAllowedPudoNotificationTemplate(item.template_name || ""))
      .map((item) => ({
        phone: normalizePhoneForPudoMatch(item.to_number || ""),
        createdAt: String(item.created_at || "")
      }))
      .filter((item) => item.phone.length > 0 && item.createdAt.length > 0);

    if (outgoingWhatsappLogs.length === 0) {
      return;
    }

    setPudoNotifications((prev) => {
      let changed = false;
      const next: PudoNotificationState = { ...prev };

      for (const item of pudoShipments) {
        const phone = normalizePhoneForPudoMatch(item.finalClientPhone || "");
        if (!phone) continue;

        const key = getPudoShipmentKey(item);
        if (!key) continue;

        const existing = next[key] || { firstSeenAt: new Date().toISOString() };
        if (existing.notifiedAt) continue;

        const firstSeenTs = new Date(existing.firstSeenAt).getTime();
        const matchingLog = outgoingWhatsappLogs
          .filter((entry) => entry.phone === phone)
          .find((entry) => {
            const sentTs = new Date(entry.createdAt).getTime();
            return Number.isFinite(sentTs) && (!Number.isFinite(firstSeenTs) || sentTs >= firstSeenTs);
          });

        if (!matchingLog) continue;

        next[key] = {
          ...existing,
          notifiedAt: matchingLog.createdAt
        };
        changed = true;
      }

      return changed ? next : prev;
    });
  }, [pudoShipments, sharedLogs]);

  function addEmoji(emoji: string) {
    setMessageText((current) => `${current}${emoji}`);
    setEmojiOpen(false);
  }

  function togglePudoNotified(item: {
    parcelId: string;
    providerTrackingCode?: string;
    finalClientPhone: string;
    recipient: string;
  }) {
    const key = getPudoShipmentKey(item);
    if (!key) return;

    setPudoNotifications((prev) => {
      const existing = prev[key] || { firstSeenAt: new Date().toISOString() };
      const nextStatus = existing.notifiedAt
        ? { firstSeenAt: existing.firstSeenAt }
        : { ...existing, notifiedAt: new Date().toISOString() };

      return {
        ...prev,
        [key]: nextStatus
      };
    });
  }

  function markPudoNotifiedByPhone(phoneInput: string, notifiedAtIso?: string) {
    const phone = normalizePhoneForPudoMatch(phoneInput || "");
    if (!phone) return;

    setPudoNotifications((prev) => {
      const nowIso = notifiedAtIso || new Date().toISOString();
      let changed = false;
      const next: PudoNotificationState = { ...prev };

      for (const item of pudoShipments) {
        if (normalizePhoneForPudoMatch(item.finalClientPhone || "") !== phone) continue;

        const key = getPudoShipmentKey(item);
        if (!key) continue;

        const existing = next[key] || { firstSeenAt: nowIso };
        if (existing.notifiedAt) continue;

        next[key] = {
          ...existing,
          firstSeenAt: existing.firstSeenAt || nowIso,
          notifiedAt: nowIso
        };
        changed = true;
      }

      return changed ? next : prev;
    });
  }

  function prefillPickupCttTemplate(
    phoneInput: string,
    finalClientNameInput: string,
    trackingInput: string,
    messageTypeInput = "Pick Up Point",
    notesInput = ""
  ) {
    const phoneDigits = digitsOnly(phoneInput || "");
    const formattedPhone = phoneDigits.length === 9
      ? `+351${phoneDigits}`
      : phoneDigits.startsWith("351")
        ? `+${phoneDigits}`
        : phoneDigits
          ? `+${phoneDigits}`
          : "";

    setGenericTemplateName("order_pick_up_1");
    setGenericLanguage("pt_PT");
    setGenericTo(formattedPhone);
    setGenericBodyVars((prev) => ({
      ...prev,
      1: String(finalClientNameInput || "").trim(),
      2: String(trackingInput || "").trim(),
      3: "",
      4: ""
    }));
    setGenericTrackerContext({
      clientName: String(finalClientNameInput || "").trim(),
      parcelId: String(trackingInput || "").trim(),
      messageType: String(messageTypeInput || "").trim() || "Pick Up Point",
      notes: String(notesInput || "").trim()
    });
    setGenericStatus("Template pickup CTT pré-preenchido");

    window.setTimeout(() => {
      const templateCard = document.getElementById("tracker-template-console");
      if (templateCard) {
        templateCard.scrollIntoView({ behavior: "smooth", block: "start" });
      }
      trackerTemplateToInputRef.current?.focus();
      trackerTemplateToInputRef.current?.select();
    }, 80);
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

  function saveContactNote(value: string) {
    if (!activeContactPhone) {
      return;
    }
    setContactNotes((prev) => ({
      ...prev,
      [activeContactPhone]: value
    }));
  }

  function createReminder() {
    if (!activeContactPhone || !newReminderNote.trim() || !newReminderAt.trim()) {
      return;
    }

    setTeamReminders((prev) => [
      {
        id: `r-${Date.now()}`,
        phone: activeContactPhone,
        note: newReminderNote.trim(),
        dueAt: newReminderAt,
        done: false
      },
      ...prev
    ]);

    setNewReminderNote("");
    setNewReminderAt("");
  }

  function completeReminder(reminderId: string) {
    setTeamReminders((prev) =>
      prev.map((item) => (item.id === reminderId ? { ...item, done: true } : item))
    );
  }

  // Caderno pessoal
  function addNote() {
    if (!noteContent.trim()) return;
    setPersonalNotes((prev) => [
      {
        id: `n-${Date.now()}`,
        title: noteTitle.trim() || nowLabel(),
        content: noteContent.trim(),
        color: noteColor,
        createdAt: nowLabel()
      },
      ...prev
    ]);
    setNoteTitle("");
    setNoteContent("");
  }

  function deleteNote(id: string) {
    setPersonalNotes((prev) => prev.filter((n) => n.id !== id));
    if (editingNoteId === id) setEditingNoteId(null);
  }

  function updateNote(id: string, changes: Partial<PersonalNote>) {
    setPersonalNotes((prev) => prev.map((n) => (n.id === id ? { ...n, ...changes } : n)));
  }

  // Calendário pessoal
  function addCalendarEvent() {
    if (!selectedDate || !newEventTitle.trim()) return;
    setCalendarEvents((prev) => [
      ...prev,
      { id: `e-${Date.now()}`, date: selectedDate, title: newEventTitle.trim(), time: newEventTime }
    ]);
    setNewEventTitle("");
    setNewEventTime("");
  }

  function deleteCalendarEvent(id: string) {
    setCalendarEvents((prev) => prev.filter((e) => e.id !== id));
  }

  function prevMonth() {
    if (calMonth === 0) { setCalMonth(11); setCalYear((y) => y - 1); }
    else setCalMonth((m) => m - 1);
  }

  function nextMonth() {
    if (calMonth === 11) { setCalMonth(0); setCalYear((y) => y + 1); }
    else setCalMonth((m) => m + 1);
  }

  function formatCalcValue(value: number) {
    if (!Number.isFinite(value)) return "Erro";
    return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(8)));
  }

  function applyCalcOperation(a: number, b: number, op: "+" | "-" | "*" | "/") {
    switch (op) {
      case "+": return a + b;
      case "-": return a - b;
      case "*": return a * b;
      case "/": return b === 0 ? NaN : a / b;
      default: return b;
    }
  }

  function inputCalcDigit(digit: string) {
    if (calcDisplay === "Erro") {
      setCalcDisplay(digit);
      setCalcWaitingNext(false);
      return;
    }

    if (calcWaitingNext) {
      setCalcDisplay(digit);
      setCalcWaitingNext(false);
      return;
    }

    setCalcDisplay((prev) => (prev === "0" ? digit : `${prev}${digit}`));
  }

  function inputCalcDecimal() {
    if (calcDisplay === "Erro") {
      setCalcDisplay("0.");
      setCalcWaitingNext(false);
      return;
    }

    if (calcWaitingNext) {
      setCalcDisplay("0.");
      setCalcWaitingNext(false);
      return;
    }

    if (!calcDisplay.includes(".")) {
      setCalcDisplay((prev) => `${prev}.`);
    }
  }

  function clearCalculator() {
    setCalcDisplay("0");
    setCalcStored(null);
    setCalcOperator(null);
    setCalcWaitingNext(false);
  }

  function toggleCalcSign() {
    if (calcDisplay === "0" || calcDisplay === "Erro") return;
    setCalcDisplay((prev) => (prev.startsWith("-") ? prev.slice(1) : `-${prev}`));
  }

  function applyCalcPercent() {
    if (calcDisplay === "Erro") return;
    const current = Number(calcDisplay);
    if (!Number.isFinite(current)) {
      setCalcDisplay("Erro");
      return;
    }
    setCalcDisplay(formatCalcValue(current / 100));
  }

  function chooseCalcOperator(nextOp: "+" | "-" | "*" | "/") {
    const current = Number(calcDisplay);
    if (!Number.isFinite(current)) {
      setCalcDisplay("Erro");
      return;
    }

    if (calcStored === null || calcOperator === null || calcWaitingNext) {
      setCalcStored(current);
      setCalcOperator(nextOp);
      setCalcWaitingNext(true);
      return;
    }

    const result = applyCalcOperation(calcStored, current, calcOperator);
    const formatted = formatCalcValue(result);
    setCalcDisplay(formatted);
    setCalcStored(Number.isFinite(result) ? result : null);
    setCalcOperator(nextOp);
    setCalcWaitingNext(true);
  }

  function evaluateCalculator() {
    if (calcOperator === null || calcStored === null) return;
    const current = Number(calcDisplay);
    if (!Number.isFinite(current)) {
      setCalcDisplay("Erro");
      return;
    }

    const result = applyCalcOperation(calcStored, current, calcOperator);
    const formatted = formatCalcValue(result);
    setCalcDisplay(formatted);
    setCalcStored(Number.isFinite(result) ? result : null);
    setCalcOperator(null);
    setCalcWaitingNext(true);
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
            bodyVariables: vars.length ? vars : previewBodyVars,
            trackerContext: genericTrackerContext || undefined
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
    setStatusText("A enviar media...");
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
        const existing = current.find((c) => digitsOnly(c.phone) === targetPhone);
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
      setStatusText(response.ok ? "Media enviada" : `Falhou (${response.status})`);
      if (response.ok) {
        setComposeMedia(null);
      }
    } catch {
      setStatusText("Falha no envio de media");
    } finally {
      setComposeMediaLoading(false);
    }
  }

  async function fetchMetaTemplates() {
    if (!phoneNumberId.trim()) {
      setMetaTemplatesStatus("O Phone Number ID é obrigatório para carregar templates");
      return;
    }

    setMetaTemplatesLoading(true);
    setMetaTemplatesStatus("A carregar templates...");

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
        setMetaTemplatesStatus(`Falha ao carregar templates (${response.status})`);
        return;
      }

      const rows: Array<Record<string, unknown>> = Array.isArray(data?.data) ? data.data : [];
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
        approved.length > 0 ? `${approved.length} templates aprovados carregados` : "Não foram encontrados templates aprovados"
      );

      if (approved.length > 0 && !approved.some((item) => item.name === genericTemplateName)) {
        setGenericTemplateName(approved[0].name);
        setGenericLanguage(approved[0].language || "pt_PT");
      }
    } catch (error) {
      setMetaTemplatesStatus(error instanceof Error ? error.message : "Falha ao carregar templates");
    } finally {
      setMetaTemplatesLoading(false);
    }
  }

  useEffect(() => {
    fetchMetaTemplates();
  }, [phoneNumberId, wabaId]);

  // Load persistent team state (shared across devices) and then enable autosave.
  useEffect(() => {
    let cancelled = false;

    fetch(apiUrl("/api/state"))
      .then((response) => response.json())
      .then((payload) => {
        if (cancelled) {
          return;
        }

        const data = payload?.data && typeof payload.data === "object" ? payload.data : {};

        if (data.contacts && typeof data.contacts === "object") {
          setSavedContacts(data.contacts as Record<string, string>);
        }
        if (data.contact_notes && typeof data.contact_notes === "object") {
          setContactNotes(data.contact_notes as Record<string, string>);
        }
        if (Array.isArray(data.team_reminders)) {
          setTeamReminders(data.team_reminders as TeamReminder[]);
        }
        if (Array.isArray(data.personal_notes)) {
          setPersonalNotes(data.personal_notes as PersonalNote[]);
        }
        if (Array.isArray(data.calendar_events)) {
          setCalendarEvents(data.calendar_events as CalendarEvent[]);
        }
      })
      .catch(() => {})
      .finally(() => {
        if (!cancelled) {
          setCloudStateReady(true);
        }
      });

    return () => {
      cancelled = true;
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!cloudStateReady) {
      return;
    }

    const syncHandle = window.setTimeout(() => {
      fetch(apiUrl("/api/state"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contacts: savedContacts,
          contact_notes: contactNotes,
          team_reminders: teamReminders,
          personal_notes: personalNotes,
          calendar_events: calendarEvents
        })
      }).catch(() => {});
    }, 700);

    return () => {
      window.clearTimeout(syncHandle);
    };
  }, [
    apiUrl,
    calendarEvents,
    cloudStateReady,
    contactNotes,
    personalNotes,
    savedContacts,
    teamReminders
  ]);

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

  // Persist template history
  useEffect(() => {
    try { localStorage.setItem("wa_template_history", JSON.stringify(templateHistory)); } catch {}
  }, [templateHistory]);

  // Persist team notes/reminders
  useEffect(() => {
    try { localStorage.setItem("wa_contact_notes", JSON.stringify(contactNotes)); } catch {}
  }, [contactNotes]);

  useEffect(() => {
    try { localStorage.setItem("wa_team_reminders", JSON.stringify(teamReminders)); } catch {}
  }, [teamReminders]);

  useEffect(() => {
    try { localStorage.setItem("wa_personal_notes", JSON.stringify(personalNotes)); } catch {}
  }, [personalNotes]);

  useEffect(() => {
    try { localStorage.setItem("wa_calendar_events", JSON.stringify(calendarEvents)); } catch {}
  }, [calendarEvents]);

  // SSE – delivery status ticks & scheduled_sent events
  useEffect(() => {
    const url = apiUrl("/api/events");
    const evtSource = new EventSource(url);
    evtSource.addEventListener("status", (e) => {
      try {
        const { messageId, status } = JSON.parse((e as MessageEvent).data);
        if (messageId && status) {
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
    evtSource.addEventListener("inbound", (e) => {
      try {
        const data = JSON.parse((e as MessageEvent).data) as {
          messageId?: string;
          from?: string;
          contactName?: string;
          text?: string;
          mediaType?: string;
          mediaId?: string;
          status?: string;
        };

        const fromDigits = digitsOnly(String(data.from || "").trim());
        if (!fromDigits) {
          return;
        }

        const inboundApiId = String(data.messageId || "").trim();
        const inboundText = String(data.text || "[mensagem recebida]").trim() || "[mensagem recebida]";
        const inboundMediaType = String(data.mediaType || "").trim().toLowerCase();
        const inboundMediaId = String(data.mediaId || "").trim();
        const inboundMediaUrl = inboundMediaId ? apiUrl(`/api/media/${encodeURIComponent(inboundMediaId)}`) : "";
        const inboundTime = nowLabel();

        setConversations((current) => {
          const existing = current.find((item) => digitsOnly(item.phone) === fromDigits);

          if (existing) {
            if (inboundApiId && existing.messages.some((item) => item.apiMessageId === inboundApiId)) {
              return current;
            }

            const nextMessage: ConversationMessage = {
              id: `in-${inboundApiId || Date.now()}`,
              direction: "in",
              text: inboundText,
              time: inboundTime,
              apiMessageId: inboundApiId || undefined,
              mediaType: inboundMediaType || undefined,
              mediaUrl: inboundMediaUrl || undefined
            };

            return current
              .map((item) =>
                item.id !== existing.id
                  ? item
                  : {
                      ...item,
                      lastAt: inboundTime,
                      unread: item.id === activeConversationId ? item.unread : item.unread + 1,
                      messages: [...item.messages, nextMessage]
                    }
              )
              .sort((a, b) => (a.id === existing.id ? -1 : b.id === existing.id ? 1 : 0));
          }

          const created: ConversationContact = {
            id: `c-in-${Date.now()}`,
            name: String(data.contactName || "").trim() || resolveContactName(fromDigits, savedContacts),
            phone: fromDigits,
            unread: activeConversationId ? 1 : 0,
            lastAt: inboundTime,
            messages: [
              {
                id: `in-${inboundApiId || Date.now()}`,
                direction: "in",
                text: inboundText,
                time: inboundTime,
                apiMessageId: inboundApiId || undefined,
                mediaType: inboundMediaType || undefined,
                mediaUrl: inboundMediaUrl || undefined
              }
            ]
          };

          return [created, ...current];
        });
      } catch {}
    });
    evtSource.addEventListener("bot_outbound", (e) => {
      try {
        const data = JSON.parse((e as MessageEvent).data) as {
          to?: string;
          text?: string;
          messageId?: string;
          status?: string;
        };

        const toDigits = digitsOnly(String(data.to || "").trim());
        if (!toDigits) {
          return;
        }

        const outboundApiId = String(data.messageId || "").trim();
        const outboundText = String(data.text || "[mensagem bot]").trim() || "[mensagem bot]";
        const outboundTime = nowLabel();

        setConversations((current) => {
          const existing = current.find((item) => digitsOnly(item.phone) === toDigits);

          const nextMessage: ConversationMessage = {
            id: `bot-${outboundApiId || Date.now()}`,
            direction: "out",
            text: outboundText,
            time: outboundTime,
            apiMessageId: outboundApiId || undefined,
            deliveryStatus: "sent"
          };

          if (existing) {
            if (outboundApiId && existing.messages.some((item) => item.apiMessageId === outboundApiId)) {
              return current;
            }

            return current
              .map((item) =>
                item.id !== existing.id
                  ? item
                  : {
                      ...item,
                      lastAt: outboundTime,
                      messages: [...item.messages, nextMessage]
                    }
              )
              .sort((a, b) => (a.id === existing.id ? -1 : b.id === existing.id ? 1 : 0));
          }

          const created: ConversationContact = {
            id: `c-bot-${Date.now()}`,
            name: resolveContactName(toDigits, savedContacts),
            phone: toDigits,
            unread: 0,
            lastAt: outboundTime,
            messages: [nextMessage]
          };

          return [created, ...current];
        });
      } catch {}
    });
    return () => evtSource.close();
  }, [activeConversationId, apiUrl, savedContacts]);

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

  const sentHistory = useMemo(() => {
    const chatEntries = conversations.flatMap((contact) =>
      contact.messages
        .filter((message) => message.direction === "out")
        .map((message) => ({
          id: message.id,
          channel: "chat",
          to: `${resolveContactName(contact.phone, savedContacts)} (${contact.phone})`,
          content: message.text,
          status: message.deliveryStatus || "sent",
          time: message.time,
          ts: Number(String(message.id || "").split("-")[1] || 0)
        }))
    );

    const templateEntries = templateHistory.map((item) => ({
      id: item.id,
      channel: "template",
      to: item.to,
      content: item.previewText,
      status: item.status,
      time: item.time,
      ts: Number(String(item.id || "").split("-")[1] || 0)
    }));

    return [...chatEntries, ...templateEntries].sort((a, b) => b.ts - a.ts);
  }, [conversations, savedContacts, templateHistory]);

  const displayedHistory = useMemo(() => {
    if (sharedLogs.length > 0) {
      return sharedLogs.map((item) => ({
        id: String(item.id),
        channel: String(item.channel || "chat"),
        to: item.contact_name
          ? `${item.contact_name} (${String(item.to_number || "")})`
          : String(item.to_number || ""),
        content: String(item.message_text || item.template_name || "[sem conteúdo]"),
        status: String(item.status || "sent"),
        time: new Date(item.created_at).toLocaleString(),
        ts: new Date(item.created_at).getTime()
      }));
    }

    return sentHistory;
  }, [sentHistory, sharedLogs]);

  const inboundWebhookSenders = useMemo(() => {
    const inboundItems = sharedLogs
      .filter((item) => String(item.direction || "").toLowerCase() === "in")
      .filter((item) => String(item.channel || "").toLowerCase() === "chat")
      .filter((item) => String(item.to_number || "").trim().length > 0)
      .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());

    const byPhone = new Map<string, {
      phone: string;
      name: string;
      count: number;
      lastAt: string;
      lastText: string;
    }>();

    for (const item of inboundItems) {
      const phone = digitsOnly(String(item.to_number || ""));
      if (!phone) {
        continue;
      }

      const createdAt = new Date(item.created_at);
      const timeLabel = isNaN(createdAt.getTime())
        ? String(item.created_at || "")
        : createdAt.toLocaleString();
      const lastText = String(item.message_text || "").trim();
      const name = String(item.contact_name || "").trim() || resolveContactName(phone, savedContacts);

      const existing = byPhone.get(phone);
      if (!existing) {
        byPhone.set(phone, {
          phone,
          name,
          count: 1,
          lastAt: timeLabel,
          lastText
        });
        continue;
      }

      existing.count += 1;
    }

    return Array.from(byPhone.values());
  }, [savedContacts, sharedLogs]);

  const filteredHistory = useMemo(() => {
    const q = historySearch.trim().toLowerCase();
    return displayedHistory.filter((item) => {
      const matchesText = !q
        || item.to.toLowerCase().includes(q)
        || item.content.toLowerCase().includes(q);

      if (!matchesText) {
        return false;
      }

      if (!historyDate) {
        return true;
      }

      const itemDate = new Date(item.ts);
      const yyyy = String(itemDate.getFullYear());
      const mm = String(itemDate.getMonth() + 1).padStart(2, "0");
      const dd = String(itemDate.getDate()).padStart(2, "0");
      return `${yyyy}-${mm}-${dd}` === historyDate;
    });
  }, [displayedHistory, historyDate, historySearch]);

  const trackerRows = useMemo(() => {
    if (sharedLogs.length > 0) {
      return sharedLogs.map((item) => {
        const message = String(item.message_text || item.template_name || "[sem conteúdo]");
        const status = String(item.status || "unknown");
        return {
          id: String(item.id),
          clientName: String(item.contact_name || resolveContactName(String(item.to_number || ""), savedContacts) || "-"),
          message,
          clientPhone: String(item.to_number || "-"),
          parcelId: extractParcelCode(message),
          messageType: formatMessageType(item.channel),
          dateSent: item.created_at ? new Date(item.created_at).toLocaleString("pt-PT") : "-",
          smsClicksend: String(item.channel || "").toLowerCase() === "sms" ? "Yes" : "No",
          status,
          messageTitle: String(item.template_name || "WhatsApp Message")
        };
      });
    }

    return filteredHistory.map((item) => ({
      id: String(item.id),
      clientName: "-",
      message: item.content,
      clientPhone: item.to,
      parcelId: extractParcelCode(item.content),
      messageType: item.channel === "template" ? "Template" : "Text",
      dateSent: item.time,
      smsClicksend: item.channel === "sms" ? "Yes" : "No",
      status: item.status,
      messageTitle: item.channel === "template" ? "Whatsapp Template" : "WhatsApp Message"
    }));
  }, [filteredHistory, savedContacts, sharedLogs]);

  const filteredTrackerRows = useMemo(() => {
    const query = trackerSearchQuery.trim().toLowerCase();
    if (!query) {
      return trackerRows;
    }

    return trackerRows.filter((row) => {
      const fields = {
        clientName: String(row.clientName || "").toLowerCase(),
        clientPhone: String(row.clientPhone || "").toLowerCase(),
        parcelId: String(row.parcelId || "").toLowerCase(),
        messageTitle: String(row.messageTitle || "").toLowerCase(),
        status: String(row.status || "").toLowerCase()
      };

      if (trackerSearchField === "all") {
        return Object.values(fields).some((value) => value.includes(query));
      }

      return fields[trackerSearchField].includes(query);
    });
  }, [trackerRows, trackerSearchField, trackerSearchQuery]);

  const trackerTotalPages = Math.max(1, Math.ceil(filteredTrackerRows.length / trackerPageSize));

  const paginatedTrackerRows = useMemo(() => {
    const start = (trackerPage - 1) * trackerPageSize;
    return filteredTrackerRows.slice(start, start + trackerPageSize);
  }, [filteredTrackerRows, trackerPage, trackerPageSize]);

  useEffect(() => {
    setTrackerPage(1);
  }, [trackerSearchField, trackerSearchQuery, trackerPageSize]);

  useEffect(() => {
    if (trackerPage > trackerTotalPages) {
      setTrackerPage(trackerTotalPages);
    }
  }, [trackerPage, trackerTotalPages]);

  function buildSharedLogsSignature(rows: SharedLogItem[]) {
    return rows
      .map((item) => `${item.id}:${item.created_at}:${item.status || ""}:${item.api_message_id || ""}`)
      .join("|");
  }

  function loadSharedLogs(options?: { silent?: boolean }) {
    const silent = options?.silent === true;

    if (silent && sharedLogsInFlightRef.current) {
      return;
    }

    if (!silent) {
      setSharedLogsLoading(true);
    }
    setSharedLogsError("");
    sharedLogsInFlightRef.current = true;

    fetch(apiUrl("/api/logs?limit=300"))
      .then((response) => response.json())
      .then((data) => {
        const rows = Array.isArray(data?.data) ? (data.data as SharedLogItem[]) : [];
        const nextSignature = buildSharedLogsSignature(rows);

        if (nextSignature !== sharedLogsSignatureRef.current) {
          sharedLogsSignatureRef.current = nextSignature;
          setSharedLogs(rows);
        }

        if (data?.warning === "supabase_not_configured") {
          setSharedLogsError("Supabase ainda não está configurado no backend. A mostrar histórico local.");
        }
      })
      .catch(() => {
        setSharedLogsError("Não foi possível carregar histórico partilhado. A mostrar histórico local.");
      })
      .finally(() => {
        sharedLogsInFlightRef.current = false;
        if (!silent) {
          setSharedLogsLoading(false);
        }
      });
  }

  function loadTmsDashboard() {
    setTmsLoading(true);
    setTmsError("");

    fetch(apiUrl("/api/tms/dashboard"))
      .then(async (response) => {
        const data = await parseResponse(response);
        if (!response.ok || !data?.data) {
          throw new Error(
            String(
              data?.details ||
              data?.error ||
              data?.raw ||
              `Falha TMS (${response.status})`
            )
          );
        }

        setTmsDashboard(data.data as TmsDashboardData);
      })
      .catch((error) => {
        setTmsError(error instanceof Error ? error.message : "Não foi possível carregar dados do TMS.");
      })
      .finally(() => {
        setTmsLoading(false);
      });
  }

  function loadConsumiveis() {
    setConsumiveisLoading(true);
    setConsumiveisError("");

    fetch(apiUrl("/api/consumiveis?limit=100"))
      .then(async (response) => {
        const data = await parseResponse(response);
        if (!response.ok) {
          throw new Error(String(data?.details || data?.error || `Falha Consumiveis (${response.status})`));
        }

        const rows = Array.isArray(data?.data) ? data.data : [];
        const columns: string[] = Array.isArray(data?.meta?.columns)
          ? (data.meta.columns as unknown[])
              .map((column) => String(column || ""))
              .filter((column) => column.trim().length > 0)
          : [];

        const preferredConsumiveisOrder = [
          "Client Name",
          "Date Sent",
          "Date Sent ",
          "Date Sent  ",
          "Date Sent   ",
          "Date Sent     ",
          "Tabela",
          "Tipo de Cliente",
          "Texto",
          "Texto 1",
          "Text",
          "Texto 2"
        ];

        const orderedColumns = [
          ...preferredConsumiveisOrder.filter((column) => columns.includes(column)),
          ...columns.filter((column) => !preferredConsumiveisOrder.includes(column))
        ];

        setConsumiveisColumns(orderedColumns);
        setConsumiveisRows(
          rows.map((row: Record<string, unknown>) => ({
            id: String(row.id || ""),
            fields: row.fields && typeof row.fields === "object"
              ? Object.fromEntries(
                  Object.entries(row.fields as Record<string, unknown>).map(([key, value]) => [
                    String(key || ""),
                    String(value || "-")
                  ])
                )
              : {},
            url: String(row.url || "") || undefined
          }))
        );
      })
      .catch((error) => {
        setConsumiveisError(error instanceof Error ? error.message : "Não foi possível carregar consumiveis.");
      })
      .finally(() => {
        setConsumiveisLoading(false);
      });
  }

  function loadFeedbackTracker() {
    setFeedbackLoading(true);
    setFeedbackError("");

    fetch(apiUrl("/api/feedback-tracker?limit=200"))
      .then(async (response) => {
        const data = await parseResponse(response);
        if (!response.ok) {
          throw new Error(String(data?.details || data?.error || `Falha Feedback Tracker (${response.status})`));
        }

        const rows = Array.isArray(data?.data) ? data.data : [];

        setFeedbackColumns(FEEDBACK_COLUMNS_ORDER);
        setFeedbackRows(
          rows.map((row: Record<string, unknown>) => {
            const rawFields = row.fields && typeof row.fields === "object"
              ? Object.fromEntries(
                  Object.entries(row.fields as Record<string, unknown>).map(([key, value]) => [
                    String(key || ""),
                    String(value || "-")
                  ])
                )
              : {};

            const normalizedLookup = new Map<string, string>();
            for (const [key, value] of Object.entries(rawFields)) {
              const normalized = normalizeFeedbackColumnKey(key);
              if (!normalizedLookup.has(normalized)) {
                normalizedLookup.set(normalized, String(value || "-"));
              }
            }

            const orderedFields = Object.fromEntries(
              FEEDBACK_COLUMNS_ORDER.map((targetColumn) => {
                const candidates = [targetColumn, ...(FEEDBACK_COLUMN_ALIASES[targetColumn] || [])];

                for (const candidate of candidates) {
                  const exact = rawFields[candidate];
                  if (exact !== undefined && String(exact || "").length > 0) {
                    return [targetColumn, String(exact || "-")];
                  }
                }

                for (const candidate of candidates) {
                  const normalized = normalizeFeedbackColumnKey(candidate);
                  if (normalizedLookup.has(normalized)) {
                    return [targetColumn, String(normalizedLookup.get(normalized) || "-")];
                  }
                }

                return [targetColumn, "-"];
              })
            );

            return {
              id: String(row.id || ""),
              fields: orderedFields,
              url: String(row.url || "") || undefined
            };
          })
        );
      })
      .catch((error) => {
        setFeedbackError(error instanceof Error ? error.message : "Não foi possível carregar feedback tracker.");
      })
      .finally(() => {
        setFeedbackLoading(false);
      });
  }

  function loadDeliveredShipments(page = deliveredPage) {
    const targetPage = Number.isFinite(page) ? Math.max(1, Math.trunc(page)) : 1;

    setDeliveredLoading(true);
    setDeliveredError("");

    fetch(apiUrl(`/api/tms/delivered?page=${targetPage}&limit=${deliveredPageSize}`))
      .then(async (response) => {
        const data = await parseResponse(response);
        if (!response.ok) {
          throw new Error(String(data?.details || data?.error || `Falha TMS Delivered (${response.status})`));
        }

        const rows = Array.isArray(data?.data) ? data.data : [];
        const total = Number(data?.meta?.total || rows.length) || rows.length;
        setDeliveredRows(rows as TmsDeliveredShipment[]);
        setDeliveredTotal(total);
        setDeliveredPage(targetPage);
      })
      .catch((error) => {
        setDeliveredError(error instanceof Error ? error.message : "Não foi possível carregar entregues do Linke portal.");
      })
      .finally(() => {
        setDeliveredLoading(false);
      });
  }

  async function createConsumivelEntry(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!consumiveisForm.clientName.trim()) {
      setConsumiveisError("Client Name é obrigatório.");
      return;
    }

    setConsumiveisSaving(true);
    setConsumiveisError("");

    try {
      const response = await fetch(apiUrl("/api/consumiveis"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(consumiveisForm)
      });

      const data = await parseResponse(response);
      if (!response.ok) {
        throw new Error(String(data?.details || data?.error || `Falha Consumiveis (${response.status})`));
      }

      setConsumiveisForm((current) => ({
        ...current,
        clientName: "",
        tabela: "",
        tipoCliente: "",
        texto: "",
        texto1: "",
        text: "",
        texto2: ""
      }));
      loadConsumiveis();
    } catch (error) {
      setConsumiveisError(error instanceof Error ? error.message : "Não foi possível criar registo de consumiveis.");
    } finally {
      setConsumiveisSaving(false);
    }
  }

  useEffect(() => {
    loadSharedLogs();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Keep shared logs fresh in all views so inbound messages are not missed.
  useEffect(() => {
    const intervalId = window.setInterval(() => {
      loadSharedLogs({ silent: true });
    }, 15000);

    return () => {
      window.clearInterval(intervalId);
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Rehydrate inbound chat from shared logs for resilience when SSE reconnects.
  useEffect(() => {
    const inboundLogs = sharedLogs
      .filter((item) => String(item.direction || "").toLowerCase() === "in")
      .filter((item) => String(item.channel || "").toLowerCase() === "chat")
      .filter((item) => String(item.to_number || "").trim().length > 0)
      .sort((a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime());

    if (inboundLogs.length === 0) {
      return;
    }

    setConversations((current) => {
      let next = [...current];

      for (const item of inboundLogs) {
        const fromDigits = digitsOnly(String(item.to_number || ""));
        if (!fromDigits) {
          continue;
        }

        const apiMessageId = String(item.api_message_id || "").trim();
        const text = String(item.message_text || "[mensagem recebida]").trim() || "[mensagem recebida]";
        const mediaInfo = extractInboundMediaFromLog(item);
        const mediaUrl = mediaInfo.mediaId ? apiUrl(`/api/media/${encodeURIComponent(mediaInfo.mediaId)}`) : "";
        const parsedTime = new Date(item.created_at);
        const timeLabel = isNaN(parsedTime.getTime())
          ? nowLabel()
          : parsedTime.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

        const existingIndex = next.findIndex((conversation) => digitsOnly(conversation.phone) === fromDigits);
        if (existingIndex >= 0) {
          const existing = next[existingIndex];
          const alreadyExists = apiMessageId
            ? existing.messages.some((message) => message.apiMessageId === apiMessageId)
            : existing.messages.some(
                (message) =>
                  message.direction === "in" &&
                  message.text === text &&
                  message.time === timeLabel
              );

          if (alreadyExists) {
            continue;
          }

          const updated = {
            ...existing,
            lastAt: timeLabel,
            unread: existing.id === activeConversationId ? existing.unread : existing.unread + 1,
            messages: [
              ...existing.messages,
              {
                id: `in-log-${item.id}`,
                direction: "in" as const,
                text,
                time: timeLabel,
                apiMessageId: apiMessageId || undefined,
                mediaType: mediaInfo.mediaType || undefined,
                mediaUrl: mediaUrl || undefined
              }
            ]
          };

          next[existingIndex] = updated;
          continue;
        }

        next = [
          {
            id: `c-log-${item.id}`,
            name: String(item.contact_name || "").trim() || resolveContactName(fromDigits, savedContacts),
            phone: fromDigits,
            unread: activeConversationId ? 1 : 0,
            lastAt: timeLabel,
            messages: [
              {
                id: `in-log-${item.id}`,
                direction: "in" as const,
                text,
                time: timeLabel,
                apiMessageId: apiMessageId || undefined,
                mediaType: mediaInfo.mediaType || undefined,
                mediaUrl: mediaUrl || undefined
              }
            ]
          },
          ...next
        ];
      }

      return next;
    });
  }, [activeConversationId, savedContacts, sharedLogs]);

  useEffect(() => {
    if (activeView === "tracker") {
      loadTmsDashboard();
    }
  }, [activeView]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (activeView === "consumiveis" && consumiveisRows.length === 0 && !consumiveisLoading) {
      loadConsumiveis();
    }
  }, [activeView]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (activeView === "feedback" && feedbackRows.length === 0 && !feedbackLoading) {
      loadFeedbackTracker();
    }
  }, [activeView]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (activeView === "feedback" && deliveredRows.length === 0 && !deliveredLoading) {
      loadDeliveredShipments(1);
    }
  }, [activeView]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (feedbackPage > feedbackTotalPages) {
      setFeedbackPage(feedbackTotalPages);
    }
  }, [feedbackPage, feedbackTotalPages]);

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
      setStatusText("Faltam campos obrigatórios");
      setResponseText("Número de destino e mensagem são obrigatórios.");
      return;
    }

    setLoading(true);
    setStatusText("A enviar...");

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
        const matching = current.find((item) => digitsOnly(item.phone) === digitsOnly(targetPhone));

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

      setStatusText(response.ok ? "Aceite pela API (entrega pendente)" : `Falhou (${response.status})`);
      setResponseText(JSON.stringify(data, null, 2));
      if (response.ok) {
        setMessageText("");
      }
    } catch (error) {
      setStatusText("Backend indisponível");
      setResponseText(
        error instanceof Error
          ? `${error.message}\n\nVerifica se o backend está ativo e acessível em ${backendBaseUrl || "same-origin /api"}.`
          : `Erro desconhecido\n\nVerifica se o backend está ativo e acessível em ${backendBaseUrl || "same-origin /api"}.`
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
      setMediaStatusText("Ficheiro em falta");
      setMediaResponseText("Escolhe primeiro um ficheiro.");
      return;
    }

    setMediaLoading(true);
    setMediaStatusText("A carregar...");

    try {
      const formData = new FormData();
      formData.append("file", mediaFile);
      formData.append("messaging_product", "whatsapp");

      const response = await fetch(apiUrl("/api/media/upload"), {
        method: "POST",
        body: formData
      });

      const data = await response.json();
      setMediaStatusText(response.ok ? "Media carregada" : `Falhou (${response.status})`);
      setMediaResponseText(JSON.stringify(data, null, 2));
    } catch (error) {
      setMediaStatusText("Erro de rede");
      setMediaResponseText(error instanceof Error ? error.message : "Erro desconhecido");
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
      setGenericStatus("Faltam campos obrigatórios");
      setGenericResponse("Número de destino e nome do template são obrigatórios.");
      return;
    }

    if (missingIndexes.length > 0) {
      setGenericStatus("Faltam variáveis do template");
      setGenericResponse(
        `Preenche as variáveis obrigatórias dos índices: ${missingIndexes.join(", ")}.`
      );
      return;
    }

    if (needsUrlButtonVariable && !genericButtonUrlVariable.trim()) {
      setGenericStatus("Falta variável do botão URL");
      setGenericResponse("Este template inclui um botão URL dinâmico e precisa dessa variável.");
      return;
    }

    // Feature 4: Schedule mode
    if (useSchedule) {
      if (!scheduleAt) {
        setGenericStatus("Seleciona data/hora para agendar");
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
          setGenericStatus("Agendado ✓");
        } else {
          setGenericStatus(`Falha no agendamento: ${data?.error || response.status}`);
        }
      } catch {
        setGenericStatus("Falha no pedido de agendamento");
      }
      return;
    }

    setGenericLoading(true);
    setGenericStatus("A enviar...");

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
          buttonUrlVariable: needsUrlButtonVariable ? genericButtonUrlVariable.trim() : "",
          trackerContext: genericTrackerContext || undefined
        })
      });

      const data = await response.json();
      setGenericStatus(response.ok ? "Template aceite" : `Falhou (${response.status})`);
      setGenericResponse(JSON.stringify(data, null, 2));

      if (
        response.ok &&
        (isAllowedPudoNotificationTemplate(genericTemplateName, genericLanguage) ||
          isPudoTrackerContext(genericTrackerContext))
      ) {
        markPudoNotifiedByPhone(genericTo, new Date().toISOString());
      }

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
      setGenericStatus("Erro de rede");
      setGenericResponse(error instanceof Error ? error.message : "Erro desconhecido");

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

  async function sendSmsFallback() {
    const to = digitsOnly(smsTo || genericTo);
    const message = smsText.trim();

    if (!to || !message) {
      setSmsStatus("Faltam campos obrigatórios");
      setSmsResponse("Preenche número e mensagem SMS.");
      return;
    }

    setSmsLoading(true);
    setSmsStatus("A enviar SMS...");

    try {
      const response = await fetch(apiUrl("/api/sms/clicksend"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ to, message })
      });

      const data = await parseResponse(response);
      setSmsStatus(response.ok ? "SMS enviado ✓" : `Falhou (${response.status})`);
      setSmsResponse(JSON.stringify(data, null, 2));
    } catch {
      setSmsStatus("Erro de rede");
      setSmsResponse("Não foi possível contactar o backend SMS.");
    } finally {
      setSmsLoading(false);
    }
  }

  if (!authUser) {
    return (
      <div className="auth-screen">
        <div className="auth-card">
          <div className="auth-brand-row">
            <img className="workspace-logo" src="https://portal.linke.pt/assets/img/logo/logo.svg" alt="Linke" />
            <div>
              <h1>Login do Workspace</h1>
              <p>Entra com um dos perfis da equipa para aceder ao painel de operações WhatsApp.</p>
            </div>
          </div>

          <form className="auth-form" onSubmit={handleLogin}>
            <label>
              Utilizador
              <input
                value={loginUsername}
                onChange={(event) => setLoginUsername(event.target.value)}
                placeholder="nathalia_ribeiro"
                autoComplete="username"
              />
            </label>
            <label>
              Password
              <input
                type="password"
                value={loginPassword}
                onChange={(event) => setLoginPassword(event.target.value)}
                placeholder="••••••••••"
                autoComplete="current-password"
              />
            </label>

            {loginError ? <p className="auth-error">{loginError}</p> : null}

            <button className="btn btn-primary auth-submit" type="submit" disabled={loginLoading}>
              {loginLoading ? "A entrar..." : "Entrar no workspace"}
            </button>
          </form>
        </div>
      </div>
    );
  }

  return (
    <>
    <div className="workspace-app-shell">
      <header className="workspace-topbar">
        <div className="workspace-brand">
          <img className="workspace-logo" src="https://portal.linke.pt/assets/img/logo/logo.svg" alt="Linke" />
          <span className="workspace-brand-divider" aria-hidden="true" />
          <strong>Linke Ops Dashboard</strong>
        </div>
        <div className="workspace-user">
          <span className="workspace-user-avatar">{authUser.displayName.charAt(0).toUpperCase()}</span>
          <span>{authUser.displayName}</span>
          <button type="button" className="workspace-logout" onClick={handleLogout}>
            Sair
          </button>
        </div>
      </header>

      <div className="workspace-frame">
        <aside className="workspace-sidebar-nav">
          <nav className="workspace-nav">
            <a href="#overview" className="workspace-nav-link active" onClick={() => setActiveView("workspace")}>
              <span className="workspace-nav-icon"><SidebarIcon name="overview" /></span>
              <span>Overview</span>
            </a>
            <a href="#api-console" className="workspace-nav-link" onClick={() => setActiveView("workspace")}>
              <span className="workspace-nav-icon"><SidebarIcon name="chat" /></span>
              <span>Chat Console</span>
            </a>
            <a href="#logs-page" className="workspace-nav-link" onClick={() => setActiveView("workspace")}>
              <span className="workspace-nav-icon"><SidebarIcon name="logs" /></span>
              <span>Message Logs</span>
            </a>
            <a href="#media-console" className="workspace-nav-link" onClick={() => setActiveView("workspace")}>
              <span className="workspace-nav-icon"><SidebarIcon name="upload" /></span>
              <span>Media Upload</span>
            </a>
            <a href="#generic-template-console" className="workspace-nav-link" onClick={() => setActiveView("workspace")}>
              <span className="workspace-nav-icon"><SidebarIcon name="templates" /></span>
              <span>Template Notifications</span>
            </a>
            <a href="#caderno-pessoal" className="workspace-nav-link" onClick={() => setActiveView("workspace")}>
              <span className="workspace-nav-icon"><SidebarIcon name="notes" /></span>
              <span>Notes &amp; Calendar</span>
            </a>
            <button
              type="button"
              className={`workspace-nav-link workspace-nav-button${activeView === "consumiveis" ? " active" : ""}`}
              onClick={() => setActiveView("consumiveis")}
            >
              <span className="workspace-nav-icon">📦</span>
              <span>Consumiveis</span>
            </button>
            <button
              type="button"
              className={`workspace-nav-link workspace-nav-button${activeView === "tracker" ? " active" : ""}`}
              onClick={() => {
                setActiveView("tracker");
                loadSharedLogs();
              }}
            >
              <span className="workspace-nav-icon"><SidebarIcon name="logs" /></span>
              <span>Client Tracker</span>
            </button>
            <button
              type="button"
              className={`workspace-nav-link workspace-nav-button${activeView === "feedback" ? " active" : ""}`}
              onClick={() => setActiveView("feedback")}
            >
              <span className="workspace-nav-icon">📝</span>
              <span>Feedback Tracker</span>
            </button>
          </nav>
        </aside>

        <main className="workspace-content">
          {activeView === "workspace" ? (
            <div className="page">
      <header className="hero" id="overview">
        <div className="badge">www.linke.pt</div>
        <h1>Workspace da Equipa para Operações WhatsApp</h1>
        <p>
          Gere conversas, templates e envios num único lugar. Feito para o trabalho diário
          de uma equipa pequena, com contexto partilhado e foco na execução.
        </p>
        <div className="hero-actions">
          <a href="#api-console" className="btn btn-primary">
            Abrir Workspace
          </a>
          <a href="#team-tools" className="btn btn-secondary">
            Ver Notas da Equipa
          </a>
          <a href="#logs-page" className="btn btn-secondary">
            Ver Logs
          </a>
        </div>
      </header>

      <section className="panel" id="atalhos-linke">
        <h2>Atalhos Rápidos Linke</h2>
        <p>
          Acesso direto às ferramentas mais usadas pela equipa de mediação logística.
        </p>
        <div className="shortcut-grid">
          {quickApps.map((app) => (
            <a
              key={app.url}
              className="shortcut-card"
              href={app.url}
              target="_blank"
              rel="noreferrer"
            >
              <div className="shortcut-card-head">
                <img className="shortcut-icon" src={app.icon} alt={`${app.name} logo`} loading="lazy" />
                <strong>{app.name}</strong>
              </div>
              <span>{app.description}</span>
              <small>{app.url}</small>
            </a>
          ))}
        </div>
      </section>

      <section className="panel" id="api-console">
        <h2>Consola de Mensagens WhatsApp Cloud API</h2>
        <p>
          Baseado na documentação oficial: <strong>POST /{`{Version}`}/{`{Phone-Number-ID}`}/messages</strong>
          com autenticação Bearer e payload JSON, encaminhado pelo backend da equipa.
        </p>

        <div className="wa-console">
          <aside className="wa-sidebar">
            <div className="wa-search">
              <input
                placeholder="Pesquisar contactos..."
                value={contactSearch}
                onChange={(e) => setContactSearch(e.target.value)}
              />
            </div>

            <div className="wa-contact-list">
              {conversations.length === 0 ? (
                <div className="wa-empty-contacts">
                  Ainda não há conversas. Envia uma mensagem para criar histórico.
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
                      <small>{last?.text || "Sem mensagens"}</small>
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
                👤 {contactBookOpen ? "Esconder contactos" : "Gerir contactos"}
              </button>
            </div>
            {contactBookOpen ? (
              <div className="wa-contact-book">
                <div className="wa-contact-book-add">
                  <input
                    placeholder="Telefone (dígitos)"
                    value={newContactPhone}
                    onChange={(e) => setNewContactPhone(e.target.value)}
                  />
                  <input
                    placeholder="Nome"
                    value={newContactName}
                    onChange={(e) => setNewContactName(e.target.value)}
                    onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); saveContact(); } }}
                  />
                  <button type="button" className="wa-contact-book-save" onClick={saveContact}>Guardar contacto</button>
                </div>
                {Object.keys(savedContacts).length > 0 ? (
                  <div className="wa-contact-book-list">
                    {Object.entries(savedContacts).map(([digits, name]) => (
                      <div key={digits} className="wa-contact-book-row">
                        <div className="wa-contact-book-meta">
                          <strong>{name}</strong>
                          <span>{digits}</span>
                        </div>
                        <button type="button" className="wa-contact-book-del" onClick={() => removeContact(digits)}>×</button>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="wa-empty-contacts">Ainda não existem contactos guardados.</p>
                )}
              </div>
            ) : null}
          </aside>

          <div className="wa-phone">
            <header className="wa-phone-header">
              <div className="wa-avatar">LN</div>
              <div>
                <strong>{activeConversation ? resolveContactName(activeConversation.phone, savedContacts) : "Chat de cliente"}</strong>
                <small>{toNumber || activeConversation?.phone || "Sem destinatário"}</small>
              </div>
              <span className={`wa-live-status ${loading ? "busy" : ""}`}>{statusText}</span>
            </header>

            <main className="wa-thread">
              {(activeConversation?.messages || []).map((message) => (
                <article key={message.id} className={`wa-msg ${message.direction === "in" ? "in" : "out"}`}>
                  {message.mediaUrl && (message.mediaType === "image" || message.mediaType === "sticker") ? (
                    <img className="wa-msg-media" src={message.mediaUrl} alt={message.mediaType || "media"} loading="lazy" />
                  ) : null}
                  {message.mediaUrl && message.mediaType === "video" ? (
                    <video className="wa-msg-media" src={message.mediaUrl} controls preload="metadata" />
                  ) : null}
                  {message.mediaUrl && message.mediaType === "audio" ? (
                    <audio className="wa-msg-audio" src={message.mediaUrl} controls preload="metadata" />
                  ) : null}
                  {message.mediaUrl && message.mediaType === "document" ? (
                    <a className="wa-msg-doc" href={message.mediaUrl} target="_blank" rel="noreferrer">Abrir documento</a>
                  ) : null}
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
                  <time>{loading ? "a enviar" : "rascunho"}</time>
                </article>
              ) : null}
            </main>

            <form className="wa-compose" onSubmit={sendMessage}>
              <label>
                Número de destino (E.164)
                <input
                  value={toNumber}
                  onChange={(event) => setToNumber(event.target.value)}
                  placeholder="3519XXXXXXXX"
                />
              </label>
              <label>
                Mensagem
                <div className="wa-message-bar">
                  <div className="wa-emoji-wrap">
                    <button
                      type="button"
                      className="wa-emoji"
                      onClick={() => setEmojiOpen((open) => !open)}
                      aria-label="Abrir seletor de emoji"
                      title="Abrir seletor de emoji"
                    >
                      🙂
                    </button>
                    {emojiOpen ? (
                      <div className="wa-emoji-picker" role="menu" aria-label="Seletor de emoji">
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
                    placeholder="Escreve a resposta..."
                  />
                  <label className="wa-attach-btn" title="Anexar ficheiro">
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
                    {composeMediaLoading ? "A enviar..." : "Enviar ficheiro"}
                  </button>
                  <button type="button" className="wa-attach-clear" onClick={() => setComposeMedia(null)}>×</button>
                </div>
              ) : null}
              <button className="wa-send" type="submit" disabled={loading}>
                {loading ? "A enviar..." : "Enviar mensagem"}
              </button>
            </form>

            <section className="team-tools" id="team-tools">
              <h3>Notas e Lembretes da Equipa</h3>
              <p>
                Contexto interno por contacto para não perder informação entre turnos.
              </p>

              <label>
                Nota interna ({activeContactPhone || "sem contacto ativo"})
                <textarea
                  value={activeContactNote}
                  onChange={(event) => saveContactNote(event.target.value)}
                  placeholder="Ex.: Cliente pediu confirmação amanhã de manhã."
                  rows={3}
                  disabled={!activeContactPhone}
                />
              </label>

              <div className="team-reminder-grid">
                <input
                  value={newReminderNote}
                  onChange={(event) => setNewReminderNote(event.target.value)}
                  placeholder="Lembrete (ex.: ligar para validar morada)"
                />
                <input
                  type="datetime-local"
                  value={newReminderAt}
                  onChange={(event) => setNewReminderAt(event.target.value)}
                />
                <button
                  type="button"
                  className="btn btn-secondary team-reminder-create"
                  onClick={createReminder}
                  disabled={!activeContactPhone || !newReminderNote.trim() || !newReminderAt}
                >
                  + Criar lembrete
                </button>
              </div>

              {activeContactReminders.length > 0 ? (
                <div className="team-reminder-list">
                  {activeContactReminders.map((item) => (
                    <article key={item.id} className="team-reminder-item">
                      <div>
                        <strong>{item.note}</strong>
                        <small>{new Date(item.dueAt).toLocaleString("pt-PT")}</small>
                      </div>
                      <button type="button" className="btn btn-secondary" onClick={() => completeReminder(item.id)}>
                        Concluído
                      </button>
                    </article>
                  ))}
                </div>
              ) : (
                <p className="team-reminder-empty">Sem lembretes pendentes para este contacto.</p>
              )}
            </section>

            <details className="wa-details">
              <summary>Detalhes da API</summary>

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
                  Versão da API
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
                  <h3>Endpoint Resolvido</h3>
                  <pre>{endpoint}</pre>
                </article>
                <article className="card code-block">
                  <h3>Relay Backend</h3>
                  <pre>{apiUrl("/api/messages/send")}</pre>
                </article>
                <article className="card code-block">
                  <h3>Payload do Pedido</h3>
                  <pre>{JSON.stringify(payload, null, 2)}</pre>
                </article>
                <article className="card code-block">
                  <h3>Exemplo cURL</h3>
                  <pre>{curlCommand}</pre>
                </article>
                <article className="card code-block">
                  <h3>Resposta da API</h3>
                  <pre>{responseText}</pre>
                </article>
              </div>
            </details>
          </div>
        </div>
      </section>

      <section className="panel" id="logs-page">
        <h2>Logs de Mensagens</h2>
        <p>
          Pesquisa por nome, número e data em todos os envios registados.
        </p>

        <div className="sent-history-toolbar">
          <button
            type="button"
            className="btn btn-secondary"
            onClick={() => loadSharedLogs()}
            disabled={sharedLogsLoading}
          >
            {sharedLogsLoading ? "A atualizar..." : "Atualizar logs"}
          </button>
          <span className="status">{filteredHistory.length}/{displayedHistory.length} mensagens</span>
        </div>

        <section className="sent-history-panel">
          <div className="webhook-debug-panel">
            <h3>Debug webhook inbound (números recentes)</h3>
            {sharedLogsLoading ? (
              <p>A carregar sinais de inbound...</p>
            ) : null}
            {inboundWebhookSenders.length === 0 ? (
              <p>Sem inbound webhook visível ainda. Se não aparecer, confirme no Meta Dashboard se o webhook recebe eventos de mensagens.</p>
            ) : (
              <div className="webhook-debug-list">
                {inboundWebhookSenders.slice(0, 20).map((sender) => (
                  <article key={`inbound-debug-${sender.phone}`} className="webhook-debug-item">
                    <header>
                      <strong>{sender.name}</strong>
                      <span>{sender.lastAt}</span>
                    </header>
                    <p>Número: {sender.phone}</p>
                    <p>Mensagens inbound: {sender.count}</p>
                    <p>Última: {sender.lastText || "[sem texto]"}</p>
                  </article>
                ))}
              </div>
            )}
          </div>

          <div className="sent-history-filters">
            <input
              placeholder="Pesquisar por nome, número ou texto"
              value={historySearch}
              onChange={(e) => setHistorySearch(e.target.value)}
            />
            <input
              type="date"
              value={historyDate}
              onChange={(e) => setHistoryDate(e.target.value)}
            />
            <button
              type="button"
              className="btn btn-secondary"
              onClick={() => { setHistorySearch(""); setHistoryDate(""); }}
            >
              Limpar
            </button>
          </div>
          {sharedLogsLoading ? (
            <p>A carregar histórico partilhado...</p>
          ) : null}
          {sharedLogsError ? (
            <p className="status">{sharedLogsError}</p>
          ) : null}
          {filteredHistory.length === 0 ? (
            <p>Ainda não existem mensagens enviadas no histórico.</p>
          ) : (
            <div className="sent-history-list">
              {filteredHistory.map((item) => (
                <article key={`${item.channel}-${item.id}`} className="sent-history-item">
                  <header>
                    <strong>{item.channel === "template" ? "Template" : "Mensagem"}</strong>
                    <span>{item.time}</span>
                  </header>
                  <p>Para: {item.to}</p>
                  <p>{item.content}</p>
                  <span className={`status sent-history-status sent-history-status-${statusTone(item.status, item.channel)}`}>
                    <span className="sent-history-dot" aria-hidden="true" />
                    Estado: {item.status}
                  </span>
                </article>
              ))}
            </div>
          )}
        </section>
      </section>

      <section className="panel" id="media-console">
        <div className="api-actions">
          <h2>Consola de Upload de Media</h2>
          <button
            type="button"
            className="btn btn-secondary"
            onClick={() => setMediaConsoleExpanded((current) => !current)}
          >
            {mediaConsoleExpanded ? "Recolher secção" : "Expandir secção"}
          </button>
        </div>

        {mediaConsoleExpanded ? (
          <>
            <p>
              Baseado na documentação oficial: <strong>POST /{`{Version}`}/{`{Phone-Number-ID}`}/media</strong>
              com multipart form-data. Este endpoint devolve um <strong>id</strong> de media para usar
              em mensagens de media.
            </p>

            <form className="api-form" onSubmit={uploadMedia}>
              <label>
                Selecionar Ficheiro de Media
                <input
                  type="file"
                  onChange={(event) => setMediaFile(event.target.files?.[0] || null)}
                  accept="image/*,video/*,audio/*,.pdf,.doc,.docx,.txt,.webp"
                />
              </label>

              <div className="api-actions">
                <button className="btn btn-primary" type="submit" disabled={mediaLoading}>
                  {mediaLoading ? "A carregar..." : "Carregar media"}
                </button>
                <span className="status">Estado: {mediaStatusText}</span>
              </div>
            </form>

            <div className="code-grid">
              <article className="card code-block">
                <h3>Endpoint Graph (Upload)</h3>
                <pre>{`https://graph.facebook.com/${apiVersion}/${phoneNumberId}/media`}</pre>
              </article>
              <article className="card code-block">
                <h3>Relay Backend</h3>
                <pre>{apiUrl("/api/media/upload")}</pre>
              </article>
              <article className="card code-block">
                <h3>Exemplo cURL</h3>
                <pre>{mediaCurlCommand}</pre>
              </article>
              <article className="card code-block">
                <h3>Resposta do Upload</h3>
                <pre>{mediaResponseText}</pre>
              </article>
            </div>
          </>
        ) : (
          <p className="status">Secção recolhida para manter o workspace mais leve.</p>
        )}
      </section>

      <section className="panel" id="generic-template-console">
        <div className="api-actions">
          <h2>Notificações por Template</h2>
          <button
            type="button"
            className="btn btn-secondary"
            onClick={() => setTemplateConsoleExpanded((current) => !current)}
          >
            {templateConsoleExpanded ? "Recolher secção" : "Expandir secção"}
          </button>
        </div>

        {!templateConsoleExpanded ? (
          <p className="status">Secção recolhida para manter o workspace mais leve.</p>
        ) : (
          <>
        <p>
          Escolhe um template aprovado da tua conta Meta, preenche variáveis, pré-visualiza
          a mensagem e envia a notificação.
        </p>

        <div className="template-toolbar">
          <input
            value={wabaId}
            onChange={(event) => setWabaId(event.target.value)}
            placeholder="Opcional: colar WABA ID"
          />
          <button
            className="btn btn-secondary"
            type="button"
            onClick={fetchMetaTemplates}
            disabled={metaTemplatesLoading}
          >
            {metaTemplatesLoading ? "A carregar..." : "Atualizar templates"}
          </button>
          <span className="status">{metaTemplatesStatus}</span>
        </div>

        <form className="api-form" onSubmit={sendGenericTemplate}>
          <label>
            Número de destino (E.164)
            <input
              value={genericTo}
              onChange={(event) => setGenericTo(event.target.value)}
              placeholder="+351912858229"
            />
          </label>

          <label>
            Template (Aprovado)
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
              {metaTemplates.length === 0 ? <option value="">Sem templates carregados</option> : null}
              {metaTemplates.map((template) => (
                <option key={template.id || template.name} value={template.name}>
                  {template.name} ({template.language || "pt_PT"})
                </option>
              ))}
            </select>
          </label>

          <label>
            Código de idioma
            <input
              value={genericLanguage}
              onChange={(event) => setGenericLanguage(event.target.value)}
              placeholder="pt_PT"
            />
          </label>

          <span className="status">
            Variáveis obrigatórias no body: {requiredBodyVarCount} {requiredBodyVarCount > 0 ? `(índices: ${requiredBodyIndexes.join(", ")})` : ""}
          </span>

          {requiredBodyVarCount > 0 ? (
            <div className="template-var-grid">
              {requiredBodyIndexes.map((index) => (
                <label key={index}>
                  Variável {`{{${index}}}`}
                  <input
                    value={genericBodyVars[index] || ""}
                    onChange={(event) =>
                      setGenericBodyVars((current) => ({
                        ...current,
                        [index]: event.target.value
                      }))
                    }
                    placeholder={`Valor para {{${index}}}`}
                  />
                </label>
              ))}
            </div>
          ) : null}

          {requiredBodyVarCount > 0 ? (
            <div className="preset-bar">
              <button type="button" className="btn btn-secondary" onClick={savePreset}>
                💾 Guardar preset
              </button>
              {templatePresets[genericTemplateName] ? (
                <button type="button" className="btn btn-secondary" onClick={loadPreset}>
                  📂 Carregar preset
                </button>
              ) : null}
            </div>
          ) : null}

          <article className="template-chat-box">
            <header>
              <strong>Caixa de Texto do Template</strong>
              <span>{genericTemplateName || "Sem template selecionado"}</span>
            </header>
            <div className="template-thread">
              <article className="wa-msg in">
                <p>Template selecionado: {genericTemplateName || "-"}</p>
                <time>{genericLanguage || "pt_PT"}</time>
              </article>
              <article className="wa-msg out">
                <p>{selectedTemplatePreview || selectedTemplateBody || "Sem texto no body do template selecionado"}</p>
                <time>pré-visualização</time>
              </article>
            </div>
          </article>

          <article className="sms-fallback-box">
            <header>
              <strong>Fallback SMS (ClickSend)</strong>
              <span>Usar quando WhatsApp falhar</span>
            </header>
            <div className="sms-fallback-thread">
              <article className="sms-msg out">
                <p>{smsText.trim() || "Escreve aqui a mensagem SMS de fallback..."}</p>
                <time>pré-visualização</time>
              </article>
            </div>
            <div className="sms-fallback-form">
              <input
                value={smsTo}
                onChange={(event) => setSmsTo(event.target.value)}
                placeholder="Número SMS (E.164)"
              />
              <textarea
                value={smsText}
                onChange={(event) => setSmsText(event.target.value)}
                rows={2}
                placeholder="Mensagem SMS (fallback ClickSend)"
              />
              <div className="sms-fallback-actions">
                <button
                  type="button"
                  className="btn btn-sms"
                  onClick={sendSmsFallback}
                  disabled={smsLoading || !smsText.trim() || !digitsOnly(smsTo || genericTo)}
                >
                  {smsLoading ? "A enviar SMS..." : "Enviar SMS fallback"}
                </button>
                <span className="status">Estado: {smsStatus}</span>
              </div>
              <details className="sms-response">
                <summary>Resposta SMS</summary>
                <pre>{smsResponse}</pre>
              </details>
            </div>
          </article>

          {needsUrlButtonVariable ? (
            <label>
              Variável do Botão URL (obrigatória)
              <input
                value={genericButtonUrlVariable}
                onChange={(event) => setGenericButtonUrlVariable(event.target.value)}
                placeholder="variável dinâmica de URL"
              />
            </label>
          ) : null}

          <label className="schedule-toggle">
            <input
              type="checkbox"
              checked={useSchedule}
              onChange={(e) => setUseSchedule(e.target.checked)}
            />
            Agendar para mais tarde
          </label>
          {useSchedule ? (
            <label>
              Enviar em
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
              {genericLoading ? "A enviar..." : useSchedule ? "⏰ Agendar mensagem" : "Enviar notificação"}
            </button>
            <span className="status">Estado: {genericStatus}</span>
          </div>
        </form>

        <div className="code-grid">
          <article className="card code-block">
            <h3>Relay Backend</h3>
            <pre>{apiUrl("/api/templates/send-generic")}</pre>
          </article>
          <article className="card code-block">
            <h3>Resposta do Template</h3>
            <pre>{genericResponse}</pre>
          </article>
        </div>

        {scheduledItems.length > 0 ? (
          <section className="template-history">
            <h3>⏰ Mensagens Agendadas</h3>
            <div className="template-history-list">
              {scheduledItems.map((item) => (
                <article key={item.id} className="template-history-item">
                  <header>
                    <strong>{item.templateName}</strong>
                    <span>{item.scheduledAt ? new Date(item.scheduledAt).toLocaleString() : ""}</span>
                  </header>
                  <p>Para: {item.to}</p>
                  <span className={`status ${item.status === "sent" ? "status-ok" : item.status === "failed" ? "status-err" : ""}`}>
                    Estado: {item.status}
                  </span>
                </article>
              ))}
            </div>
          </section>
        ) : null}

        <details className="bulk-send-section">
          <summary>📋 Envio em Massa (CSV)</summary>
          <div className="bulk-send-body">
            <p>Um destinatário por linha: <code>telefone,var1,var2,...</code> - usa o template e idioma atuais.</p>
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
                  ? `A enviar ${bulkProgress.sent}/${bulkProgress.total}...`
                  : "Enviar para todos"}
              </button>
              {bulkProgress.total > 0 && !bulkRunning ? (
                <span className="status">{bulkProgress.sent}/{bulkProgress.total} enviados</span>
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
          </>
        )}
      </section>

      <section className="panel caderno-panel" id="caderno-pessoal">
        <h2>📓 Caderno &amp; Calendário</h2>
        <p>As tuas notas e compromissos são guardados automaticamente no browser e sincronizados na cloud quando o backend está ativo.</p>

        <div className="caderno-layout">

          {/* ── BLOCO DE NOTAS ── */}
          <div className="caderno-notas">
            <div className="caderno-capa">
              <span>Bloco de Notas</span>
            </div>

            <div className="caderno-formulario">
              <div className="nota-cores">
                {(["amarelo", "verde", "azul", "rosa", "branco"] as const).map((c) => (
                  <button
                    key={c}
                    type="button"
                    aria-label={c}
                    className={`cor-btn cor-${c}${noteColor === c ? " ativo" : ""}`}
                    onClick={() => setNoteColor(c)}
                  />
                ))}
              </div>
              <input
                className="nota-titulo-input"
                placeholder="Título (opcional)"
                value={noteTitle}
                onChange={(e) => setNoteTitle(e.target.value)}
              />
              <textarea
                className="nota-conteudo"
                placeholder="Escreve aqui a tua nota..."
                value={noteContent}
                rows={3}
                onChange={(e) => setNoteContent(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) addNote();
                }}
              />
              <button type="button" className="btn btn-primary btn-sm" onClick={addNote}>
                + Adicionar nota
              </button>
            </div>

            <div className="notas-lista">
              {personalNotes.length === 0 ? (
                <p className="notas-vazio">Nenhuma nota ainda. Começa a escrever acima.</p>
              ) : null}
              {personalNotes.map((nota) => (
                <div key={nota.id} className={`nota-card nota-cor-${nota.color}`}>
                  <div className="nota-header">
                    {editingNoteId === nota.id ? (
                      <input
                        className="nota-titulo-edit"
                        value={nota.title}
                        onChange={(e) => updateNote(nota.id, { title: e.target.value })}
                      />
                    ) : (
                      <strong className="nota-titulo">{nota.title}</strong>
                    )}
                    <div className="nota-acoes">
                      <button
                        type="button"
                        title={editingNoteId === nota.id ? "Guardar" : "Editar"}
                        onClick={() => setEditingNoteId(editingNoteId === nota.id ? null : nota.id)}
                      >
                        {editingNoteId === nota.id ? "✓" : "✏️"}
                      </button>
                      <button type="button" title="Apagar" onClick={() => deleteNote(nota.id)}>
                        ✕
                      </button>
                    </div>
                  </div>

                  {editingNoteId === nota.id ? (
                    <textarea
                      className="nota-conteudo nota-conteudo-edit"
                      value={nota.content}
                      rows={4}
                      onChange={(e) => updateNote(nota.id, { content: e.target.value })}
                    />
                  ) : (
                    <p className="nota-texto">{nota.content}</p>
                  )}
                  <small className="nota-data">{nota.createdAt}</small>
                </div>
              ))}
            </div>
          </div>

          {/* ── CALENDÁRIO ── */}
          <div className="caderno-calendario">
            <div className="cal-cabecalho">
              <button type="button" className="cal-nav" onClick={prevMonth}>‹</button>
              <span className="cal-mes-label">
                {new Date(calYear, calMonth).toLocaleDateString("pt-PT", { month: "long", year: "numeric" })}
              </span>
              <button type="button" className="cal-nav" onClick={nextMonth}>›</button>
            </div>

            <div className="cal-grid">
              {["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"].map((d) => (
                <div key={d} className="cal-dow">{d}</div>
              ))}
              {calendarDays.map((cell, i) => {
                if (!cell) return <div key={`empty-${i}`} className="cal-cell cal-vazio" />;
                const hasEvents = calendarEvents.some((e) => e.date === cell.dateStr);
                const isToday = cell.dateStr === todayStr;
                const isSelected = cell.dateStr === selectedDate;
                return (
                  <button
                    key={cell.dateStr}
                    type="button"
                    className={[
                      "cal-cell",
                      isToday ? "cal-hoje" : "",
                      isSelected ? "cal-selecionado" : "",
                      hasEvents ? "cal-tem-evento" : ""
                    ].filter(Boolean).join(" ")}
                    onClick={() => setSelectedDate(isSelected ? null : cell.dateStr)}
                  >
                    {cell.day}
                    {hasEvents && <span className="cal-ponto" />}
                  </button>
                );
              })}
            </div>

            {selectedDate ? (
              <div className="cal-eventos">
                <h4 className="cal-data-titulo">
                  {new Date(`${selectedDate}T12:00:00`).toLocaleDateString("pt-PT", {
                    weekday: "long", day: "numeric", month: "long"
                  })}
                </h4>
                <div className="cal-novo-evento">
                  <input
                    placeholder="Título do evento"
                    value={newEventTitle}
                    onChange={(e) => setNewEventTitle(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && addCalendarEvent()}
                  />
                  <input
                    type="time"
                    value={newEventTime}
                    onChange={(e) => setNewEventTime(e.target.value)}
                  />
                  <button type="button" className="btn btn-primary btn-sm" onClick={addCalendarEvent}>
                    +
                  </button>
                </div>
                {eventsForSelectedDate.length === 0 ? (
                  <p className="cal-sem-eventos">Sem eventos neste dia.</p>
                ) : (
                  <ul className="cal-lista-eventos">
                    {eventsForSelectedDate.map((ev) => (
                      <li key={ev.id} className="cal-evento-item">
                        {ev.time && <span className="cal-evento-hora">{ev.time}</span>}
                        <span className="cal-evento-titulo">{ev.title}</span>
                        <button
                          type="button"
                          className="cal-remover"
                          onClick={() => deleteCalendarEvent(ev.id)}
                        >
                          ✕
                        </button>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            ) : (
              <p className="cal-dica">Clica num dia para ver ou adicionar eventos.</p>
            )}

            <div className="cute-calculator" aria-label="Calculadora">
              <div className="cute-calculator-header">
                <span>🧮 Mini Calculadora</span>
                <small>contas rápidas</small>
              </div>
              <div className="cute-calculator-display-wrap">
                <div className="cute-calculator-expression">{calcExpression || " "}</div>
                <div className="cute-calculator-display">{calcDisplay}</div>
              </div>
              <div className="cute-calculator-grid">
                <button type="button" className="calc-btn calc-func" onClick={clearCalculator}>AC</button>
                <button type="button" className="calc-btn calc-func" onClick={toggleCalcSign}>+/-</button>
                <button type="button" className="calc-btn calc-func" onClick={applyCalcPercent}>%</button>
                <button type="button" className="calc-btn calc-op" onClick={() => chooseCalcOperator("/")}>÷</button>

                <button type="button" className="calc-btn" onClick={() => inputCalcDigit("7")}>7</button>
                <button type="button" className="calc-btn" onClick={() => inputCalcDigit("8")}>8</button>
                <button type="button" className="calc-btn" onClick={() => inputCalcDigit("9")}>9</button>
                <button type="button" className="calc-btn calc-op" onClick={() => chooseCalcOperator("*")}>×</button>

                <button type="button" className="calc-btn" onClick={() => inputCalcDigit("4")}>4</button>
                <button type="button" className="calc-btn" onClick={() => inputCalcDigit("5")}>5</button>
                <button type="button" className="calc-btn" onClick={() => inputCalcDigit("6")}>6</button>
                <button type="button" className="calc-btn calc-op" onClick={() => chooseCalcOperator("-")}>-</button>

                <button type="button" className="calc-btn" onClick={() => inputCalcDigit("1")}>1</button>
                <button type="button" className="calc-btn" onClick={() => inputCalcDigit("2")}>2</button>
                <button type="button" className="calc-btn" onClick={() => inputCalcDigit("3")}>3</button>
                <button type="button" className="calc-btn calc-op" onClick={() => chooseCalcOperator("+")}>+</button>

                <button type="button" className="calc-btn calc-zero" onClick={() => inputCalcDigit("0")}>0</button>
                <button type="button" className="calc-btn" onClick={inputCalcDecimal}>.</button>
                <button type="button" className="calc-btn calc-eq" onClick={evaluateCalculator}>=</button>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="panel cta" id="contact">
        <h2>Client Messages Tracker</h2>
        <p>Abre uma vista clara em formato tabela com mensagens enviadas, estado e detalhes por cliente.</p>
        <button
          type="button"
          className="btn btn-primary"
          onClick={() => {
            setActiveView("tracker");
            loadSharedLogs();
          }}
        >
          Abrir Tracker Completo
        </button>
      </section>
            </div>
          ) : activeView === "tracker" ? (
            <section className="panel tracker-page" id="client-tracker-page">
              <div className="tracker-header">
                <div>
                  <h2>Client WhatsApp Messages Tracker</h2>
                  <p>Vista estilo tabela para acompanhar mensagens, estado e dados operacionais.</p>
                </div>
                <div className="tracker-actions">
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={loadTmsDashboard}
                    disabled={tmsLoading}
                  >
                    {tmsLoading ? "A atualizar TMS..." : "Atualizar TMS"}
                  </button>
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={() => loadSharedLogs()}
                    disabled={sharedLogsLoading}
                  >
                    {sharedLogsLoading ? "A atualizar..." : "Atualizar"}
                  </button>
                  <button
                    type="button"
                    className="btn btn-primary"
                    onClick={() => setActiveView("workspace")}
                  >
                    Voltar ao Workspace
                  </button>
                </div>
              </div>

              <section className="tms-panel">
                <header className="tms-panel-head">
                  <div>
                    <h3>Control Panel TMS</h3>
                    <p>Estado atual dos serviços e envios pendentes de aceitação.</p>
                  </div>
                  <span className="status">
                    {tmsDashboard?.meta?.fetchedAt
                      ? `Atualizado: ${new Date(tmsDashboard.meta.fetchedAt).toLocaleString("pt-PT")}`
                      : "Sem dados"}
                  </span>
                </header>

                {tmsError ? <p className="status">{tmsError}</p> : null}

                {tmsDashboard ? (
                  <>
                    <div className="tms-kpis">
                      {tmsDashboard.infoBoxes.map((box) => (
                        <article key={box.label} className="tms-kpi-card">
                          <span>{box.label}</span>
                          <strong>{box.value}</strong>
                          <small>{box.trend || ""}</small>
                        </article>
                      ))}
                    </div>

                    <div className="tms-grid">
                      <article className="tms-block">
                        <h4>Estado atual dos serviços</h4>
                        <div className="tms-table-wrap">
                          <table className="tms-table">
                            <thead>
                              <tr>
                                <th>Serviço</th>
                                <th>⏱</th>
                                <th>✓</th>
                                <th>🛒</th>
                                <th>🚚</th>
                                <th>✅</th>
                                <th>⚠</th>
                                <th>⚠ Ongoing</th>
                              </tr>
                            </thead>
                            <tbody>
                              {tmsDashboard.serviceStatus.rows.map((row) => (
                                <tr key={row.service}>
                                  <td>{row.service}</td>
                                  <td>{row.pending}</td>
                                  <td>{row.accepted}</td>
                                  <td>{row.pickup}</td>
                                  <td>{row.transport}</td>
                                  <td>{row.delivered}</td>
                                  <td>{row.incidence}</td>
                                  <td>{row.incidenceOngoing ?? row.incidence}</td>
                                </tr>
                              ))}
                              {tmsDashboard.serviceStatus.totals ? (
                                <tr className="tms-total-row">
                                  <td>TOTAL</td>
                                  <td>{tmsDashboard.serviceStatus.totals.pending}</td>
                                  <td>{tmsDashboard.serviceStatus.totals.accepted}</td>
                                  <td>{tmsDashboard.serviceStatus.totals.pickup}</td>
                                  <td>{tmsDashboard.serviceStatus.totals.transport}</td>
                                  <td>{tmsDashboard.serviceStatus.totals.delivered}</td>
                                  <td>{tmsDashboard.serviceStatus.totals.incidence}</td>
                                  <td>{tmsDashboard.serviceStatus.totals.incidence}</td>
                                </tr>
                              ) : null}
                            </tbody>
                          </table>
                        </div>
                      </article>

                      <article className="tms-block">
                        <h4>Envios pendentes de aceitação</h4>
                        <div className="tms-pending-list">
                          {tmsDashboard.pendingAcceptance.map((row) => (
                            <div key={`${row.customer}-${row.shipments}`} className="tms-pending-item">
                              <span>{row.customer}</span>
                              <strong>{row.shipments}</strong>
                            </div>
                          ))}
                          {tmsDashboard.pendingAcceptance.length === 0 ? (
                            <p className="tms-empty">Sem envios pendentes de aceitação.</p>
                          ) : null}
                        </div>
                      </article>
                    </div>

                    <article className="tms-block">
                      <div className="tms-incidences-head">
                        <button
                          type="button"
                          className="btn btn-secondary"
                          onClick={() => setShowIncidencesPanel((open) => !open)}
                        >
                          {showIncidencesPanel ? "Ocultar motivos de incidência" : "Ver motivos de incidência"}
                        </button>
                      </div>
                      <div className="tracker-filter-buttons" role="group" aria-label="Ver detalhes do tracker TMS">
                        <button
                          type="button"
                          className="btn btn-secondary"
                          onClick={() => setShowIncidenceDetails((open) => !open)}
                        >
                          {showIncidenceDetails ? "Ocultar detalhe incidência" : "Ver detalhe incidência"}
                        </button>
                        <button
                          type="button"
                          className="btn btn-secondary"
                          onClick={() => setShowPudoDetails((open) => !open)}
                        >
                          {showPudoDetails ? "Ocultar detalhe PUDO" : "Ver detalhe PUDO"}
                        </button>
                      </div>
                      {showIncidencesPanel ? (
                        <>
                          {tmsDashboard.incidences.length === 0 ? (
                            <p className="tms-empty">Sem incidências disponíveis.</p>
                          ) : (
                            <div className="tms-incidences-wrap">
                              <table className="tms-incidences-table">
                                <thead>
                                  <tr>
                                    <th>Incidência</th>
                                    <th>Envio</th>
                                    <th>Recolha</th>
                                    <th>Visível App</th>
                                    <th>Ativo</th>
                                    <th>Pos</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {tmsDashboard.incidences.map((item) => (
                                    <tr key={`inc-${item.id}`}>
                                      <td>{item.name}</td>
                                      <td>{item.shipment ? "Sim" : "Não"}</td>
                                      <td>{item.pickup ? "Sim" : "Não"}</td>
                                      <td>{item.appVisible ? "Sim" : "Não"}</td>
                                      <td>{item.active ? "Sim" : "Não"}</td>
                                      <td>{item.sort}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          )}

                        </>
                      ) : null}

                      {showIncidenceDetails ? (
                        <div className="tms-incidence-details">
                          <h5>Incidências Ongoing (envios em incidência)</h5>
                          {incidenceShipments.length === 0 ? (
                            <p className="tms-empty">Sem envios em incidência neste momento.</p>
                          ) : (
                            <>
                              <div className="tms-incidences-wrap">
                                <table className="tms-incidences-table">
                                  <thead>
                                    <tr>
                                      <th>Parcel ID</th>
                                      <th>Tracking Number</th>
                                      <th>Service</th>
                                      <th>Sender</th>
                                      <th>Destinatário</th>
                                      <th>Final Client Phone</th>
                                      <th>Cobrança</th>
                                      <th>Status</th>
                                      <th>Incidência</th>
                                      <th>Ação</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {paginatedIncidenceShipments.map((item, index) => (
                                      <tr key={`${item.parcelId || "parcel"}-${incidenceDetailsPage}-${index}`}>
                                        <td>{item.parcelId || item.providerTrackingCode || "-"}</td>
                                        <td>{item.providerTrackingCode || "-"}</td>
                                        <td>{item.service || "-"}</td>
                                        <td>{item.sender || "-"}</td>
                                        <td>{item.recipient || "-"}</td>
                                        <td>{item.finalClientPhone || "-"}</td>
                                        <td>{item.hasCharge ? `€${item.chargeAmount ? ` ${item.chargeAmount}` : ""}` : "-"}</td>
                                        <td>{item.status || "-"}</td>
                                        <td>{item.incidence || "-"}</td>
                                        <td>
                                          <button
                                            type="button"
                                            className="btn btn-secondary tms-mini-btn"
                                            onClick={() =>
                                              prefillPickupCttTemplate(
                                                item.finalClientPhone || "",
                                                item.recipient || "",
                                                item.providerTrackingCode || item.parcelId || "",
                                                "Incident",
                                                item.incidence || ""
                                              )
                                            }
                                            disabled={!digitsOnly(item.finalClientPhone || "")}
                                          >
                                            Preencher template
                                          </button>
                                        </td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                              {incidenceDetailsTotalPages > 1 ? (
                                <div className="tms-pagination">
                                  <button
                                    type="button"
                                    className="btn btn-secondary tms-mini-btn"
                                    onClick={() => setIncidenceDetailsPage((page) => Math.max(1, page - 1))}
                                    disabled={incidenceDetailsPage <= 1}
                                  >
                                    Anterior
                                  </button>
                                  <span>
                                    Página {incidenceDetailsPage} de {incidenceDetailsTotalPages}
                                  </span>
                                  <button
                                    type="button"
                                    className="btn btn-secondary tms-mini-btn"
                                    onClick={() => setIncidenceDetailsPage((page) => Math.min(incidenceDetailsTotalPages, page + 1))}
                                    disabled={incidenceDetailsPage >= incidenceDetailsTotalPages}
                                  >
                                    Seguinte
                                  </button>
                                </div>
                              ) : null}
                            </>
                          )}
                        </div>
                      ) : null}

                      {showPudoDetails ? (
                        <div className="tms-incidence-details">
                          <h5>
                            Parcels em Pickup Point (PUDO)
                            {pudoOverdueCount > 0 ? ` - ${pudoOverdueCount} pendente(s) > 24h` : ""}
                          </h5>
                          {pudoShipments.length === 0 ? (
                            <p className="tms-empty">Sem envios PUDO neste momento.</p>
                          ) : (
                            <>
                              <div className="tms-incidences-wrap">
                                <table className="tms-incidences-table">
                                  <thead>
                                    <tr>
                                      <th>Parcel ID</th>
                                      <th>Tracking Number</th>
                                      <th>Service</th>
                                      <th>Sender</th>
                                      <th>Destinatário</th>
                                      <th>Final Client Phone</th>
                                      <th>Cobrança</th>
                                      <th>Status</th>
                                      <th>Incidência</th>
                                      <th>Notificação</th>
                                      <th>Ação</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                  {paginatedPudoShipments.map((item, index) => {
                                    const key = getPudoShipmentKey(item);
                                    const notificationState = pudoNotifications[key];
                                    const firstSeenTs = new Date(notificationState?.firstSeenAt || "").getTime();
                                    const overdue =
                                      !notificationState?.notifiedAt &&
                                      Number.isFinite(firstSeenTs) &&
                                      Date.now() - firstSeenTs >= 24 * 60 * 60 * 1000;

                                    return (
                                      <tr
                                        key={`${item.parcelId || "parcel"}-pudo-${pudoDetailsPage}-${index}`}
                                        className={overdue ? "tms-row-overdue" : ""}
                                      >
                                        <td>{item.parcelId || item.providerTrackingCode || "-"}</td>
                                        <td>{item.providerTrackingCode || "-"}</td>
                                        <td>{item.service || "-"}</td>
                                        <td>{item.sender || "-"}</td>
                                        <td>{item.recipient || "-"}</td>
                                        <td>{item.finalClientPhone || "-"}</td>
                                        <td>{item.hasCharge ? `€${item.chargeAmount ? ` ${item.chargeAmount}` : ""}` : "-"}</td>
                                        <td>{item.status || "-"}</td>
                                        <td>{item.incidence || "-"}</td>
                                        <td>
                                          {notificationState?.notifiedAt ? (
                                            <span className="tms-notify-status ok">
                                              Notificado {new Date(notificationState.notifiedAt).toLocaleString("pt-PT")}
                                            </span>
                                          ) : overdue ? (
                                            <span className="tms-notify-status overdue">Pendente &gt; 24h</span>
                                          ) : (
                                            <span className="tms-notify-status pending">Pendente</span>
                                          )}
                                        </td>
                                        <td>
                                          <div className="tracker-pudo-actions">
                                            <button
                                              type="button"
                                              className="btn btn-secondary tms-mini-btn"
                                              onClick={() =>
                                                prefillPickupCttTemplate(
                                                  item.finalClientPhone || "",
                                                  item.recipient || "",
                                                  item.providerTrackingCode || item.parcelId || "",
                                                  "Pick Up Point",
                                                  item.incidence || ""
                                                )
                                              }
                                            >
                                              Preencher template
                                            </button>
                                            <button
                                              type="button"
                                              className="btn btn-secondary tms-mini-btn"
                                              onClick={() => togglePudoNotified(item)}
                                            >
                                              {notificationState?.notifiedAt ? "Desmarcar" : "Marcar notificado"}
                                            </button>
                                          </div>
                                        </td>
                                      </tr>
                                    );
                                  })}
                                  </tbody>
                                </table>
                              </div>
                              {pudoDetailsTotalPages > 1 ? (
                                <div className="tms-pagination">
                                  <button
                                    type="button"
                                    className="btn btn-secondary tms-mini-btn"
                                    onClick={() => setPudoDetailsPage((page) => Math.max(1, page - 1))}
                                    disabled={pudoDetailsPage <= 1}
                                  >
                                    Anterior
                                  </button>
                                  <span>
                                    Página {pudoDetailsPage} de {pudoDetailsTotalPages}
                                  </span>
                                  <button
                                    type="button"
                                    className="btn btn-secondary tms-mini-btn"
                                    onClick={() => setPudoDetailsPage((page) => Math.min(pudoDetailsTotalPages, page + 1))}
                                    disabled={pudoDetailsPage >= pudoDetailsTotalPages}
                                  >
                                    Seguinte
                                  </button>
                                </div>
                              ) : null}
                            </>
                          )}
                        </div>
                      ) : null}
                    </article>
                  </>
                ) : (
                  <p className="tms-empty">{tmsLoading ? "A carregar painel TMS..." : "Sem dados do TMS."}</p>
                )}
              </section>

              <section className="tms-panel tracker-console-panel">
                <div className="tracker-console-grid">
                  <article id="tracker-template-console" className="tms-block tracker-console-card">
                    <div className="tracker-console-card-head">
                      <h4>Template</h4>
                      <button
                        className="btn btn-secondary"
                        type="button"
                        onClick={fetchMetaTemplates}
                        disabled={metaTemplatesLoading}
                      >
                        {metaTemplatesLoading ? "A carregar..." : "Atualizar templates"}
                      </button>
                    </div>

                    <article className="template-chat-box tracker-template-chat-box">
                      <header>
                        <strong>{genericTemplateName || "Template"}</strong>
                        <span>{genericLanguage || "pt_PT"}</span>
                      </header>
                      <div className="template-thread">
                        <article className="wa-msg in">
                          <p>{genericTo || "Número (E.164)"}</p>
                          <time>{metaTemplatesStatus}</time>
                        </article>
                        <article className="wa-msg out">
                          <p>{selectedTemplatePreview || selectedTemplateBody || "Sem texto no body do template selecionado"}</p>
                          <time>{genericLoading ? "a enviar" : genericStatus}</time>
                        </article>
                      </div>
                    </article>

                    <form className="api-form tracker-template-form" onSubmit={sendGenericTemplate}>
                      <label>
                        Número (E.164)
                        <input
                          ref={trackerTemplateToInputRef}
                          value={genericTo}
                          onChange={(event) => setGenericTo(event.target.value)}
                          placeholder="+351912858229"
                        />
                      </label>

                      <label>
                        Template
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
                          {metaTemplates.length === 0 ? <option value="">Sem templates carregados</option> : null}
                          {metaTemplates.map((template) => (
                            <option key={template.id || template.name} value={template.name}>
                              {template.name} ({template.language || "pt_PT"})
                            </option>
                          ))}
                        </select>
                      </label>

                      <label>
                        Idioma
                        <input
                          value={genericLanguage}
                          onChange={(event) => setGenericLanguage(event.target.value)}
                          placeholder="pt_PT"
                        />
                      </label>

                      {requiredBodyVarCount > 0 ? (
                        <div className="template-var-grid">
                          {requiredBodyIndexes.map((index) => (
                            <label key={index}>
                              Variável {`{{${index}}}`}
                              <input
                                value={genericBodyVars[index] || ""}
                                onChange={(event) =>
                                  setGenericBodyVars((current) => ({
                                    ...current,
                                    [index]: event.target.value
                                  }))
                                }
                                placeholder={`Valor para {{${index}}}`}
                              />
                            </label>
                          ))}
                        </div>
                      ) : null}

                      {needsUrlButtonVariable ? (
                        <label>
                          Variável do botão URL
                          <input
                            value={genericButtonUrlVariable}
                            onChange={(event) => setGenericButtonUrlVariable(event.target.value)}
                            placeholder="variável dinâmica de URL"
                          />
                        </label>
                      ) : null}

                      <div className="api-actions">
                        <button className="wa-send" type="submit" disabled={genericLoading}>
                          {genericLoading ? "A enviar..." : "Enviar template"}
                        </button>
                        <span className={`wa-live-status ${genericLoading ? "busy" : ""}`}>
                          {genericLoading ? "A enviar" : "Inativo"}
                        </span>
                      </div>
                      <span className="status">{metaTemplatesStatus}</span>
                    </form>
                  </article>

                  <article className="tms-block tracker-console-history">
                    <h4>Histórico recente</h4>
                    {displayedHistory.length === 0 ? (
                      <p className="tms-empty">Sem histórico disponível.</p>
                    ) : (
                      <div className="sent-history-list">
                        {displayedHistory.map((item) => (
                          <article key={`tracker-console-history-${item.channel}-${item.id}`} className="sent-history-item">
                            <header>
                              <strong>{item.channel === "template" ? "Template" : "Mensagem"}</strong>
                              <span>{item.time}</span>
                            </header>
                            <p>Para: {item.to}</p>
                            <p>{item.content}</p>
                            <span className={`status sent-history-status sent-history-status-${statusTone(item.status, item.channel)}`}>
                              <span className="sent-history-dot" aria-hidden="true" />
                              Estado: {item.status}
                            </span>
                          </article>
                        ))}
                      </div>
                    )}
                  </article>
                </div>
              </section>

              {sharedLogsError ? <p className="status">{sharedLogsError}</p> : null}

              <div className="tracker-table-wrap">
                <div className="tracker-filters">
                  <div className="tracker-filter-buttons" role="group" aria-label="Pesquisar por campo">
                    {[
                      { key: "all", label: "All" },
                      { key: "parcelId", label: "Parcel ID" },
                      { key: "clientPhone", label: "Phone" },
                      { key: "clientName", label: "Client" },
                      { key: "messageTitle", label: "Message Title" },
                      { key: "status", label: "Status" }
                    ].map((option) => (
                      <button
                        key={option.key}
                        type="button"
                        className={`tracker-filter-btn${trackerSearchField === option.key ? " active" : ""}`}
                        onClick={() => setTrackerSearchField(option.key as typeof trackerSearchField)}
                      >
                        {option.label}
                      </button>
                    ))}
                  </div>
                  <input
                    className="tracker-search-input"
                    value={trackerSearchQuery}
                    onChange={(event) => setTrackerSearchQuery(event.target.value)}
                    placeholder={`Search by ${trackerSearchField === "all" ? "any field" : trackerSearchField}...`}
                  />
                </div>

                <table className="tracker-table">
                  <thead>
                    <tr>
                      <th>Client Name</th>
                      <th>Client Phone</th>
                      <th>Mensagem</th>
                      <th>Parcel ID</th>
                      <th>Message Type</th>
                      <th>Date Sent</th>
                      <th>sms Clicksend</th>
                      <th>Status</th>
                      <th>Message Title</th>
                      <th>Ação</th>
                    </tr>
                  </thead>
                  <tbody>
                    {paginatedTrackerRows.length === 0 ? (
                      <tr>
                        <td colSpan={10} className="tracker-empty">Ainda não existem mensagens para mostrar.</td>
                      </tr>
                    ) : (
                      paginatedTrackerRows.map((row) => (
                        <tr key={`tracker-${row.id}`}>
                          <td>{row.clientName}</td>
                          <td>{row.clientPhone}</td>
                          <td>{row.message}</td>
                          <td>{row.parcelId}</td>
                          <td>{row.messageType}</td>
                          <td>{row.dateSent}</td>
                          <td>{row.smsClicksend}</td>
                          <td>
                            <span className={`status sent-history-status sent-history-status-${statusTone(row.status, row.messageType)}`}>
                              <span className="sent-history-dot" aria-hidden="true" />
                              {row.status}
                            </span>
                          </td>
                          <td>{row.messageTitle}</td>
                          <td>
                            <button
                              type="button"
                              className="btn btn-secondary tms-mini-btn"
                              onClick={() => prefillPickupCttTemplate(row.clientPhone, row.clientName, row.parcelId, row.messageType, row.message)}
                              disabled={!digitsOnly(row.clientPhone || "")}
                            >
                              Preencher template
                            </button>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>

                <div className="tracker-pagination">
                  <div className="tracker-page-size">
                    <span>Rows per page</span>
                    <select
                      value={trackerPageSize}
                      onChange={(event) => setTrackerPageSize(Number(event.target.value) || 15)}
                    >
                      <option value={10}>10</option>
                      <option value={15}>15</option>
                      <option value={25}>25</option>
                      <option value={50}>50</option>
                    </select>
                  </div>

                  <span className="tracker-page-label">
                    {filteredTrackerRows.length === 0
                      ? "0 results"
                      : `${(trackerPage - 1) * trackerPageSize + 1}-${Math.min(trackerPage * trackerPageSize, filteredTrackerRows.length)} of ${filteredTrackerRows.length}`}
                  </span>

                  <div className="tracker-page-actions">
                    <button
                      type="button"
                      className="btn btn-secondary tracker-page-btn"
                      onClick={() => setTrackerPage((page) => Math.max(1, page - 1))}
                      disabled={trackerPage <= 1}
                    >
                      Previous
                    </button>
                    <span>Page {trackerPage} / {trackerTotalPages}</span>
                    <button
                      type="button"
                      className="btn btn-secondary tracker-page-btn"
                      onClick={() => setTrackerPage((page) => Math.min(trackerTotalPages, page + 1))}
                      disabled={trackerPage >= trackerTotalPages}
                    >
                      Next
                    </button>
                  </div>
                </div>
              </div>
            </section>
          ) : activeView === "consumiveis" ? (
            <section className="panel tracker-page" id="consumiveis-page">
              <div className="tracker-header">
                <div>
                  <h2>Consumiveis</h2>
                  <p>Vista dedicada com sincronização Notion e criação de novos registos.</p>
                </div>
                <div className="tracker-actions">
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={loadConsumiveis}
                    disabled={consumiveisLoading || consumiveisSaving}
                  >
                    {consumiveisLoading ? "A atualizar..." : "Atualizar consumiveis"}
                  </button>
                  <button
                    type="button"
                    className="btn btn-primary"
                    onClick={() => setActiveView("workspace")}
                  >
                    Voltar ao Workspace
                  </button>
                </div>
              </div>

              <section className="panel">
                <h3>Novo Registo</h3>
                <form className="api-form" onSubmit={createConsumivelEntry}>
                  <div className="template-var-grid">
                    <label>
                      Client Name
                      <input
                        value={consumiveisForm.clientName}
                        onChange={(event) => setConsumiveisForm((current) => ({ ...current, clientName: event.target.value }))}
                        placeholder="Nome do cliente"
                      />
                    </label>
                    <label>
                      Date Sent
                      <input
                        type="date"
                        value={consumiveisForm.dateSent}
                        onChange={(event) => setConsumiveisForm((current) => ({ ...current, dateSent: event.target.value }))}
                      />
                    </label>
                    <label>
                      Tabela
                      <input
                        value={consumiveisForm.tabela}
                        onChange={(event) => setConsumiveisForm((current) => ({ ...current, tabela: event.target.value }))}
                      />
                    </label>
                    <label>
                      Tipo de Cliente
                      <input
                        value={consumiveisForm.tipoCliente}
                        onChange={(event) => setConsumiveisForm((current) => ({ ...current, tipoCliente: event.target.value }))}
                      />
                    </label>
                    <label>
                      Texto
                      <input
                        value={consumiveisForm.texto}
                        onChange={(event) => setConsumiveisForm((current) => ({ ...current, texto: event.target.value }))}
                      />
                    </label>
                    <label>
                      Texto 1
                      <input
                        value={consumiveisForm.texto1}
                        onChange={(event) => setConsumiveisForm((current) => ({ ...current, texto1: event.target.value }))}
                      />
                    </label>
                    <label>
                      Text
                      <input
                        value={consumiveisForm.text}
                        onChange={(event) => setConsumiveisForm((current) => ({ ...current, text: event.target.value }))}
                      />
                    </label>
                    <label>
                      Texto 2
                      <input
                        value={consumiveisForm.texto2}
                        onChange={(event) => setConsumiveisForm((current) => ({ ...current, texto2: event.target.value }))}
                      />
                    </label>
                  </div>
                  <div className="api-actions">
                    <button className="btn btn-primary" type="submit" disabled={consumiveisSaving}>
                      {consumiveisSaving ? "A guardar..." : "Adicionar registo"}
                    </button>
                    <span className="status">{consumiveisRows.length} registos</span>
                  </div>
                </form>
              </section>

              {consumiveisError ? <p className="status">{consumiveisError}</p> : null}

              <div className="tracker-table-wrap">
                <table className="tracker-table consumiveis-table">
                  <thead>
                    <tr>
                      {(consumiveisColumns.length > 0 ? consumiveisColumns : ["Item"]).map((column) => (
                        <th key={`cons-col-${column}`}>{column}</th>
                      ))}
                      <th>Ação</th>
                    </tr>
                  </thead>
                  <tbody>
                    {consumiveisRows.length === 0 ? (
                      <tr>
                        <td colSpan={(consumiveisColumns.length > 0 ? consumiveisColumns.length : 1) + 1} className="tracker-empty">
                          {consumiveisLoading ? "A carregar consumiveis..." : "Sem dados de consumiveis para mostrar."}
                        </td>
                      </tr>
                    ) : (
                      consumiveisRows.map((row) => (
                        <tr key={row.id}>
                          {(consumiveisColumns.length > 0 ? consumiveisColumns : ["Item"]).map((column) => (
                            <td key={`cons-cell-${row.id}-${column}`}>{row.fields[column] || "-"}</td>
                          ))}
                          <td>
                            {row.url ? (
                              <a className="btn btn-secondary tms-mini-btn" href={row.url} target="_blank" rel="noreferrer">
                                Ver no Notion
                              </a>
                            ) : (
                              <span className="status">-</span>
                            )}
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </section>
          ) : (
            <section className="panel tracker-page" id="feedback-tracker-page">
              <div className="tracker-header">
                <div>
                  <h2>Feedback Tracker</h2>
                  <p>Base de dados Notion em tempo real para acompanhamento de feedback da operação.</p>
                </div>
                <div className="tracker-actions">
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={loadFeedbackTracker}
                    disabled={feedbackLoading}
                  >
                    {feedbackLoading ? "A atualizar..." : "Atualizar feedback"}
                  </button>
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={() => setActiveView("tracker")}
                  >
                    Abrir Client Tracker
                  </button>
                  <button
                    type="button"
                    className="btn btn-primary"
                    onClick={() => setActiveView("workspace")}
                  >
                    Voltar ao Workspace
                  </button>
                </div>
              </div>

              <section className="panel">
                <div className="tracker-header">
                  <div>
                    <h3>Entregues (Linke Portal API)</h3>
                    <p>Dados live de envios entregues do Linke Portal (status=5).</p>
                  </div>
                  <div className="tracker-actions">
                    <label>
                      Ano
                      <select
                        value={deliveredYearFilter}
                        onChange={(event) => setDeliveredYearFilter(event.target.value)}
                      >
                        <option value="all">Todos</option>
                        {deliveredAvailableYears.map((year) => (
                          <option key={`delivered-year-${year}`} value={year}>{year}</option>
                        ))}
                      </select>
                    </label>
                    <label>
                      Buscar
                      <input
                        type="text"
                        value={deliveredSearchQuery}
                        onChange={(event) => setDeliveredSearchQuery(event.target.value)}
                        placeholder="Parcel ID, Tracking, Data Recolha..."
                      />
                    </label>
                    <button
                      type="button"
                      className="btn btn-secondary"
                      onClick={() => loadDeliveredShipments(deliveredPage)}
                      disabled={deliveredLoading}
                    >
                      {deliveredLoading ? "A atualizar..." : "Atualizar entregues"}
                    </button>
                  </div>
                </div>

                {deliveredError ? <p className="status">{deliveredError}</p> : null}
                {!deliveredError ? (
                  <p className="status">
                    {sortedDeliveredRows.length} registos visíveis (ano: {deliveredYearFilter === "all" ? "Todos" : deliveredYearFilter})
                  </p>
                ) : null}

                <div className="tracker-table-wrap delivered-scroll-wrap">
                  <table className="tracker-table">
                    <thead>
                      <tr>
                        <th>Parcel ID</th>
                        <th>Tracking Number</th>
                        <th>Service</th>
                        <th>Sender</th>
                        <th>Destinatário</th>
                        <th>Final Client Phone</th>
                        <th>Data Recolha</th>
                        <th>Data Entrega</th>
                        <th>Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedDeliveredRows.length === 0 ? (
                        <tr>
                          <td colSpan={9} className="tracker-empty">
                            {deliveredLoading ? "A carregar entregues..." : "Sem dados de entregues para mostrar."}
                          </td>
                        </tr>
                      ) : (
                        sortedDeliveredRows.map((row, index) => (
                          <tr key={`delivered-${row.parcelId || row.providerTrackingCode || index}-${index}`}>
                            <td>{row.parcelId || "-"}</td>
                            <td>{row.providerTrackingCode || "-"}</td>
                            <td>{row.service || "-"}</td>
                            <td>{row.sender || "-"}</td>
                            <td>{row.recipient || "-"}</td>
                            <td>{row.finalClientPhone || "-"}</td>
                            <td>{row.pickupDate || "-"}</td>
                            <td>{row.deliveryDate || "-"}</td>
                            <td>{row.status || "-"}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>

                <div className="tracker-pagination">
                  <div className="tracker-page-size">
                    <span>Rows per page</span>
                    <span>250</span>
                  </div>

                  <span className="tracker-page-label">
                    {(deliveredTotal || deliveredRows.length) === 0
                      ? "0 results"
                      : `${(deliveredPage - 1) * deliveredPageSize + 1}-${Math.min(deliveredPage * deliveredPageSize, deliveredTotal || deliveredRows.length)} of ${deliveredTotal || deliveredRows.length}`}
                  </span>

                  <div className="tracker-page-actions">
                    <button
                      type="button"
                      className="btn btn-secondary tracker-page-btn"
                      onClick={() => loadDeliveredShipments(Math.max(1, deliveredPage - 1))}
                      disabled={deliveredPage <= 1 || deliveredLoading}
                    >
                      Previous
                    </button>
                    <span>Page {deliveredPage} / {deliveredTotalPages}</span>
                    <button
                      type="button"
                      className="btn btn-secondary tracker-page-btn"
                      onClick={() => loadDeliveredShipments(Math.min(deliveredTotalPages, deliveredPage + 1))}
                      disabled={deliveredPage >= deliveredTotalPages || deliveredLoading}
                    >
                      Next
                    </button>
                  </div>
                </div>
              </section>

              <section className="panel">
                <p>Vista sincronizada com a tua base Notion do Feedback Tracker.</p>
                <span className="status">{filteredSortedFeedbackRows.length} registos com Data Entrega</span>
              </section>

              {feedbackError ? <p className="status">{feedbackError}</p> : null}

              <div className="tracker-table-wrap">
                <table className="tracker-table consumiveis-table">
                  <thead>
                    <tr>
                      {(feedbackColumns.length > 0 ? feedbackColumns : ["Item"]).map((column) => (
                        <th key={`feedback-col-${column}`}>{column}</th>
                      ))}
                      <th>Ação</th>
                    </tr>
                  </thead>
                  <tbody>
                    {paginatedFeedbackRows.length === 0 ? (
                      <tr>
                        <td colSpan={(feedbackColumns.length > 0 ? feedbackColumns.length : 1) + 1} className="tracker-empty">
                          {feedbackLoading ? "A carregar feedback tracker..." : "Sem dados com Data Entrega para mostrar."}
                        </td>
                      </tr>
                    ) : (
                      paginatedFeedbackRows.map((row) => (
                        <tr key={row.id}>
                          {(feedbackColumns.length > 0 ? feedbackColumns : ["Item"]).map((column) => (
                            <td key={`feedback-cell-${row.id}-${column}`}>{row.fields[column] || "-"}</td>
                          ))}
                          <td>
                            {row.url ? (
                              <a className="btn btn-secondary tms-mini-btn" href={row.url} target="_blank" rel="noreferrer">
                                Ver no Notion
                              </a>
                            ) : (
                              <span className="status">-</span>
                            )}
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>

                <div className="tracker-pagination">
                  <div className="tracker-page-size">
                    <span>Rows per page</span>
                    <span>100</span>
                  </div>

                  <span className="tracker-page-label">
                    {filteredSortedFeedbackRows.length === 0
                      ? "0 results"
                      : `${(feedbackPage - 1) * feedbackPageSize + 1}-${Math.min(feedbackPage * feedbackPageSize, filteredSortedFeedbackRows.length)} of ${filteredSortedFeedbackRows.length}`}
                  </span>

                  <div className="tracker-page-actions">
                    <button
                      type="button"
                      className="btn btn-secondary tracker-page-btn"
                      onClick={() => setFeedbackPage((page) => Math.max(1, page - 1))}
                      disabled={feedbackPage <= 1}
                    >
                      Previous
                    </button>
                    <span>Page {feedbackPage} / {feedbackTotalPages}</span>
                    <button
                      type="button"
                      className="btn btn-secondary tracker-page-btn"
                      onClick={() => setFeedbackPage((page) => Math.min(feedbackTotalPages, page + 1))}
                      disabled={feedbackPage >= feedbackTotalPages}
                    >
                      Next
                    </button>
                  </div>
                </div>
              </div>
            </section>
          )}
        </main>
      </div>
    </div>

    </>
  );
}

export default App;
