import { FormEvent, useEffect, useMemo, useRef, useState } from "react";

const docsFacts = [
  { label: "Version", value: "v23.0" },
  { label: "Method", value: "POST" },
  { label: "Endpoint", value: "/{Version}/{Phone-Number-ID}/messages" },
  { label: "Auth", value: "Bearer token" }
];

type RadioStation = {
  name: string;
  genre: string;
  url: string;
  emoji: string;
};

const radioStations: RadioStation[] = [
  { name: "Antena 1", genre: "Cultura · Notícias", url: "https://streaming.rtp.pt/live/a1/a1", emoji: "📻" },
  { name: "Antena 3", genre: "Rock · Alternativo", url: "https://streaming.rtp.pt/live/a3/a3", emoji: "🎸" },
  { name: "Antena 2", genre: "Música Clássica", url: "https://streaming.rtp.pt/live/a2/a2", emoji: "🎻" },
  { name: "TSF", genre: "Notícias · Debates", url: "https://icecast.tsf.pt/tsf-128k", emoji: "📰" },
  { name: "Rádio Comercial", genre: "Pop · Hits", url: "https://mcr.iosys.pt/live/comercial", emoji: "🌟" },
  { name: "RFM", genre: "Pop · Dance", url: "https://mcr.iosys.pt/live/rfm", emoji: "🎵" },
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
  }
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
};

type CalendarEvent = {
  id: string;
  date: string;
  title: string;
  time: string;
};

type AuthUser = {
  username: string;
  displayName: string;
};

function digitsOnly(value: string) {
  return String(value || "").replace(/\D/g, "");
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
  const [mediaStatusText, setMediaStatusText] = useState("Inativo");
  const [mediaResponseText, setMediaResponseText] = useState("Ainda não foi enviado nenhum ficheiro.");
  const [genericTo, setGenericTo] = useState(import.meta.env.VITE_DEFAULT_TO_NUMBER ?? "");
  const [genericTemplateName, setGenericTemplateName] = useState(
    import.meta.env.VITE_DEFAULT_TEMPLATE_NAME ?? "order_pickup_ctt"
  );
  const [genericLanguage, setGenericLanguage] = useState("pt_PT");
  const [genericBodyVars, setGenericBodyVars] = useState<Record<number, string>>({});
  const [genericButtonUrlVariable, setGenericButtonUrlVariable] = useState("");
  const [genericLoading, setGenericLoading] = useState(false);
  const [genericStatus, setGenericStatus] = useState("Inativo");
  const [genericResponse, setGenericResponse] = useState("Ainda não foi enviado nenhum template.");
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
  // Radio player
  const audioRef = useRef<HTMLAudioElement | null>(null);
  const [radioPlaying, setRadioPlaying] = useState(false);
  const [radioLoading, setRadioLoading] = useState(false);
  const [radioError, setRadioError] = useState(false);
  const [radioCurrentIdx, setRadioCurrentIdx] = useState(0);
  const [radioVolume, setRadioVolume] = useState(() => {
    try { return parseFloat(localStorage.getItem("wa_radio_volume") || "0.7"); } catch { return 0.7; }
  });
  const [radioDrawerOpen, setRadioDrawerOpen] = useState(false);
  const [radioCustomUrl, setRadioCustomUrl] = useState("");

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

  // Radio: build stream URL through backend proxy (only for custom URLs)
  function proxyStreamUrl(sourceUrl: string) {
    if (!backendBaseUrl) return sourceUrl;
    return `${backendBaseUrl}/api/radio/proxy?url=${encodeURIComponent(sourceUrl)}`;
  }

  // Built-in stations play directly in browser (they have CORS headers).
  // Proxy is only used for custom user-supplied URLs where CORS is unknown.
  function resolvedStreamUrl(idx: number) {
    return radioStations[idx].url;
  }

  // Radio player controls
  function radioToggle() {
    const audio = audioRef.current;
    if (!audio) return;
    if (radioPlaying) {
      audio.pause();
      setRadioPlaying(false);
    } else {
      setRadioError(false);
      setRadioLoading(true);
      audio.src = resolvedStreamUrl(radioCurrentIdx);
      audio.load();
      audio.play().catch(() => {
        setRadioLoading(false);
        setRadioError(true);
        setRadioPlaying(false);
      });
    }
  }

  function radioChangeStation(idx: number) {
    const audio = audioRef.current;
    if (!audio) return;
    setRadioCurrentIdx(idx);
    setRadioError(false);
    if (radioPlaying) {
      audio.pause();
      setRadioLoading(true);
      audio.src = resolvedStreamUrl(idx);
      audio.load();
      audio.play().catch(() => {
        setRadioLoading(false);
        setRadioError(true);
        setRadioPlaying(false);
      });
    }
  }

  function radioPlayCustomUrl() {
    const trimmed = radioCustomUrl.trim();
    if (!trimmed || !/^https?:\/\//i.test(trimmed)) return;
    const audio = audioRef.current;
    if (!audio) return;
    setRadioError(false);
    setRadioLoading(true);
    audio.src = proxyStreamUrl(trimmed);
    audio.load();
    audio.play().catch(() => { setRadioLoading(false); setRadioError(true); setRadioPlaying(false); });
    setRadioDrawerOpen(false);
  }

  function radioNext() {
    radioChangeStation((radioCurrentIdx + 1) % radioStations.length);
  }

  function radioPrev() {
    radioChangeStation((radioCurrentIdx - 1 + radioStations.length) % radioStations.length);
  }

  function completeReminder(reminderId: string) {
    setTeamReminders((prev) =>
      prev.map((item) => (item.id === reminderId ? { ...item, done: true } : item))
    );
  }

  // Radio: init audio element once
  useEffect(() => {
    const audio = new Audio();
    audio.volume = radioVolume;
    audio.addEventListener("playing", () => { setRadioLoading(false); setRadioPlaying(true); });
    audio.addEventListener("waiting", () => setRadioLoading(true));
    audio.addEventListener("pause", () => { setRadioPlaying(false); setRadioLoading(false); });
    audio.addEventListener("error", () => { setRadioError(true); setRadioLoading(false); setRadioPlaying(false); });
    audioRef.current = audio;
    return () => { audio.pause(); audioRef.current = null; };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Radio: sync volume
  useEffect(() => {
    if (audioRef.current) audioRef.current.volume = radioVolume;
    try { localStorage.setItem("wa_radio_volume", String(radioVolume)); } catch {}
  }, [radioVolume]);

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
      setStatusText(response.ok ? "Media enviada" : `Falhou (${response.status})`);
      if (response.ok) setComposeMedia(null);
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

  function loadSharedLogs() {
    setSharedLogsLoading(true);
    setSharedLogsError("");

    fetch(apiUrl("/api/logs?limit=300"))
      .then((response) => response.json())
      .then((data) => {
        if (Array.isArray(data?.data)) {
          setSharedLogs(data.data as SharedLogItem[]);
          if (data?.warning === "supabase_not_configured") {
            setSharedLogsError("Supabase ainda não está configurado no backend. A mostrar histórico local.");
          }
        } else {
          setSharedLogs([]);
        }
      })
      .catch(() => {
        setSharedLogsError("Não foi possível carregar histórico partilhado. A mostrar histórico local.");
      })
      .finally(() => {
        setSharedLogsLoading(false);
      });
  }

  useEffect(() => {
    loadSharedLogs();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

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
          buttonUrlVariable: needsUrlButtonVariable ? genericButtonUrlVariable.trim() : ""
        })
      });

      const data = await response.json();
      setGenericStatus(response.ok ? "Template aceite" : `Falhou (${response.status})`);
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

  const radioStation = radioStations[radioCurrentIdx];

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
            <a href="#overview" className="workspace-nav-link active">
              <span className="workspace-nav-icon"><SidebarIcon name="overview" /></span>
              <span>Overview</span>
            </a>
            <a href="#api-console" className="workspace-nav-link">
              <span className="workspace-nav-icon"><SidebarIcon name="chat" /></span>
              <span>Chat Console</span>
            </a>
            <a href="#logs-page" className="workspace-nav-link">
              <span className="workspace-nav-icon"><SidebarIcon name="logs" /></span>
              <span>Message Logs</span>
            </a>
            <a href="#media-console" className="workspace-nav-link">
              <span className="workspace-nav-icon"><SidebarIcon name="upload" /></span>
              <span>Media Upload</span>
            </a>
            <a href="#generic-template-console" className="workspace-nav-link">
              <span className="workspace-nav-icon"><SidebarIcon name="templates" /></span>
              <span>Template Notifications</span>
            </a>
            <a href="#caderno-pessoal" className="workspace-nav-link">
              <span className="workspace-nav-icon"><SidebarIcon name="notes" /></span>
              <span>Notes &amp; Calendar</span>
            </a>
          </nav>
        </aside>

        <main className="workspace-content">
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
            onClick={loadSharedLogs}
            disabled={sharedLogsLoading}
          >
            {sharedLogsLoading ? "A atualizar..." : "Atualizar logs"}
          </button>
          <span className="status">{filteredHistory.length}/{displayedHistory.length} mensagens</span>
        </div>

        <section className="sent-history-panel">
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
                  <span className="status">Estado: {item.status}</span>
                </article>
              ))}
            </div>
          )}
        </section>
      </section>

      <section className="panel" id="media-console">
        <h2>Consola de Upload de Media</h2>
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
      </section>

      <section className="panel" id="generic-template-console">
        <h2>Notificações por Template</h2>
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

        <section className="template-history">
          <h3>Histórico de Templates Enviados</h3>
          {templateHistory.length === 0 ? (
            <p>Ainda não foram enviadas notificações de template.</p>
          ) : (
            <div className="template-history-list">
              {templateHistory.map((item) => (
                <article key={item.id} className="template-history-item">
                  <header>
                    <strong>{item.templateName}</strong>
                    <span>{item.time}</span>
                  </header>
                  <p>Para: {item.to}</p>
                  <p>{item.previewText}</p>
                  <span className="status">Estado: {item.status}</span>
                </article>
              ))}
            </div>
          )}
        </section>

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
        <h2>Workspace pronto para o dia a dia da equipa?</h2>
        <p>Organiza comunicação, notas e lembretes num só local.</p>
        <a className="btn btn-primary" href="mailto:hello@linke.pt">
          Contactar hello@linke.pt
        </a>
      </section>
          </div>
        </main>
      </div>
    </div>

    {/* ── Sticky Radio Bar ── */}
    <div className={`radio-bar${radioDrawerOpen ? " radio-bar--open" : ""}`}>
      {radioDrawerOpen && (
        <div className="radio-drawer">
          <p className="radio-drawer-titulo">Escolher Estação</p>
          {radioStations.map((station, idx) => (
            <button
              key={station.url}
              type="button"
              className={`radio-station-item${idx === radioCurrentIdx ? " ativo" : ""}`}
              onClick={() => { radioChangeStation(idx); setRadioDrawerOpen(false); }}
            >
              <span className="radio-station-emoji">{station.emoji}</span>
              <span className="radio-station-info">
                <strong>{station.name}</strong>
                <small>{station.genre}</small>
              </span>
              {idx === radioCurrentIdx && radioPlaying && <span className="radio-equalizer">▶</span>}
            </button>
          ))}
          <div className="radio-custom-url">
            <input
              type="url"
              placeholder="URL de stream personalizado (https://...)" 
              value={radioCustomUrl}
              onChange={(e) => setRadioCustomUrl(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && radioPlayCustomUrl()}
            />
            <button type="button" onClick={radioPlayCustomUrl} title="Ouvir URL">▶</button>
          </div>
        </div>
      )}

      <div className="radio-controls">
        <button type="button" className="radio-btn radio-btn-sm" onClick={radioPrev} title="Anterior">⏮</button>

        <button
          type="button"
          className={`radio-btn radio-btn-play${radioLoading ? " loading" : ""}`}
          onClick={radioToggle}
          title={radioPlaying ? "Pausar" : "Ouvir"}
        >
          {radioLoading ? "⏳" : radioPlaying ? "⏸" : "▶"}
        </button>

        <button type="button" className="radio-btn radio-btn-sm" onClick={radioNext} title="Próxima">⏭</button>

        <div className="radio-info" onClick={() => setRadioDrawerOpen((o) => !o)}>
          <span className="radio-emoji">{radioStation.emoji}</span>
          <span className="radio-text">
            <strong>{radioStation.name}</strong>
            <small>{radioError ? "Erro – tenta outra estação" : radioPlaying ? "● AO VIVO" : radioStation.genre}</small>
          </span>
          <span className="radio-caret">{radioDrawerOpen ? "▾" : "▸"}</span>
        </div>

        <label className="radio-volume" title="Volume">
          🔊
          <input
            type="range"
            min={0}
            max={1}
            step={0.02}
            value={radioVolume}
            onChange={(e) => setRadioVolume(parseFloat(e.target.value))}
          />
        </label>
      </div>
    </div>
    </>
  );
}

export default App;
