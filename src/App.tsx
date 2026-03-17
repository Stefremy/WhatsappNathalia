import { FormEvent, useMemo, useState } from "react";

const capabilities = [
  {
    title: "Unified Cloud API",
    text: "Expose your services in one stable endpoint with clean auth, monitoring, and usage controls."
  },
  {
    title: "No-Code + Pro-Code",
    text: "Use Linke modules as a reliable fallback when Make scenarios break under scale."
  },
  {
    title: "WhatsApp Business Flows",
    text: "Launch templates, automations, and customer events connected to your own API backbone."
  }
];

const plans = [
  {
    name: "Starter",
    price: "EUR49/mo",
    note: "Best for pilots and single-product teams"
  },
  {
    name: "Growth",
    price: "EUR199/mo",
    note: "Best for active cloud operations and multi-client usage"
  },
  {
    name: "Enterprise",
    price: "Custom",
    note: "Dedicated infrastructure, SLA, private integration track"
  }
];

const docsFacts = [
  { label: "Version", value: "v23.0" },
  { label: "Method", value: "POST" },
  { label: "Endpoint", value: "/{Version}/{Phone-Number-ID}/messages" },
  { label: "Auth", value: "Bearer token" }
];

function App() {
  const [apiVersion, setApiVersion] = useState(
    import.meta.env.VITE_WHATSAPP_API_VERSION ?? "v23.0"
  );
  const [phoneNumberId, setPhoneNumberId] = useState(
    import.meta.env.VITE_WHATSAPP_PHONE_NUMBER_ID ?? "configured in backend"
  );
  const backendBaseUrl = import.meta.env.VITE_BACKEND_BASE_URL ?? "http://localhost:3001";
  const [toNumber, setToNumber] = useState(
    import.meta.env.VITE_DEFAULT_TO_NUMBER ?? "+351912858229"
  );
  const [messageText, setMessageText] = useState("Hello from Linke Cloud API frontend.");
  const [loading, setLoading] = useState(false);
  const [statusText, setStatusText] = useState("Idle");
  const [responseText, setResponseText] = useState("No request sent yet.");
  const [mediaFile, setMediaFile] = useState<File | null>(null);
  const [mediaLoading, setMediaLoading] = useState(false);
  const [mediaStatusText, setMediaStatusText] = useState("Idle");
  const [mediaResponseText, setMediaResponseText] = useState("No upload sent yet.");
  const [templateTo, setTemplateTo] = useState(
    import.meta.env.VITE_DEFAULT_TO_NUMBER ?? "+351912858229"
  );
  const [customerName, setCustomerName] = useState("Cliente");
  const [shipmentCode, setShipmentCode] = useState("1215");
  const [pickupDate, setPickupDate] = useState("12/02/2026");
  const [templateLoading, setTemplateLoading] = useState(false);
  const [templateStatus, setTemplateStatus] = useState("Idle");
  const [templateResponse, setTemplateResponse] = useState("No template request sent yet.");
  const [genericTo, setGenericTo] = useState(import.meta.env.VITE_DEFAULT_TO_NUMBER ?? "+351912858229");
  const [genericTemplateName, setGenericTemplateName] = useState(
    import.meta.env.VITE_DEFAULT_TEMPLATE_NAME ?? "order_pickup_ctt"
  );
  const [genericLanguage, setGenericLanguage] = useState("pt_PT");
  const [genericVars, setGenericVars] = useState("John|12345|Jasper's Market, 1234 Baker street. Palo Alto, CA 94301|Referencia - valor");
  const [genericButtonUrlVariable, setGenericButtonUrlVariable] = useState("");
  const [genericLoading, setGenericLoading] = useState(false);
  const [genericStatus, setGenericStatus] = useState("Idle");
  const [genericResponse, setGenericResponse] = useState("No generic template request sent yet.");
  const [feedbackTo, setFeedbackTo] = useState(import.meta.env.VITE_DEFAULT_TO_NUMBER ?? "+351912858229");
  const [feedbackTemplateName, setFeedbackTemplateName] = useState(
    import.meta.env.VITE_FEEDBACK_TEMPLATE_NAME ?? "feedback_request_template"
  );
  const [feedbackLanguageCode, setFeedbackLanguageCode] = useState("pt_PT");
  const [feedbackCustomerName, setFeedbackCustomerName] = useState("JOANA");
  const [feedbackStoreName, setFeedbackStoreName] = useState("Patricia fashion star");
  const [feedbackLoading, setFeedbackLoading] = useState(false);
  const [feedbackStatus, setFeedbackStatus] = useState("Idle");
  const [feedbackResponse, setFeedbackResponse] = useState("No feedback template request sent yet.");

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
      const response = await fetch(`${backendBaseUrl}/api/messages/send`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          to: toNumber,
          text: messageText
        })
      });

      const data = await response.json();
      setStatusText(response.ok ? "Message accepted" : `Failed (${response.status})`);
      setResponseText(JSON.stringify(data, null, 2));
    } catch (error) {
      setStatusText("Network error");
      setResponseText(error instanceof Error ? error.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }

  const curlCommand = [
    `curl -X POST \"${backendBaseUrl}/api/messages/send\"`,
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

      const response = await fetch(`${backendBaseUrl}/api/media/upload`, {
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
    `curl -X POST "${backendBaseUrl}/api/media/upload"`,
    '  -H "Content-Type: multipart/form-data"',
    '  -F "file=@/path/to/file.jpg"',
    '  -F "messaging_product=whatsapp"'
  ].join("\n");

  async function sendReturnToSenderTemplate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!templateTo.trim() || !customerName.trim() || !shipmentCode.trim() || !pickupDate.trim()) {
      setTemplateStatus("Missing required fields");
      setTemplateResponse("Recipient, customer name, shipment code and pickup date are required.");
      return;
    }

    setTemplateLoading(true);
    setTemplateStatus("Sending...");

    try {
      const response = await fetch(`${backendBaseUrl}/api/templates/send-return-to-sender`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          to: templateTo,
          customerName,
          shipmentCode,
          pickupDate
        })
      });

      const data = await response.json();
      setTemplateStatus(response.ok ? "Template accepted" : `Failed (${response.status})`);
      setTemplateResponse(JSON.stringify(data, null, 2));
    } catch (error) {
      setTemplateStatus("Network error");
      setTemplateResponse(error instanceof Error ? error.message : "Unknown error");
    } finally {
      setTemplateLoading(false);
    }
  }

  async function sendGenericTemplate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    const variables = genericVars
      .split("|")
      .map((value) => value.trim())
      .filter(Boolean);

    if (!genericTo.trim() || !genericTemplateName.trim()) {
      setGenericStatus("Missing required fields");
      setGenericResponse("Recipient and template name are required.");
      return;
    }

    setGenericLoading(true);
    setGenericStatus("Sending...");

    try {
      const response = await fetch(`${backendBaseUrl}/api/templates/send-generic`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          to: genericTo,
          templateName: genericTemplateName,
          languageCode: genericLanguage,
          bodyVariables: variables,
          buttonUrlVariable: genericButtonUrlVariable
        })
      });

      const data = await response.json();
      setGenericStatus(response.ok ? "Template accepted" : `Failed (${response.status})`);
      setGenericResponse(JSON.stringify(data, null, 2));
    } catch (error) {
      setGenericStatus("Network error");
      setGenericResponse(error instanceof Error ? error.message : "Unknown error");
    } finally {
      setGenericLoading(false);
    }
  }

  async function sendFeedbackTemplate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!feedbackTo.trim() || !feedbackCustomerName.trim() || !feedbackStoreName.trim()) {
      setFeedbackStatus("Missing required fields");
      setFeedbackResponse("Recipient, customer name and store name are required.");
      return;
    }

    setFeedbackLoading(true);
    setFeedbackStatus("Sending...");

    try {
      const response = await fetch(`${backendBaseUrl}/api/templates/send-feedback-request`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          to: feedbackTo,
          customerName: feedbackCustomerName,
          storeName: feedbackStoreName,
          templateName: feedbackTemplateName,
          languageCode: feedbackLanguageCode
        })
      });

      const data = await response.json();
      setFeedbackStatus(response.ok ? "Template accepted" : `Failed (${response.status})`);
      setFeedbackResponse(JSON.stringify(data, null, 2));
    } catch (error) {
      setFeedbackStatus("Network error");
      setFeedbackResponse(error instanceof Error ? error.message : "Unknown error");
    } finally {
      setFeedbackLoading(false);
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

      <section className="panel" id="capabilities">
        <h2>Built for fast cloud API business execution</h2>
        <div className="grid three">
          {capabilities.map((item) => (
            <article key={item.title} className="card">
              <h3>{item.title}</h3>
              <p>{item.text}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="panel" id="plans">
        <h2>Simple plans to launch and scale</h2>
        <div className="grid three">
          {plans.map((plan) => (
            <article key={plan.name} className="card plan">
              <h3>{plan.name}</h3>
              <p className="price">{plan.price}</p>
              <p>{plan.note}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="panel" id="api-console">
        <h2>WhatsApp Cloud API Message Console</h2>
        <p>
          Based on official docs: <strong>POST /{`{Version}`}/{`{Phone-Number-ID}`}/messages</strong>
          with Bearer authentication and JSON payload, relayed by your backend for secure
          Notion logging.
        </p>

        <div className="facts">
          {docsFacts.map((fact) => (
            <div key={fact.label} className="fact">
              <span>{fact.label}</span>
              <strong>{fact.value}</strong>
            </div>
          ))}
        </div>

        <form className="api-form" onSubmit={sendMessage}>
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

          <label>
            Recipient Number (E.164)
            <input
              value={toNumber}
              onChange={(event) => setToNumber(event.target.value)}
              placeholder="3519XXXXXXXX"
            />
          </label>

          <label>
            Text Message Body
            <textarea
              value={messageText}
              onChange={(event) => setMessageText(event.target.value)}
              rows={4}
            />
          </label>

          <div className="api-actions">
            <button className="btn btn-primary" type="submit" disabled={loading}>
              {loading ? "Sending..." : "Send Text Message"}
            </button>
            <span className="status">Status: {statusText}</span>
          </div>
        </form>

        <div className="code-grid">
          <article className="card code-block">
            <h3>Resolved Endpoint</h3>
            <pre>{endpoint}</pre>
          </article>
          <article className="card code-block">
            <h3>Backend Relay</h3>
            <pre>{`${backendBaseUrl}/api/messages/send`}</pre>
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
            <pre>{`${backendBaseUrl}/api/media/upload`}</pre>
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

      <section className="panel" id="pickup-template-console">
        <h2>Pickup Template Console</h2>
        <p>
          Sends template <strong>entrega_de_volta_ao_remetente</strong> with variables:
          customer name, shipment code, and pickup date.
        </p>

        <form className="api-form" onSubmit={sendReturnToSenderTemplate}>
          <label>
            Recipient Number (E.164)
            <input
              value={templateTo}
              onChange={(event) => setTemplateTo(event.target.value)}
              placeholder="+351912858229"
            />
          </label>

          <label>
            Variable {"{{1}}"} Customer Name
            <input
              value={customerName}
              onChange={(event) => setCustomerName(event.target.value)}
              placeholder="Nathalia"
            />
          </label>

          <label>
            Variable {"{{2}}"} Shipment Code
            <input
              value={shipmentCode}
              onChange={(event) => setShipmentCode(event.target.value)}
              placeholder="1215"
            />
          </label>

          <label>
            Variable {"{{3}}"} Pickup Date
            <input
              value={pickupDate}
              onChange={(event) => setPickupDate(event.target.value)}
              placeholder="12/2/12"
            />
          </label>

          <div className="api-actions">
            <button className="btn btn-primary" type="submit" disabled={templateLoading}>
              {templateLoading ? "Sending..." : "Send Pickup Template"}
            </button>
            <span className="status">Status: {templateStatus}</span>
          </div>
        </form>

        <div className="code-grid">
          <article className="card code-block">
            <h3>Backend Relay</h3>
            <pre>{`${backendBaseUrl}/api/templates/send-return-to-sender`}</pre>
          </article>
          <article className="card code-block">
            <h3>Template Response</h3>
            <pre>{templateResponse}</pre>
          </article>
        </div>
      </section>

      <section className="panel" id="generic-template-console">
        <h2>Generic Template Console</h2>
        <p>
          For new templates like this one, paste template name/language and variable values in order.
          Use <strong>|</strong> to separate variables.
        </p>

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
            Template Name
            <input
              value={genericTemplateName}
              onChange={(event) => setGenericTemplateName(event.target.value)}
              placeholder="your_template_name"
            />
          </label>

          <label>
            Language Code
            <input
              value={genericLanguage}
              onChange={(event) => setGenericLanguage(event.target.value)}
              placeholder="pt_PT"
            />
          </label>

          <label>
            Body Variables (ordered, separated by |)
            <textarea
              value={genericVars}
              onChange={(event) => setGenericVars(event.target.value)}
              rows={3}
            />
          </label>

          <label>
            URL Button Variable (optional)
            <input
              value={genericButtonUrlVariable}
              onChange={(event) => setGenericButtonUrlVariable(event.target.value)}
              placeholder="optional dynamic suffix/parameter for first URL button"
            />
          </label>

          <div className="api-actions">
            <button className="btn btn-primary" type="submit" disabled={genericLoading}>
              {genericLoading ? "Sending..." : "Send Generic Template"}
            </button>
            <span className="status">Status: {genericStatus}</span>
          </div>
        </form>

        <div className="code-grid">
          <article className="card code-block">
            <h3>Backend Relay</h3>
            <pre>{`${backendBaseUrl}/api/templates/send-generic`}</pre>
          </article>
          <article className="card code-block">
            <h3>Template Response</h3>
            <pre>{genericResponse}</pre>
          </article>
        </div>
      </section>

      <section className="panel" id="feedback-template-console">
        <h2>Feedback Request Template Console</h2>
        <p>
          Sends your customer feedback message template (example: "Deixe 5 estrelas") with
          customer and store variables.
        </p>

        <form className="api-form" onSubmit={sendFeedbackTemplate}>
          <label>
            Recipient Number (E.164)
            <input
              value={feedbackTo}
              onChange={(event) => setFeedbackTo(event.target.value)}
              placeholder="+351912858229"
            />
          </label>

          <label>
            Template Name
            <input
              value={feedbackTemplateName}
              onChange={(event) => setFeedbackTemplateName(event.target.value)}
              placeholder="feedback_request_template"
            />
          </label>

          <label>
            Language Code
            <input
              value={feedbackLanguageCode}
              onChange={(event) => setFeedbackLanguageCode(event.target.value)}
              placeholder="pt_PT"
            />
          </label>

          <label>
            Variable {"{{1}}"} Customer Name
            <input
              value={feedbackCustomerName}
              onChange={(event) => setFeedbackCustomerName(event.target.value)}
              placeholder="JOANA"
            />
          </label>

          <label>
            Variable {"{{2}}"} Store Name
            <input
              value={feedbackStoreName}
              onChange={(event) => setFeedbackStoreName(event.target.value)}
              placeholder="Patricia fashion star"
            />
          </label>

          <div className="api-actions">
            <button className="btn btn-primary" type="submit" disabled={feedbackLoading}>
              {feedbackLoading ? "Sending..." : "Send Feedback Template"}
            </button>
            <span className="status">Status: {feedbackStatus}</span>
          </div>
        </form>

        <div className="code-grid">
          <article className="card code-block">
            <h3>Backend Relay</h3>
            <pre>{`${backendBaseUrl}/api/templates/send-feedback-request`}</pre>
          </article>
          <article className="card code-block">
            <h3>Template Response</h3>
            <pre>{feedbackResponse}</pre>
          </article>
        </div>
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
