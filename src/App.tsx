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
  const [toNumber, setToNumber] = useState("");
  const [messageText, setMessageText] = useState("Hello from Linke Cloud API frontend.");
  const [loading, setLoading] = useState(false);
  const [statusText, setStatusText] = useState("Idle");
  const [responseText, setResponseText] = useState("No request sent yet.");
  const [mediaFile, setMediaFile] = useState<File | null>(null);
  const [mediaLoading, setMediaLoading] = useState(false);
  const [mediaStatusText, setMediaStatusText] = useState("Idle");
  const [mediaResponseText, setMediaResponseText] = useState("No upload sent yet.");

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
