# Linke Cloud API Frontend

Frontend website for `www.linke.pt`, built with React + TypeScript + Vite.

## Documentation

- See `docs/README.md` for versioned API references and architecture notes.

## WhatsApp Cloud API setup

Create a `.env` file in project root:

```bash
VITE_WHATSAPP_API_VERSION=v23.0
VITE_WHATSAPP_PHONE_NUMBER_ID=configured_in_backend
VITE_BACKEND_BASE_URL=http://localhost:3001
```

For production (Vercel/serverless), do one of the following:

- Leave `VITE_BACKEND_BASE_URL` empty/not set (recommended). The frontend will call `/api/*` on the same domain.
- Or set `VITE_BACKEND_BASE_URL=https://<your-production-domain>`.

Do not set `VITE_BACKEND_BASE_URL` to `http://localhost:3001` in production.

The frontend includes a **WhatsApp Cloud API Message Console** using:

- `POST https://graph.facebook.com/{Version}/{Phone-Number-ID}/messages`
- `Authorization: Bearer <token>`
- `Content-Type: application/json`

## Backend for Notion logging

The backend creates rows and updates statuses in a specific Notion database.

1. Install backend dependencies:

```bash
npm --prefix backend install
```

2. Configure backend env:

```bash
cp backend/.env.example backend/.env
```

3. Set required values in `backend/.env`:

- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_ACCESS_TOKEN`
- `WHATSAPP_WEBHOOK_VERIFY_TOKEN`
- `NOTION_API_KEY`
- `NOTION_DATABASE_ID`

4. Run backend:

```bash
npm --prefix backend run dev
```

### Backend endpoints

- `POST /api/messages/send` -> sends WhatsApp text and creates Notion row
- `POST /api/messages/status` -> updates a Notion row status by message ID
- `POST /api/media/upload` -> uploads media to WhatsApp Cloud API and returns media ID
- `POST /api/templates/send-return-to-sender` -> sends template `entrega_de_volta_ao_remetente`
- `POST /api/templates/send-generic` -> sends any approved template using dynamic variables
- `POST /api/templates/send-feedback-request` -> sends feedback request template with 2 variables
- `GET /api/templates` -> fetches templates; supports `phoneNumberId` (lookup WABA) or direct `wabaId`, returning all pages by default (`fetchAll=true`)
- `GET /webhook` and `POST /webhook` -> WhatsApp webhook verify + status updates + inbound message logging

### Conversation history tracking (Notion)

When `NOTION_ENABLED=true`, the backend now records:

- Outbound messages sent via API routes
- Delivery/read status updates from webhook events
- Inbound customer messages from webhook `messages` events

This gives you a continuous inbound + outbound conversation history in Notion.

### Media upload relay

Use multipart form-data to upload media:

```bash
curl -X POST "http://localhost:3001/api/media/upload" \
	-F "file=@/path/to/file.jpg" \
	-F "messaging_product=whatsapp"
```

Response example:

```json
{
	"id": "123456789012345"
}
```

### Notion database properties

Ensure your Notion database has these properties (or override names in env):

- `Name` (title)
- `Message ID` (rich text)
- `To` (rich text)
- `Text` (rich text)
- `Status` (rich text)
- `Updated At` (date)

## Run locally

```bash
npm install
npm run dev
```

## Build

```bash
npm run build
npm run preview
```

## Cron on Vercel (Pro)

This project is configured to run scheduled backend endpoints using Vercel Cron.

Configuration file:

- `vercel.json` (field: `crons`)

Cron endpoints triggered:

- GET /api/cron/auto-notificacao-envio
- GET /api/cron/auto-notificacao-envio-em-transporte
- GET /api/cron/auto-notificacao-incidencia

Required environment variables in Vercel:

- `CRON_SECRET` (must match backend validation)

Optional runtime tuning variables:

- `SCHEDULED_MESSAGES_CONCURRENCY` (default: `5`, range: `1-20`) to control parallel scheduled message sends.
- `LOGS_FETCH_ALL_MAX_ROWS` (default: `1000`, range: `100-10000`) to cap `/api/logs?limit=all` payload size.
- `SOFT_WARN_THROTTLE_MS` (default: `60000`) to throttle repeated soft warning logs.
- `REQUEST_TRACE_SLOW_MS` (default: `1500`) to log slow request traces.
- `REQUEST_TRACE_LOG_ALL` (default: `false`) to log trace envelopes for all requests.
- `ADMIN_PERF_SECRET` (optional, falls back to `CRON_SECRET`) to protect `/api/admin/perf`.
- `ADMIN_PERF_PERSIST_ENABLED` (default: `false`) to persist request perf events in Postgres.
- `OUTBOUND_JOB_QUEUE_ENABLED` (default: `false`) to use Postgres queue for scheduled template sends.
- `OUTBOUND_JOB_QUEUE_CLAIM_LIMIT` (default: `20`) jobs claimed per processing run.
- `OUTBOUND_JOB_QUEUE_MAX_ATTEMPTS` (default: `3`) retry cap for queued jobs.

Observability behavior:

- Every request gets `X-Request-Id` in response headers (or reuses incoming `x-request-id`).
- Server logs include `requestId` on key fallback/error paths for easier production traceability.
- `GET /api/admin/perf` returns route latency/failure aggregates and recent trace samples.
- `GET /api/admin/perf?source=db` reads persisted metrics when `ADMIN_PERF_PERSIST_ENABLED=true`.
- When `OUTBOUND_JOB_QUEUE_ENABLED=true`, scheduled messages are persisted in Postgres and drained by `/api/messages/process-scheduled` with retries.

Notes:

- Vercel Cron sends requests to your production deployment.
- Keep the GitHub workflow `.github/workflows/cron-endpoints.yml` only for manual fallback (`workflow_dispatch`) to avoid duplicate scheduled triggers.
