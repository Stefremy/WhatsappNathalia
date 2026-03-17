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
- `GET /api/templates` -> fetches templates associated with a phone number ID (via WABA lookup), returning all pages by default (`fetchAll=true`)
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
