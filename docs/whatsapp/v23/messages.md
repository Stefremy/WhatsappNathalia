# WhatsApp Cloud API - Messages API (v23.0)

Reference for sending and receiving WhatsApp messages including text, media, templates, interactive messages, reactions, and status tracking.

## Base URL

- `https://graph.facebook.com`

## Endpoint

- `POST /{Version}/{Phone-Number-ID}/messages`

## Authentication

- `Authorization: Bearer <token>`

## Required Headers

- `Authorization`
- `Content-Type: application/json` (or other supported media type)

## Core Request Shape

```json
{
  "messaging_product": "whatsapp",
  "recipient_type": "individual",
  "to": "3519XXXXXXXX",
  "type": "text",
  "text": {
    "body": "Hello from Linke"
  }
}
```

## Common Base Properties

- `messaging_product` (string, required)
- `recipient_type` (`individual` or `group`, required)
- `to` (string, required)
- `type` (string, required)
- `context.message_id` (optional, for replies)

## Common Message Families

- Text
- Image / Video / Audio / Document / Sticker
- Reaction
- Interactive (button, list, product, flow, and related action/header/body/footer objects)
- Template
- Contacts
- Location

## Success Response (200)

```json
{
  "messaging_product": "whatsapp",
  "contacts": [
    {
      "input": "3519XXXXXXXX",
      "wa_id": "3519XXXXXXXX"
    }
  ],
  "messages": [
    {
      "id": "wamid.XXXX",
      "message_status": "accepted"
    }
  ]
}
```

Known `message_status` values:

- `accepted`
- `held_for_quality_assessment`
- `paused`

## Current Project Usage

- Backend relay endpoint: `POST /api/messages/send`
- Backend file: `backend/src/server.js`
- Frontend console: `src/App.tsx`
