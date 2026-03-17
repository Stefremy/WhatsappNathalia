# WhatsApp Cloud API - Media Upload API (v23.0)

Reference for uploading media files (images, videos, audio, documents, stickers) and retrieving media IDs for use in message send requests.

## Base URL

- `https://graph.facebook.com`

## Endpoint

- `POST /{Version}/{Phone-Number-ID}/media`

## Authentication

- `Authorization: Bearer <token>`

## Request Content Type

- `multipart/form-data`

## Multipart Fields

- `file` (binary)
- `messaging_product` (string, usually `whatsapp`)

## Example cURL

```bash
curl -X POST "https://graph.facebook.com/v23.0/<PHONE_NUMBER_ID>/media" \
  -H "Authorization: Bearer <TOKEN>" \
  -F "file=@/path/to/file.jpg" \
  -F "messaging_product=whatsapp"
```

## Success Response (200)

```json
{
  "id": "123456789012345"
}
```

## Current Project Usage

- Backend relay endpoint: `POST /api/media/upload`
- Backend file: `backend/src/server.js`
- Frontend media console: `src/App.tsx`
