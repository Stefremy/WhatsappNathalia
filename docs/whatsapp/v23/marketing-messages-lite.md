# WhatsApp Cloud API - Marketing Messages Lite API (v23.0)

Reference for sending marketing template messages with delivery optimization and optional policy/activity settings.

## Base URL

- `https://graph.facebook.com`

## Endpoint

- `POST /{Version}/{Phone-Number-ID}/marketing_messages`

## Authentication

- `Authorization: Bearer <token>`

## Required Headers

- `Authorization`
- `Content-Type: application/json`

## Path Parameters

- `Version` (required, example `v23.0`)
- `Phone-Number-ID` (required)

## Request Body (Required)

Schema: `MarketingMessageRequestPayload`

```json
{
  "messaging_product": "whatsapp",
  "recipient_type": "individual",
  "to": "3519XXXXXXXX",
  "type": "template",
  "template": {
    "name": "your_template_name",
    "language": {
      "code": "en_US"
    },
    "components": []
  },
  "product_policy": "CLOUD_API_FALLBACK",
  "message_activity_sharing": true
}
```

### Field Rules

- `messaging_product` must be `"whatsapp"`.
- `recipient_type` must be `"individual"`.
- `type` must be `"template"`.
- `template.name` and `template.language` are required.
- `product_policy` is optional: `CLOUD_API_FALLBACK` or `STRICT`.
- `message_activity_sharing` is optional boolean.
- Additional properties are not allowed.

## Success Response (200)

Schema: `MarketingMessageResponsePayload`

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

`message_status` values:

- `accepted`
- `held_for_quality_assessment`
- `paused`

## Common Error Responses

- `400` Bad Request (invalid payload)
- `401` Unauthorized (invalid/missing token)
- `403` Forbidden (template approval/permission issue)

## Example cURL

```bash
curl -X POST "https://graph.facebook.com/v23.0/<PHONE_NUMBER_ID>/marketing_messages" \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "messaging_product": "whatsapp",
    "recipient_type": "individual",
    "to": "3519XXXXXXXX",
    "type": "template",
    "template": {
      "name": "promo_template",
      "language": { "code": "en_US" }
    },
    "product_policy": "CLOUD_API_FALLBACK",
    "message_activity_sharing": true
  }'
```

## Current Project Status

- Not yet implemented as backend relay endpoint.
- Recommended route: `POST /api/marketing/send-template`.
- Recommended logging: create Notion row with template metadata and returned `messages[0].id`.
