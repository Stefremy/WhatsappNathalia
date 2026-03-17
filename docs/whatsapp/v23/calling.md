# WhatsApp Cloud API - Calling API (v23.0)

Reference for checking call permissions and initiating/managing WhatsApp VoIP calls.

## Base URL

- `https://graph.facebook.com`

## Endpoints

- `GET /{Version}/{Phone-Number-ID}/call_permissions`
- `POST /{Version}/{Phone-Number-ID}/calls`

## Authentication

- `Authorization: Bearer <token>`

## GET call_permissions

Checks whether a business can call a user and which call actions are currently allowed.

### Query Parameters

- `user_wa_id` (required)

### Permission Status Values

- `granted`
- `pending`
- `denied`
- `expired`

### Action Names

- `start_call`
- `send_call_permission_request`

### Typical Responses

- `200` success
- `400` invalid parameters
- `403` forbidden
- `429` rate limited
- `500` server error

### Success Shape (summary)

```json
{
  "messaging_product": "whatsapp",
  "permission": {
    "status": "granted",
    "expiration_time": 1730000000
  },
  "actions": [
    {
      "action_name": "start_call",
      "can_perform_action": true,
      "limits": [
        {
          "time_period": "daily",
          "current_usage": 0,
          "max_allowed": 100
        }
      ]
    }
  ]
}
```

## POST calls

Used to initiate or manage a call.

### Supported Actions

- `connect`
- `pre_accept`
- `accept`
- `reject`
- `terminate`

### Request Payload Types

1. `CallRequestPayload`

```json
{
  "messaging_product": "whatsapp",
  "to": "3519XXXXXXXX",
  "action": "connect",
  "session": {
    "sdp_type": "offer",
    "sdp": "v=0 ..."
  },
  "biz_opaque_callback_data": "optional-tracking-string"
}
```

2. `CallTerminateRequestPayload`

```json
{
  "messaging_product": "whatsapp",
  "call_id": "CALL_ID",
  "action": "terminate"
}
```

### Success Response Types

- `CallResponsePayload` (contains `calls[]` with IDs)
- `CallTerminateResponsePayload` (contains `success` boolean)

### Important Note

- Error code `138006` indicates missing call request permission from the user.

## Current Project Status

- Calling API is documented but not yet implemented in backend routes.
- Recommended future routes:
  - `GET /api/calls/permissions?user_wa_id=...`
  - `POST /api/calls/manage`
