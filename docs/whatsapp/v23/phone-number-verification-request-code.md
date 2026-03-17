# WhatsApp Cloud API - Phone Number Verification Request Code API (v23.0)

Reference for requesting verification codes for WhatsApp Business phone numbers via SMS or voice call.

## Base URL

- `https://graph.facebook.com`

## Endpoint

- `POST /{Version}/{Phone-Number-ID}/request_code`

## Purpose

- Initial verification for newly added business phone numbers.
- Re-verification when phone number state requires it.
- Switching between SMS and voice verification methods.

## Authentication

- `Authorization: Bearer <token>`

## Required Headers

- `Authorization`
- `Content-Type: application/json`

## Path Parameters

- `Version` (required): Graph API version, example `v23.0`.
- `Phone-Number-ID` (required): WhatsApp Business phone number status entity ID.

## Request Body (Required)

Schema: `RequestCodeRequest`

```json
{
  "code_method": "SMS",
  "language": "en_US"
}
```

### Request Fields

- `code_method` (required): `SMS` or `VOICE`.
- `language` (required): locale for the verification message.

## Success Response (200)

Schema: `RequestCodeResponse`

```json
{
  "success": true
}
```

## Error Model

Schema: `GraphAPIError`

```json
{
  "error": {
    "message": "...",
    "type": "...",
    "code": 100,
    "error_subcode": 123,
    "fbtrace_id": "...",
    "is_transient": true,
    "error_user_title": "...",
    "error_user_msg": "..."
  }
}
```

## Common Error Statuses

- `400` invalid parameters or malformed request
- `401` invalid or missing access token
- `403` insufficient permissions
- `404` phone number not found/inaccessible
- `422` valid request but not processable at this time
- `429` rate limited
- `500` internal server error

## Example cURL

```bash
curl -X POST "https://graph.facebook.com/v23.0/<PHONE_NUMBER_ID>/request_code" \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "code_method": "SMS",
    "language": "en_US"
  }'
```

## Operational Notes

- Standard Graph API rate limits apply.
- Additional anti-abuse throttling may apply to repeated code requests.
- Verification codes are delivered only to the registered number and expire shortly.
- Repeated failed attempts can lead to temporary blocking.

## Current Project Status

- Not yet implemented as backend relay endpoint.
- Recommended route: `POST /api/phone-number/request-code`.
- Recommended logs: write a Notion row with phone number ID, code method, language, and response status.
