# WhatsApp Business Account Phone Number Verification - Verify Code API (v23.0)

Reference for verifying phone number verification codes sent during WhatsApp Business phone number verification.

## Base URL

- `https://graph.facebook.com`

## Endpoint

- `POST /{Version}/{Phone-Number-ID}/verify_code`

## Purpose

- Complete phone number verification during initial setup.
- Finalize registration or migration verification flow.
- Confirm ownership after receiving a code via SMS or voice call.

## Authentication

- `Authorization: Bearer <token>`

## Required Headers

- `Authorization`
- `Content-Type: application/json`

## Path Parameters

- `Version` (required): Graph API version, example `v23.0`.
- `Phone-Number-ID` (required): phone number ID to verify.

## Request Body (Required)

Schema: `VerifyCodeRequest`

```json
{
  "code": "123456"
}
```

### Request Fields

- `code` (required): verification code received by SMS or voice.

## Success Response (200)

Schema: `VerifyCodeResponse`

```json
{
  "success": true,
  "id": "<PHONE_NUMBER_ID>"
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

- `400` invalid request or code format
- `401` invalid or missing access token
- `403` insufficient permissions
- `404` phone number not found/inaccessible
- `422` invalid or expired verification code
- `429` too many verification attempts
- `500` server error

## Example cURL

```bash
curl -X POST "https://graph.facebook.com/v23.0/<PHONE_NUMBER_ID>/verify_code" \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "code": "123456"
  }'
```

## Operational Notes

- Verification codes are generally single-use and short-lived (around minutes).
- Failed attempts are rate-limited to prevent abuse.
- Use retry logic with exponential backoff for transient failures.

## Current Project Status

- Not yet implemented as backend relay endpoint.
- Recommended route: `POST /api/phone-number/verify-code`.
- Recommended logs: store phone number ID, verification attempt outcome, and error details for audit.
