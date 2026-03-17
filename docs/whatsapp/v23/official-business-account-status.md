# WhatsApp Business Account Official Business Account Status API (v23.0)

Reference for retrieving and updating Official Business Account (OBA) status for WhatsApp Business Account phone numbers.

## Base URL

- `https://graph.facebook.com`

## Endpoints

- `GET /{Version}/{Phone-Number-ID}/official_business_account`
- `POST /{Version}/{Phone-Number-ID}/official_business_account`

## Authentication

- `Authorization: Bearer <token>`

## GET official_business_account

Retrieve OBA status and status message for a phone number.

### Query Parameters

- `fields` (optional): comma-separated fields to return.
  - Supported fields: `oba_status`, `status_message`

### Success Response (200)

Schema: `OfficialBusinessAccountStatus`

```json
{
  "id": "<PHONE_NUMBER_ID>",
  "oba_status": "UNDER_REVIEW",
  "status_message": "Application is being reviewed"
}
```

`oba_status` enum values:

- `PENDING`
- `APPROVED`
- `REJECTED`
- `UNDER_REVIEW`
- `EXPIRED`
- `CANCELLED`

### Common Errors

- `400` invalid parameters
- `401` unauthorized
- `403` insufficient permissions
- `404` phone number not found
- `422` request cannot be processed for requested fields/rules
- `429` rate limited
- `500` server error

## POST official_business_account

Submit/update OBA application or status transition request.

Typical use cases:

- Submit initial OBA application
- Withdraw pending application
- Resubmit after rejection

### Request Body

Schema: `OfficialBusinessAccountUpdateRequest`

```json
{
  "business_website_url": "https://example.com",
  "primary_country_of_operation": "PT",
  "primary_language": "pt",
  "parent_business_or_brand": "Example Brand",
  "supporting_links": [
    "https://example.com/about",
    "https://example.com/news-1",
    "https://example.com/news-2",
    "https://example.com/news-3",
    "https://example.com/press"
  ],
  "additional_supporting_information": "Business notability and operations details"
}
```

Request notes:

- `business_website_url` required
- `primary_country_of_operation` required
- `supporting_links` optional, typically 5 to 10 links when provided

### Success Response (200)

Schema: `OfficialBusinessAccountUpdateResponse`

```json
{
  "success": true,
  "message": "Application submitted",
  "tracking_id": "TRACK-123",
  "updated_status": {
    "id": "<PHONE_NUMBER_ID>",
    "oba_status": "PENDING",
    "status_message": "Application received"
  }
}
```

### Common Errors

- `400` invalid payload or state transition
- `401` unauthorized
- `403` insufficient permissions
- `404` phone number not found
- `409` invalid transition or conflicting request
- `422` violates business rules
- `429` rate limited
- `500` server error

## Standard Error Model

`GraphAPIError`:

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

## Example cURL

GET status:

```bash
curl -X GET "https://graph.facebook.com/v23.0/<PHONE_NUMBER_ID>/official_business_account?fields=oba_status,status_message" \
  -H "Authorization: Bearer <TOKEN>"
```

POST application update:

```bash
curl -X POST "https://graph.facebook.com/v23.0/<PHONE_NUMBER_ID>/official_business_account" \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "business_website_url": "https://example.com",
    "primary_country_of_operation": "PT"
  }'
```

## Operational Notes

- Standard Graph API rate limits apply.
- OBA status may change during review; cache conservatively.
- Use exponential backoff for transient failures.

## Current Project Status

- Not yet implemented as backend relay endpoints.
- Recommended future routes:
  - `GET /api/official-business-account/status`
  - `POST /api/official-business-account/update`
- Recommended logging: store transitions, payload snapshots, status messages, and tracking IDs.
