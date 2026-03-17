# WhatsApp Cloud API - Settings API (v23.0)

Reference for retrieving and updating WhatsApp Business phone number settings such as calling, user identity change, payload encryption, and storage configuration.

## Base URL

- `https://graph.facebook.com`

## Endpoints

- `GET /{Version}/{Phone-Number-ID}/settings`
- `POST /{Version}/{Phone-Number-ID}/settings`

## Authentication

- `Authorization: Bearer <token>`

## GET settings

Retrieve current settings for a phone number.

### Query Parameters

- `include_sip_credentials` (optional boolean): include SIP credentials in response (requires additional permissions).

### Success Response (200)

Schema: `PhoneNumberSettingsResponse`

Top-level fields:

- `calling`
- `payload_encryption`
- `storage_configuration`

### Common Errors

- `400` invalid request parameters
- `403` insufficient permissions

## POST settings

Update phone number settings.

Only one feature setting can be specified per request.

### Accepted Request Types

1. `CallingSettingsRequest`

```json
{
  "calling": {
    "status": "enabled",
    "call_icon_visibility": "visible",
    "video": { "status": "enabled" },
    "sip": { "status": "disabled" },
    "srtp_key_exchange_protocol": "DTLS-SRTP"
  }
}
```

2. `UserIdentityChangeSettingsRequest`

```json
{
  "user_identity_change": {
    "enabled": true
  }
}
```

3. `PayloadEncryptionSettingsRequest`

```json
{
  "payload_encryption": {
    "status": "enabled",
    "client_encryption_key": "<BASE64_PUBLIC_KEY>"
  }
}
```

4. `StorageConfigurationSettingsRequest`

```json
{
  "storage_configuration": {
    "enabled": true,
    "region": "eu"
  }
}
```

### Success Response (200)

```json
{
  "success": true
}
```

### Common Errors

- `400` invalid parameters
- `403` insufficient permissions

## Key Response Objects (Summary)

### CallingSettingsResponse

- `status`: `enabled` or `disabled`
- `call_icon_visibility`: `visible` or `hidden`
- `ip_addresses.default`
- `callback_permission_status`
- Optional: `srtp_key_exchange_protocol`, `call_hours`, `call_icons`, `sip`, `video`, `audio`, `restrictions`

### PayloadEncryptionSettingsResponse

- `status`: `enabled` or `disabled`
- Optional: `client_encryption_key_fingerprint`, `cloud_encryption_key`

### StorageConfigurationSettingsResponse

- `status`: `default` or `in_country_storage_enabled`
- Optional: `data_localization_region`

## Example cURL

GET:

```bash
curl -X GET "https://graph.facebook.com/v23.0/<PHONE_NUMBER_ID>/settings?include_sip_credentials=false" \
  -H "Authorization: Bearer <TOKEN>"
```

POST (calling settings):

```bash
curl -X POST "https://graph.facebook.com/v23.0/<PHONE_NUMBER_ID>/settings" \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "calling": {
      "status": "enabled"
    }
  }'
```

## Current Project Status

- Settings API is documented but not yet implemented in backend routes.
- Recommended future routes:
  - `GET /api/settings/phone-number`
  - `POST /api/settings/phone-number`
