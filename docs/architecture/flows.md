# Integration Flows

## 1. Send Text Message + Log

1. Frontend posts to `POST /api/messages/send`.
2. Backend sends official WhatsApp Message API request.
3. Backend stores request/response metadata in Notion.
4. Frontend receives WhatsApp response.

## 2. Upload Media + Receive Media ID

1. Frontend uploads multipart form-data to `POST /api/media/upload`.
2. Backend forwards to official WhatsApp Media Upload API.
3. Backend returns media `id` to frontend.
4. Optional: backend writes upload log to Notion.

## 3. Delivery/Read Status Updates

1. WhatsApp webhook posts status events to backend `POST /webhook`.
2. Backend extracts `messageId` and `status`.
3. Backend finds Notion row by `Message ID`.
4. Backend updates `Status` and `Updated At`.

## 4. Calling API (Planned)

1. Check permissions via `GET /{version}/{phone-number-id}/call_permissions`.
2. Manage call lifecycle via `POST /{version}/{phone-number-id}/calls`.
3. Add backend relay endpoints before exposing in frontend.

## 5. Marketing Messages Lite (Planned)

1. Frontend submits template send request to backend relay.
2. Backend calls `POST /{version}/{phone-number-id}/marketing_messages`.
3. Backend stores template send metadata and message ID in Notion.
4. Webhook status updates continue lifecycle tracking for the same message ID.

## 6. Phone Number Verification Request Code (Planned)

1. Frontend admin flow submits verification request to backend relay.
2. Backend calls `POST /{version}/{phone-number-id}/request_code` with `code_method` and `language`.
3. Backend records request and outcome for audit tracking.
4. Operator enters received code in a later verification step flow.

## 7. Phone Number Verification Verify Code (Planned)

1. Operator submits the received code through a secured admin flow.
2. Backend calls `POST /{version}/{phone-number-id}/verify_code` with `{ code }`.
3. Backend records success/failure details for audit and recovery workflows.
4. On success, phone number state is marked as verified in internal operations tracking.

## 8. Phone Number Settings Management (Planned)

1. Admin UI reads current settings through backend relay (`GET /settings`).
2. Operator updates one setting group per request (`POST /settings`).
3. Backend enforces one-feature-per-request contract before calling Graph API.
4. Backend records setting changes and responses for audit traceability.

## 9. Official Business Account Status Lifecycle (Planned)

1. Admin reads current OBA status via backend relay (`GET /official_business_account`).
2. Admin submits application/update actions via backend relay (`POST /official_business_account`).
3. Backend validates state transition intent before forwarding to Graph API.
4. Backend records OBA status transitions, status messages, and tracking IDs for compliance.
