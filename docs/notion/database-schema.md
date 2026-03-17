# Notion Database Schema Reference

This project writes WhatsApp message and media logs to a specific Notion database.

## Required Properties

- `Name` (title)
- `Message ID` (rich text)
- `To` (rich text)
- `Text` (rich text)
- `Status` (rich text)
- `Updated At` (date)

## Environment Variable Overrides

These env vars allow custom Notion property names:

- `NOTION_PROP_TITLE`
- `NOTION_PROP_MESSAGE_ID`
- `NOTION_PROP_TO`
- `NOTION_PROP_TEXT`
- `NOTION_PROP_STATUS`
- `NOTION_PROP_UPDATED_AT`

## Notes

- Message sends create rows.
- Delivery/read updates modify existing rows by `Message ID`.
- Media uploads can also create log rows when Notion config is enabled.
