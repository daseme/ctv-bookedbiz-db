# Insertion Order File Browser

## Problem

The "Open Folder" link on the AE dashboard uses `file:///K:/...` URLs, which browsers block from HTTP pages. AEs cannot access their insertion order files from the dashboard.

## Solution

Serve insertion order files directly from the already-mounted K: drive (`/mnt/k-drive/Insertion Orders/`) via new Flask routes. No file copying, no sync, no cleanup.

## Routes

All routes require `@login_required`. Base path: `/reports/insertion-orders/`.

| Route | Purpose |
|---|---|
| `GET /reports/insertion-orders/<ae_name>` | List customer folders for an AE |
| `GET /reports/insertion-orders/<ae_name>/<customer_folder>` | List files in a customer folder |
| `GET /reports/insertion-orders/<ae_name>/<customer_folder>/<filename>` | Serve a file (PDF inline, others download) |

## Security

- Path traversal protection: reject any segment containing `..`
- All segments validated against actual directory contents
- Files served only from the IO base path via `send_from_directory`
- Same auth as AE dashboard

## Template Changes

Replace the `file:///` link in `ae-dashboard-personal.html` with a link to the AE folder listing route.

The folder/file listing pages use minimal styling consistent with the existing dashboard (Nord theme).

## File Display

- PDFs: open inline in browser (`Content-Disposition: inline`)
- Other files: download (`Content-Disposition: attachment`)
- File listing shows name, size, and modified date

## No New Dependencies

Uses Flask's built-in `send_from_directory`. No file copying, no background jobs, no database changes.
