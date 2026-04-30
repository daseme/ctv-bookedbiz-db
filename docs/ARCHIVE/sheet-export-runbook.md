# Sheet Export Endpoint — Runbook

**Endpoint:** `GET /api/revenue/sheet-export`
**Purpose:** Feeds Kurt's `Revenue Master.xlsx` Power Query refresh. See
`docs/superpowers/specs/2026-04-20-revenue-sheet-export-design.md` for design.

## Environment setup

The endpoint requires the env var `SHEET_EXPORT_TOKEN` to be set on the
server. Without it, the endpoint returns `503`.

- **Dev:** add to the dev stack's `.env` or export before running Flask:
  ```bash
  export SHEET_EXPORT_TOKEN=dev-secret-abc123
  ```
- **Prod:** set in systemd env file for `spotops-dev.service` (or
  equivalent): `/etc/systemd/system/spotops.service.d/override.conf`:
  ```
  [Service]
  Environment="SHEET_EXPORT_TOKEN=<production-value>"
  ```
  Then `systemctl daemon-reload && systemctl restart spotops`.

## Token rotation

1. Generate new token: `openssl rand -hex 32`.
2. Update env on server and restart Flask.
3. Kurt opens the workbook → unhides `Config` tab → updates `ApiToken`
   cell → saves.
4. Refresh from Excel.

## Smoke test (post-deploy)

```bash
curl -sS -H "X-SpotOps-Token: $SHEET_EXPORT_TOKEN" \
  http://localhost:5000/api/revenue/sheet-export \
  | jq '.metadata'
```

Expected: `{"generated_at": "...", "hash_version": "v1", "row_count": N, ...}`
with N > 0 on a populated DB.

## Failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| `503 {"error": "...misconfigured..."}` | `SHEET_EXPORT_TOKEN` env var not set | Set env var, restart Flask |
| `401 {"error": "Authentication required"}` | Client token doesn't match env var | Check Config tab token matches server env |
| Empty `rows` array on populated DB | `start_month`/`end_month` query params frame nothing | Remove params, refresh |
| Power Query refresh errors on `hash_version` mismatch | Server emits different `hash_version` than Config tab expects | Bump PQ Config → same version, or roll server back |
