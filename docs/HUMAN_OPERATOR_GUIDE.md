# SpotOps — Human Operator Guide

**Audience:** People who run day-to-day SpotOps processes. You don't need to be a developer.
**Purpose:** Safe, plain-English procedures for the recurring work, plus clear escalation paths when something looks wrong.
**Last reviewed:** 2026-04-30

> If you're a developer or LLM agent: this guide is intentionally light on technical detail. See [RUNBOOKS.md](RUNBOOKS.md) for command-line ops, [DEV_WORKFLOW.md](DEV_WORKFLOW.md) for code changes, [ARCHITECTURE.md](ARCHITECTURE.md) for system internals.

---

## What SpotOps is

A revenue dashboard and database for Crossings TV ad-sales data. It pulls the daily commercial log from the K: drive, tracks monthly bookings, and exposes reports through a web dashboard plus a Power Query workbook (`Revenue Master.xlsx`).

**You access it over Tailscale.** The public internet does not reach this system by design.

**Almost everything is automatic.** The only manual processes you run are:
1. **Monthly close** — once a month, after accounting closes the month
2. **Yearly recap refresh** — occasional, optional, mid-cycle data refresh
3. **User management** — when someone joins, leaves, or changes role
4. **Investigating something that broke** — rare; you'll see a notification first

Everything else (daily imports, backups, the dashboard) runs itself.

---

## What's automatic — daily imports

The system pulls the K-drive commercial log four times a day and updates the database. You don't run this; you **monitor** it.

### What runs and when

| Roughly | What | Where it lands |
|---|---|---|
| 03:00, 09:00, 15:00, 21:00 Pacific | The K: drive `Commercial Log.xlsx` is copied to the server | An internal staging folder |
| 30 min later (≈03:30, 09:30, 15:30, 21:30) | The new spots are imported into the database | The dashboard's data |

### What "good" looks like

- The dashboard footer (bottom of every page) shows a recent timestamp — within the last 6 hours.
- You receive a ✓ notification on the **ntfy** phone app for each successful run.
- Spot counts for the current month tick up across the day.

### Validation checklist (when in doubt)

- [ ] Dashboard footer shows today's date and is **not** flagged as "stale" (yellow triangle)
- [ ] Recent ntfy notifications show check marks, no rotating-light alerts
- [ ] Current-month spot count is rising or stable, not zero

### Common failures

- **🚨 "Daily update FAILED" ntfy alert.** Something tripped during import. Don't try to fix it yourself — escalate.
- **No notifications at all for >24 hours.** Either notifications broke or imports broke. Escalate.
- **Dashboard footer says >24 hours old (yellow triangle).** Imports stopped firing or are failing silently.

### Escalate if

- Two or more consecutive failure alerts
- Dashboard footer hasn't moved in over 24 hours
- A failure alert mentions a database error or schema error

---

## Monthly Close

### When to do this

After accounting completes month-end close (timing varies — usually within the first 1–2 weeks of the following month). The trigger is "the month is officially closed and the cash revenue recap on K: drive is up to date."

### Inputs needed

- The current year's recap on K: drive: `K:/Traffic/Media library/<YEAR>.xlsx` (e.g., `2026.xlsx`)
- SSH access to the server as your usual user
- Confirmation from accounting that they're done with the month

### Step-by-step procedure

1. **SSH into the server** (over Tailscale):
   ```
   ssh <you>@spotops
   ```

2. **Move into the SpotOps directory:**
   ```
   cd /opt/spotops
   ```

3. **Copy the recap from K: drive** for the year you're closing, e.g. 2026:
   ```
   ./scripts/update_yearly_recap.sh 2026
   ```
   This only copies the file (`K:/.../<YEAR> Cash Revenue Recap.xlsx` → `data/raw/2026.xlsx`). It does **not** import yet.

4. **Run the importer** to load the recap and **close** every month in the file:
   ```
   uv run python cli/import_closed_data.py data/raw/2026.xlsx --year 2026 --closed-by "<your name>"
   ```
   Substitute the year and your name. **Do not pass `--skip-closed`** — that flag is for the optional mid-cycle refresh in the next section. A typical import is a few minutes.

5. **Optional immediate backup** (the nightly Dropbox snapshot covers this within 24 h, but if you want to lock in the close right away):
   ```
   uv run python cli_db_sync.py backup
   ```

### Validation checklist

- [ ] Importer output ends with success (no `ERROR:` lines, no "Import failed")
- [ ] Open the dashboard. The month you just closed shows **🟢 CLOSED** (green), not 🟡 OPEN (yellow)
- [ ] Spot counts for the closed month look right (not zero, not orders of magnitude off from prior months)
- [ ] A nightly database snapshot lands in Dropbox under `/database.db` within ~24 hours

### Common failures

- **"K drive not accessible at /mnt/k-drive"** (from the recap-copy step). The server lost the K-drive share. Run `sudo mount /mnt/k-drive` then re-run step 3.
- **"Source file not found"** (from the recap-copy step). The recap on K: drive isn't named what the script expects, or accounting saved it to a different folder. Check K: drive directly.
- **🟡 yellow (OPEN) instead of 🟢 green (CLOSED) after a successful import.** This means the close-month flag wasn't applied. The most common cause is the `--skip-closed` flag was used on `cli/import_closed_data.py` (which is for refreshes, not closing). Re-run step 4 without that flag.

### What NOT to do

- **Don't pass `--skip-closed`** during monthly close. That flag is for mid-month refreshes that intentionally leave months open. Using it here is the #1 mistake — the data imports but the months stay open.
- **Don't re-run the close on a month already closed** unless you mean to replace the data. Closed months will be **replaced**, and the close timestamp will move.
- **Don't edit the database file directly.** Ever.

### Escalate if

- The importer fails partway through — the database may be partway updated
- Spot counts for *prior* (already-closed) months changed unexpectedly after running
- The dashboard month-status indicator stays 🟡 yellow even after a clean re-run

---

## Yearly Recap Refresh (optional, mid-cycle)

### When to do this

Occasionally, when you want the current year's actuals refreshed from the latest K: drive recap **without closing any months**. Typical reasons: end-of-quarter spot-check, board-meeting prep, or accounting fixed something mid-month.

### Inputs needed

- Same as Monthly Close: the current year's `<YEAR>.xlsx` on K: drive
- SSH access

### Step-by-step procedure

Same first two steps as Monthly Close, then a **different** importer invocation that uses `--skip-closed` so already-closed months are left alone.

1. SSH in, `cd /opt/spotops`.
2. Copy the recap:
   ```
   ./scripts/update_yearly_recap.sh 2026
   ```
3. Run the importer in **refresh mode** with `--skip-closed`:
   ```
   uv run python cli/import_closed_data.py data/raw/2026.xlsx --year 2026 --closed-by "<your name>" --skip-closed
   ```
   The `--skip-closed` flag is the difference between refresh and close. With it, only currently-open months are touched.

### Validation checklist

- [ ] Spot counts changed for the current/recent months
- [ ] Already-closed months still show 🟢 (CLOSED) — the refresh did NOT touch them
- [ ] No alarms fired about the database backup

### Escalate if

- Any month that was already closed flips back to 🟡 (OPEN) after the run

---

## User Management

### When to do this

- A new person needs dashboard access
- Someone leaves and should lose access
- Someone's email changes (e.g., name change, domain change)

You must be signed in as an **admin** to manage users.

### Inputs needed

- The person's **Tailscale account email** — must match exactly what they sign into Tailscale with
- Their **role:**
  - **admin** — full access including user management
  - **management** — all reports, all AEs
  - **AE** — own AE dashboard only

### Step-by-step procedure (add a user)

1. Sign into the dashboard.
2. Navigate to `/users/`.
3. Click **Create**.
4. Fill in first name, last name, email (their Tailscale email), role.
5. Save.
6. Tell them to visit the dashboard URL — they'll be signed in automatically.

### Step-by-step procedure (remove a user)

1. Sign into the dashboard.
2. Navigate to `/users/`.
3. Find their row, click **Delete**.

### Step-by-step procedure (change a user's email)

1. Sign in, go to `/users/`.
2. Click their row, **Edit**, update the email, save.

### Validation checklist

- [ ] The user's row appears at `/users/` with the right role
- [ ] (For new users) Have them try the dashboard while you watch — they should land directly on a logged-in page, not see "not authorized"

### Common failures

- **"Your Tailscale account is not authorized. Ask an admin to add you."** The email in the users table doesn't exactly match what they're signed into Tailscale with. Edit the row.
- **"Login requires Tailscale."** They're not connected to the tailnet. They need to install or reconnect Tailscale first; this is not a SpotOps issue.

### What NOT to do

- **Don't ask for or set a password.** There aren't any. The system trusts Tailscale.
- **Don't delete your own admin account.** The system blocks this; if it didn't, you'd lock yourself out.
- **Don't add an email that doesn't match their Tailscale account.** They'll just see the "not authorized" message.

### Escalate if

- A user has the right email in `/users/` and is on the tailnet but still sees "not authorized"

---

## How sign-in actually works

You don't see passwords because there aren't any. The system trusts Tailscale to know who you are.

What happens when you visit the dashboard:

1. Your Tailscale client connects you to the tailnet.
2. The server asks Tailscale "who is this connection?" through a local channel.
3. Tailscale answers with your account email.
4. If that email exists in the SpotOps `users` table, you're signed in with whatever role's on that row.
5. If it doesn't, you see "Your Tailscale account is not authorized."

Practical implications:

- **You must be on Tailscale** to use the dashboard at all. There is no public login page.
- **Email match is exact.** No aliases, no case-insensitive matching at the SpotOps level. The string in the users table must equal the string Tailscale reports.
- **Adding a user = adding a row.** No invite emails, no password resets, no two-factor setup at the SpotOps layer. Tailscale itself handles all of that.

---

## Backups (what to glance at occasionally)

You don't run backups — they're automatic. But you should know they exist so you can spot when something stops working.

| Backup | What it covers | How often | How you check it |
|---|---|---|---|
| **Litestream → Backblaze** | The live database (continuous) | Every few seconds | Not directly visible — assume it's working unless told otherwise |
| **Nightly Dropbox snapshot** | Full database file | Once nightly (~02:04) | Check the SpotOps Dropbox folder; `database.db` should have today's date |
| **Restic → Backblaze** | Imported files, processed data, uploads | Once nightly (~02:30) | Not visible to you |

If you go more than **48 hours** without a fresh `database.db` in Dropbox, escalate.

---

## When something looks wrong

### "Dashboard footer is stale" (yellow triangle on the timestamp)

- **< 6 hours stale:** probably fine; the next scheduled import is on its way
- **6–24 hours stale:** one or two runs failed; check ntfy notifications for failure alerts
- **> 24 hours stale:** the system flags this with a yellow warning. **Escalate.**

### "A number on the dashboard looks wrong"

Don't try to fix it in the database. Capture:
- The URL of the page
- A screenshot
- What you expected vs. what you saw

Then escalate. "Wrong number" cases are usually one of: a misunderstanding of how that specific report works, a broken filter, or a real bug. All three need a developer.

### "I can't sign in"

Diagnose in this order:
1. Is your Tailscale client showing as connected? (Tailscale icon in your tray)
2. Is the email you're signed into Tailscale with the same as what's in `/users/`?
3. If both are right and dashboard still says not authorized: an admin needs to look at the users table.

### "The script I ran got interrupted partway"

Don't re-run it. Note:
- Which script
- Roughly when
- Whether the dashboard's data looks partly updated

Escalate. Re-running an interrupted import without a developer's review can compound the problem.

### "I got a phone alert"

| Alert | What to do |
|---|---|
| ✓ "Daily update completed successfully" | Nothing — informational |
| ✓ "CTV Commercial Import" success | Nothing — informational |
| 🚨 "Daily update FAILED" (priority 5, rotating-light icon) | Note the time, then escalate. The error is in the logs |
| Anything mentioning "database error", "migration", "schema" | **Escalate immediately.** Don't run anything else |

---

## What NOT to touch

- The production database file at `/srv/spotops/db/production.db` — never write to it directly, never copy over it
- Anything under `/etc/` on the server — system-wide configuration
- The `Data` tab of `Revenue Master.xlsx` — Power Query overwrites it on every refresh. **Type forecasts into the `Forecasts` tab**, not the Data tab. There's a yellow banner on row 1 saying so.
- Anything under `/srv/spotops/` (live data and backups)
- Files starting with `.env` — they hold credentials

---

## When to escalate to a developer

**Always:**
- A failure that hasn't been seen before
- Numbers that don't match between two places (dashboard vs workbook) where you expect them to match
- Any database / schema / migration error in a notification or log

**Probably:**
- A dashboard page is broken or shows a stack trace
- The system is slow for more than 30 minutes
- You ran something twice by accident and aren't sure of the state
- The script you ran got interrupted

**Maybe (try the basics first):**
- A daily import didn't run at the expected time → check ntfy first; the run might just have been delayed by a few minutes
- The dashboard is briefly slow → wait 5 minutes and reload before escalating

When you escalate, include: what you were doing, what you saw, the time, and (if applicable) the URL or notification text.

---

## Related docs

- [RUNBOOKS.md](RUNBOOKS.md) — exact commands for ops recovery (writing for a developer or savvy ops person)
- [ARCHITECTURE.md](ARCHITECTURE.md) — what the system looks like under the hood
- [DEV_WORKFLOW.md](DEV_WORKFLOW.md) — for code changes
