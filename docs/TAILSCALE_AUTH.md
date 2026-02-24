# Sign-in via Tailscale

Authentication uses **Tailscale identity** only (no passwords). The app trusts the `Tailscale-User-Login` (email) and `Tailscale-User-Name` (display name) headers that Tailscale Serve adds when traffic comes through your tailnet.

## Does it work?

Yes. When someone opens the app **via your Tailscale Serve URL**:

1. Tailscale Serve proxies the request to your app (on localhost).
2. Tailscale sets `Tailscale-User-Login` (e.g. `alice@company.com`) and `Tailscale-User-Name` (e.g. `Alice Smith`).
3. The app looks up a user by that email in the `users` table. If found, they are logged in and redirected to reports.
4. If no user exists for that email, they see: *"Your Tailscale account is not authorized. Ask an admin to add you."*

**Required:** The app must be reached **only** through Tailscale Serve (app listening on localhost). If the app is reachable directly (e.g. on `0.0.0.0` or a public URL), the headers could be spoofed.

## What is the domain / URL?

The URL is **not** something you configure in the app. Tailscale assigns it when you use **Tailscale Serve** on the machine running the app.

Format:

```text
https://<machine-name>.<tailnet-name>.ts.net
```

- **&lt;machine-name&gt;** – The Tailscale name of the device (e.g. `spotops-dev`, `my-laptop`). You can see it in the Tailscale admin or with `tailscale status`.
- **&lt;tailnet-name&gt;** – Your tailnet’s name. Examples:
  - **Tailscale for Teams / paid:** often your domain, e.g. `company.com` → `company.com` in the URL.
  - **Personal / free:** often a generated name like `pango-lin.ts.net` or similar.

**Examples:**

- `https://spotops-dev.company.com.ts.net` (machine `spotops-dev`, tailnet `company.com`)
- `https://amelie-workstation.pango-lin.ts.net` (from Tailscale’s docs)

To see **your** URL:

1. On the machine that runs the app, run:
   ```bash
   tailscale serve 5100
   ```
2. The CLI prints the URL, e.g.:
   ```text
   Available within your tailnet:
   https://your-machine.your-tailnet.ts.net
   ```
3. You can also check in **Tailscale admin** → **Machines** → select the machine → see its MagicDNS name; the Serve URL is `https://<that-name>` (with the port you configured in Serve).

Users should **bookmark or use that HTTPS URL** to open the app. Opening `http://localhost:5100` (or any non-Serve URL) will not send the identity headers, so they will not be logged in.

## Setup checklist

1. **App listens on localhost only** (e.g. `127.0.0.1:5100` or `localhost:5100`).
2. **Tailscale Serve** proxies that port, e.g. `tailscale serve 5100` (or your app port).
3. **Users table** has a row per allowed user with `email` matching their Tailscale login (same as `Tailscale-User-Login`). Create users via Admin → User Management, or `scripts/create_admin_user.py`.
4. **HTTPS:** Tailscale Serve uses HTTPS; your tailnet must have HTTPS/MagicDNS enabled (the `tailscale serve` flow will prompt if needed).

Once that’s in place, sign-in via Tailscale works: users open the Serve URL and are logged in automatically if their email exists in the app.
