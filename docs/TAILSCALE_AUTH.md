# Sign-in via Tailscale

Authentication uses **Tailscale identity** only (no passwords). The app calls the **Tailscale Local API** (`/localapi/v0/whois`) over the Unix socket to resolve the logged-in user for each request.

## Does it work?

Yes. When someone opens the app **over your tailnet**:

1. The request arrives from a Tailscale IP (e.g. `100.x.y.z`) on your Pi.
2. The app calls the Tailscale Local API (`whois?addr=<remote-addr>`) via `/var/run/tailscale/tailscaled.sock`.
3. The response includes the user’s Tailscale login (e.g. `alice@company.com`) and node name.
4. The app looks up a user by that email in the `users` table. If found, they are logged in and redirected to reports.
5. If no user exists for that email, they see: *"Your Tailscale account is not authorized. Ask an admin to add you."*

## How do users reach the app?

You do **not** need `tailscale serve` for identity. The app only requires:

- The Pi is connected to your tailnet (Tailscale running).
- Users connect to the Pi over Tailscale (e.g. `http://pi-ctv:8000/` or `http://100.x.y.z:8000/` where `100.x.y.z` is the Pi’s Tailscale IP).

Once a request hits the app from a Tailscale IP, the Local API `whois` call returns the correct login identity.

## Setup checklist

1. **Pi is on your tailnet** (Tailscale up).
2. **App is bound to a safe interface/port** (e.g. `0.0.0.0:8000` but only reachable over your private network).
3. **Users table** has a row per allowed user with `email` matching their Tailscale login. Create users via Admin → User Management, or `scripts/create_admin_user.py`.
4. (Optional) Use MagicDNS or your own DNS to make `http://pi-ctv:8000/` easier to reach from client machines on the tailnet.

Once that’s in place, sign-in via Tailscale works: users connect over the tailnet and are logged in automatically if their email exists in the app.
