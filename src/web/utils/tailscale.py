import logging
from typing import Optional, Tuple

import requests_unixsocket

logger = logging.getLogger(__name__)


def get_tailscale_identity(remote_addr: Optional[str]) -> Optional[Tuple[str, Optional[str]]]:
    """
    Resolve Tailscale user identity for a client IP using the Tailscale local API.

    Returns:
        (login_email, node_name) or None if lookup fails.
    """
    if not remote_addr:
        logger.warning("Tailscale identity lookup: missing remote_addr")
        return None

    try:
        session = requests_unixsocket.Session()
        url = (
            "http+unix://%2Fvar%2Frun%2Ftailscale%2Ftailscaled.sock"
            f"/localapi/v0/whois?addr={remote_addr}"
        )
        resp = session.get(url, timeout=2)
        resp.raise_for_status()
        info = resp.json()

        user_profile = info.get("UserProfile") or {}
        node_info = info.get("Node") or {}
        login = (user_profile.get("LoginName") or "").strip().lower()
        node_name = (node_info.get("Name") or "").strip() or None

        if not login:
            logger.warning("Tailscale whois returned no LoginName for addr %s", remote_addr)
            return None

        return login, node_name
    except Exception as e:
        logger.error("Tailscale whois failed for addr %s: %s", remote_addr, e)
        return None

