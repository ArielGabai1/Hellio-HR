"""Agent state -- track processed emails via backend API."""

import logging
import httpx
from auth import BACKEND_URL, authed_request

log = logging.getLogger("hellio-agent")


def is_email_processed(email_id: str) -> bool:
    try:
        resp = authed_request(
            httpx.get, f"{BACKEND_URL}/api/agent/processed-emails/{email_id}")
        resp.raise_for_status()
        return resp.json()["processed"]
    except Exception as e:
        log.warning(f"Failed to check processed status for {email_id}: {e}")
        return False


def mark_email_processed(email_id: str, email_type: str,
                         action_taken: str) -> str:
    resp = authed_request(
        httpx.post, f"{BACKEND_URL}/api/agent/processed-emails",
        json={"email_id": email_id, "email_type": email_type,
              "action_taken": action_taken})
    if resp.status_code == 409:
        return f"Already processed: {email_id}"
    resp.raise_for_status()
    return f"Marked processed: {email_id}"
