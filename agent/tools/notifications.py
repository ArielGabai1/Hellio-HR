"""Notifications -- DB record per event, Slack digest per cycle."""

import logging
import os
from uuid import UUID

import httpx
from strands import tool
from auth import BACKEND_URL, authed_request

log = logging.getLogger("hellio-agent")
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "")

_ACTIONS = {
    "candidate_ingested": {
        "summary": "New candidate profile created",
        "action": "Review profile and approve welcome email draft",
    },
    "candidate_updated": {
        "summary": "Returning candidate, profile updated",
        "action": "Review updated profile and approve follow-up email draft",
    },
    "candidate_cv_missing": {
        "summary": "Candidate email without CV attachment",
        "action": "Review and approve draft requesting CV",
    },
    "position_ingested": {
        "summary": "New position added to the system",
        "action": "Review position and approve email draft to hiring manager",
    },
    "position_updated": {
        "summary": "Position details updated",
        "action": "Review updated position and approve email draft",
    },
    "position_info_missing": {
        "summary": "Position email missing required details",
        "action": "Review and approve draft requesting missing information",
    },
    "attachments_skipped": {
        "summary": "Too many attachments, asked sender to resend",
        "action": "Review and approve overflow draft",
    },
}

UI_BASE = os.environ.get("UI_BASE_URL", "http://localhost")

_cycle_events = []


def clear_cycle_events():
    _cycle_events.clear()


def flush_cycle_report():
    if not _cycle_events or not SLACK_WEBHOOK:
        return
    n = len(_cycle_events)
    title = f"Hellio HR Agent - {n} new {'item' if n == 1 else 'items'} to review"
    blocks = [{"type": "header", "text": {"type": "plain_text", "text": title}}]
    for ev in _cycle_events:
        text = f"*{ev['name']}*\n{ev['summary']}\nAction: {ev['action']}"
        if ev["url"]:
            text += f"\n<{ev['url']}|Review in Hellio HR>"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
        blocks.append({"type": "divider"})
    blocks.pop()  # remove trailing divider
    try:
        resp = httpx.post(SLACK_WEBHOOK, timeout=10, json={"text": title, "blocks": blocks})
        log.info(f"Slack: {title}") if resp.status_code == 200 else log.warning(f"Slack error: {resp.status_code}")
    except Exception as e:
        log.warning(f"Slack failed: {e}")


@tool
def notify(event_type: str, entity_name: str, entity_id: str = "") -> str:
    """Record event + post to backend. Call after each ingestion.
    event_type: candidate_ingested, position_ingested, etc.
    entity_name: candidate name or position title.
    entity_id: the UUID from ingest result (used to build review link)."""
    if event_type not in _ACTIONS:
        return f"Invalid event_type. Use: {', '.join(_ACTIONS)}"
    if not entity_name or not entity_name.strip():
        return "Error: entity_name required"
    # Require real UUID from ingestion -- prevents LLM from notifying with hallucinated data
    def _is_uuid(s):
        try: UUID(s); return True
        except (ValueError, AttributeError): return False
    if event_type != "attachments_skipped" and not _is_uuid(entity_id):
        return f"Error: entity_id must be a valid UUID from ingest result. Got: '{entity_id}'. Call ingest_candidate/ingest_position first."
    # Dedup within cycle
    key = (entity_id or entity_name.strip().lower(), event_type)
    if any((e.get("entity_id") or e["name"].lower(), e["event_type"]) == key for e in _cycle_events):
        return f"Already notified: {entity_name} ({event_type}) -- skipping duplicate"
    meta = _ACTIONS[event_type]
    summary, recommended_action = meta["summary"], meta["action"]
    # Build review URL for UI
    action_url = None
    if entity_id:
        if event_type.startswith("candidate"):
            action_url = f"{UI_BASE}/#/candidates/{entity_id}"
        elif event_type.startswith("position"):
            action_url = f"{UI_BASE}/#/positions/{entity_id}"
    _cycle_events.append({
        "name": entity_name, "entity_id": entity_id, "event_type": event_type,
        "summary": summary, "action": recommended_action, "url": action_url,
    })
    # DB notification
    payload = {
        "type": event_type,
        "summary": f"{entity_name}: {summary}. {recommended_action}",
    }
    if action_url:
        payload["action_url"] = action_url.replace(UI_BASE + "/", "")
    try:
        resp = authed_request(httpx.post, f"{BACKEND_URL}/api/agent/notifications",
                              json=payload)
        if resp.status_code >= 400:
            log.warning(f"Notify API failed: {resp.status_code}")
            return f"Notified locally: {entity_name} ({summary}) -- backend save failed"
    except Exception as e:
        log.warning(f"Notify API error: {e}")
        return f"Notified locally: {entity_name} ({summary}) -- backend save failed"
    log.info(f"Notified: {entity_name} ({summary})")
    return f"Notified: {entity_name} ({summary})"
