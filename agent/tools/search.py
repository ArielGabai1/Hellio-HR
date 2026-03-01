"""Semantic search tools -- find matching candidates/positions."""

import httpx
from strands import tool
from auth import BACKEND_URL, authed_request


def _search(url: str, name_key: str) -> str:
    resp = authed_request(httpx.get, url, timeout=30)
    if resp.status_code == 404:
        return "Not found."
    resp.raise_for_status()
    matches = resp.json()
    if not matches:
        return "No matches found."
    lines = [f"Found {len(matches)} match(es):"]
    for m in matches[:5]:
        lines.append(f"- {m.get(name_key, '?')} (ID: {m.get('id', '?')}, score: {m.get('score', 0):.2f})")
    return "\n".join(lines)


@tool
def find_matching_candidates(position_id: str) -> str:
    """Find candidates matching a position. Args: position_id = UUID."""
    return _search(f"{BACKEND_URL}/api/positions/{position_id}/suggestions", "name")


@tool
def find_matching_positions(candidate_id: str) -> str:
    """Find positions matching a candidate. Args: candidate_id = UUID."""
    return _search(f"{BACKEND_URL}/api/candidates/{candidate_id}/suggestions", "title")
