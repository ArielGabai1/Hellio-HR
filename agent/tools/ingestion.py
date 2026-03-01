"""Ingestion tools -- upload CV/job files to backend API."""

import os
import tempfile

import httpx
from strands import tool
from auth import BACKEND_URL, authed_request


def _ingest(endpoint: str, filename: str, file_obj) -> str:
    resp = authed_request(httpx.post, f"{BACKEND_URL}{endpoint}",
                          files={"file": (filename, file_obj)}, timeout=120)
    if resp.status_code in (200, 201):
        d = resp.json()
        action = "Updated" if d.get("isUpdate", False) else "Created"
        name = d.get("name", d.get("title", "?"))
        return f"{action}: {name} (ID: {d.get('id', '?')})"
    return f"Failed ({resp.status_code}): {resp.text[:200]}"


@tool
def ingest_candidate(file_path: str) -> str:
    """Ingest a candidate CV (PDF/DOCX). Args: file_path = path on disk."""
    if not os.path.isfile(file_path):
        return f"Error: File not found at {file_path}"
    with open(file_path, "rb") as f:
        return _ingest("/api/ingest/cv", os.path.basename(file_path), f)


@tool
def ingest_position(file_path: str = "", email_body: str = "") -> str:
    """Ingest a position from file or email text. Args: file_path OR email_body."""
    if file_path:
        if not os.path.isfile(file_path):
            return f"Error: File not found at {file_path}"
        with open(file_path, "rb") as f:
            return _ingest("/api/ingest/job", os.path.basename(file_path), f)
    if email_body:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(email_body)
            tmp = f.name
        try:
            with open(tmp, "rb") as f:
                return _ingest("/api/ingest/job", "position.txt", f)
        finally:
            os.unlink(tmp)
    return "Error: Provide file_path or email_body"
