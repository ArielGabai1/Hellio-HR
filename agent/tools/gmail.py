"""Gmail MCP wrappers -- search/read/mark are plain, download/draft are @tool."""

import logging
import os
import re
import uuid

from strands import tool

log = logging.getLogger("hellio-agent")
_mcp_client = None


def set_mcp_client(client):
    global _mcp_client
    _mcp_client = client


def _call_mcp(name: str, args: dict) -> str:
    tid = f"w_{uuid.uuid4().hex[:8]}"
    res = _mcp_client.call_tool_sync(tid, name, args)
    text = "\n".join(i["text"] for i in res["content"] if "text" in i)
    log.info(f"MCP {name} -> {len(text)} chars")
    return text


def _call_mcp_safe(name: str, args: dict) -> str:
    """For @tool functions -- returns error string instead of raising."""
    try:
        return _call_mcp(name, args)
    except Exception as e:
        log.error(f"MCP {name} failed: {e}")
        return f"Error: {e}"


def _detect_ext(path: str) -> str:
    try:
        with open(path, "rb") as f:
            h = f.read(4)
        if h == b"%PDF":
            return ".pdf"
        if h[:2] == b"PK":
            return ".docx"
    except Exception:
        pass
    return ".txt"


# Plain functions -- Python calls directly (raise on failure)

def search_emails(query: str, max_results: int = 10) -> list[str]:
    raw = _call_mcp("search_emails", {"query": query, "maxResults": max_results})
    if not raw.strip():
        return []
    # Gmail MCP returns "Id: <hex>" per message; anchor to line start to avoid matching other hex fields
    return re.findall(r"^.*[Ii][Dd][:\s]+([0-9a-f]+)", raw, re.MULTILINE)


def read_email(message_id: str) -> str:
    return _call_mcp("read_email", {"messageId": message_id})


def mark_as_read(message_id: str) -> str:
    return _call_mcp("modify_email", {
        "messageId": message_id, "removeLabelIds": ["UNREAD"],
    })


# @tool -- LLM calls these

@tool
def download_attachment(message_id: str, attachment_id: str) -> str:
    """Download one attachment to /tmp/downloads/. Args: message_id, attachment_id.
    Returns: 'Downloaded to /tmp/downloads/<filename>' on success."""
    dl_dir = "/tmp/downloads"
    os.makedirs(dl_dir, exist_ok=True)
    safe = f"attach_{uuid.uuid4().hex[:8]}"
    path = os.path.join(dl_dir, safe)
    _call_mcp_safe("download_attachment", {
        "messageId": message_id, "attachmentId": attachment_id,
        "savePath": dl_dir + "/", "filename": safe,
    })
    if not os.path.isfile(path):
        return "Error: download failed"
    ext = _detect_ext(path)
    new_path = path + ext
    os.rename(path, new_path)
    log.info(f"Downloaded {new_path} ({os.path.getsize(new_path)} bytes)")
    return f"Downloaded to {new_path}"


@tool
def draft_email(to: str, subject: str, body: str,
                thread_id: str = "", in_reply_to: str = "") -> str:
    """Create a draft reply (never sends). Args: to, subject, body, thread_id, in_reply_to."""
    args = {"to": [to], "subject": subject, "body": body}
    if thread_id:
        args["threadId"] = thread_id
    if in_reply_to:
        args["inReplyTo"] = in_reply_to
    return _call_mcp_safe("draft_email", args)
