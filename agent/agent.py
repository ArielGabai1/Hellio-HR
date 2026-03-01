"""Hellio HR Agent -- polls Gmail, processes emails via LLM."""

import logging
import os
import signal
import sys
import time

import httpx
from mcp import stdio_client, StdioServerParameters
from strands import Agent
from strands.models import BedrockModel
from strands.tools.mcp import MCPClient

from strands.types.exceptions import MaxTokensReachedException
from tools.gmail import (
    set_mcp_client, search_emails, read_email, mark_as_read,
    download_attachment, draft_email,
)
from tools.ingestion import ingest_candidate, ingest_position
from tools.search import find_matching_candidates, find_matching_positions
from tools.notifications import notify, clear_cycle_events, flush_cycle_report
from state import is_email_processed, mark_email_processed

_RULES_PATH = os.path.join(os.path.dirname(__file__), "hr_rules.txt")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logging.getLogger("strands").setLevel(logging.WARNING)
log = logging.getLogger("hellio-agent")

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "300"))
AGENT_TIMEOUT = int(os.environ.get("MAX_AGENT_ITERATIONS", "10")) * 30  # ~30s per iteration
GMAIL_TARGET = os.environ.get("GMAIL_TARGET_ADDRESS", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
GMAIL_MCP_PATH = "/opt/gmail-mcp/dist/index.js"
_shutdown = False

TOOLS = [
    download_attachment, draft_email,
    ingest_candidate, ingest_position,
    find_matching_candidates, find_matching_positions,
    notify,
]


def _build_system_prompt(rules: str) -> str:
    return f"""You are Hellio HR Agent.

CRITICAL: Process attachments ONE AT A TIME. Finish ALL 5 steps for one attachment before starting the next.

RULES:
- NEVER send emails. Only create drafts via draft_email.
- IDs and names come from tool results. Read the result of ingest_candidate/ingest_position to get the real ID and name. NEVER use placeholder text.
- NEVER call find_matching or notify before ingestion -- you need the real ID/name from the ingest result.

FIRST: Count the attachments in the email.
- If 3 or fewer: process ALL of them, then STOP. Do nothing else.
- If more than 3: process the FIRST 3 only, then draft_email asking sender to resend the rest, then notify(event_type="attachments_skipped", entity_name="X files skipped") where X = total - 3. Then STOP.

FOR EACH CANDIDATE ATTACHMENT (in this exact order):
1. download_attachment(message_id, attachment_id)
2. ingest_candidate(file_path) -- read the result, e.g. "Created: Jane Smith (ID: abc-123)"
3. find_matching_positions(candidate_id="abc-123") -- use the real ID from step 2
4. notify -- if step 2 said "Created" use event_type="candidate_ingested", if "Updated" use event_type="candidate_updated". Use the real name AND entity_id from step 2.
5. draft_email -- personalized reply that mentions the candidate's specific skills and matching positions from steps 2-3. Each draft must be unique to that candidate.
Then move to the next attachment.

FOR EACH POSITION ATTACHMENT (in this exact order):
1. download_attachment(message_id, attachment_id)
2. ingest_position(file_path) -- read the result, e.g. "Created: DevOps Engineer (ID: xyz-789)"
3. find_matching_candidates(position_id="xyz-789") -- use the real ID from step 2
4. notify -- if step 2 said "Created" use event_type="position_ingested", if "Updated" use event_type="position_updated". Use the real name AND entity_id from step 2.
5. draft_email -- confirmation to hiring manager with matches
Then move to the next attachment.

After processing all attachments: STOP. Do not draft extra emails or call notify again.

CANDIDATE EMAIL (no CV): draft_email requesting CV, notify(event_type="candidate_cv_missing", entity_name="Unknown").
POSITION EMAIL (no file): ingest_position(email_body=<text>), find_matching_candidates, notify, draft confirmation.

HR WORKFLOW RULES:
{rules}"""


def _handle_signal(sig, frame):
    global _shutdown
    log.info("Shutdown signal received...")
    _shutdown = True


def _wait_for_backend(retries=30, delay=2):
    url = f"{os.environ.get('BACKEND_URL', 'http://hellio-hr:8000')}/api/health"
    for i in range(retries):
        try:
            if httpx.get(url, timeout=5).status_code == 200:
                log.info("Backend healthy")
                return
        except Exception:
            pass
        log.info(f"Waiting for backend... ({i+1}/{retries})")
        time.sleep(delay)
    log.error(f"Backend unavailable after {retries * delay}s")
    sys.exit(1)


class _Timeout(Exception):
    pass

def _timeout_handler(signum, frame):
    raise _Timeout()

def _process_email(agent, mid, email_type, raw):
    agent.messages.clear()
    prev = signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(AGENT_TIMEOUT)
    try:
        agent(f"Process this {email_type} email.\nMessage ID: {mid}\n\n{raw}")
    except _Timeout:
        log.warning(f"Timeout ({AGENT_TIMEOUT}s) on {mid} -- partial processing, marking done")
    except MaxTokensReachedException:
        log.warning(f"Max tokens on {mid} -- partial processing, marking done")
    except Exception:
        log.error(f"Agent failed on {mid}", exc_info=True)
        log.warning(f"Email {mid} not marked processed -- will retry next cycle")
        signal.alarm(0)
        signal.signal(signal.SIGALRM, prev)
        return
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, prev)
    mark_as_read(mid)
    mark_email_processed(mid, email_type, "processed")
    log.info(f"Done: {mid}")


def run():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    if not GMAIL_TARGET:
        log.error("GMAIL_TARGET_ADDRESS not set")
        sys.exit(1)

    _wait_for_backend()
    with open(_RULES_PATH) as f:
        rules = f.read()
    system_prompt = _build_system_prompt(rules)

    creds = os.environ.get("GMAIL_CREDENTIALS_DIR", "/home/agent/.gmail-mcp")
    gmail_mcp = MCPClient(lambda: stdio_client(StdioServerParameters(
        command="node", args=[GMAIL_MCP_PATH],
        env={
            "HOME": os.environ.get("HOME", "/home/agent"),
            "PATH": os.environ.get("PATH", ""),
            "GMAIL_OAUTH_PATH": os.path.join(creds, "gcp-oauth.keys.json"),
            "GMAIL_CREDENTIALS_PATH": os.path.join(creds, "credentials.json"),
        },
    )))

    model = BedrockModel(
        model_id=os.environ.get("AGENT_MODEL_ID", "amazon.nova-lite-v1:0"),
        region_name=AWS_REGION,
        max_tokens=10000,
    )

    log.info(f"Starting agent (poll={POLL_INTERVAL}s)")
    log.info(f"Monitoring: {GMAIL_TARGET}+candidates/positions@gmail.com")

    with gmail_mcp:
        set_mcp_client(gmail_mcp)
        agent = Agent(model=model, system_prompt=system_prompt, tools=TOOLS)
        log.info(f"Agent ready ({len(TOOLS)} tools)")

        cycle = 0
        while not _shutdown:
            cycle += 1
            log.info(f"--- Cycle {cycle} ---")
            try:
                clear_cycle_events()

                cand_ids = search_emails(
                    f"to:{GMAIL_TARGET}+candidates@gmail.com is:unread")
                pos_ids = search_emails(
                    f"to:{GMAIL_TARGET}+positions@gmail.com is:unread")

                if not cand_ids and not pos_ids:
                    log.info("No new emails")
                else:
                    for email_type, ids in [("candidate", cand_ids), ("position", pos_ids)]:
                        for mid in ids:
                            if is_email_processed(mid):
                                continue
                            log.info(f"{email_type.title()} email: {mid}")
                            try:
                                raw = read_email(mid)
                                _process_email(agent, mid, email_type, raw)
                            except Exception:
                                log.error(f"Failed on {mid}", exc_info=True)

            except Exception:
                log.error("Cycle error", exc_info=True)
            finally:
                flush_cycle_report()

            if _shutdown:
                break
            log.info(f"Sleeping {POLL_INTERVAL}s...")
            for _ in range(POLL_INTERVAL):
                if _shutdown:
                    break
                time.sleep(1)

    log.info("Shutdown complete.")


if __name__ == "__main__":
    run()
