import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone

_PROMPTS_DIR = os.path.join(os.path.dirname(__file__), "prompts")

# Per-1K-token prices for cost estimation
TOKEN_PRICES = {
    "nova": {"input": 0.00006, "output": 0.00024},
    "titan-embed": {"input": 0.0001, "output": 0},
}

LOGS_DIR = os.environ.get("LOGS_DIR", "/data/logs")


async def extract_fields(raw_text: str, entity_type: str, heuristic_fields: dict) -> dict:
    """Extract structured fields from document text via LLM.

    Returns {"fields": dict, "usage": {"model", "input_tokens", "output_tokens"}}.
    """
    prompt = _load_prompt(f"extract_{entity_type}_v1.txt")
    filled = prompt.format(raw_text=raw_text, heuristic_fields=json.dumps(heuristic_fields))

    text, usage = await _call_bedrock(filled)
    fields = _parse_json(text)
    return {"fields": fields, "usage": {**usage, "model": "nova"}, "raw_response": text}


async def generate_summary(raw_text: str, entity_type: str) -> dict:
    """Generate a 2-3 sentence summary via LLM.

    Returns {"summary": str, "usage": {"model", "input_tokens", "output_tokens"}}.
    """
    prompt = _load_prompt(f"summarize_{entity_type}_v1.txt")
    filled = prompt.format(raw_text=raw_text)

    text, usage = await _call_bedrock(filled)
    return {"summary": text.strip(), "usage": {**usage, "model": "nova"}, "raw_response": text}


# =============================================================================
# LLM provider -- AWS Bedrock Nova Lite
# =============================================================================

_bedrock_client = None


def _get_bedrock_client():
    global _bedrock_client
    if _bedrock_client is None:
        import boto3
        _bedrock_client = boto3.client("bedrock-runtime", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    return _bedrock_client


async def _call_bedrock(prompt: str) -> tuple:
    """Call AWS Bedrock Nova Lite via thread executor (boto3 is sync)."""
    import asyncio

    def _invoke():
        client = _get_bedrock_client()
        body = json.dumps({
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "inferenceConfig": {"maxTokens": 4096, "temperature": 0.1},
        })
        response = client.invoke_model(
            modelId="amazon.nova-lite-v1:0",
            contentType="application/json",
            accept="application/json",
            body=body,
        )
        return json.loads(response["body"].read())

    result = await asyncio.get_running_loop().run_in_executor(None, _invoke)
    msg = result.get("output", {}).get("message", {})
    text = msg.get("content", [{}])[0].get("text", "")
    usage_data = result.get("usage", {})
    usage = {
        "input_tokens": usage_data.get("inputTokens", 0),
        "output_tokens": usage_data.get("outputTokens", 0),
    }
    return text, usage


# =============================================================================
# Helpers
# =============================================================================

def _load_prompt(filename: str) -> str:
    path = os.path.join(_PROMPTS_DIR, filename)
    with open(path, "r") as f:
        return f.read()


def _parse_json(text: str) -> dict:
    """Extract JSON from LLM response, handling markdown fences and preamble."""
    text = text.strip()
    # Strip markdown fences
    fence_match = re.search(r"```(?:json)?\s*\n?(.*?)```", text, re.DOTALL)
    if fence_match:
        text = fence_match.group(1).strip()
    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Try to find JSON object in text
    brace_match = re.search(r"\{.*\}", text, re.DOTALL)
    if brace_match:
        try:
            return json.loads(brace_match.group(0))
        except json.JSONDecodeError:
            pass
    raise RuntimeError(f"Failed to parse JSON from LLM response: {text[:200]}")


# =============================================================================
# Extraction logging (JSONL)
# =============================================================================

def log_extraction(entry: dict) -> None:
    """Append a JSON log entry to the daily extraction log file."""
    os.makedirs(LOGS_DIR, exist_ok=True)
    now = datetime.now(timezone.utc)
    entry["timestamp"] = now.isoformat()
    filename = f"extractions_{now.strftime('%Y-%m-%d')}.jsonl"
    path = os.path.join(LOGS_DIR, filename)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, default=str) + "\n")


def compute_stats(token_prices: dict) -> dict:
    """Aggregate extraction stats from all log files."""
    entries = read_all_logs()
    if not entries:
        return {"total_extractions": 0, "success": 0, "partial": 0, "failed": 0,
                "by_model": {}, "avg_duration_ms": 0}

    status_counts = defaultdict(int)
    by_model = defaultdict(lambda: {"count": 0, "total_input_tokens": 0, "total_output_tokens": 0})
    total_duration = 0

    for e in entries:
        status_counts[e.get("status", "unknown")] += 1
        m = by_model[e.get("model", "unknown")]
        m["count"] += 1
        m["total_input_tokens"] += e.get("input_tokens", 0)
        m["total_output_tokens"] += e.get("output_tokens", 0)
        total_duration += e.get("duration_ms", 0)

    for model, stats in by_model.items():
        prices = token_prices.get(model, {"input": 0, "output": 0})
        stats["estimated_cost_usd"] = round(
            stats["total_input_tokens"] / 1000 * prices["input"]
            + stats["total_output_tokens"] / 1000 * prices["output"], 4)

    return {"total_extractions": len(entries),
            "success": status_counts["success"], "partial": status_counts["partial"],
            "failed": status_counts["failed"], "by_model": dict(by_model),
            "avg_duration_ms": round(total_duration / len(entries))}


def read_all_logs() -> list:
    """Read all extraction log entries across all daily files."""
    entries = []
    if not os.path.isdir(LOGS_DIR):
        return entries
    for fname in sorted(os.listdir(LOGS_DIR)):
        if not fname.endswith(".jsonl"):
            continue
        path = os.path.join(LOGS_DIR, fname)
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return entries


