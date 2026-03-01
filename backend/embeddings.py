"""Embedding generation, storage, similarity search, and match explanations.

Uses AWS Titan Embed Text v1 (1536 dimensions) via Bedrock.
"""

import json
import os
import time

import db
from llm import _get_bedrock_client, _call_bedrock, _load_prompt, log_extraction

_EMBED_DIM = 1536


# -- Text builders ------------------------------------------------------------

def build_candidate_text(c):
    """Build embedding text from a candidate dict (as returned by db.get_candidate)."""
    tmpl = _load_prompt("embed_candidate_v1.txt")
    exp_lines = []
    for e in c.get("experience", []):
        bullets = "; ".join(e.get("bullets", []))
        exp_lines.append(f"{e['title']} at {e['company']} ({e.get('startDate', '')} - {e.get('endDate') or 'Present'}): {bullets}")
    certs = ", ".join(f"{ct['name']} ({ct.get('year', '')})" for ct in c.get("certifications", []))
    return tmpl.format(
        name=c.get("name", ""),
        experience_level=c.get("experienceLevel", ""),
        location=(c.get("contact", {}) or {}).get("location", ""),
        skills=", ".join(c.get("skills", [])),
        languages=", ".join(c.get("languages", [])),
        summary=c.get("summary", ""),
        experience="\n".join(exp_lines),
        certifications=certs,
    )


def build_position_text(p):
    """Build embedding text from a position dict (as returned by db.get_position)."""
    tmpl = _load_prompt("embed_position_v1.txt")
    return tmpl.format(
        title=p.get("title", ""),
        company=p.get("company", ""),
        experience_level=p.get("experienceLevel", ""),
        location=p.get("location", ""),
        work_arrangement=p.get("workArrangement", ""),
        tech_stack=", ".join(p.get("techStack", [])),
        requirements="; ".join(p.get("requirements", [])),
        nice_to_have="; ".join(p.get("niceToHave", [])),
        responsibilities="; ".join(p.get("responsibilities", [])),
        summary=p.get("summary", ""),
    )


# -- Embedding generation -----------------------------------------------------

async def generate_embedding(text):
    """Generate a 1536-dim embedding vector via AWS Titan Embed Text v1."""
    import asyncio

    def _invoke():
        client = _get_bedrock_client()
        body = json.dumps({"inputText": text})
        response = client.invoke_model(
            modelId="amazon.titan-embed-text-v1",
            contentType="application/json",
            accept="application/json",
            body=body,
        )
        return json.loads(response["body"].read())

    result = await asyncio.get_running_loop().run_in_executor(None, _invoke)
    return result["embedding"]


# -- Storage ------------------------------------------------------------------

def _vec_to_str(vec):
    return "[" + ",".join(str(v) for v in vec) + "]"


async def _store_embedding(table, entity_id, embedding, text):
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE {table} SET embedding = $1::vector, embedding_text = $2 WHERE id = $3",
            _vec_to_str(embedding), text, db._uuid(entity_id),
        )


# -- End-to-end embed ---------------------------------------------------------

_ENTITY_CONFIG = {
    "candidate": {"fetch": db.get_candidate, "build": build_candidate_text, "table": "candidates"},
    "position":  {"fetch": db.get_position,  "build": build_position_text,  "table": "positions"},
}


async def _embed_entity(entity_type, entity_id):
    """Fetch entity, build text, generate embedding, store, log."""
    cfg = _ENTITY_CONFIG[entity_type]
    entity = await cfg["fetch"](entity_id)
    if not entity:
        raise ValueError(f"{entity_type.title()} {entity_id} not found")
    text = cfg["build"](entity)
    t0 = time.time()
    embedding = await generate_embedding(text)
    duration_ms = round((time.time() - t0) * 1000)
    await _store_embedding(cfg["table"], entity_id, embedding, text)
    log_extraction({
        "type": "embedding", "entity_type": entity_type, "entity_id": str(entity_id),
        "model": "titan-embed", "status": "success",
        "input_tokens": len(text) // 4, "output_tokens": 0,
        "duration_ms": duration_ms,
    })
    return text


async def embed_candidate(cid):
    """Fetch candidate, build text, generate embedding, store."""
    return await _embed_entity("candidate", cid)


async def embed_position(pid):
    """Fetch position, build text, generate embedding, store."""
    return await _embed_entity("position", pid)


# -- Experience level filtering -----------------------------------------------

_LEVEL_ORDER = ["junior", "mid", "senior", "lead", "staff"]


def _compatible_levels(level, max_distance=1):
    """Return experience levels within max_distance steps on the ladder.

    Returns None if level is unknown (caller should skip filtering).
    """
    level = (level or "").lower()
    if level not in _LEVEL_ORDER:
        return None
    idx = _LEVEL_ORDER.index(level)
    lo = max(0, idx - max_distance)
    hi = min(len(_LEVEL_ORDER) - 1, idx + max_distance)
    return [_LEVEL_ORDER[i] for i in range(lo, hi + 1)]


# -- Similarity search --------------------------------------------------------

_SUGGEST_CONFIGS = {
    "candidates_for_position": {
        "fetch_entity": db.get_position,
        "query": """
            SELECT c.id, c.name, c.experience_level, c.location, c.summary,
                   1 - (c.embedding <=> p.embedding) AS score
            FROM candidates c, positions p
            WHERE p.id = $1
              AND c.embedding IS NOT NULL AND p.embedding IS NOT NULL
              AND c.id NOT IN (SELECT candidate_id FROM candidate_positions WHERE position_id = $1)
              AND ($3::text[] IS NULL OR c.experience_level = ANY($3))
            ORDER BY c.embedding <=> p.embedding LIMIT $2""",
        "skills_query": "SELECT candidate_id AS eid, skill AS name FROM candidate_skills WHERE candidate_id = ANY($1)",
        "format": lambda r, skills: {
            "id": str(r["id"]), "name": r["name"],
            "score": round(max(0.0, float(r["score"])), 4),
            "experienceLevel": r["experience_level"],
            "location": r["location"] or "",
            "skills": skills.get(r["id"], []),
            "summary": r["summary"] or "",
        },
    },
    "positions_for_candidate": {
        "fetch_entity": db.get_candidate,
        "query": """
            SELECT p.id, p.title, p.company, p.location, p.experience_level,
                   1 - (p.embedding <=> c.embedding) AS score
            FROM positions p, candidates c
            WHERE c.id = $1
              AND p.embedding IS NOT NULL AND c.embedding IS NOT NULL
              AND p.status = 'open'
              AND p.id NOT IN (SELECT position_id FROM candidate_positions WHERE candidate_id = $1)
              AND ($3::text[] IS NULL OR p.experience_level = ANY($3))
            ORDER BY p.embedding <=> c.embedding LIMIT $2""",
        "skills_query": "SELECT position_id AS eid, skill AS name FROM position_skills WHERE position_id = ANY($1)",
        "format": lambda r, skills: {
            "id": str(r["id"]), "title": r["title"], "company": r["company"],
            "score": round(max(0.0, float(r["score"])), 4),
            "location": r["location"] or "",
            "experienceLevel": r["experience_level"],
            "techStack": skills.get(r["id"], []),
        },
    },
}


async def _suggest(cfg_key, entity_id, limit=3, min_score=0.3):
    cfg = _SUGGEST_CONFIGS[cfg_key]
    pool = await db.get_pool()
    entity = await cfg["fetch_entity"](entity_id)
    levels = _compatible_levels(entity.get("experienceLevel") if entity else None)

    async with pool.acquire() as conn:
        rows = await conn.fetch(cfg["query"], db._uuid(entity_id), limit, levels)

    rows = [r for r in rows if max(0.0, float(r["score"])) >= min_score]
    if not rows:
        return []

    ids = [r["id"] for r in rows]
    async with pool.acquire() as conn:
        skill_rows = await conn.fetch(cfg["skills_query"], ids)

    skill_map = {}
    for s in skill_rows:
        skill_map.setdefault(s["eid"], []).append(s["name"])

    results = [cfg["format"](r, skill_map) for r in rows]

    # Add match explanations
    if cfg_key == "positions_for_candidate":
        for i, r in enumerate(rows):
            position = await db.get_position(str(r["id"]))
            results[i]["explanation"] = await explain_match(entity, position)
    elif cfg_key == "candidates_for_position":
        for i, r in enumerate(rows):
            candidate = await db.get_candidate(str(r["id"]))
            results[i]["explanation"] = await explain_match(candidate, entity)

    return results


async def suggest_candidates_for_position(pid, limit=3, min_score=0.3):
    return await _suggest("candidates_for_position", pid, limit, min_score)


async def suggest_positions_for_candidate(cid, limit=3, min_score=0.3):
    return await _suggest("positions_for_candidate", cid, limit, min_score)


# -- Match explanation --------------------------------------------------------

async def explain_match(candidate, position):
    """Generate a 1-2 sentence explanation of why candidate matches position."""
    tmpl = _load_prompt("explain_match_v1.txt")
    prompt = tmpl.format(
        candidate_name=candidate.get("name", ""),
        candidate_level=candidate.get("experienceLevel", ""),
        candidate_skills=", ".join(candidate.get("skills", [])),
        candidate_summary=candidate.get("summary", ""),
        position_title=position.get("title", ""),
        position_level=position.get("experienceLevel", ""),
        position_tech=", ".join(position.get("techStack", [])),
        position_requirements="; ".join(position.get("requirements", [])),
    )
    t0 = time.time()
    text, usage = await _call_bedrock(prompt)
    duration_ms = round((time.time() - t0) * 1000)
    log_extraction({
        "type": "explanation", "model": "nova", "status": "success",
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
        "duration_ms": duration_ms,
    })
    return text.strip()
