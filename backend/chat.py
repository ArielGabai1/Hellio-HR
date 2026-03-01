import re
import db
from llm import _call_bedrock, _load_prompt, TOKEN_PRICES

# Tables the LLM is allowed to query (excludes users -- contains password hashes)
ALLOWED_TABLES = {
    "candidates", "positions", "candidate_skills", "position_skills",
    "candidate_languages", "experience",
    "education", "certifications", "candidate_positions",
    "position_requirements", "documents",
}

FORBIDDEN_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|EXECUTE|EXEC|COPY|SET|VACUUM|ANALYZE)\b",
    re.IGNORECASE,
)

DANGEROUS_FUNCTIONS = re.compile(
    r"\b(pg_sleep|pg_read_file|pg_write_file|lo_import|lo_export|dblink|"
    r"current_setting|set_config|pg_ls_dir|pg_stat_file|pg_terminate_backend|pg_cancel_backend)\b",
    re.IGNORECASE,
)

# Extract table names from FROM/JOIN clauses (handles quoted identifiers)
TABLE_REF_PATTERN = re.compile(
    r'(?:FROM|JOIN)\s+"?([a-zA-Z_][a-zA-Z0-9_.]*)"?', re.IGNORECASE
)

MAX_QUERY_LENGTH = 2000
MAX_HISTORY = 20

META_PATTERNS = ["summarize", "recap", "what did we discuss", "what have we talked"]


# --- SQL helpers --------------------------------------------------------------

def _strip_sql_fences(text: str) -> str:
    text = re.sub(r"```(?:sql)?\s*\n?", "", text)
    return text.replace("```", "").strip()


def _extract_sql(raw: str) -> str:
    text = _strip_sql_fences(raw.strip())

    if text.upper().startswith("SELECT"):
        return text

    match = re.search(r"(SELECT\b.+)", text, re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1).strip()

    return text  # let validate_sql reject it


def _fix_apostrophe_escaping(sql: str) -> str:
    # \' -> '' for SQL-standard escaping (Be'er Sheva, Ra'anana, etc.)
    return sql.replace("\\'", "''")


def _early_response(answer, sql, trace, usage1, total_usage):
    return {
        "answer": answer, "sql": sql, "trace": trace,
        "usage": {"model": "nova", **total_usage,
                  "step1_tokens": usage1, "step2_tokens": {"input": 0, "output": 0}},
    }


# --- SQL validation -----------------------------------------------------------

def validate_sql(sql: str) -> str:
    sql = _strip_sql_fences(sql)
    sql = _fix_apostrophe_escaping(sql)

    if not sql:
        raise ValueError("Empty query")

    # Strip trailing semicolons
    sql = sql.rstrip(";").strip()

    if not sql:
        raise ValueError("Empty query after stripping")

    # Length check
    if len(sql) > MAX_QUERY_LENGTH:
        raise ValueError("Query too long")

    # Must start with SELECT
    if not sql.upper().lstrip().startswith("SELECT"):
        raise ValueError("Query must start with SELECT")

    # Forbid comments
    if "--" in sql or "/*" in sql:
        raise ValueError("SQL comments are forbidden")

    # Forbid semicolons mid-query (multi-statement)
    if ";" in sql:
        raise ValueError("Multiple statements (semicolon) are forbidden")

    # Forbid destructive keywords
    if FORBIDDEN_KEYWORDS.search(sql):
        raise ValueError("Destructive SQL keyword is forbidden")

    # Forbid dangerous functions
    if DANGEROUS_FUNCTIONS.search(sql):
        raise ValueError("Dangerous function is forbidden")

    # Forbid INTO clause
    if re.search(r"\bINTO\b", sql, re.IGNORECASE):
        raise ValueError("INTO clause is forbidden")

    # Table allowlist
    refs = TABLE_REF_PATTERN.findall(sql)
    for ref in refs:
        # Handle schema-qualified names like information_schema.tables
        table = ref.split(".")[-1].lower()
        schema = ref.split(".")[0].lower() if "." in ref else None

        if schema in ("information_schema", "pg_catalog") or table.startswith("pg_"):
            raise ValueError(f"System table '{ref}' is forbidden")
        if table == "users":
            raise ValueError("The 'users' table is forbidden (contains credentials)")
        if table not in ALLOWED_TABLES:
            raise ValueError(f"Unknown table '{ref}' is forbidden")

    # Auto-append LIMIT if missing
    if not re.search(r"\bLIMIT\b", sql, re.IGNORECASE):
        sql += " LIMIT 50"

    return sql


# --- Answer hallucination validation ------------------------------------------

SKIP_NAMES = {
    # Israeli cities
    "tel aviv", "be'er sheva", "beer sheva", "ra'anana", "raanana",
    "rishon lezion", "petah tikva", "bnei brak", "kfar saba",
    "ramat gan", "herzliya", "holon", "ashdod", "haifa", "jerusalem",
    "netanya", "rehovot",
    # Common phrases that look like proper names
    "query results", "no results", "based on", "senior level",
    "junior level", "mid level", "go lang", "new york", "san francisco",
    "los angeles", "united states", "united kingdom",
    "based on query results", "no results found",
    # Position title fragments
    "platform engineer", "cloud architect", "growing startup",
    "pipeline specialist", "security focused",
}

COUNT_COLUMNS = {"count", "total", "cnt", "num", "candidate_count", "position_count", "skill_count"}


def validate_answer(answer: str, rows: list, columns: list, question: str = "") -> dict:
    if not answer or (not rows and "no result" in answer.lower()):
        return {"hallucination_warning": False, "answer": answer}

    warning = False
    reasons = []
    question_lower = question.lower()

    # Collect all string values from results for entity checking
    result_values = set()
    for row in rows:
        for v in (row.values() if isinstance(row, dict) else row):
            if isinstance(v, str) and len(v) > 2:
                result_values.add(v.lower())

    # Collect aggregate count values from results
    aggregate_values = set()
    for row in rows:
        if isinstance(row, dict):
            for k, v in row.items():
                if k.lower() in COUNT_COLUMNS and isinstance(v, (int, float)):
                    aggregate_values.add(int(v))

    # Check for name-like capitalized words in the answer that aren't in results
    names_in_answer = re.findall(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b", answer)
    for name in names_in_answer:
        # Skip known non-name phrases
        if name.lower() in SKIP_NAMES:
            continue
        # Skip very short phrases (likely not real names)
        if len(name.replace(" ", "")) < 5:
            continue
        # Skip names that appear in the original question (user mentioned them)
        if name.lower() in question_lower:
            continue
        if name.lower() not in result_values:
            parts = name.lower().split()
            found = any(
                any(part in val for part in parts)
                for val in result_values
            )
            # Also check against the question text
            if not found:
                found = any(part in question_lower for part in parts)
            if not found:
                warning = True
                reasons.append(f"Name '{name}' not found in results")

    # Check count claims vs actual row count AND aggregate values
    count_claims = re.findall(r"\b(\d+)\s+(?:candidate|position|result|record|row)", answer, re.IGNORECASE)
    for claim in count_claims:
        claimed = int(claim)
        actual = len(rows)
        if claimed == actual or claimed == 0:
            continue
        # Check against aggregate column values (e.g. COUNT(*) returns 1 row with value 41)
        if claimed in aggregate_values:
            continue
        # Check if the number appears as a value in any result
        number_in_results = any(
            str(claimed) in str(v) for row in rows
            for v in (row.values() if isinstance(row, dict) else row)
        )
        if not number_in_results:
            warning = True
            reasons.append(f"Claimed {claimed} but got {actual} rows")

    result = {"hallucination_warning": warning, "answer": answer}
    if warning:
        result["answer"] += "\n\n[Note: This answer may contain inaccuracies. Please verify against the retrieved data.]"
        result["hallucination_reasons"] = reasons
    return result


# --- Query execution ----------------------------------------------------------

async def execute_readonly(sql: str) -> dict:
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            stmt = await conn.prepare(sql)
            attrs = stmt.get_attributes()
            columns = [a.name for a in attrs]
            records = await stmt.fetch()
            rows = [dict(r) for r in records]
            # Convert non-serializable types
            for row in rows:
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat()
                    elif isinstance(v, (bytes, memoryview)):
                        row[k] = str(v)
                    elif not isinstance(v, (str, int, float, bool, type(None), list)):
                        row[k] = str(v)
    return {"columns": columns, "rows": rows, "rowCount": len(rows)}


# --- Pipeline helpers (used by tests) -----------------------------------------

def _format_history(history: list) -> str:
    if not history:
        return ""
    recent = history[-MAX_HISTORY:]
    return "\n".join(f"{m.get('role', 'user')}: {m.get('content', '')}" for m in recent)


def _format_results(rows: list, columns: list) -> str:
    if not rows:
        return "(no results)"
    lines = [" | ".join(columns)]
    lines.extend(" | ".join(str(row.get(c, "")) for c in columns) for row in rows[:50])
    return "\n".join(lines)


# --- Main pipeline ------------------------------------------------------------

async def ask(question: str, history: list = None) -> dict:
    """Two-step SQL-RAG pipeline: question -> SQL -> DB -> grounded answer."""
    history = history or []
    zero_usage = {"input": 0, "output": 0}

    if any(p in question.lower() for p in META_PATTERNS):
        return _early_response(
            "I can answer questions about candidates and positions in the database. Please ask a specific question.",
            None, None, zero_usage, {"input_tokens": 0, "output_tokens": 0})

    schema = _load_prompt("schema_summary_v1.txt")
    total_usage = {"input_tokens": 0, "output_tokens": 0}

    # --- Step 1: Generate SQL ---
    recent = history[-MAX_HISTORY:] if history else []
    history_text = "\n".join(f"{m.get('role', 'user')}: {m.get('content', '')}" for m in recent)
    history_section = f"\nConversation history:\n{history_text}\n" if history_text else ""

    sql_prompt = _load_prompt("sql_generation_v1.txt").format(
        schema_summary=schema, history_section=history_section, question=question)

    sql_raw, usage1 = await _call_bedrock(sql_prompt)
    total_usage["input_tokens"] += usage1["input_tokens"]
    total_usage["output_tokens"] += usage1["output_tokens"]

    sql_raw = sql_raw.strip()

    if sql_raw.upper() == "IRRELEVANT":
        return _early_response(
            "I can only answer questions about candidates and positions in the HR database. Could you rephrase your question?",
            None, None, usage1, total_usage)

    sql_raw = _extract_sql(sql_raw)

    try:
        sql = validate_sql(sql_raw)
    except ValueError as e:
        return _early_response(
            f"I wasn't able to generate a valid query for that question. ({e}) Please try rephrasing.",
            sql_raw, None, usage1, total_usage)

    try:
        trace = await execute_readonly(sql)
    except Exception:
        return _early_response(
            "The query failed to execute. Please try a different question.",
            sql, None, usage1, total_usage)

    # --- Step 2: Generate grounded answer ---
    rows, columns = trace["rows"], trace["columns"]
    if not rows:
        results_text = "(no results)"
    else:
        lines = [" | ".join(columns)]
        lines.extend(" | ".join(str(row.get(c, "")) for c in columns) for row in rows[:50])
        results_text = "\n".join(lines)

    answer_prompt = _load_prompt("answer_generation_v1.txt").format(
        question=question, sql=sql, row_count=trace["rowCount"], results=results_text)

    answer_raw, usage2 = await _call_bedrock(answer_prompt)
    total_usage["input_tokens"] += usage2["input_tokens"]
    total_usage["output_tokens"] += usage2["output_tokens"]

    validation = validate_answer(answer_raw.strip(), rows, columns, question)

    return {
        "answer": validation["answer"],
        "sql": sql,
        "trace": {"columns": columns, "rows": rows[:20], "rowCount": trace["rowCount"]},
        "hallucination_warning": validation["hallucination_warning"],
        "usage": {"model": "nova", **total_usage, "step1_tokens": usage1, "step2_tokens": usage2},
    }
