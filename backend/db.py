import os
import re
from uuid import UUID

import asyncpg
import bcrypt

_pool = None

# Matches numbers like "16,000" or "16000"
_SALARY_NUM_RE = re.compile(r"(\d{1,3}(?:,\d{3})+|\d{4,6})")


def parse_salary(compensation: str) -> tuple:
    """Extract (min, max) integers from a compensation string. Returns (None, None) if not parseable."""
    if not compensation:
        return None, None
    nums = [int(m.replace(",", "")) for m in _SALARY_NUM_RE.findall(compensation)]
    # Filter to plausible monthly NIS salaries (4-digit to 6-digit)
    nums = [n for n in nums if 1000 <= n <= 999999]
    if not nums:
        return None, None
    return min(nums), max(nums)


async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=2, max_size=10)
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def _uuid(val):
    return UUID(val) if isinstance(val, str) else val


# --- Detail-row helpers (shared by insert + update) --------------------------

async def _save_candidate_details(conn, cid, data):
    """Insert skills, languages, experience, education, certifications for a candidate."""
    for skill in data.get("skills", []):
        await conn.execute(
            "INSERT INTO candidate_skills (candidate_id, skill) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            cid, skill)
    for lang in data.get("languages", []):
        await conn.execute(
            "INSERT INTO candidate_languages (candidate_id, language) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            cid, lang)
    for i, exp in enumerate(data.get("experience", [])):
        description = "\n".join(exp.get("bullets", []))
        await conn.execute("""
            INSERT INTO experience (candidate_id, title, company, location, start_date, end_date, description, sort_order)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        """, cid, exp["title"], exp["company"], exp.get("location"),
            exp.get("startDate"), exp.get("endDate"), description, i)
    for edu in data.get("education", []):
        await conn.execute("""
            INSERT INTO education (candidate_id, degree, institution, start_date, end_date)
            VALUES ($1,$2,$3,$4,$5)
        """, cid, edu["degree"], edu["institution"], edu.get("startDate"), edu.get("endDate"))
    for cert in data.get("certifications", []):
        await conn.execute(
            "INSERT INTO certifications (candidate_id, name, year) VALUES ($1,$2,$3)",
            cid, cert["name"], cert.get("year"))


async def _save_position_details(conn, pid, data):
    """Insert skills, requirements, niceToHave, responsibilities for a position."""
    for skill in data.get("techStack", []):
        await conn.execute(
            "INSERT INTO position_skills (position_id, skill) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            pid, skill)
    for i, req in enumerate(data.get("requirements", [])):
        await conn.execute(
            "INSERT INTO position_requirements (position_id, item, type, sort_order) VALUES ($1,$2,$3,$4)",
            pid, req, "required", i)
    for i, req in enumerate(data.get("niceToHave", [])):
        await conn.execute(
            "INSERT INTO position_requirements (position_id, item, type, sort_order) VALUES ($1,$2,$3,$4)",
            pid, req, "nice_to_have", i)
    for i, resp in enumerate(data.get("responsibilities", [])):
        await conn.execute(
            "INSERT INTO position_requirements (position_id, item, type, sort_order) VALUES ($1,$2,$3,$4)",
            pid, resp, "responsibility", i)


# --- Shared fetchers ---------------------------------------------------------

async def _fetch_candidates(conn, cids):
    """Batch-fetch all related data for candidate IDs, return assembled dicts."""
    if not cids:
        return []
    rows = await conn.fetch("SELECT * FROM candidates WHERE id = ANY($1) ORDER BY name", cids)
    if not rows:
        return []

    skills = await conn.fetch(
        "SELECT candidate_id, skill FROM candidate_skills WHERE candidate_id = ANY($1)", cids
    )
    langs = await conn.fetch(
        "SELECT candidate_id, language FROM candidate_languages WHERE candidate_id = ANY($1)", cids
    )
    exps = await conn.fetch(
        "SELECT * FROM experience WHERE candidate_id = ANY($1) ORDER BY sort_order", cids
    )
    edus = await conn.fetch(
        "SELECT * FROM education WHERE candidate_id = ANY($1)", cids
    )
    certs = await conn.fetch(
        "SELECT * FROM certifications WHERE candidate_id = ANY($1)", cids
    )
    junctions = await conn.fetch(
        "SELECT candidate_id, position_id FROM candidate_positions WHERE candidate_id = ANY($1)", cids
    )

    skill_map = _group_by(skills, "candidate_id")
    lang_map = _group_by(langs, "candidate_id")
    exp_map = _group_by(exps, "candidate_id")
    edu_map = _group_by(edus, "candidate_id")
    cert_map = _group_by(certs, "candidate_id")
    junction_map = _group_by(junctions, "candidate_id")

    return [
        _assemble_candidate(r, skill_map, lang_map, exp_map, edu_map, cert_map, junction_map)
        for r in rows
    ]


async def _fetch_positions(conn, pids):
    """Batch-fetch all related data for position IDs, return assembled dicts."""
    if not pids:
        return []
    rows = await conn.fetch("SELECT * FROM positions WHERE id = ANY($1) ORDER BY title", pids)
    if not rows:
        return []

    skills = await conn.fetch(
        "SELECT position_id, skill FROM position_skills WHERE position_id = ANY($1)", pids
    )
    reqs = await conn.fetch(
        "SELECT * FROM position_requirements WHERE position_id = ANY($1) ORDER BY sort_order", pids
    )
    junctions = await conn.fetch(
        "SELECT candidate_id, position_id FROM candidate_positions WHERE position_id = ANY($1)", pids
    )

    skill_map = _group_by(skills, "position_id")
    req_map = _group_by(reqs, "position_id")
    junction_map = _group_by(junctions, "position_id")

    return [_assemble_position(r, skill_map, req_map, junction_map) for r in rows]


# --- Candidates ---------------------------------------------------------------

async def get_all_candidates():
    pool = await get_pool()
    async with pool.acquire() as conn:
        cids = [r["id"] for r in await conn.fetch("SELECT id FROM candidates ORDER BY name")]
        return await _fetch_candidates(conn, cids)


async def get_candidate(cid):
    pool = await get_pool()
    async with pool.acquire() as conn:
        results = await _fetch_candidates(conn, [_uuid(cid)])
        return results[0] if results else None


async def insert_candidate(data):
    """Insert a candidate and all related data in a transaction. Returns candidate dict."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            contact = data.get("contact", {})
            cid_param = _uuid(data["id"]) if data.get("id") else None
            row = await conn.fetchrow("""
                INSERT INTO candidates (id, name, status, experience_level, phone, email,
                    location, linkedin, github, summary, cv_file)
                VALUES (COALESCE($1::uuid, gen_random_uuid()),$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id
            """, cid_param, data["name"], data["status"], data["experienceLevel"],
                contact.get("phone"), contact.get("email"), contact.get("location"),
                contact.get("linkedin"), contact.get("github"),
                data.get("summary", ""), data.get("cvFile"))
            cid = row["id"]
            await _save_candidate_details(conn, cid, data)

    return await get_candidate(str(cid))


async def _find_candidate_by(column, value):
    """Case-insensitive lookup by column. Column is always hardcoded at call site."""
    if not value:
        return None
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT id FROM candidates WHERE LOWER({column}) = LOWER($1) ORDER BY created_at DESC LIMIT 1",
            value)
    if row is None:
        return None
    return await get_candidate(str(row["id"]))


async def find_candidate_by_email(email):
    return await _find_candidate_by("email", email)


async def find_candidate_by_linkedin(linkedin):
    return await _find_candidate_by("linkedin", linkedin)


async def update_candidate(cid, data):
    """Update candidate core row and replace all detail rows. Preserves junction table."""
    pool = await get_pool()
    cid_uuid = _uuid(cid)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id FROM candidates WHERE id = $1", cid_uuid)
        if row is None:
            return None

        async with conn.transaction():
            contact = data.get("contact", {})
            await conn.execute("""
                UPDATE candidates SET
                    name = $2, status = $3, experience_level = $4,
                    phone = $5, email = $6, location = $7,
                    linkedin = $8, github = $9,
                    summary = $10, cv_file = $11, updated_at = NOW()
                WHERE id = $1
            """, cid_uuid, data["name"], data["status"], data["experienceLevel"],
                contact.get("phone"), contact.get("email"), contact.get("location"),
                contact.get("linkedin"), contact.get("github"),
                data.get("summary", ""), data.get("cvFile"))

            # Delete + re-insert detail rows
            await conn.execute("DELETE FROM candidate_skills WHERE candidate_id = $1", cid_uuid)
            await conn.execute("DELETE FROM candidate_languages WHERE candidate_id = $1", cid_uuid)
            await conn.execute("DELETE FROM experience WHERE candidate_id = $1", cid_uuid)
            await conn.execute("DELETE FROM education WHERE candidate_id = $1", cid_uuid)
            await conn.execute("DELETE FROM certifications WHERE candidate_id = $1", cid_uuid)
            await _save_candidate_details(conn, cid_uuid, data)

    return await get_candidate(str(cid_uuid))


# --- Positions ----------------------------------------------------------------

async def get_all_positions():
    pool = await get_pool()
    async with pool.acquire() as conn:
        pids = [r["id"] for r in await conn.fetch("SELECT id FROM positions ORDER BY title")]
        return await _fetch_positions(conn, pids)


async def get_position(pid):
    pool = await get_pool()
    async with pool.acquire() as conn:
        results = await _fetch_positions(conn, [_uuid(pid)])
        return results[0] if results else None


async def find_position_by_title_company(title, company):
    """Case-insensitive title+company lookup. Returns position dict or None."""
    if not title or not company:
        return None
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id FROM positions WHERE LOWER(title) = LOWER($1) AND LOWER(company) = LOWER($2) ORDER BY created_at DESC LIMIT 1",
            title, company)
    if row is None:
        return None
    return await get_position(str(row["id"]))


async def insert_position(data):
    """Insert a position and all related data in a transaction. Returns position dict."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            hm = data.get("hiringManager") or {}
            pid_param = _uuid(data["id"]) if data.get("id") else None
            row = await conn.fetchrow("""
                INSERT INTO positions (id, title, status, company, hiring_manager_name,
                    hiring_manager_title, hiring_manager_email, experience_level, location,
                    work_arrangement, compensation, salary_min, salary_max, timeline, summary, job_file)
                VALUES (COALESCE($1::uuid, gen_random_uuid()),$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING id
            """, pid_param, data["title"], data["status"], data["company"],
                hm.get("name") or "", hm.get("title") or "", hm.get("email") or "",
                data["experienceLevel"], data["location"], data["workArrangement"],
                data.get("compensation", ""), data.get("salaryMin"), data.get("salaryMax"),
                data.get("timeline", ""), data.get("summary", ""), data.get("jobFile"))
            pid = row["id"]
            await _save_position_details(conn, pid, data)

    return await get_position(str(pid))


async def update_position(pid, data):
    # Auto-parse salary from compensation if not explicitly provided
    if "salaryMin" not in data and data.get("compensation"):
        data["salaryMin"], data["salaryMax"] = parse_salary(data["compensation"])

    pool = await get_pool()
    pid_uuid = _uuid(pid)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id FROM positions WHERE id = $1", pid_uuid)
        if row is None:
            return None

        async with conn.transaction():
            hm = data.get("hiringManager") or {}
            await conn.execute("""
                UPDATE positions SET
                    title = $2, status = $3, company = $4,
                    hiring_manager_name = $5, hiring_manager_title = $6, hiring_manager_email = $7,
                    experience_level = $8, location = $9, work_arrangement = $10,
                    compensation = $11, salary_min = $12, salary_max = $13,
                    timeline = $14, job_file = $15, summary = $16
                WHERE id = $1
            """, pid_uuid, data["title"], data["status"], data["company"],
                hm.get("name") or "", hm.get("title") or "", hm.get("email") or "",
                data["experienceLevel"], data["location"], data["workArrangement"],
                data.get("compensation", ""), data.get("salaryMin"), data.get("salaryMax"),
                data.get("timeline", ""), data.get("jobFile"), data.get("summary", ""))

            # Replace detail rows
            await conn.execute("DELETE FROM position_skills WHERE position_id = $1", pid_uuid)
            await conn.execute("DELETE FROM position_requirements WHERE position_id = $1", pid_uuid)
            await _save_position_details(conn, pid_uuid, data)

    return await get_position(pid)


# --- Assignments --------------------------------------------------------------

async def assign_position(candidate_id, position_id):
    pool = await get_pool()
    cid, pid = _uuid(candidate_id), _uuid(position_id)
    async with pool.acquire() as conn:
        c = await conn.fetchrow("SELECT id FROM candidates WHERE id = $1", cid)
        if c is None:
            raise ValueError(f"Candidate {candidate_id} not found")
        p = await conn.fetchrow("SELECT id FROM positions WHERE id = $1", pid)
        if p is None:
            raise ValueError(f"Position {position_id} not found")
        await conn.execute("""
            INSERT INTO candidate_positions (candidate_id, position_id)
            VALUES ($1, $2) ON CONFLICT DO NOTHING
        """, cid, pid)


async def unassign_position(candidate_id, position_id):
    pool = await get_pool()
    cid, pid = _uuid(candidate_id), _uuid(position_id)
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM candidate_positions WHERE candidate_id = $1 AND position_id = $2",
            cid, pid)


# --- Auth ---------------------------------------------------------------------

async def create_user(username, hashed_password, role="hr-viewer"):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (username, password, role) VALUES ($1, $2, $3)
            ON CONFLICT (username) DO NOTHING
        """, username, hashed_password, role)


async def get_user(username):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE username = $1", username)
    if row is None:
        return None
    return {"id": row["id"], "username": row["username"], "password": row["password"], "role": row["role"]}


# --- Documents ----------------------------------------------------------------

async def insert_document(entity_type, entity_id, filename, file_type=None, stored_path=None, raw_text=None):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO documents (entity_type, entity_id, filename, file_type, stored_path, raw_text)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, entity_type, _uuid(entity_id), filename, file_type, stored_path, raw_text)


async def get_ingested_documents():
    """Return all ingested documents with linked entity names."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT d.entity_type, d.entity_id, d.filename, d.created_at,
                   c.name AS candidate_name, p.title AS position_title
            FROM documents d
            LEFT JOIN candidates c ON d.entity_type = 'candidate' AND d.entity_id = c.id
            LEFT JOIN positions p ON d.entity_type = 'position' AND d.entity_id = p.id
            ORDER BY d.created_at DESC
        """)
    return [dict(r) for r in rows]


async def get_documents(entity_type, entity_id):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM documents WHERE entity_type = $1 AND entity_id = $2 ORDER BY id
        """, entity_type, _uuid(entity_id))
    return [
        {**dict(r), "entity_id": str(r["entity_id"])}
        for r in rows
    ]


async def _delete_entity(entity_type, table, entity_id):
    """Delete an entity and its documents in a transaction."""
    pool = await get_pool()
    uid = _uuid(entity_id)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                f"DELETE FROM documents WHERE entity_type='{entity_type}' AND entity_id=$1", uid)
            result = await conn.execute(f"DELETE FROM {table} WHERE id=$1", uid)
            if result == "DELETE 0":
                raise ValueError(f"{entity_type.title()} {entity_id} not found")


async def delete_candidate(candidate_id):
    await _delete_entity("candidate", "candidates", candidate_id)


async def delete_position(position_id):
    await _delete_entity("position", "positions", position_id)


# --- Helpers ------------------------------------------------------------------

def _group_by(rows, key):
    result = {}
    for r in rows:
        result.setdefault(r[key], []).append(r)
    return result


def _assemble_candidate(row, skill_map, lang_map, exp_map, edu_map, cert_map, junction_map):
    cid = row["id"]
    exps = exp_map.get(cid, [])
    return {
        "id": str(cid),
        "name": row["name"],
        "status": row["status"],
        "experienceLevel": row["experience_level"],
        "contact": {
            "phone": row["phone"] or "",
            "email": row["email"] or "",
            "location": row["location"] or "",
            "linkedin": row["linkedin"] or "",
            "github": row["github"] or "",
        },
        "languages": [r["language"] for r in lang_map.get(cid, [])],
        "skills": [r["skill"] for r in skill_map.get(cid, [])],
        "summary": row["summary"],
        "experience": [_exp_to_dict(e) for e in exps],
        "education": [_edu_to_dict(e) for e in edu_map.get(cid, [])],
        "certifications": [{"name": c["name"], "year": c["year"]} for c in cert_map.get(cid, [])],
        "cvFile": row["cv_file"],
        "updatedAt": row["updated_at"].isoformat() if row.get("updated_at") else None,
        "positionIds": [str(j["position_id"]) for j in junction_map.get(cid, [])],
    }


def _assemble_position(row, skill_map, req_map, junction_map):
    pid = row["id"]
    reqs = req_map.get(pid, [])
    return {
        "id": str(pid),
        "title": row["title"],
        "status": row["status"],
        "company": row["company"],
        "hiringManager": {
            "name": row["hiring_manager_name"],
            "title": row["hiring_manager_title"] or "",
            "email": row["hiring_manager_email"] or "",
        },
        "experienceLevel": row["experience_level"],
        "requirements": [r["item"] for r in reqs if r["type"] == "required"],
        "niceToHave": [r["item"] for r in reqs if r["type"] == "nice_to_have"],
        "responsibilities": [r["item"] for r in reqs if r["type"] == "responsibility"],
        "techStack": [r["skill"] for r in skill_map.get(pid, [])],
        "location": row["location"],
        "workArrangement": row["work_arrangement"],
        "compensation": row["compensation"] or "",
        "salaryMin": row["salary_min"],
        "salaryMax": row["salary_max"],
        "timeline": row["timeline"] or "",
        "summary": row["summary"],
        "jobFile": row["job_file"],
        "candidateIds": [str(j["candidate_id"]) for j in junction_map.get(pid, [])],
    }


def _exp_to_dict(exp):
    desc = exp["description"] or ""
    return {
        "title": exp["title"],
        "company": exp["company"],
        "location": exp["location"] or "",
        "startDate": exp["start_date"],
        "endDate": exp["end_date"],
        "bullets": [b for b in desc.split("\n") if b] if desc else [],
    }


def _edu_to_dict(edu):
    return {
        "degree": edu["degree"],
        "institution": edu["institution"],
        "startDate": edu["start_date"],
        "endDate": edu["end_date"],
    }


# --- Agent state (Exercise 6) ------------------------------------------------

async def mark_email_processed(email_id, email_type, action_taken, draft_id=None):
    pool = await get_pool()
    row = await pool.fetchrow(
        """INSERT INTO agent_processed_emails (email_id, email_type, action_taken, draft_id)
           VALUES ($1, $2, $3, $4) RETURNING *""",
        email_id, email_type, action_taken, draft_id,
    )
    return dict(row)


async def is_email_processed(email_id):
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT 1 FROM agent_processed_emails WHERE email_id = $1", email_id
    )
    return row is not None


async def list_processed_emails():
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM agent_processed_emails ORDER BY processed_at DESC"
    )
    return [dict(r) for r in rows]


async def create_notification(type, summary, action_url=None, related_email_id=None):
    pool = await get_pool()
    row = await pool.fetchrow(
        """INSERT INTO agent_notifications (type, summary, action_url, related_email_id)
           VALUES ($1, $2, $3, $4) RETURNING *""",
        type, summary, action_url, related_email_id,
    )
    return dict(row)


async def list_notifications(status=None):
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM agent_notifications WHERE ($1::text IS NULL OR status = $1) ORDER BY created_at DESC",
        status)
    return [dict(r) for r in rows]


async def update_notification_status(notification_id, status):
    pool = await get_pool()
    row = await pool.fetchrow(
        """UPDATE agent_notifications SET status = $1 WHERE id = $2 RETURNING *""",
        status, notification_id,
    )
    return dict(row) if row else None


# --- Seed ---------------------------------------------------------------------

USERS = [
    {"username": "admin", "password": "admin", "role": "hr-editor"},
    {"username": "viewer", "password": "viewer", "role": "hr-viewer"},
]


async def seed():
    for u in USERS:
        hashed = bcrypt.hashpw(u["password"].encode(), bcrypt.gensalt()).decode()
        await create_user(u["username"], hashed, u["role"])
        print(f"User: {u['username']}/{u['password']} ({u['role']})")
    print("Seed complete.")
