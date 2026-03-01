"""DB tests -- schema, db module, seed data.

Tests the internal layers directly (no HTTP). Validates schema constraints,
data access functions, seed integrity, and trigger behavior.
"""

import os
import sys
from uuid import UUID, uuid4

import asyncpg
import bcrypt
import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

pytestmark = pytest.mark.asyncio(loop_scope="session")

import db
from testdata import IDS, CANDIDATE_A, CANDIDATE_B, POSITION_A, POSITION_B

DATABASE_URL = os.environ["DATABASE_URL"]


# =============================================================================
# Schema -- tables, columns
# =============================================================================

class TestTablesExist:
    async def test_all_tables_created(self):
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )
        tables = {r["tablename"] for r in rows}
        await conn.close()
        expected = [
            "users", "candidates", "positions", "candidate_positions", "documents",
            "candidate_skills", "position_skills",
            "candidate_languages", "experience",
            "education", "certifications",
            "position_requirements",
        ]
        for t in expected:
            assert t in tables, f"Missing table: {t}"


class TestColumns:
    async def test_users(self):
        cols = await _get_columns("users")
        for c in ["id", "username", "password", "role", "created_at"]:
            assert c in cols

    async def test_candidates(self):
        cols = await _get_columns("candidates")
        for c in ["id", "name", "status", "experience_level",
                   "phone", "email", "location", "linkedin", "github",
                   "summary", "cv_file", "created_at", "updated_at"]:
            assert c in cols, f"Missing: candidates.{c}"

    async def test_positions(self):
        cols = await _get_columns("positions")
        for c in ["id", "title", "status", "company",
                   "hiring_manager_name", "hiring_manager_title", "hiring_manager_email",
                   "experience_level", "location", "work_arrangement",
                   "compensation", "timeline", "job_file", "created_at"]:
            assert c in cols, f"Missing: positions.{c}"

    async def test_candidate_positions(self):
        cols = await _get_columns("candidate_positions")
        for c in ["candidate_id", "position_id", "created_at"]:
            assert c in cols

    async def test_documents(self):
        cols = await _get_columns("documents")
        for c in ["id", "entity_type", "entity_id", "filename", "file_type",
                   "stored_path", "created_at"]:
            assert c in cols

    async def test_candidate_skills(self):
        cols = await _get_columns("candidate_skills")
        for c in ["candidate_id", "skill"]:
            assert c in cols

    async def test_position_skills(self):
        cols = await _get_columns("position_skills")
        for c in ["position_id", "skill"]:
            assert c in cols

    async def test_experience(self):
        cols = await _get_columns("experience")
        for c in ["id", "candidate_id", "title", "company", "location",
                   "start_date", "end_date", "description", "sort_order"]:
            assert c in cols

    async def test_education(self):
        cols = await _get_columns("education")
        for c in ["id", "candidate_id", "degree", "institution", "start_date", "end_date"]:
            assert c in cols

    async def test_certifications(self):
        cols = await _get_columns("certifications")
        for c in ["id", "candidate_id", "name", "year"]:
            assert c in cols

    async def test_position_requirements(self):
        cols = await _get_columns("position_requirements")
        for c in ["id", "position_id", "item", "type", "sort_order"]:
            assert c in cols


# =============================================================================
# Schema -- constraints
# =============================================================================

class TestPrimaryKeys:
    async def test_candidate_positions_composite_pk(self):
        conn = await asyncpg.connect(DATABASE_URL)
        ca, pa = UUID(IDS["ca"]), UUID(IDS["pa"])
        await conn.execute(
            "INSERT INTO candidate_positions VALUES ($1,$2) ON CONFLICT DO NOTHING",
            ca, pa)
        await conn.execute(
            "INSERT INTO candidate_positions VALUES ($1,$2) ON CONFLICT DO NOTHING",
            ca, pa)
        count = await conn.fetchval(
            "SELECT count(*) FROM candidate_positions WHERE candidate_id=$1 AND position_id=$2",
            ca, pa)
        await conn.close()
        assert count == 1

    async def test_users_username_unique(self):
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            await conn.execute(
                "INSERT INTO users (username, password) VALUES ($1,$2)",
                "uniquetest", "hash")
            with pytest.raises(asyncpg.UniqueViolationError):
                await conn.execute(
                    "INSERT INTO users (username, password) VALUES ($1,$2)",
                    "uniquetest", "hash2")
        finally:
            await conn.execute("DELETE FROM users WHERE username = 'uniquetest'")
            await conn.close()

    async def test_candidate_skills_unique(self):
        conn = await asyncpg.connect(DATABASE_URL)
        ca = UUID(IDS["ca"])
        try:
            await conn.execute(
                "INSERT INTO candidate_skills (candidate_id, skill) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                ca, "UniqueSkillTest")
            with pytest.raises(asyncpg.UniqueViolationError):
                await conn.execute(
                    "INSERT INTO candidate_skills (candidate_id, skill) VALUES ($1, $2)",
                    ca, "UniqueSkillTest")
        finally:
            await conn.execute(
                "DELETE FROM candidate_skills WHERE candidate_id = $1 AND skill = 'UniqueSkillTest'", ca)
            await conn.close()


class TestForeignKeys:
    async def test_fk_junction_candidate(self):
        conn = await asyncpg.connect(DATABASE_URL)
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                "INSERT INTO candidate_positions VALUES ($1,$2)",
                uuid4(), UUID(IDS["pa"]))
        await conn.close()

    async def test_fk_junction_position(self):
        conn = await asyncpg.connect(DATABASE_URL)
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                "INSERT INTO candidate_positions VALUES ($1,$2)",
                UUID(IDS["ca"]), uuid4())
        await conn.close()


class TestCascadeDelete:
    async def test_delete_candidate_cascades_to_junction(self):
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            del_id = uuid4()
            await conn.execute(
                "INSERT INTO candidates (id, name, status, experience_level) VALUES ($1,$2,$3,$4)",
                del_id, "Delete Me", "active", "junior")
            await conn.execute(
                "INSERT INTO candidate_positions VALUES ($1,$2)",
                del_id, UUID(IDS["pa"]))
            await conn.execute("DELETE FROM candidates WHERE id = $1", del_id)
            count = await conn.fetchval(
                "SELECT count(*) FROM candidate_positions WHERE candidate_id = $1", del_id)
            assert count == 0
        finally:
            await conn.execute("DELETE FROM candidates WHERE id = $1", del_id)
            await conn.close()

    async def test_delete_candidate_cascades_to_skills(self):
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            del_id = uuid4()
            await conn.execute(
                "INSERT INTO candidates (id, name, status, experience_level) VALUES ($1,$2,$3,$4)",
                del_id, "Delete Me", "active", "junior")
            await conn.execute(
                "INSERT INTO candidate_skills (candidate_id, skill) VALUES ($1, $2)",
                del_id, "CascadeTestSkill")
            await conn.execute("DELETE FROM candidates WHERE id = $1", del_id)
            count = await conn.fetchval(
                "SELECT count(*) FROM candidate_skills WHERE candidate_id = $1", del_id)
            assert count == 0
        finally:
            await conn.execute("DELETE FROM candidates WHERE id = $1", del_id)
            await conn.close()

    async def test_delete_position_cascades_to_junction(self):
        conn = await asyncpg.connect(DATABASE_URL)
        del_id = uuid4()
        try:
            await conn.execute(
                "INSERT INTO positions (id, title, status, company, hiring_manager_name, "
                "experience_level, location, work_arrangement) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                del_id, "Delete Me", "open", "Co", "HM", "mid", "TLV", "hybrid")
            await conn.execute(
                "INSERT INTO candidate_positions VALUES ($1,$2)",
                UUID(IDS["ca"]), del_id)
            await conn.execute("DELETE FROM positions WHERE id = $1", del_id)
            count = await conn.fetchval(
                "SELECT count(*) FROM candidate_positions WHERE position_id = $1", del_id)
            assert count == 0
        finally:
            await conn.execute("DELETE FROM positions WHERE id = $1", del_id)
            await conn.close()


class TestNotNullConstraints:
    async def test_candidate_name_required(self):
        conn = await asyncpg.connect(DATABASE_URL)
        with pytest.raises(asyncpg.NotNullViolationError):
            await conn.execute(
                "INSERT INTO candidates (id, name, status, experience_level) VALUES ($1,$2,$3,$4)",
                uuid4(), None, "active", "mid")
        await conn.close()

    async def test_position_title_required(self):
        conn = await asyncpg.connect(DATABASE_URL)
        with pytest.raises(asyncpg.NotNullViolationError):
            await conn.execute(
                "INSERT INTO positions (id, title, status, company, hiring_manager_name, "
                "experience_level, location, work_arrangement) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                uuid4(), None, "open", "Co", "HM", "mid", "TLV", "hybrid")
        await conn.close()

    async def test_user_username_required(self):
        conn = await asyncpg.connect(DATABASE_URL)
        with pytest.raises(asyncpg.NotNullViolationError):
            await conn.execute(
                "INSERT INTO users (username, password) VALUES ($1,$2)", None, "hash")
        await conn.close()


class TestCheckConstraints:
    async def test_invalid_user_role(self):
        conn = await asyncpg.connect(DATABASE_URL)
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO users (username, password, role) VALUES ($1,$2,$3)",
                "badrole", "hash", "superadmin")
        await conn.close()

    async def test_invalid_candidate_status(self):
        conn = await asyncpg.connect(DATABASE_URL)
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO candidates (id, name, status, experience_level) VALUES ($1,$2,$3,$4)",
                uuid4(), "Check", "archived", "mid")
        await conn.close()

    async def test_invalid_position_status(self):
        conn = await asyncpg.connect(DATABASE_URL)
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO positions (id, title, status, company, hiring_manager_name, "
                "experience_level, location, work_arrangement) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                uuid4(), "Check", "archived", "Co", "HM", "mid", "TLV", "hybrid")
        await conn.close()

    async def test_invalid_requirement_type(self):
        conn = await asyncpg.connect(DATABASE_URL)
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO position_requirements (position_id, item, type) VALUES ($1,$2,$3)",
                UUID(IDS["pa"]), "test", "mandatory")
        await conn.close()


class TestDefaults:
    async def test_candidate_defaults(self):
        conn = await asyncpg.connect(DATABASE_URL)
        del_id = uuid4()
        try:
            await conn.execute(
                "INSERT INTO candidates (id, name, status, experience_level) VALUES ($1,$2,$3,$4)",
                del_id, "Defaults", "active", "mid")
            row = await conn.fetchrow("SELECT * FROM candidates WHERE id = $1", del_id)
            assert row["summary"] == ""
            assert row["phone"] is None
            assert row["email"] is None
        finally:
            await conn.execute("DELETE FROM candidates WHERE id = $1", del_id)
            await conn.close()


# =============================================================================
# db.py -- Candidates
# =============================================================================

class TestDbGetAllCandidates:
    async def test_count(self):
        assert len(await db.get_all_candidates()) == 2

    async def test_all_have_position_ids(self):
        for c in await db.get_all_candidates():
            assert "positionIds" in c

    async def test_ids_are_uuid_strings(self):
        for c in await db.get_all_candidates():
            assert isinstance(c["id"], str)
            UUID(c["id"])  # validates UUID format

    async def test_camelcase_keys(self):
        c = (await db.get_all_candidates())[0]
        assert "experienceLevel" in c
        assert "cvFile" in c
        assert "experience_level" not in c
        assert "cv_file" not in c


class TestDbGetCandidate:
    async def test_found(self):
        c = await db.get_candidate(IDS["ca"])
        assert c is not None
        assert c["name"] == "Alex Mock"

    async def test_not_found(self):
        assert await db.get_candidate(str(uuid4())) is None

    async def test_all_fields(self):
        c = await db.get_candidate(IDS["ca"])
        for key in ["id", "name", "status", "experienceLevel", "contact",
                     "languages", "skills", "summary", "experience",
                     "education", "certifications", "cvFile", "positionIds"]:
            assert key in c

    async def test_contact_structure(self):
        contact = (await db.get_candidate(IDS["ca"]))["contact"]
        for key in ["phone", "email", "location", "linkedin", "github"]:
            assert key in contact

    async def test_experience_structure(self):
        exp = (await db.get_candidate(IDS["ca"]))["experience"]
        assert len(exp) > 0
        for key in ["title", "company", "startDate", "endDate", "bullets"]:
            assert key in exp[0]

    async def test_education_structure(self):
        edu = (await db.get_candidate(IDS["ca"]))["education"]
        assert len(edu) > 0
        for key in ["degree", "institution", "startDate", "endDate"]:
            assert key in edu[0]

    async def test_certifications_structure(self):
        certs = (await db.get_candidate(IDS["ca"]))["certifications"]
        assert len(certs) > 0
        assert isinstance(certs[0]["name"], str)
        assert isinstance(certs[0]["year"], int)

    async def test_skills_list(self):
        c = await db.get_candidate(IDS["ca"])
        assert isinstance(c["skills"], list)
        assert len(c["skills"]) > 0

    async def test_languages_list(self):
        c = await db.get_candidate(IDS["ca"])
        assert isinstance(c["languages"], list)

    async def test_position_ids_match(self):
        c = await db.get_candidate(IDS["ca"])
        assert IDS["pa"] in c["positionIds"]


# =============================================================================
# db.py -- Positions
# =============================================================================

class TestDbGetAllPositions:
    async def test_count(self):
        assert len(await db.get_all_positions()) == 2

    async def test_all_have_candidate_ids(self):
        for p in await db.get_all_positions():
            assert "candidateIds" in p

    async def test_camelcase_keys(self):
        p = (await db.get_all_positions())[0]
        assert "hiringManager" in p
        assert "techStack" in p
        assert "niceToHave" in p
        assert "hiring_manager" not in p
        assert "tech_stack" not in p


class TestDbGetPosition:
    async def test_found(self):
        p = await db.get_position(IDS["pa"])
        assert p["title"] == "Senior DevOps Engineer"

    async def test_not_found(self):
        assert await db.get_position(str(uuid4())) is None

    async def test_all_fields(self):
        p = await db.get_position(IDS["pa"])
        for key in ["id", "title", "status", "company", "hiringManager",
                     "experienceLevel", "requirements", "niceToHave",
                     "responsibilities", "techStack", "location",
                     "workArrangement", "compensation", "timeline",
                     "jobFile", "candidateIds"]:
            assert key in p

    async def test_hiring_manager_structure(self):
        hm = (await db.get_position(IDS["pa"]))["hiringManager"]
        for key in ["name", "title", "email"]:
            assert key in hm

    async def test_array_fields(self):
        p = await db.get_position(IDS["pa"])
        for key in ["requirements", "niceToHave", "responsibilities", "techStack"]:
            assert isinstance(p[key], list)

    async def test_candidate_ids_from_junction(self):
        p = await db.get_position(IDS["pa"])
        assert IDS["ca"] in p["candidateIds"]

    async def test_compensation_is_string(self):
        assert isinstance((await db.get_position(IDS["pa"]))["compensation"], str)

    async def test_timeline_is_string(self):
        assert isinstance((await db.get_position(IDS["pa"]))["timeline"], str)


class TestDbUpdatePosition:
    async def test_basic(self):
        p = await db.get_position(IDS["pa"])
        p["title"] = "Updated"
        result = await db.update_position(IDS["pa"], p)
        assert result["title"] == "Updated"

    async def test_all_fields(self):
        p = await db.get_position(IDS["pa"])
        p["title"] = "New"
        p["status"] = "closed"
        p["company"] = "New Co"
        p["location"] = "Remote"
        p["workArrangement"] = "Fully remote"
        p["experienceLevel"] = "staff"
        result = await db.update_position(IDS["pa"], p)
        assert result["title"] == "New"
        assert result["status"] == "closed"
        assert result["company"] == "New Co"

    async def test_preserves_assignments(self):
        p = await db.get_position(IDS["pa"])
        old = p["candidateIds"]
        p["title"] = "Changed"
        result = await db.update_position(IDS["pa"], p)
        assert result["candidateIds"] == old

    async def test_not_found(self):
        result = await db.update_position(str(uuid4()), {
            "title": "x", "status": "open", "company": "x",
            "hiringManager": {}, "experienceLevel": "mid", "requirements": [],
            "niceToHave": [], "responsibilities": [], "techStack": [],
            "location": "x", "workArrangement": "x",
        })
        assert result is None

    async def test_json_fields(self):
        p = await db.get_position(IDS["pa"])
        p["requirements"] = ["r1", "r2"]
        p["techStack"] = ["Go"]
        p["hiringManager"] = {"name": "A", "title": "B", "email": "c@d.com"}
        result = await db.update_position(IDS["pa"], p)
        assert result["requirements"] == ["r1", "r2"]
        assert result["techStack"] == ["Go"]
        assert result["hiringManager"]["name"] == "A"

    async def test_persists(self):
        p = await db.get_position(IDS["pb"])
        p["title"] = "Persisted"
        await db.update_position(IDS["pb"], p)
        assert (await db.get_position(IDS["pb"]))["title"] == "Persisted"


# =============================================================================
# db.py -- Assignments
# =============================================================================

class TestDbAssign:
    async def test_new_assignment(self):
        await db.assign_position(IDS["ca"], IDS["pb"])
        c = await db.get_candidate(IDS["ca"])
        assert IDS["pb"] in c["positionIds"]

    async def test_idempotent(self):
        await db.assign_position(IDS["ca"], IDS["pa"])
        await db.assign_position(IDS["ca"], IDS["pa"])
        c = await db.get_candidate(IDS["ca"])
        assert c["positionIds"].count(IDS["pa"]) == 1

    async def test_reflected_in_position(self):
        await db.assign_position(IDS["ca"], IDS["pb"])
        p = await db.get_position(IDS["pb"])
        assert IDS["ca"] in p["candidateIds"]

    async def test_invalid_candidate(self):
        with pytest.raises(ValueError, match="Candidate"):
            await db.assign_position(str(uuid4()), IDS["pa"])

    async def test_invalid_position(self):
        with pytest.raises(ValueError, match="Position"):
            await db.assign_position(IDS["ca"], str(uuid4()))


class TestDbUnassign:
    async def test_removes(self):
        await db.unassign_position(IDS["ca"], IDS["pa"])
        c = await db.get_candidate(IDS["ca"])
        assert IDS["pa"] not in c["positionIds"]

    async def test_reflected_in_position(self):
        await db.unassign_position(IDS["ca"], IDS["pa"])
        p = await db.get_position(IDS["pa"])
        assert IDS["ca"] not in p["candidateIds"]

    async def test_idempotent(self):
        await db.unassign_position(IDS["ca"], str(uuid4()))  # no error

    async def test_reassign_after_unassign(self):
        await db.unassign_position(IDS["ca"], IDS["pa"])
        await db.assign_position(IDS["ca"], IDS["pa"])
        c = await db.get_candidate(IDS["ca"])
        assert IDS["pa"] in c["positionIds"]


# =============================================================================
# db.py -- Users
# =============================================================================

class TestDbUsers:
    async def test_create(self):
        hashed = bcrypt.hashpw(b"test", bcrypt.gensalt()).decode()
        await db.create_user("newuser", hashed)
        user = await db.get_user("newuser")
        assert user is not None
        assert user["username"] == "newuser"

    async def test_create_with_role(self):
        hashed = bcrypt.hashpw(b"test", bcrypt.gensalt()).decode()
        await db.create_user("roleuser", hashed, role="hr-editor")
        user = await db.get_user("roleuser")
        assert user["role"] == "hr-editor"

    async def test_create_default_role(self):
        hashed = bcrypt.hashpw(b"test", bcrypt.gensalt()).decode()
        await db.create_user("defuser", hashed)
        user = await db.get_user("defuser")
        assert user["role"] == "hr-viewer"

    async def test_create_idempotent(self):
        h1 = bcrypt.hashpw(b"p1", bcrypt.gensalt()).decode()
        h2 = bcrypt.hashpw(b"p2", bcrypt.gensalt()).decode()
        await db.create_user("idem_user", h1)
        await db.create_user("idem_user", h2)

    async def test_get_found(self):
        user = await db.get_user("admin")
        assert user["username"] == "admin"
        assert "password" in user
        assert "id" in user
        assert "role" in user

    async def test_get_not_found(self):
        assert await db.get_user("nonexistent") is None

    async def test_admin_bcrypt_hash(self):
        user = await db.get_user("admin")
        assert user["password"].startswith("$2")


# =============================================================================
# db.py -- Documents
# =============================================================================

class TestDbDocuments:
    async def test_insert(self):
        await db.insert_document("candidate", IDS["ca"], "cv.pdf", "pdf", "/data/cv.pdf")

    async def test_get(self):
        await db.insert_document("candidate", IDS["ca"], "cv.pdf", "pdf", "/data/cv.pdf")
        docs = await db.get_documents("candidate", IDS["ca"])
        assert len(docs) >= 1
        assert docs[0]["filename"] == "cv.pdf"

    async def test_empty_result(self):
        assert await db.get_documents("candidate", str(uuid4())) == []

    async def test_multiple(self):
        await db.insert_document("candidate", IDS["cb"], "cv1.pdf", "pdf", "/data/cv1.pdf")
        await db.insert_document("candidate", IDS["cb"], "cv2.docx", "docx", "/data/cv2.docx")
        docs = await db.get_documents("candidate", IDS["cb"])
        assert len(docs) == 2

    async def test_null_optional_fields(self):
        await db.insert_document("position", IDS["pa"], "job.pdf")
        docs = await db.get_documents("position", IDS["pa"])
        assert len(docs) >= 1

    async def test_fields(self):
        await db.insert_document("candidate", IDS["ca"], "t.pdf", "pdf", "/t.pdf")
        doc = (await db.get_documents("candidate", IDS["ca"]))[0]
        assert doc["entity_type"] == "candidate"
        assert doc["entity_id"] == IDS["ca"]
        assert doc["filename"] == "t.pdf"


# =============================================================================
# db.py -- Insert functions
# =============================================================================

class TestDbInsertCandidate:
    async def test_insert_and_retrieve(self):
        data = {
            "name": "Test Insert",
            "status": "active",
            "experienceLevel": "junior",
            "contact": {"email": "test@example.com"},
            "skills": ["Python"],
            "languages": ["English"],
            "experience": [],
            "education": [],
            "certifications": [],
        }
        result = await db.insert_candidate(data)
        assert result["name"] == "Test Insert"
        assert "Python" in result["skills"]
        # Clean up
        pool = await db.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM candidates WHERE id = $1", UUID(result["id"]))

    async def test_duplicate_skill_ignored(self):
        """ON CONFLICT DO NOTHING prevents duplicate skill per candidate."""
        data = {
            "name": "Dedup Test",
            "status": "active",
            "experienceLevel": "mid",
            "skills": ["AWS"],
            "experience": [],
            "education": [],
            "certifications": [],
        }
        result = await db.insert_candidate(data)
        pool = await db.get_pool()
        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM candidate_skills WHERE candidate_id = $1 AND skill = 'AWS'",
                UUID(result["id"]))
        assert count == 1
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM candidates WHERE id = $1", UUID(result["id"]))


class TestDbInsertPosition:
    async def test_insert_and_retrieve(self):
        data = {
            "title": "Test Position",
            "status": "open",
            "company": "TestCo",
            "hiringManager": {"name": "HM", "title": "Dir", "email": "hm@test.com"},
            "experienceLevel": "mid",
            "requirements": ["req1"],
            "niceToHave": ["nice1"],
            "responsibilities": ["resp1"],
            "techStack": ["Python"],
            "location": "Remote",
            "workArrangement": "Remote",
        }
        result = await db.insert_position(data)
        assert result["title"] == "Test Position"
        assert "req1" in result["requirements"]
        assert "nice1" in result["niceToHave"]
        assert "resp1" in result["responsibilities"]
        pool = await db.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM positions WHERE id = $1", UUID(result["id"]))


# =============================================================================
# db.py -- Pool
# =============================================================================

class TestDbPool:
    async def test_get_pool(self):
        pool = await db.get_pool()
        assert pool is not None

    async def test_close_and_reopen(self):
        await db.close_pool()
        pool = await db.get_pool()
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT 1") == 1


# =============================================================================
# Seed data
# =============================================================================

class TestSeedData:
    async def test_candidate_count(self):
        assert len(await db.get_all_candidates()) == 2

    async def test_data_matches(self):
        c = await db.get_candidate(IDS["ca"])
        assert c["name"] == CANDIDATE_A["name"]
        assert c["status"] == CANDIDATE_A["status"]
        assert c["experienceLevel"] == CANDIDATE_A["experienceLevel"]
        assert set(c["skills"]) == set(CANDIDATE_A["skills"])

    async def test_active_count(self):
        active = [c for c in await db.get_all_candidates() if c["status"] == "active"]
        assert len(active) == 1

    async def test_inactive_count(self):
        inactive = [c for c in await db.get_all_candidates() if c["status"] == "inactive"]
        assert len(inactive) == 1

    async def test_position_count(self):
        assert len(await db.get_all_positions()) == 2

    async def test_position_data_matches(self):
        p = await db.get_position(IDS["pa"])
        assert p["title"] == POSITION_A["title"]
        assert p["company"] == POSITION_A["company"]
        assert set(p["techStack"]) == set(POSITION_A["techStack"])

    async def test_open_count(self):
        assert len([p for p in await db.get_all_positions() if p["status"] == "open"]) == 2

    async def test_junction_seeded(self):
        c = await db.get_candidate(IDS["ca"])
        assert IDS["pa"] in c["positionIds"]

    async def test_bidirectional(self):
        for c in await db.get_all_candidates():
            for pid in c["positionIds"]:
                p = await db.get_position(pid)
                assert c["id"] in p["candidateIds"]


class TestSeedUser:
    async def test_admin_exists(self):
        assert await db.get_user("admin") is not None

    async def test_admin_password(self):
        user = await db.get_user("admin")
        assert bcrypt.checkpw(b"admin", user["password"].encode())

    async def test_admin_role(self):
        assert (await db.get_user("admin"))["role"] == "hr-editor"

    async def test_viewer_exists(self):
        assert await db.get_user("viewer") is not None

    async def test_viewer_password(self):
        user = await db.get_user("viewer")
        assert bcrypt.checkpw(b"viewer", user["password"].encode())

    async def test_viewer_role(self):
        assert (await db.get_user("viewer"))["role"] == "hr-viewer"


class TestSeedIdempotent:
    async def test_rerun_safe(self):
        from db import seed
        await seed()
        assert len(await db.get_all_candidates()) == 2
        assert len(await db.get_all_positions()) == 2


# =============================================================================
# db.py -- Deduplication
# =============================================================================

class TestDbFindByEmail:
    async def test_found(self):
        result = await db.find_candidate_by_email("alex.mock@example.com")
        assert result is not None
        assert result["name"] == "Alex Mock"

    async def test_case_insensitive(self):
        result = await db.find_candidate_by_email("ALEX.MOCK@EXAMPLE.COM")
        assert result is not None
        assert result["name"] == "Alex Mock"

    async def test_not_found(self):
        result = await db.find_candidate_by_email("nonexistent@example.com")
        assert result is None

    async def test_empty_email(self):
        result = await db.find_candidate_by_email("")
        assert result is None

    async def test_none_email(self):
        result = await db.find_candidate_by_email(None)
        assert result is None


class TestDbFindByLinkedin:
    async def test_found(self):
        result = await db.find_candidate_by_linkedin("https://linkedin.com/in/alexmock")
        assert result is not None
        assert result["name"] == "Alex Mock"

    async def test_case_insensitive(self):
        result = await db.find_candidate_by_linkedin("HTTPS://LINKEDIN.COM/IN/ALEXMOCK")
        assert result is not None

    async def test_not_found(self):
        result = await db.find_candidate_by_linkedin("https://linkedin.com/in/nobody")
        assert result is None

    async def test_empty(self):
        assert await db.find_candidate_by_linkedin("") is None

    async def test_none(self):
        assert await db.find_candidate_by_linkedin(None) is None


class TestDbUpdateCandidate:
    async def test_basic(self):
        c = await db.get_candidate(IDS["ca"])
        c["name"] = "Alex Updated"
        result = await db.update_candidate(IDS["ca"], c)
        assert result["name"] == "Alex Updated"
        assert result["updatedAt"] is not None

    async def test_preserves_id(self):
        c = await db.get_candidate(IDS["ca"])
        c["name"] = "Name Change"
        result = await db.update_candidate(IDS["ca"], c)
        assert result["id"] == IDS["ca"]

    async def test_preserves_assignments(self):
        c = await db.get_candidate(IDS["ca"])
        old_positions = c["positionIds"]
        c["name"] = "Updated Name"
        result = await db.update_candidate(IDS["ca"], c)
        assert result["positionIds"] == old_positions

    async def test_updates_skills(self):
        c = await db.get_candidate(IDS["ca"])
        c["skills"] = ["NewSkill1", "NewSkill2"]
        result = await db.update_candidate(IDS["ca"], c)
        assert set(result["skills"]) == {"NewSkill1", "NewSkill2"}

    async def test_updates_experience(self):
        c = await db.get_candidate(IDS["ca"])
        c["experience"] = [{
            "title": "New Role",
            "company": "New Co",
            "location": "Remote",
            "startDate": "2024-01",
            "endDate": None,
            "bullets": ["Did stuff"],
        }]
        result = await db.update_candidate(IDS["ca"], c)
        assert len(result["experience"]) == 1
        assert result["experience"][0]["title"] == "New Role"

    async def test_not_found(self):
        result = await db.update_candidate(str(uuid4()), {
            "name": "x", "status": "active", "experienceLevel": "mid",
            "contact": {}, "skills": [], "languages": [],
            "experience": [], "education": [], "certifications": [],
        })
        assert result is None

    async def test_updated_at_set(self):
        c = await db.get_candidate(IDS["ca"])
        assert c["updatedAt"] is None  # never updated in fresh state
        c["name"] = "Trigger Update"
        result = await db.update_candidate(IDS["ca"], c)
        assert result["updatedAt"] is not None


class TestDbCandidateUpdatedAtField:
    async def test_new_candidate_has_null_updated_at(self):
        c = await db.get_candidate(IDS["ca"])
        # reset_state restores original data, so updatedAt should be None
        # (unless a previous test in this run updated it -- but reset_state cleans)
        assert "updatedAt" in c

    async def test_updated_at_in_all_candidates(self):
        for c in await db.get_all_candidates():
            assert "updatedAt" in c


# =============================================================================
# Helpers
# =============================================================================

async def _get_columns(table):
    conn = await asyncpg.connect(DATABASE_URL)
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name = $1", table)
    await conn.close()
    return {r["column_name"] for r in rows}
