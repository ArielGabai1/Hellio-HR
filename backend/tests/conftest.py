"""Shared fixtures for backend tests."""

import os
import sys

import asyncpg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import db
from testdata import IDS, CANDIDATE_A, CANDIDATE_B, POSITION_A, POSITION_B

# Always use a dedicated test database to avoid destroying production data.
# Parse the container's DATABASE_URL but swap the DB name to hellio_test.
_raw_url = os.environ.get(
    "DATABASE_URL", "postgresql://hellio:hellio_dev@localhost:5432/hellio_test"
)
_parts = _raw_url.rsplit("/", 1)
DATABASE_URL = _parts[0] + "/hellio_test"

os.environ["DATABASE_URL"] = DATABASE_URL
os.environ.setdefault("JWT_SECRET", "test-secret")
os.environ.setdefault("FILES_ROOT", "/tmp/test_uploads")
os.makedirs("/tmp/test_uploads/cvs", exist_ok=True)
os.makedirs("/tmp/test_uploads/jobs", exist_ok=True)


@pytest.fixture(scope="session")
def event_loop_policy():
    import asyncio
    return asyncio.DefaultEventLoopPolicy()


@pytest_asyncio.fixture(scope="session")
async def setup_db():
    """Create test DB, run schema + seed users + insert test data once per session."""
    # Connect to maintenance DB to create hellio_test if needed
    maint_url = _parts[0] + "/postgres"
    maint = await asyncpg.connect(maint_url)
    exists = await maint.fetchval(
        "SELECT 1 FROM pg_database WHERE datname = 'hellio_test'"
    )
    if not exists:
        await maint.execute("CREATE DATABASE hellio_test")
    await maint.close()

    conn = await asyncpg.connect(DATABASE_URL)
    schema_path = os.path.join(os.path.dirname(__file__), "..", "schema.sql")
    with open(schema_path) as f:
        await conn.execute(f.read())
    await conn.close()

    from db import seed
    await seed()

    await db.get_pool()

    # Insert test candidates and positions
    ca = await db.insert_candidate(CANDIDATE_A)
    cb = await db.insert_candidate(CANDIDATE_B)
    pa = await db.insert_position(POSITION_A)
    pb = await db.insert_position(POSITION_B)

    IDS["ca"] = ca["id"]
    IDS["cb"] = cb["id"]
    IDS["pa"] = pa["id"]
    IDS["pb"] = pb["id"]

    # Create initial junction (candidate A -> position A)
    await db.assign_position(IDS["ca"], IDS["pa"])
    # candidate B -> position B
    await db.assign_position(IDS["cb"], IDS["pb"])

    yield
    await db.close_pool()


@pytest_asyncio.fixture(autouse=True)
async def reset_state(setup_db):
    """Reset mutable data between tests for isolation."""
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        # Reset junctions
        await conn.execute("DELETE FROM candidate_positions")
        ca_uuid = IDS["ca"]
        cb_uuid = IDS["cb"]
        pa_uuid = IDS["pa"]
        pb_uuid = IDS["pb"]
        from uuid import UUID
        await conn.execute(
            "INSERT INTO candidate_positions (candidate_id, position_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            UUID(ca_uuid), UUID(pa_uuid))
        await conn.execute(
            "INSERT INTO candidate_positions (candidate_id, position_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            UUID(cb_uuid), UUID(pb_uuid))

        # Reset position A to original values
        hm_a = POSITION_A["hiringManager"]
        await conn.execute("""
            UPDATE positions SET
                title=$2, status=$3, company=$4,
                hiring_manager_name=$5, hiring_manager_title=$6, hiring_manager_email=$7,
                experience_level=$8, location=$9, work_arrangement=$10,
                compensation=$11, timeline=$12
            WHERE id=$1""",
            UUID(pa_uuid), POSITION_A["title"], POSITION_A["status"], POSITION_A["company"],
            hm_a["name"], hm_a["title"], hm_a["email"],
            POSITION_A["experienceLevel"], POSITION_A["location"], POSITION_A["workArrangement"],
            POSITION_A.get("compensation", ""), POSITION_A.get("timeline", ""))

        # Reset position A detail tables
        await conn.execute("DELETE FROM position_skills WHERE position_id = $1", UUID(pa_uuid))
        await conn.execute("DELETE FROM position_requirements WHERE position_id = $1", UUID(pa_uuid))
        await db._save_position_details(conn, UUID(pa_uuid), POSITION_A)

        # Reset candidate A to original values
        await conn.execute("""
            UPDATE candidates SET
                name=$2, status=$3, experience_level=$4,
                phone=$5, email=$6, location=$7, linkedin=$8, github=$9,
                summary=$10, cv_file=$11, updated_at=NULL
            WHERE id=$1""",
            UUID(ca_uuid), CANDIDATE_A["name"], CANDIDATE_A["status"], CANDIDATE_A["experienceLevel"],
            CANDIDATE_A["contact"].get("phone"), CANDIDATE_A["contact"].get("email"),
            CANDIDATE_A["contact"].get("location"), CANDIDATE_A["contact"].get("linkedin"),
            CANDIDATE_A["contact"].get("github"),
            CANDIDATE_A.get("summary", ""), CANDIDATE_A.get("cvFile"))

        # Reset candidate A detail tables
        await conn.execute("DELETE FROM candidate_skills WHERE candidate_id = $1", UUID(ca_uuid))
        await conn.execute("DELETE FROM candidate_languages WHERE candidate_id = $1", UUID(ca_uuid))
        await conn.execute("DELETE FROM experience WHERE candidate_id = $1", UUID(ca_uuid))
        await conn.execute("DELETE FROM education WHERE candidate_id = $1", UUID(ca_uuid))
        await conn.execute("DELETE FROM certifications WHERE candidate_id = $1", UUID(ca_uuid))
        await db._save_candidate_details(conn, UUID(ca_uuid), CANDIDATE_A)

        # Clear embeddings
        await conn.execute("UPDATE candidates SET embedding = NULL, embedding_text = NULL")
        await conn.execute("UPDATE positions SET embedding = NULL, embedding_text = NULL")

        # Clean up test artifacts (CASCADE handles child tables)
        await conn.execute("DELETE FROM candidates WHERE id NOT IN ($1, $2)", UUID(ca_uuid), UUID(cb_uuid))
        await conn.execute("DELETE FROM positions WHERE id NOT IN ($1, $2)", UUID(pa_uuid), UUID(pb_uuid))
        await conn.execute("DELETE FROM users WHERE username NOT IN ('admin', 'viewer')")
        await conn.execute("DELETE FROM documents")

        # Agent state (Exercise 6)
        await conn.execute("DELETE FROM agent_notifications")
        await conn.execute("DELETE FROM agent_processed_emails")
    yield


@pytest.fixture
def client(setup_db):
    from main import app
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest_asyncio.fixture
async def auth_headers(client):
    resp = await client.post("/api/auth/login", json={"username": "admin", "password": "admin"})
    return {"Authorization": f"Bearer {resp.json()['token']}"}


@pytest_asyncio.fixture
async def viewer_headers(client):
    resp = await client.post("/api/auth/login", json={"username": "viewer", "password": "viewer"})
    return {"Authorization": f"Bearer {resp.json()['token']}"}


@pytest_asyncio.fixture
async def pool(setup_db):
    return await db.get_pool()


@pytest_asyncio.fixture
async def auth_client(client):
    """AsyncClient with admin auth headers pre-set."""
    resp = await client.post("/api/auth/login", json={"username": "admin", "password": "admin"})
    token = resp.json()["token"]
    client.headers["Authorization"] = f"Bearer {token}"
    return client
