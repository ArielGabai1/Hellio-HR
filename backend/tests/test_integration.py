"""Integration tests -- API endpoints through HTTP.

Tests the full stack: FastAPI routes -> auth middleware -> db layer -> Postgres.
Every test hits real endpoints via HTTPX/ASGITransport.
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import jwt
import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

pytestmark = pytest.mark.asyncio(loop_scope="session")

from testdata import IDS

JWT_SECRET = os.environ["JWT_SECRET"]


# =============================================================================
# Health
# =============================================================================

class TestHealth:
    async def test_returns_ok(self, client):
        resp = await client.get("/api/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    async def test_no_auth_required(self, client):
        resp = await client.get("/api/health")
        assert resp.status_code == 200


# =============================================================================
# Auth -- Login
# =============================================================================

class TestLogin:
    async def test_success(self, client):
        resp = await client.post("/api/auth/login", json={"username": "admin", "password": "admin"})
        assert resp.status_code == 200
        assert "token" in resp.json()

    async def test_returns_valid_jwt(self, client):
        resp = await client.post("/api/auth/login", json={"username": "admin", "password": "admin"})
        token = resp.json()["token"]
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        assert payload["sub"] == "admin"
        assert payload["role"] == "hr-editor"
        assert "exp" in payload

    async def test_returns_role(self, client):
        resp = await client.post("/api/auth/login", json={"username": "admin", "password": "admin"})
        assert resp.json()["role"] == "hr-editor"

    async def test_viewer_login(self, client):
        resp = await client.post("/api/auth/login", json={"username": "viewer", "password": "viewer"})
        assert resp.status_code == 200
        assert resp.json()["role"] == "hr-viewer"

    async def test_wrong_password(self, client):
        resp = await client.post("/api/auth/login", json={"username": "admin", "password": "wrong"})
        assert resp.status_code == 401

    async def test_unknown_user(self, client):
        resp = await client.post("/api/auth/login", json={"username": "nobody", "password": "x"})
        assert resp.status_code == 401

    async def test_empty_body(self, client):
        resp = await client.post("/api/auth/login", json={})
        assert resp.status_code == 401

    async def test_missing_password(self, client):
        resp = await client.post("/api/auth/login", json={"username": "admin"})
        assert resp.status_code == 401

    async def test_missing_username(self, client):
        resp = await client.post("/api/auth/login", json={"password": "admin"})
        assert resp.status_code == 401


# =============================================================================
# Auth -- Token validation
# =============================================================================

class TestTokenValidation:
    async def test_no_token(self, client):
        resp = await client.get("/api/candidates")
        assert resp.status_code == 401

    async def test_valid_token(self, client, auth_headers):
        resp = await client.get("/api/candidates", headers=auth_headers)
        assert resp.status_code == 200

    async def test_expired_token(self, client):
        exp = datetime.now(timezone.utc) - timedelta(hours=1)
        token = jwt.encode({"sub": "admin", "exp": exp}, JWT_SECRET, algorithm="HS256")
        resp = await client.get("/api/candidates", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 401

    async def test_invalid_token(self, client):
        resp = await client.get("/api/candidates", headers={"Authorization": "Bearer garbage"})
        assert resp.status_code == 401

    async def test_wrong_secret(self, client):
        token = jwt.encode({"sub": "admin"}, "wrong-secret", algorithm="HS256")
        resp = await client.get("/api/candidates", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 401

    async def test_missing_bearer_prefix(self, client):
        token = jwt.encode({"sub": "admin"}, JWT_SECRET, algorithm="HS256")
        resp = await client.get("/api/candidates", headers={"Authorization": token})
        assert resp.status_code == 401

    async def test_empty_authorization(self, client):
        resp = await client.get("/api/candidates", headers={"Authorization": ""})
        assert resp.status_code == 401

    async def test_bearer_no_token(self, client):
        resp = await client.get("/api/candidates", headers={"Authorization": "Bearer "})
        assert resp.status_code == 401

    async def test_all_data_routes_require_auth(self, client):
        cid, pid = IDS["ca"], IDS["pa"]
        routes = [
            ("GET", "/api/candidates"),
            ("GET", f"/api/candidates/{cid}"),
            ("GET", f"/api/candidates/{cid}/documents"),
            ("GET", "/api/positions"),
            ("GET", f"/api/positions/{pid}"),
            ("POST", f"/api/candidates/{cid}/positions/{pid}"),
            ("DELETE", f"/api/candidates/{cid}/positions/{pid}"),
            ("PUT", f"/api/positions/{pid}"),
            ("GET", "/api/files/cvs/cv_001.pdf"),
            ("GET", "/api/auth/me"),
            ("POST", "/api/chat"),
            ("GET", f"/api/positions/{pid}/suggestions"),
            ("GET", f"/api/candidates/{cid}/suggestions"),
            ("POST", "/api/embeddings/rebuild"),
        ]
        for method, path in routes:
            resp = await client.request(method, path)
            assert resp.status_code == 401, f"{method} {path} should require auth"


# =============================================================================
# Auth -- /me endpoint
# =============================================================================

class TestAuthMe:
    async def test_returns_user_info(self, client, auth_headers):
        resp = await client.get("/api/auth/me", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["username"] == "admin"
        assert resp.json()["role"] == "hr-editor"

    async def test_viewer_info(self, client, viewer_headers):
        resp = await client.get("/api/auth/me", headers=viewer_headers)
        assert resp.status_code == 200
        assert resp.json()["username"] == "viewer"
        assert resp.json()["role"] == "hr-viewer"

    async def test_requires_auth(self, client):
        resp = await client.get("/api/auth/me")
        assert resp.status_code == 401


# =============================================================================
# RBAC -- Role enforcement
# =============================================================================

class TestRBAC:
    async def test_viewer_can_list_candidates(self, client, viewer_headers):
        resp = await client.get("/api/candidates", headers=viewer_headers)
        assert resp.status_code == 200

    async def test_viewer_can_get_candidate(self, client, viewer_headers):
        resp = await client.get(f"/api/candidates/{IDS['ca']}", headers=viewer_headers)
        assert resp.status_code == 200

    async def test_viewer_can_list_positions(self, client, viewer_headers):
        resp = await client.get("/api/positions", headers=viewer_headers)
        assert resp.status_code == 200

    async def test_viewer_can_get_position(self, client, viewer_headers):
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=viewer_headers)
        assert resp.status_code == 200

    async def test_viewer_cannot_update_position(self, client, viewer_headers):
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=viewer_headers)
        data = resp.json()
        data["title"] = "Hacked"
        resp = await client.put(f"/api/positions/{IDS['pa']}", json=data, headers=viewer_headers)
        assert resp.status_code == 403

    async def test_viewer_cannot_assign(self, client, viewer_headers):
        resp = await client.post(f"/api/candidates/{IDS['ca']}/positions/{IDS['pb']}", headers=viewer_headers)
        assert resp.status_code == 403

    async def test_viewer_cannot_unassign(self, client, viewer_headers):
        resp = await client.delete(f"/api/candidates/{IDS['ca']}/positions/{IDS['pa']}", headers=viewer_headers)
        assert resp.status_code == 403

    async def test_editor_can_update_position(self, client, auth_headers):
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=auth_headers)
        data = resp.json()
        data["title"] = "Editor Update"
        resp = await client.put(f"/api/positions/{IDS['pa']}", json=data, headers=auth_headers)
        assert resp.status_code == 200

    async def test_editor_can_assign(self, client, auth_headers):
        resp = await client.post(f"/api/candidates/{IDS['ca']}/positions/{IDS['pb']}", headers=auth_headers)
        assert resp.status_code == 201

    async def test_editor_can_unassign(self, client, auth_headers):
        resp = await client.delete(f"/api/candidates/{IDS['ca']}/positions/{IDS['pa']}", headers=auth_headers)
        assert resp.status_code == 204


# =============================================================================
# Candidates API
# =============================================================================

class TestCandidatesList:
    async def test_count(self, client, auth_headers):
        resp = await client.get("/api/candidates", headers=auth_headers)
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    async def test_response_shape(self, client, auth_headers):
        resp = await client.get("/api/candidates", headers=auth_headers)
        c = resp.json()[0]
        for key in ["id", "name", "status", "experienceLevel", "contact",
                     "languages", "skills", "summary", "experience",
                     "education", "certifications", "cvFile", "positionIds"]:
            assert key in c, f"Missing: {key}"

    async def test_contact_shape(self, client, auth_headers):
        resp = await client.get("/api/candidates", headers=auth_headers)
        contact = resp.json()[0]["contact"]
        for key in ["phone", "email", "location", "linkedin", "github"]:
            assert key in contact

    async def test_experience_shape(self, client, auth_headers):
        resp = await client.get("/api/candidates", headers=auth_headers)
        # Find Alex Mock who has experience
        alex = next(c for c in resp.json() if c["name"] == "Alex Mock")
        exp = alex["experience"]
        assert len(exp) > 0
        for key in ["title", "company", "startDate", "endDate", "bullets"]:
            assert key in exp[0]

    async def test_education_shape(self, client, auth_headers):
        resp = await client.get("/api/candidates", headers=auth_headers)
        alex = next(c for c in resp.json() if c["name"] == "Alex Mock")
        edu = alex["education"]
        assert len(edu) > 0
        for key in ["degree", "institution", "startDate", "endDate"]:
            assert key in edu[0]

    async def test_certifications_shape(self, client, auth_headers):
        resp = await client.get("/api/candidates", headers=auth_headers)
        alex = next(c for c in resp.json() if c["name"] == "Alex Mock")
        certs = alex["certifications"]
        assert len(certs) > 0
        assert isinstance(certs[0]["name"], str)
        assert isinstance(certs[0]["year"], int)

    async def test_includes_all_statuses(self, client, auth_headers):
        resp = await client.get("/api/candidates", headers=auth_headers)
        statuses = {c["status"] for c in resp.json()}
        assert "active" in statuses
        assert "inactive" in statuses


class TestCandidateById:
    async def test_found(self, client, auth_headers):
        resp = await client.get(f"/api/candidates/{IDS['ca']}", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["name"] == "Alex Mock"

    async def test_not_found(self, client, auth_headers):
        resp = await client.get(f"/api/candidates/{uuid4()}", headers=auth_headers)
        assert resp.status_code == 404

    async def test_camelcase_keys(self, client, auth_headers):
        resp = await client.get(f"/api/candidates/{IDS['ca']}", headers=auth_headers)
        data = resp.json()
        assert "experienceLevel" in data
        assert "positionIds" in data
        assert "cvFile" in data
        assert "experience_level" not in data
        assert "position_ids" not in data
        assert "cv_file" not in data

    async def test_has_position_ids(self, client, auth_headers):
        resp = await client.get(f"/api/candidates/{IDS['ca']}", headers=auth_headers)
        assert len(resp.json()["positionIds"]) > 0

    async def test_each_candidate_accessible(self, client, auth_headers):
        for cid in [IDS["ca"], IDS["cb"]]:
            resp = await client.get(f"/api/candidates/{cid}", headers=auth_headers)
            assert resp.status_code == 200, f"Candidate {cid} not found"


# =============================================================================
# Positions API
# =============================================================================

class TestPositionsList:
    async def test_count(self, client, auth_headers):
        resp = await client.get("/api/positions", headers=auth_headers)
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    async def test_response_shape(self, client, auth_headers):
        resp = await client.get("/api/positions", headers=auth_headers)
        p = resp.json()[0]
        for key in ["id", "title", "status", "company", "hiringManager",
                     "experienceLevel", "requirements", "niceToHave",
                     "responsibilities", "techStack", "location",
                     "workArrangement", "compensation", "timeline",
                     "jobFile", "candidateIds"]:
            assert key in p, f"Missing: {key}"

    async def test_hiring_manager_shape(self, client, auth_headers):
        resp = await client.get("/api/positions", headers=auth_headers)
        hm = resp.json()[0]["hiringManager"]
        for key in ["name", "title", "email"]:
            assert key in hm

    async def test_array_fields(self, client, auth_headers):
        resp = await client.get("/api/positions", headers=auth_headers)
        p = resp.json()[0]
        for key in ["requirements", "niceToHave", "responsibilities", "techStack"]:
            assert isinstance(p[key], list)

    async def test_all_positions_open(self, client, auth_headers):
        resp = await client.get("/api/positions", headers=auth_headers)
        statuses = {p["status"] for p in resp.json()}
        assert statuses == {"open"}


class TestPositionById:
    async def test_found(self, client, auth_headers):
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["title"] == "Senior DevOps Engineer"

    async def test_not_found(self, client, auth_headers):
        resp = await client.get(f"/api/positions/{uuid4()}", headers=auth_headers)
        assert resp.status_code == 404

    async def test_camelcase_keys(self, client, auth_headers):
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=auth_headers)
        data = resp.json()
        assert "hiringManager" in data
        assert "niceToHave" in data
        assert "techStack" in data
        assert "workArrangement" in data
        assert "hiring_manager" not in data
        assert "tech_stack" not in data

    async def test_has_candidate_ids(self, client, auth_headers):
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=auth_headers)
        assert IDS["ca"] in resp.json()["candidateIds"]

    async def test_each_position_accessible(self, client, auth_headers):
        for pid in [IDS["pa"], IDS["pb"]]:
            resp = await client.get(f"/api/positions/{pid}", headers=auth_headers)
            assert resp.status_code == 200, f"Position {pid} not found"


# =============================================================================
# Position Update API
# =============================================================================

class TestPositionUpdate:
    async def test_update_title(self, client, auth_headers):
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=auth_headers)
        data = resp.json()
        data["title"] = "Updated via API"
        resp = await client.put(f"/api/positions/{IDS['pa']}", json=data, headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["title"] == "Updated via API"

    async def test_persists(self, client, auth_headers):
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=auth_headers)
        data = resp.json()
        data["title"] = "Persisted"
        await client.put(f"/api/positions/{IDS['pa']}", json=data, headers=auth_headers)
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=auth_headers)
        assert resp.json()["title"] == "Persisted"

    async def test_multiple_fields(self, client, auth_headers):
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=auth_headers)
        data = resp.json()
        data["title"] = "Multi"
        data["status"] = "closed"
        data["company"] = "New Co"
        data["location"] = "Remote"
        resp = await client.put(f"/api/positions/{IDS['pa']}", json=data, headers=auth_headers)
        r = resp.json()
        assert r["title"] == "Multi"
        assert r["status"] == "closed"
        assert r["company"] == "New Co"
        assert r["location"] == "Remote"

    async def test_not_found(self, client, auth_headers):
        resp = await client.put(f"/api/positions/{uuid4()}", json={
            "title": "x", "status": "open", "company": "x",
            "hiringManager": {}, "experienceLevel": "mid", "requirements": [],
            "niceToHave": [], "responsibilities": [], "techStack": [],
            "location": "x", "workArrangement": "x",
        }, headers=auth_headers)
        assert resp.status_code == 404

    async def test_preserves_candidate_ids(self, client, auth_headers):
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=auth_headers)
        data = resp.json()
        old = data["candidateIds"]
        data["title"] = "Changed"
        await client.put(f"/api/positions/{IDS['pa']}", json=data, headers=auth_headers)
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=auth_headers)
        assert resp.json()["candidateIds"] == old

    async def test_json_arrays(self, client, auth_headers):
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=auth_headers)
        data = resp.json()
        data["requirements"] = ["R1"]
        data["techStack"] = ["T1"]
        resp = await client.put(f"/api/positions/{IDS['pa']}", json=data, headers=auth_headers)
        assert resp.json()["requirements"] == ["R1"]
        assert resp.json()["techStack"] == ["T1"]

    async def test_requires_auth(self, client):
        resp = await client.put(f"/api/positions/{IDS['pa']}", json={"title": "x"})
        assert resp.status_code == 401


# =============================================================================
# Assignments API
# =============================================================================

class TestAssign:
    async def test_assign(self, client, auth_headers):
        resp = await client.post(f"/api/candidates/{IDS['ca']}/positions/{IDS['pb']}", headers=auth_headers)
        assert resp.status_code == 201

    async def test_reflected_in_candidate(self, client, auth_headers):
        await client.post(f"/api/candidates/{IDS['ca']}/positions/{IDS['pb']}", headers=auth_headers)
        resp = await client.get(f"/api/candidates/{IDS['ca']}", headers=auth_headers)
        assert IDS["pb"] in resp.json()["positionIds"]

    async def test_reflected_in_position(self, client, auth_headers):
        await client.post(f"/api/candidates/{IDS['ca']}/positions/{IDS['pb']}", headers=auth_headers)
        resp = await client.get(f"/api/positions/{IDS['pb']}", headers=auth_headers)
        assert IDS["ca"] in resp.json()["candidateIds"]

    async def test_idempotent(self, client, auth_headers):
        r1 = await client.post(f"/api/candidates/{IDS['ca']}/positions/{IDS['pa']}", headers=auth_headers)
        r2 = await client.post(f"/api/candidates/{IDS['ca']}/positions/{IDS['pa']}", headers=auth_headers)
        assert r1.status_code == 201
        assert r2.status_code == 201

    async def test_invalid_candidate(self, client, auth_headers):
        resp = await client.post(f"/api/candidates/{uuid4()}/positions/{IDS['pa']}", headers=auth_headers)
        assert resp.status_code == 404

    async def test_invalid_position(self, client, auth_headers):
        resp = await client.post(f"/api/candidates/{IDS['ca']}/positions/{uuid4()}", headers=auth_headers)
        assert resp.status_code == 404

    async def test_requires_auth(self, client):
        resp = await client.post(f"/api/candidates/{IDS['ca']}/positions/{IDS['pb']}")
        assert resp.status_code == 401


class TestUnassign:
    async def test_unassign(self, client, auth_headers):
        resp = await client.delete(f"/api/candidates/{IDS['ca']}/positions/{IDS['pa']}", headers=auth_headers)
        assert resp.status_code == 204

    async def test_reflected_in_candidate(self, client, auth_headers):
        await client.delete(f"/api/candidates/{IDS['ca']}/positions/{IDS['pa']}", headers=auth_headers)
        resp = await client.get(f"/api/candidates/{IDS['ca']}", headers=auth_headers)
        assert IDS["pa"] not in resp.json()["positionIds"]

    async def test_reflected_in_position(self, client, auth_headers):
        await client.delete(f"/api/candidates/{IDS['ca']}/positions/{IDS['pa']}", headers=auth_headers)
        resp = await client.get(f"/api/positions/{IDS['pa']}", headers=auth_headers)
        assert IDS["ca"] not in resp.json()["candidateIds"]

    async def test_idempotent(self, client, auth_headers):
        resp = await client.delete(f"/api/candidates/{IDS['ca']}/positions/{uuid4()}", headers=auth_headers)
        assert resp.status_code == 204

    async def test_requires_auth(self, client):
        resp = await client.delete(f"/api/candidates/{IDS['ca']}/positions/{IDS['pa']}")
        assert resp.status_code == 401


class TestAssignUnassignCycle:
    async def test_full_cycle(self, client, auth_headers):
        ca, cb, pa, pb = IDS["ca"], IDS["cb"], IDS["pa"], IDS["pb"]
        # Assign
        await client.post(f"/api/candidates/{cb}/positions/{pa}", headers=auth_headers)
        resp = await client.get(f"/api/candidates/{cb}", headers=auth_headers)
        assert pa in resp.json()["positionIds"]
        # Unassign
        await client.delete(f"/api/candidates/{cb}/positions/{pa}", headers=auth_headers)
        resp = await client.get(f"/api/candidates/{cb}", headers=auth_headers)
        assert pa not in resp.json()["positionIds"]
        # Re-assign
        await client.post(f"/api/candidates/{cb}/positions/{pa}", headers=auth_headers)
        resp = await client.get(f"/api/candidates/{cb}", headers=auth_headers)
        assert pa in resp.json()["positionIds"]


# =============================================================================
# File serving API
# =============================================================================

class TestFileServing:
    async def test_missing_file(self, client, auth_headers):
        resp = await client.get("/api/files/cvs/nonexistent.pdf", headers=auth_headers)
        assert resp.status_code == 404

    async def test_path_traversal_blocked(self, client, auth_headers):
        resp = await client.get("/api/files/../../../etc/passwd", headers=auth_headers)
        assert resp.status_code in (403, 404)

    async def test_requires_auth(self, client):
        resp = await client.get("/api/files/cvs/cv_001.pdf")
        assert resp.status_code == 401


# =============================================================================
# Ingest API
# =============================================================================

# =============================================================================
# Candidate Documents API
# =============================================================================

class TestCandidateDocuments:
    async def test_empty(self, client, auth_headers):
        resp = await client.get(f"/api/candidates/{IDS['ca']}/documents", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_with_document(self, client, auth_headers):
        import db as _db
        await _db.insert_document("candidate", IDS["ca"], "cv_001.pdf", ".pdf", "/data/cv_001.pdf")
        resp = await client.get(f"/api/candidates/{IDS['ca']}/documents", headers=auth_headers)
        assert resp.status_code == 200
        docs = resp.json()
        assert len(docs) == 1
        assert docs[0]["filename"] == "cv_001.pdf"
        assert "createdAt" in docs[0]

    async def test_multiple_documents(self, client, auth_headers):
        import db as _db
        await _db.insert_document("candidate", IDS["ca"], "cv_v1.pdf", ".pdf", "/data/cv_v1.pdf")
        await _db.insert_document("candidate", IDS["ca"], "cv_v2.pdf", ".pdf", "/data/cv_v2.pdf")
        resp = await client.get(f"/api/candidates/{IDS['ca']}/documents", headers=auth_headers)
        assert len(resp.json()) == 2

    async def test_not_found(self, client, auth_headers):
        resp = await client.get(f"/api/candidates/{uuid4()}/documents", headers=auth_headers)
        assert resp.status_code == 404

    async def test_requires_auth(self, client):
        resp = await client.get(f"/api/candidates/{IDS['ca']}/documents")
        assert resp.status_code == 401

    async def test_viewer_can_read(self, client, viewer_headers):
        resp = await client.get(f"/api/candidates/{IDS['ca']}/documents", headers=viewer_headers)
        assert resp.status_code == 200


# =============================================================================
# Candidate updatedAt field
# =============================================================================

class TestCandidateUpdatedAt:
    async def test_null_for_new_candidate(self, client, auth_headers):
        resp = await client.get(f"/api/candidates/{IDS['ca']}", headers=auth_headers)
        assert resp.json()["updatedAt"] is None

    async def test_present_in_list(self, client, auth_headers):
        resp = await client.get("/api/candidates", headers=auth_headers)
        for c in resp.json():
            assert "updatedAt" in c


class TestIngestFiles:
    async def test_inventory(self, client, auth_headers):
        resp = await client.get("/api/ingest/files", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "cvs" in data
        assert "jobs" in data

    async def test_requires_auth(self, client):
        resp = await client.get("/api/ingest/files")
        assert resp.status_code == 401


class TestIngestStats:
    async def test_empty_stats(self, client, auth_headers):
        resp = await client.get("/api/ingest/stats", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "total_extractions" in data
        assert "by_model" in data

    async def test_requires_auth(self, client):
        resp = await client.get("/api/ingest/stats")
        assert resp.status_code == 401


# =============================================================================
# Ingest Upload -- RBAC
# =============================================================================

class TestIngestCvRBAC:
    """Viewer cannot upload CVs; editor can; no-auth gets 401."""

    async def test_viewer_cannot_upload_cv(self, client, viewer_headers):
        import io
        files = {"file": ("test.pdf", io.BytesIO(b"%PDF-1.4 fake"), "application/pdf")}
        resp = await client.post("/api/ingest/cv", files=files, headers=viewer_headers)
        assert resp.status_code == 403

    async def test_requires_auth(self, client):
        import io
        files = {"file": ("test.pdf", io.BytesIO(b"%PDF-1.4 fake"), "application/pdf")}
        resp = await client.post("/api/ingest/cv", files=files)
        assert resp.status_code == 401

    async def test_rejects_unsupported_extension(self, client, auth_headers):
        import io
        files = {"file": ("test.jpg", io.BytesIO(b"fake image"), "image/jpeg")}
        resp = await client.post("/api/ingest/cv", files=files, headers=auth_headers)
        assert resp.status_code == 400

    async def test_rejects_missing_file(self, client, auth_headers):
        resp = await client.post("/api/ingest/cv", headers=auth_headers)
        assert resp.status_code == 422

    async def test_rejects_oversized_file(self, client, auth_headers):
        import io
        # 21 MB > 20 MB limit
        big = io.BytesIO(b"%PDF-1.4 " + b"x" * (21 * 1024 * 1024))
        files = {"file": ("big.pdf", big, "application/pdf")}
        resp = await client.post("/api/ingest/cv", files=files, headers=auth_headers)
        assert resp.status_code == 400


class TestIngestJobRBAC:
    """Viewer cannot upload jobs; editor can; no-auth gets 401."""

    async def test_viewer_cannot_upload_job(self, client, viewer_headers):
        import io
        files = {"file": ("test.txt", io.BytesIO(b"Subject: Test Job\n\nDescription"), "text/plain")}
        resp = await client.post("/api/ingest/job", files=files, headers=viewer_headers)
        assert resp.status_code == 403

    async def test_requires_auth(self, client):
        import io
        files = {"file": ("test.txt", io.BytesIO(b"job content"), "text/plain")}
        resp = await client.post("/api/ingest/job", files=files)
        assert resp.status_code == 401

    async def test_rejects_unsupported_extension(self, client, auth_headers):
        import io
        files = {"file": ("test.pdf", io.BytesIO(b"fake pdf"), "application/pdf")}
        resp = await client.post("/api/ingest/job", files=files, headers=auth_headers)
        assert resp.status_code == 400

    async def test_rejects_missing_file(self, client, auth_headers):
        resp = await client.post("/api/ingest/job", headers=auth_headers)
        assert resp.status_code == 422


class TestIngestFilesRBAC:
    """Viewer CAN read inventory (read-only); editor can too."""

    async def test_viewer_can_read_inventory(self, client, viewer_headers):
        resp = await client.get("/api/ingest/files", headers=viewer_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "cvs" in data
        assert "jobs" in data

    async def test_viewer_can_read_stats(self, client, viewer_headers):
        resp = await client.get("/api/ingest/stats", headers=viewer_headers)
        assert resp.status_code == 200


class TestIngestDeleteRBAC:
    """Viewer cannot delete ingested entities."""

    async def test_viewer_cannot_delete_candidate(self, client, viewer_headers):
        resp = await client.delete(f"/api/candidates/{IDS['ca']}", headers=viewer_headers)
        assert resp.status_code == 403

    async def test_viewer_cannot_delete_position(self, client, viewer_headers):
        resp = await client.delete(f"/api/positions/{IDS['pa']}", headers=viewer_headers)
        assert resp.status_code == 403


# =============================================================================
# Chat API
# =============================================================================

class TestChatEndpoint:
    async def test_requires_auth(self, client):
        resp = await client.post("/api/chat", json={"question": "hello"})
        assert resp.status_code == 401

    async def test_empty_question_400(self, client, auth_headers):
        resp = await client.post("/api/chat", json={"question": ""}, headers=auth_headers)
        assert resp.status_code == 400

    async def test_missing_question_400(self, client, auth_headers):
        resp = await client.post("/api/chat", json={}, headers=auth_headers)
        assert resp.status_code == 400

    async def test_returns_response_fields(self, client, auth_headers):
        resp = await client.post("/api/chat", json={"question": "list open positions"}, headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "answer" in data
        assert "sql" in data
        assert "usage" in data

    async def test_usage_has_model(self, client, auth_headers):
        resp = await client.post("/api/chat", json={"question": "count candidates"}, headers=auth_headers)
        data = resp.json()
        assert data["usage"]["model"] == "nova"

    async def test_viewer_can_access(self, client, viewer_headers):
        resp = await client.post("/api/chat", json={"question": "list candidates"}, headers=viewer_headers)
        assert resp.status_code == 200

    async def test_with_history(self, client, auth_headers):
        history = [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi"},
        ]
        resp = await client.post("/api/chat", json={"question": "list candidates", "history": history}, headers=auth_headers)
        assert resp.status_code == 200

    async def test_invalid_history_type_handled(self, client, auth_headers):
        resp = await client.post("/api/chat", json={"question": "list candidates", "history": "not a list"}, headers=auth_headers)
        assert resp.status_code == 200

    async def test_question_too_long(self, client, auth_headers):
        resp = await client.post("/api/chat", json={"question": "x" * 5001}, headers=auth_headers)
        assert resp.status_code == 400

    async def test_special_characters(self, client, auth_headers):
        resp = await client.post("/api/chat", json={"question": "candidates with <script>alert('xss')</script>"}, headers=auth_headers)
        assert resp.status_code == 200

    async def test_sql_injection_attempt(self, client, auth_headers):
        resp = await client.post("/api/chat", json={"question": '"; DROP TABLE candidates; --'}, headers=auth_headers)
        assert resp.status_code == 200
        # Should not crash; answer should indicate inability or irrelevance

    async def test_empty_history(self, client, auth_headers):
        resp = await client.post("/api/chat", json={"question": "list candidates", "history": []}, headers=auth_headers)
        assert resp.status_code == 200
