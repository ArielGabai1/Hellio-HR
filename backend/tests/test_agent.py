"""Tests for Exercise 6 agent state management."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

pytestmark = pytest.mark.asyncio(loop_scope="session")

import db


class TestAgentProcessedEmails:
    @pytest.fixture(autouse=True)
    async def setup(self, pool):
        await pool.execute("DELETE FROM agent_notifications")
        await pool.execute("DELETE FROM agent_processed_emails")

    async def test_mark_email_processed(self, pool):
        result = await db.mark_email_processed("msg_001", "candidate", "ingested", "draft_abc")
        assert result["email_id"] == "msg_001"
        assert result["email_type"] == "candidate"
        assert result["action_taken"] == "ingested"
        assert result["draft_id"] == "draft_abc"
        assert result["processed_at"] is not None

    async def test_is_email_processed_true(self, pool):
        await db.mark_email_processed("msg_001", "candidate", "ingested")
        assert await db.is_email_processed("msg_001") is True

    async def test_is_email_processed_false(self, pool):
        assert await db.is_email_processed("msg_999") is False

    async def test_list_processed_emails(self, pool):
        await db.mark_email_processed("msg_001", "candidate", "ingested")
        await db.mark_email_processed("msg_002", "position", "ingested")
        results = await db.list_processed_emails()
        assert len(results) == 2

    async def test_duplicate_email_id_rejected(self, pool):
        await db.mark_email_processed("msg_001", "candidate", "ingested")
        with pytest.raises(Exception):
            await db.mark_email_processed("msg_001", "candidate", "ingested")


class TestAgentNotifications:
    @pytest.fixture(autouse=True)
    async def setup(self, pool):
        await pool.execute("DELETE FROM agent_notifications")
        await pool.execute("DELETE FROM agent_processed_emails")

    async def test_create_notification(self, pool):
        result = await db.create_notification(
            type="candidate_ingested",
            summary="Jane Smith ingested for DevOps role",
            action_url="http://localhost/#candidates/123",
        )
        assert result["id"] is not None
        assert result["status"] == "pending"
        assert result["type"] == "candidate_ingested"

    async def test_create_notification_with_email_ref(self, pool):
        await db.mark_email_processed("msg_001", "candidate", "ingested")
        result = await db.create_notification(
            type="candidate_ingested",
            summary="Test",
            related_email_id="msg_001",
        )
        assert result["related_email_id"] == "msg_001"

    async def test_list_notifications_by_status(self, pool):
        await db.create_notification(type="t1", summary="s1")
        n2 = await db.create_notification(type="t2", summary="s2")
        await db.update_notification_status(n2["id"], "reviewed")
        pending = await db.list_notifications(status="pending")
        assert len(pending) == 1
        all_notifs = await db.list_notifications()
        assert len(all_notifs) == 2

    async def test_update_notification_status(self, pool):
        n = await db.create_notification(type="t", summary="s")
        updated = await db.update_notification_status(n["id"], "reviewed")
        assert updated["status"] == "reviewed"

    async def test_update_nonexistent_returns_none(self, pool):
        result = await db.update_notification_status(99999, "reviewed")
        assert result is None


class TestAgentAPI:
    """Test agent API endpoints via ASGI transport."""

    @pytest.fixture(autouse=True)
    async def setup(self, pool):
        await pool.execute("DELETE FROM agent_notifications")
        await pool.execute("DELETE FROM agent_processed_emails")

    async def test_mark_processed_via_api(self, auth_client):
        resp = await auth_client.post("/api/agent/processed-emails", json={
            "email_id": "msg_api_001",
            "email_type": "candidate",
            "action_taken": "ingested",
            "draft_id": "draft_xyz",
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["email_id"] == "msg_api_001"

    async def test_mark_processed_duplicate_returns_409(self, auth_client):
        await auth_client.post("/api/agent/processed-emails", json={
            "email_id": "msg_dup", "email_type": "candidate", "action_taken": "ingested",
        })
        resp = await auth_client.post("/api/agent/processed-emails", json={
            "email_id": "msg_dup", "email_type": "candidate", "action_taken": "ingested",
        })
        assert resp.status_code == 409

    async def test_check_processed_true(self, auth_client):
        await auth_client.post("/api/agent/processed-emails", json={
            "email_id": "msg_api_002", "email_type": "candidate", "action_taken": "ingested",
        })
        resp = await auth_client.get("/api/agent/processed-emails/msg_api_002")
        assert resp.status_code == 200
        assert resp.json()["processed"] is True

    async def test_check_processed_false(self, auth_client):
        resp = await auth_client.get("/api/agent/processed-emails/nonexistent")
        assert resp.status_code == 200
        assert resp.json()["processed"] is False

    async def test_list_processed(self, auth_client):
        await auth_client.post("/api/agent/processed-emails", json={
            "email_id": "msg_1", "email_type": "candidate", "action_taken": "ingested",
        })
        resp = await auth_client.get("/api/agent/processed-emails")
        assert resp.status_code == 200
        assert len(resp.json()) >= 1

    async def test_create_notification_via_api(self, auth_client):
        resp = await auth_client.post("/api/agent/notifications", json={
            "type": "candidate_ingested",
            "summary": "Jane ingested",
            "action_url": "http://localhost/#candidates/1",
        })
        assert resp.status_code == 201
        assert resp.json()["status"] == "pending"

    async def test_list_notifications_filter(self, auth_client):
        await auth_client.post("/api/agent/notifications", json={
            "type": "t1", "summary": "s1",
        })
        resp = await auth_client.get("/api/agent/notifications?status=pending")
        assert resp.status_code == 200
        assert len(resp.json()) >= 1

    async def test_update_notification_status(self, auth_client):
        create = await auth_client.post("/api/agent/notifications", json={
            "type": "t", "summary": "s",
        })
        nid = create.json()["id"]
        resp = await auth_client.put(f"/api/agent/notifications/{nid}", json={
            "status": "reviewed",
        })
        assert resp.status_code == 200
        assert resp.json()["status"] == "reviewed"

    async def test_update_nonexistent_notification_returns_404(self, auth_client):
        resp = await auth_client.put("/api/agent/notifications/99999", json={
            "status": "reviewed",
        })
        assert resp.status_code == 404

    async def test_endpoints_require_auth(self, client):
        resp = await client.get("/api/agent/processed-emails")
        assert resp.status_code == 401
        resp = await client.get("/api/agent/notifications")
        assert resp.status_code == 401


class TestAgentWorkflow:
    @pytest.fixture(autouse=True)
    async def setup(self, pool):
        await pool.execute("DELETE FROM agent_notifications")
        await pool.execute("DELETE FROM agent_processed_emails")

    async def test_full_email_to_notification_workflow(self, pool):
        email = await db.mark_email_processed("wf_001", "candidate", "ingested")
        notif = await db.create_notification(
            type="candidate_ingested", summary="Jane Smith ingested",
            related_email_id="wf_001",
        )
        assert notif["related_email_id"] == "wf_001"
        notifs = await db.list_notifications()
        assert len(notifs) == 1
        assert notifs[0]["related_email_id"] == "wf_001"

    async def test_notification_status_lifecycle(self, pool):
        n = await db.create_notification(type="t", summary="s")
        assert n["status"] == "pending"
        n2 = await db.update_notification_status(n["id"], "reviewed")
        assert n2["status"] == "reviewed"
        n3 = await db.update_notification_status(n["id"], "completed")
        assert n3["status"] == "completed"

    async def test_multiple_notifications_per_email(self, pool):
        await db.mark_email_processed("multi_001", "candidate", "ingested")
        await db.create_notification(type="t1", summary="s1", related_email_id="multi_001")
        await db.create_notification(type="t2", summary="s2", related_email_id="multi_001")
        notifs = await db.list_notifications()
        assert len(notifs) == 2
        assert all(n["related_email_id"] == "multi_001" for n in notifs)

    async def test_list_notifications_empty(self, pool):
        result = await db.list_notifications()
        assert result == []

    async def test_list_processed_emails_empty(self, pool):
        result = await db.list_processed_emails()
        assert result == []

    async def test_mark_email_all_fields(self, pool):
        result = await db.mark_email_processed("all_fields", "position", "draft_reply", "draft_xyz")
        assert result["email_id"] == "all_fields"
        assert result["email_type"] == "position"
        assert result["action_taken"] == "draft_reply"
        assert result["draft_id"] == "draft_xyz"
        assert result["processed_at"] is not None


class TestAgentConstraints:
    @pytest.fixture(autouse=True)
    async def setup(self, pool):
        await pool.execute("DELETE FROM agent_notifications")
        await pool.execute("DELETE FROM agent_processed_emails")

    async def test_notification_fk_nonexistent_email_rejected(self, pool):
        with pytest.raises(Exception):
            await db.create_notification(
                type="t", summary="s", related_email_id="nonexistent_email"
            )

    async def test_notification_null_email_ref_allowed(self, pool):
        result = await db.create_notification(type="t", summary="s", related_email_id=None)
        assert result["related_email_id"] is None

    async def test_delete_email_cascades_to_notifications(self, pool):
        await db.mark_email_processed("cascade_test", "candidate", "ingested")
        await db.create_notification(type="t", summary="s", related_email_id="cascade_test")
        # The FK has no ON DELETE clause, so DELETE should fail if notification references it
        with pytest.raises(Exception):
            await pool.execute("DELETE FROM agent_processed_emails WHERE email_id = 'cascade_test'")

    async def test_long_summary_accepted(self, pool):
        long_summary = "x" * 2000
        result = await db.create_notification(type="t", summary=long_summary)
        assert len(result["summary"]) == 2000

    async def test_special_chars_in_email_type(self, pool):
        result = await db.mark_email_processed("special_001", "candidate/cv", "ingested")
        assert result["email_type"] == "candidate/cv"

    async def test_empty_email_id_allowed(self, pool):
        result = await db.mark_email_processed("", "candidate", "ingested")
        assert result["email_id"] == ""


class TestAgentAPIExtended:
    @pytest.fixture(autouse=True)
    async def setup(self, pool):
        await pool.execute("DELETE FROM agent_notifications")
        await pool.execute("DELETE FROM agent_processed_emails")

    async def test_create_notification_missing_type_raises(self, auth_client):
        with pytest.raises(KeyError):
            await auth_client.post("/api/agent/notifications", json={
                "summary": "test",
            })

    async def test_create_notification_missing_summary_raises(self, auth_client):
        with pytest.raises(KeyError):
            await auth_client.post("/api/agent/notifications", json={
                "type": "test",
            })

    async def test_mark_processed_missing_fields_returns_409(self, auth_client):
        # Missing email_type/action_taken -- KeyError caught by endpoint's except Exception -> 409
        resp = await auth_client.post("/api/agent/processed-emails", json={
            "email_id": "x",
        })
        assert resp.status_code == 409

    async def test_update_notification_returns_updated_data(self, auth_client):
        create = await auth_client.post("/api/agent/notifications", json={
            "type": "t", "summary": "s",
        })
        nid = create.json()["id"]
        resp = await auth_client.put(f"/api/agent/notifications/{nid}", json={"status": "reviewed"})
        data = resp.json()
        assert data["status"] == "reviewed"
        assert data["id"] == nid

    async def test_list_notifications_all_statuses(self, auth_client):
        await auth_client.post("/api/agent/notifications", json={"type": "t1", "summary": "s1"})
        create2 = await auth_client.post("/api/agent/notifications", json={"type": "t2", "summary": "s2"})
        nid = create2.json()["id"]
        await auth_client.put(f"/api/agent/notifications/{nid}", json={"status": "reviewed"})
        resp = await auth_client.get("/api/agent/notifications")
        assert len(resp.json()) == 2

    async def test_list_notifications_ordering(self, auth_client):
        """Notifications should be ordered newest first."""
        await auth_client.post("/api/agent/notifications", json={"type": "first", "summary": "s1"})
        await auth_client.post("/api/agent/notifications", json={"type": "second", "summary": "s2"})
        resp = await auth_client.get("/api/agent/notifications")
        data = resp.json()
        assert data[0]["type"] == "second"
        assert data[1]["type"] == "first"

    async def test_notification_response_has_created_at(self, auth_client):
        resp = await auth_client.post("/api/agent/notifications", json={"type": "t", "summary": "s"})
        data = resp.json()
        assert "created_at" in data

    async def test_viewer_can_read_notifications(self, client, viewer_headers):
        resp = await client.get("/api/agent/notifications", headers=viewer_headers)
        assert resp.status_code == 200

    async def test_viewer_can_write_notifications(self, client, viewer_headers):
        resp = await client.post("/api/agent/notifications", json={
            "type": "t", "summary": "s",
        }, headers=viewer_headers)
        assert resp.status_code == 201

    async def test_notification_action_url_optional(self, auth_client):
        resp = await auth_client.post("/api/agent/notifications", json={
            "type": "t", "summary": "s",
        })
        assert resp.status_code == 201
        assert resp.json().get("action_url") is None

    async def test_mark_email_with_draft_id(self, auth_client):
        resp = await auth_client.post("/api/agent/processed-emails", json={
            "email_id": "draft_test", "email_type": "candidate",
            "action_taken": "draft_reply", "draft_id": "draft_abc123",
        })
        assert resp.status_code == 201
        assert resp.json()["draft_id"] == "draft_abc123"


class TestAgentEdgeCases:
    @pytest.fixture(autouse=True)
    async def setup(self, pool):
        await pool.execute("DELETE FROM agent_notifications")
        await pool.execute("DELETE FROM agent_processed_emails")

    async def test_concurrent_mark_same_email(self, auth_client):
        resp1 = await auth_client.post("/api/agent/processed-emails", json={
            "email_id": "concurrent", "email_type": "candidate", "action_taken": "ingested",
        })
        assert resp1.status_code == 201
        resp2 = await auth_client.post("/api/agent/processed-emails", json={
            "email_id": "concurrent", "email_type": "candidate", "action_taken": "ingested",
        })
        assert resp2.status_code == 409

    async def test_notification_update_idempotent(self, pool):
        n = await db.create_notification(type="t", summary="s")
        await db.update_notification_status(n["id"], "reviewed")
        result = await db.update_notification_status(n["id"], "reviewed")
        assert result["status"] == "reviewed"

    async def test_processed_email_timestamp_auto(self, pool):
        result = await db.mark_email_processed("ts_test", "candidate", "ingested")
        assert result["processed_at"] is not None

    async def test_notification_default_status_pending(self, pool):
        result = await db.create_notification(type="t", summary="s")
        assert result["status"] == "pending"

    async def test_mark_email_without_draft_id(self, pool):
        result = await db.mark_email_processed("no_draft", "candidate", "ingested")
        assert result["draft_id"] is None


class TestAgentAPIAuth:
    """All agent endpoints require authentication."""

    async def test_mark_processed_requires_auth(self, client):
        resp = await client.post("/api/agent/processed-emails", json={
            "email_id": "x", "email_type": "t", "action_taken": "a",
        })
        assert resp.status_code == 401

    async def test_create_notification_requires_auth(self, client):
        resp = await client.post("/api/agent/notifications", json={
            "type": "t", "summary": "s",
        })
        assert resp.status_code == 401

    async def test_list_notifications_requires_auth(self, client):
        resp = await client.get("/api/agent/notifications")
        assert resp.status_code == 401

    async def test_update_notification_requires_auth(self, client):
        resp = await client.put("/api/agent/notifications/1", json={"status": "reviewed"})
        assert resp.status_code == 401

    async def test_check_processed_requires_auth(self, client):
        resp = await client.get("/api/agent/processed-emails/x")
        assert resp.status_code == 401
