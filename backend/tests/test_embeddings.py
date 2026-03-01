"""Unit and integration tests for the embeddings module."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

pytestmark = pytest.mark.asyncio(loop_scope="session")

from testdata import IDS, CANDIDATE_A, CANDIDATE_B, POSITION_A, POSITION_B
from embeddings import (
    build_candidate_text, build_position_text,
    generate_embedding, _EMBED_DIM,
    embed_candidate, embed_position,
    suggest_candidates_for_position, suggest_positions_for_candidate,
    explain_match, _compatible_levels,
)


# =============================================================================
# Text builders
# =============================================================================

class TestBuildText:
    def test_candidate_includes_name(self):
        text = build_candidate_text(CANDIDATE_A)
        assert "Alex Mock" in text

    def test_candidate_includes_skills(self):
        text = build_candidate_text(CANDIDATE_A)
        assert "AWS" in text
        assert "Kubernetes" in text

    def test_candidate_includes_experience(self):
        text = build_candidate_text(CANDIDATE_A)
        assert "Acme Corp" in text

    def test_candidate_includes_location(self):
        text = build_candidate_text(CANDIDATE_A)
        assert "Mockville" in text

    def test_candidate_deterministic(self):
        t1 = build_candidate_text(CANDIDATE_A)
        t2 = build_candidate_text(CANDIDATE_A)
        assert t1 == t2

    def test_position_includes_title(self):
        text = build_position_text(POSITION_A)
        assert "Senior DevOps Engineer" in text

    def test_position_includes_tech_stack(self):
        text = build_position_text(POSITION_A)
        assert "AWS" in text
        assert "Terraform" in text

    def test_position_includes_requirements(self):
        text = build_position_text(POSITION_A)
        assert "5+ years DevOps experience" in text

    def test_position_deterministic(self):
        t1 = build_position_text(POSITION_A)
        t2 = build_position_text(POSITION_A)
        assert t1 == t2

    def test_different_candidates_different_text(self):
        t1 = build_candidate_text(CANDIDATE_A)
        t2 = build_candidate_text(CANDIDATE_B)
        assert t1 != t2


# =============================================================================
# Embedding generation
# =============================================================================

class TestEmbedding:
    async def test_correct_dimensions(self):
        vec = await generate_embedding("hello world")
        assert len(vec) == _EMBED_DIM

    async def test_deterministic(self):
        v1 = await generate_embedding("test input")
        v2 = await generate_embedding("test input")
        assert v1 == v2

    async def test_different_input_different_output(self):
        v1 = await generate_embedding("input A")
        v2 = await generate_embedding("input B")
        assert v1 != v2

    async def test_returns_floats(self):
        vec = await generate_embedding("test")
        assert all(isinstance(v, float) for v in vec)


# =============================================================================
# Embed and store (requires DB)
# =============================================================================

class TestEmbedAndStore:
    async def test_embed_candidate(self, setup_db):
        text = await embed_candidate(IDS["ca"])
        assert "Alex Mock" in text

    async def test_embed_position(self, setup_db):
        text = await embed_position(IDS["pa"])
        assert "Senior DevOps Engineer" in text

    async def test_embed_invalid_candidate(self, setup_db):
        from uuid import uuid4
        with pytest.raises(ValueError):
            await embed_candidate(str(uuid4()))

    async def test_embed_invalid_position(self, setup_db):
        from uuid import uuid4
        with pytest.raises(ValueError):
            await embed_position(str(uuid4()))

    async def test_suggest_after_embedding(self, setup_db):
        """After embedding all entities, suggestions should return results."""
        await embed_candidate(IDS["ca"])
        await embed_candidate(IDS["cb"])
        await embed_position(IDS["pa"])
        await embed_position(IDS["pb"])

        # Position A has candidate A assigned; should suggest candidate B
        results = await suggest_candidates_for_position(IDS["pa"], min_score=0)
        assert len(results) >= 1
        ids = [r["id"] for r in results]
        assert IDS["cb"] in ids
        # Candidate A is assigned to position A, so shouldn't be suggested
        assert IDS["ca"] not in ids

    async def test_suggest_positions_after_embedding(self, setup_db):
        """After embedding, candidate suggestions should return results."""
        await embed_candidate(IDS["ca"])
        await embed_candidate(IDS["cb"])
        await embed_position(IDS["pa"])
        await embed_position(IDS["pb"])

        # Candidate A is assigned to position A; should suggest position B
        results = await suggest_positions_for_candidate(IDS["ca"], min_score=0)
        assert len(results) >= 1
        ids = [r["id"] for r in results]
        assert IDS["pb"] in ids
        assert IDS["pa"] not in ids

    async def test_suggestion_response_shape(self, setup_db):
        await embed_candidate(IDS["cb"])
        await embed_position(IDS["pa"])

        results = await suggest_candidates_for_position(IDS["pa"])
        if results:
            r = results[0]
            assert "id" in r
            assert "name" in r
            assert "score" in r
            assert isinstance(r["score"], float)
            assert "skills" in r

    async def test_empty_when_no_embeddings(self, setup_db):
        results = await suggest_candidates_for_position(IDS["pa"])
        assert results == []


# =============================================================================
# Explain match
# =============================================================================

class TestExplainMatch:
    async def test_returns_string(self):
        explanation = await explain_match(CANDIDATE_A, POSITION_A)
        assert isinstance(explanation, str)
        assert len(explanation) > 0

    async def test_mentions_candidate(self):
        explanation = await explain_match(CANDIDATE_A, POSITION_A)
        assert "Alex Mock" in explanation

    async def test_mentions_skills(self):
        explanation = await explain_match(CANDIDATE_A, POSITION_A)
        assert "AWS" in explanation or "Kubernetes" in explanation or "Terraform" in explanation


# =============================================================================
# Suggestion endpoints (HTTP)
# =============================================================================

class TestSuggestionEndpoints:
    async def test_position_suggestions_200(self, client, auth_headers, setup_db):
        resp = await client.get(f"/api/positions/{IDS['pa']}/suggestions", headers=auth_headers)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    async def test_candidate_suggestions_200(self, client, auth_headers, setup_db):
        resp = await client.get(f"/api/candidates/{IDS['ca']}/suggestions", headers=auth_headers)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    async def test_position_suggestions_404(self, client, auth_headers):
        from uuid import uuid4
        resp = await client.get(f"/api/positions/{uuid4()}/suggestions", headers=auth_headers)
        assert resp.status_code == 404

    async def test_candidate_suggestions_404(self, client, auth_headers):
        from uuid import uuid4
        resp = await client.get(f"/api/candidates/{uuid4()}/suggestions", headers=auth_headers)
        assert resp.status_code == 404

    async def test_rebuild_requires_editor(self, client, viewer_headers):
        resp = await client.post("/api/embeddings/rebuild", headers=viewer_headers)
        assert resp.status_code == 403

    async def test_rebuild_success(self, client, auth_headers, setup_db):
        resp = await client.post("/api/embeddings/rebuild", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["candidates"] >= 2
        assert data["positions"] >= 2
        assert isinstance(data["errors"], list)

    async def test_suggestions_require_auth(self, client):
        resp = await client.get(f"/api/positions/{IDS['pa']}/suggestions")
        assert resp.status_code == 401
        resp = await client.get(f"/api/candidates/{IDS['ca']}/suggestions")
        assert resp.status_code == 401
        resp = await client.post("/api/embeddings/rebuild")
        assert resp.status_code == 401

    async def test_invalid_uuid_returns_422(self, client, auth_headers):
        resp = await client.get("/api/positions/not-a-uuid/suggestions", headers=auth_headers)
        assert resp.status_code == 422
        resp = await client.get("/api/candidates/not-a-uuid/suggestions", headers=auth_headers)
        assert resp.status_code == 422


# =============================================================================
# Experience level filtering
# =============================================================================

class TestCompatibleLevels:
    def test_senior_includes_mid_and_lead(self):
        levels = _compatible_levels("senior")
        assert "mid" in levels
        assert "senior" in levels
        assert "lead" in levels
        assert "junior" not in levels
        assert "staff" not in levels

    def test_junior_includes_only_junior_and_mid(self):
        levels = _compatible_levels("junior")
        assert "junior" in levels
        assert "mid" in levels
        assert len(levels) == 2

    def test_staff_includes_lead_and_staff(self):
        levels = _compatible_levels("staff")
        assert "lead" in levels
        assert "staff" in levels
        assert "senior" not in levels

    def test_mid_includes_junior_mid_senior(self):
        levels = _compatible_levels("mid")
        assert set(levels) == {"junior", "mid", "senior"}

    def test_unknown_level_returns_none(self):
        assert _compatible_levels("principal") is None
        assert _compatible_levels("") is None
        assert _compatible_levels(None) is None

    def test_case_insensitive(self):
        assert _compatible_levels("Senior") == _compatible_levels("senior")


# =============================================================================
# Embedding determinism end-to-end
# =============================================================================

class TestEmbeddingDeterminism:
    async def test_same_candidate_twice_gives_identical_embedding(self, setup_db):
        """Embed the same candidate twice -- stored embedding must be identical."""
        import db as _db

        await embed_candidate(IDS["ca"])
        pool = await _db.get_pool()
        async with pool.acquire() as conn:
            row1 = await conn.fetchrow(
                "SELECT embedding::text, embedding_text FROM candidates WHERE id = $1",
                _db._uuid(IDS["ca"]),
            )

        # Embed again
        await embed_candidate(IDS["ca"])
        async with pool.acquire() as conn:
            row2 = await conn.fetchrow(
                "SELECT embedding::text, embedding_text FROM candidates WHERE id = $1",
                _db._uuid(IDS["ca"]),
            )

        assert row1["embedding"] == row2["embedding"]
        assert row1["embedding_text"] == row2["embedding_text"]

    async def test_same_position_twice_gives_identical_embedding(self, setup_db):
        """Embed the same position twice -- stored embedding must be identical."""
        import db as _db

        await embed_position(IDS["pa"])
        pool = await _db.get_pool()
        async with pool.acquire() as conn:
            row1 = await conn.fetchrow(
                "SELECT embedding::text, embedding_text FROM positions WHERE id = $1",
                _db._uuid(IDS["pa"]),
            )

        await embed_position(IDS["pa"])
        async with pool.acquire() as conn:
            row2 = await conn.fetchrow(
                "SELECT embedding::text, embedding_text FROM positions WHERE id = $1",
                _db._uuid(IDS["pa"]),
            )

        assert row1["embedding"] == row2["embedding"]
        assert row1["embedding_text"] == row2["embedding_text"]


# =============================================================================
# Score ordering and threshold filtering
# =============================================================================

class TestScoreOrdering:
    async def test_results_ordered_by_score_descending(self, setup_db):
        """Suggestions must come back highest-score first."""
        await embed_candidate(IDS["ca"])
        await embed_candidate(IDS["cb"])
        await embed_position(IDS["pa"])
        await embed_position(IDS["pb"])

        results = await suggest_candidates_for_position(IDS["pa"], min_score=0)
        if len(results) >= 2:
            scores = [r["score"] for r in results]
            assert scores == sorted(scores, reverse=True)

    async def test_position_results_ordered_by_score(self, setup_db):
        """Position suggestions must come back highest-score first."""
        await embed_candidate(IDS["ca"])
        await embed_candidate(IDS["cb"])
        await embed_position(IDS["pa"])
        await embed_position(IDS["pb"])

        results = await suggest_positions_for_candidate(IDS["ca"], min_score=0)
        if len(results) >= 2:
            scores = [r["score"] for r in results]
            assert scores == sorted(scores, reverse=True)

    async def test_min_score_filters_low_results(self, setup_db):
        """Results below min_score should be excluded."""
        await embed_candidate(IDS["ca"])
        await embed_candidate(IDS["cb"])
        await embed_position(IDS["pa"])

        # Very high threshold should return nothing
        results = await suggest_candidates_for_position(IDS["pa"], min_score=0.99)
        assert results == []

    async def test_score_in_valid_range(self, setup_db):
        """Scores must be between 0 and 1."""
        await embed_candidate(IDS["cb"])
        await embed_position(IDS["pa"])

        results = await suggest_candidates_for_position(IDS["pa"], min_score=0)
        for r in results:
            assert 0 <= r["score"] <= 1


# =============================================================================
# Explanation grounding (no hallucination)
# =============================================================================

class TestExplanationGrounding:
    async def test_explanation_only_references_real_skills(self):
        """Explanation should not mention skills the candidate doesn't have."""
        explanation = await explain_match(CANDIDATE_A, POSITION_A)
        # Candidate A has: AWS, Kubernetes, Terraform, Docker, Python
        # Explanation should not invent technologies
        fake_skills = ["React", "Angular", "Java", "Ruby", "Scala", "C++", "Rust"]
        for skill in fake_skills:
            assert skill not in explanation, f"Hallucinated skill: {skill}"

    async def test_explanation_references_position_title(self):
        explanation = await explain_match(CANDIDATE_A, POSITION_A)
        assert "Senior DevOps Engineer" in explanation

    async def test_candidate_b_explanation_uses_real_overlap(self):
        """Candidate B's explanation for Position A should reference actual shared tech."""
        explanation = await explain_match(CANDIDATE_B, POSITION_A)
        assert "Jordan Sample" in explanation
        # B has: Azure, Docker, Terraform, Python, Bash
        # Position A tech: AWS, Kubernetes, Terraform, Python
        # Overlap: Terraform, Python
        assert "Terraform" in explanation or "Python" in explanation

    async def test_position_suggestion_includes_explanation(self, setup_db):
        """Position suggestions for a candidate should include explanation field."""
        await embed_candidate(IDS["ca"])
        await embed_position(IDS["pa"])
        await embed_position(IDS["pb"])

        results = await suggest_positions_for_candidate(IDS["ca"], min_score=0)
        for r in results:
            assert "explanation" in r
            assert isinstance(r["explanation"], str)
            assert len(r["explanation"]) > 0

    async def test_candidate_suggestion_has_no_explanation(self, setup_db):
        """Candidate suggestions for a position should NOT include explanation."""
        await embed_candidate(IDS["cb"])
        await embed_position(IDS["pa"])

        results = await suggest_candidates_for_position(IDS["pa"], min_score=0)
        for r in results:
            assert "explanation" not in r


# =============================================================================
# Suggestion exclusion and status filtering
# =============================================================================

class TestSuggestionFiltering:
    async def test_only_open_positions_suggested(self, setup_db):
        """Closed positions should never appear in candidate suggestions."""
        import db as _db

        await embed_candidate(IDS["ca"])
        await embed_position(IDS["pa"])
        await embed_position(IDS["pb"])

        # Close position B
        pool = await _db.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE positions SET status = 'closed' WHERE id = $1",
                _db._uuid(IDS["pb"]),
            )

        results = await suggest_positions_for_candidate(IDS["ca"], min_score=0)
        ids = [r["id"] for r in results]
        # Position B should be excluded (closed)
        assert IDS["pb"] not in ids

        # Restore
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE positions SET status = 'open' WHERE id = $1",
                _db._uuid(IDS["pb"]),
            )
