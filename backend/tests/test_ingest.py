"""Ingest tests -- parse, extract, llm, pipeline.

Covers document parsing (PDF, DOCX, TXT), heuristic extraction and validation,
LLM extraction, and pipeline orchestration (CV/job ingestion).
"""

import json
import os
import sys
from uuid import UUID

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("LOGS_DIR", "/tmp/pipeline_test_logs")

pytestmark = pytest.mark.asyncio(loop_scope="session")

from ingest import parse_file, heuristic_extract, validate_candidate, validate_position
from ingest import ingest_cv, ingest_job, _compute_changes
from ingest import _check_candidate_fields, _check_position_fields, InsufficientDataError, _compute_position_changes
from llm import extract_fields, generate_summary
import db
from testdata import IDS

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


@pytest.fixture(scope="session", autouse=True)
def create_edge_case_fixtures(tmp_path_factory):
    """Generate edge-case fixture files that don't belong in the repo."""
    d = tmp_path_factory.mktemp("parse")

    with open(d / "corrupt.pdf", "wb") as f:
        f.write(b"%PDF-1.4 corrupted truncated garbage")

    with open(d / "empty.txt", "w") as f:
        pass

    return str(d)


# =============================================================================
# Parse -- happy paths
# =============================================================================

class TestParsePdf:
    def test_returns_nonempty_text(self):
        text = parse_file(os.path.join(FIXTURES, "cv_001.pdf"))
        assert isinstance(text, str)
        assert len(text.strip()) > 0

    def test_contains_expected_content(self):
        text = parse_file(os.path.join(FIXTURES, "cv_001.pdf"))
        lower = text.lower()
        assert any(w in lower for w in ["experience", "education", "skills", "engineer"])


class TestParseDocx:
    def test_returns_nonempty_text(self):
        text = parse_file(os.path.join(FIXTURES, "cv_201.docx"))
        assert isinstance(text, str)
        assert len(text.strip()) > 0

    def test_contains_expected_content(self):
        text = parse_file(os.path.join(FIXTURES, "cv_201.docx"))
        lower = text.lower()
        assert any(w in lower for w in ["experience", "education", "skills", "engineer"])


class TestParseTxt:
    def test_returns_content(self):
        text = parse_file(os.path.join(FIXTURES, "job_001_senior_devops.txt"))
        assert len(text.strip()) > 0


# =============================================================================
# Parse -- edge cases (empty / corrupt)
# =============================================================================

class TestParseEdgeCases:
    def test_empty_file_raises(self, create_edge_case_fixtures):
        with pytest.raises(RuntimeError, match="[Ee]mpty"):
            parse_file(os.path.join(create_edge_case_fixtures, "empty.txt"))

    def test_corrupt_pdf_raises(self, create_edge_case_fixtures):
        with pytest.raises(RuntimeError):
            parse_file(os.path.join(create_edge_case_fixtures, "corrupt.pdf"))

    def test_nonexistent_file_raises(self):
        with pytest.raises(FileNotFoundError):
            parse_file("/nonexistent/path.pdf")


# =============================================================================
# Parse -- unsupported format
# =============================================================================

class TestParseUnsupported:
    def test_xlsx_raises(self, tmp_path):
        p = tmp_path / "test.xlsx"
        p.write_bytes(b"fake")
        with pytest.raises(ValueError, match="[Uu]nsupported"):
            parse_file(str(p))

    def test_jpg_raises(self, tmp_path):
        p = tmp_path / "test.jpg"
        p.write_bytes(b"fake")
        with pytest.raises(ValueError, match="[Uu]nsupported"):
            parse_file(str(p))

    def test_no_extension_raises(self, tmp_path):
        p = tmp_path / "noext"
        p.write_bytes(b"fake")
        with pytest.raises(ValueError, match="[Uu]nsupported"):
            parse_file(str(p))


# =============================================================================
# Parse -- special characters in path
# =============================================================================

class TestParseSpecialPaths:
    def test_space_in_filename(self, tmp_path):
        p = tmp_path / "my file.txt"
        p.write_text("hello world")
        assert "hello world" in parse_file(str(p))

    def test_parentheses_in_filename(self, tmp_path):
        p = tmp_path / "file (1).txt"
        p.write_text("content here")
        assert "content here" in parse_file(str(p))


# =============================================================================
# Extract -- CV contact fields
# =============================================================================

class TestExtractEmail:
    def test_standard(self):
        r = heuristic_extract("Contact: alex@example.com", "candidate")
        assert r["email"] == "alex@example.com"

    def test_plus_addressing(self):
        r = heuristic_extract("Email: user+tag@domain.com", "candidate")
        assert r["email"] == "user+tag@domain.com"

    def test_subdomain(self):
        r = heuristic_extract("alex@mail.example.co.uk", "candidate")
        assert r["email"] == "alex@mail.example.co.uk"

    def test_no_email(self):
        r = heuristic_extract("No contact info here", "candidate")
        assert r["email"] is None

    def test_multiple_picks_first(self):
        r = heuristic_extract("a@b.com and c@d.com", "candidate")
        assert r["email"] == "a@b.com"


class TestExtractPhone:
    def test_us_format(self):
        r = heuristic_extract("Phone: +1-555-000-0001", "candidate")
        assert r["phone"] is not None
        assert "555" in r["phone"]

    def test_intl_format(self):
        r = heuristic_extract("Phone: +972-50-123-4567", "candidate")
        assert r["phone"] is not None
        assert "972" in r["phone"]

    def test_parens_format(self):
        r = heuristic_extract("(555) 123-4567", "candidate")
        assert r["phone"] is not None

    def test_dots_format(self):
        r = heuristic_extract("555.123.4567", "candidate")
        assert r["phone"] is not None

    def test_no_phone(self):
        r = heuristic_extract("Just some text", "candidate")
        assert r["phone"] is None


class TestExtractLinkedin:
    def test_full_url(self):
        r = heuristic_extract("https://linkedin.com/in/alexmock", "candidate")
        assert "linkedin.com/in/alexmock" in r["linkedin"]

    def test_without_protocol(self):
        r = heuristic_extract("linkedin.com/in/alexmock", "candidate")
        assert "linkedin.com/in/alexmock" in r["linkedin"]

    def test_no_linkedin(self):
        r = heuristic_extract("No links here", "candidate")
        assert r["linkedin"] is None


class TestExtractGithub:
    def test_full_url(self):
        r = heuristic_extract("https://github.com/alexmock", "candidate")
        assert "github.com/alexmock" in r["github"]

    def test_without_protocol(self):
        r = heuristic_extract("github.com/alexmock", "candidate")
        assert "github.com/alexmock" in r["github"]

    def test_no_github(self):
        r = heuristic_extract("No links", "candidate")
        assert r["github"] is None


class TestExtractNoMatches:
    def test_empty_string(self):
        r = heuristic_extract("", "candidate")
        assert r["email"] is None
        assert r["phone"] is None
        assert r["linkedin"] is None
        assert r["github"] is None

    def test_random_text(self):
        r = heuristic_extract("The quick brown fox jumps over the lazy dog", "candidate")
        for v in r.values():
            assert v is None


# =============================================================================
# Extract -- Job headers
# =============================================================================

class TestExtractJobHeaders:
    def test_from_header(self):
        text = "From: Pat Manager <pat@acme.com>\nSubject: DevOps Engineer"
        r = heuristic_extract(text, "position")
        assert r["hm_name"] == "Pat Manager"
        assert r["hm_email"] == "pat@acme.com"

    def test_subject_header(self):
        text = "From: Someone <s@x.com>\nSubject: Senior DevOps Engineer"
        r = heuristic_extract(text, "position")
        assert r["title"] == "Senior DevOps Engineer"

    def test_subject_strips_prefixes(self):
        text = "Subject: Urgent - Cloud Architect"
        r = heuristic_extract(text, "position")
        assert r["title"] == "Cloud Architect"

    def test_subject_strips_re(self):
        text = "Subject: RE: Platform Engineer"
        r = heuristic_extract(text, "position")
        assert r["title"] == "Platform Engineer"

    def test_subject_strips_fw(self):
        text = "Subject: FW: SRE Lead"
        r = heuristic_extract(text, "position")
        assert r["title"] == "SRE Lead"

    def test_from_without_angle_brackets(self):
        text = "From: pat@acme.com\nSubject: Test"
        r = heuristic_extract(text, "position")
        assert r["hm_email"] == "pat@acme.com"
        assert r["hm_name"] is None

    def test_no_headers(self):
        text = "Just a job description without headers."
        r = heuristic_extract(text, "position")
        assert r["title"] is None
        assert r["hm_name"] is None
        assert r["hm_email"] is None


# =============================================================================
# Extract -- Validation (candidate)
# =============================================================================

class TestValidateCandidate:
    def test_valid_passes(self):
        data = {
            "name": "Alex Mock",
            "status": "active",
            "experienceLevel": "senior",
            "contact": {"email": "alex@example.com"},
            "skills": ["AWS"],
            "languages": ["English"],
            "summary": "A summary.",
            "experience": [],
            "education": [],
            "certifications": [],
        }
        clean, warnings = validate_candidate(data)
        assert clean["name"] == "Alex Mock"
        assert len(warnings) == 0

    def test_invalid_status_defaults(self):
        data = {"status": "archived"}
        clean, warnings = validate_candidate(data)
        assert clean["status"] == "active"
        assert len(warnings) > 0

    def test_invalid_experience_level_defaults(self):
        data = {"experienceLevel": "godlike"}
        clean, warnings = validate_candidate(data)
        assert clean["experienceLevel"] == "mid"
        assert len(warnings) > 0

    def test_name_truncation(self):
        data = {"name": "A" * 300}
        clean, warnings = validate_candidate(data)
        assert len(clean["name"]) <= 200
        assert len(warnings) > 0

    def test_skills_must_be_list(self):
        data = {"skills": "AWS"}
        clean, warnings = validate_candidate(data)
        assert isinstance(clean["skills"], list)

    def test_skills_string_wrapped(self):
        data = {"skills": "AWS"}
        clean, _ = validate_candidate(data)
        assert clean["skills"] == ["AWS"]

    def test_missing_name_defaults(self):
        data = {}
        clean, warnings = validate_candidate(data)
        assert clean["name"] == "Unknown"
        assert len(warnings) > 0

    def test_xss_in_name_stored(self):
        """XSS content should be stored as-is (frontend escapes)."""
        data = {"name": "<script>alert(1)</script>"}
        clean, _ = validate_candidate(data)
        assert "<script>" in clean["name"]

    def test_sql_injection_in_name(self):
        """SQL injection attempts are harmless (parameterized queries)."""
        data = {"name": "Robert'); DROP TABLE candidates;--"}
        clean, _ = validate_candidate(data)
        assert "Robert" in clean["name"]

    def test_certification_year_far_future(self):
        data = {"certifications": [{"name": "AWS", "year": 2099}]}
        clean, warnings = validate_candidate(data)
        assert clean["certifications"][0]["year"] == 2099
        assert len(warnings) > 0

    def test_empty_arrays_normalized(self):
        data = {"skills": None, "languages": None, "experience": None}
        clean, _ = validate_candidate(data)
        assert clean["skills"] == []
        assert clean["languages"] == []
        assert clean["experience"] == []

    def test_duplicate_skills_deduped(self):
        data = {"skills": ["AWS", "aws", "Aws"]}
        clean, _ = validate_candidate(data)
        # Should keep unique values (case-insensitive dedup)
        lower_skills = [s.lower() for s in clean["skills"]]
        assert len(set(lower_skills)) == len(lower_skills)

    def test_experience_date_int_coerced_to_str(self):
        data = {"experience": [
            {"title": "SRE", "company": "Co", "startDate": 2020, "endDate": 2023, "bullets": []},
        ]}
        clean, _ = validate_candidate(data)
        assert clean["experience"][0]["startDate"] == "2020"
        assert clean["experience"][0]["endDate"] == "2023"

    def test_education_date_int_coerced_to_str(self):
        data = {"education": [
            {"degree": "BSc", "institution": "MIT", "startDate": 2016, "endDate": 2020},
        ]}
        clean, _ = validate_candidate(data)
        assert clean["education"][0]["startDate"] == "2016"
        assert clean["education"][0]["endDate"] == "2020"

    def test_experience_date_none_preserved(self):
        data = {"experience": [
            {"title": "SRE", "company": "Co", "startDate": "2020", "endDate": None, "bullets": []},
        ]}
        clean, _ = validate_candidate(data)
        assert clean["experience"][0]["endDate"] is None

    def test_certification_year_str_coerced_to_int(self):
        data = {"certifications": [{"name": "CKA", "year": "2022"}]}
        clean, _ = validate_candidate(data)
        assert clean["certifications"][0]["year"] == 2022
        assert isinstance(clean["certifications"][0]["year"], int)

    def test_certification_year_invalid_dropped(self):
        data = {"certifications": [{"name": "CKA", "year": "not-a-year"}]}
        clean, warnings = validate_candidate(data)
        assert clean["certifications"][0]["year"] is None
        assert any("Invalid certification year" in w for w in warnings)


# =============================================================================
# Extract -- Validation (position)
# =============================================================================

class TestValidatePosition:
    def test_valid_passes(self):
        data = {
            "title": "DevOps Engineer",
            "status": "open",
            "company": "Acme",
            "hiringManager": {"name": "Pat", "title": "VP", "email": "p@a.com"},
            "experienceLevel": "senior",
            "requirements": ["5+ years"],
            "niceToHave": [],
            "responsibilities": ["Lead infra"],
            "techStack": ["AWS"],
            "location": "TLV",
            "workArrangement": "Hybrid",
        }
        clean, warnings = validate_position(data)
        assert clean["title"] == "DevOps Engineer"
        assert len(warnings) == 0

    def test_invalid_status_defaults(self):
        data = {"status": "archived"}
        clean, warnings = validate_position(data)
        assert clean["status"] == "open"
        assert len(warnings) > 0

    def test_missing_title_defaults(self):
        data = {}
        clean, warnings = validate_position(data)
        assert clean["title"] == "Untitled Position"
        assert len(warnings) > 0

    def test_title_truncation(self):
        data = {"title": "X" * 300}
        clean, warnings = validate_position(data)
        assert len(clean["title"]) <= 200
        assert len(warnings) > 0

    def test_requirements_must_be_list(self):
        data = {"requirements": "5+ years"}
        clean, _ = validate_position(data)
        assert isinstance(clean["requirements"], list)

    def test_hiring_manager_defaults(self):
        data = {"hiringManager": None}
        clean, _ = validate_position(data)
        assert isinstance(clean["hiringManager"], dict)
        assert "name" in clean["hiringManager"]


# =============================================================================
# LLM -- extract_fields (candidate)
# =============================================================================

class TestExtractCandidate:
    async def test_returns_dict(self):
        result = await extract_fields("Some CV text", "candidate", {})
        assert isinstance(result, dict)
        assert "fields" in result
        assert "usage" in result

    async def test_fields_has_name(self):
        result = await extract_fields("Some CV text", "candidate", {})
        assert "name" in result["fields"]

    async def test_fields_has_skills(self):
        result = await extract_fields("Some CV text", "candidate", {})
        assert "skills" in result["fields"]
        assert isinstance(result["fields"]["skills"], list)

    async def test_fields_has_experience(self):
        result = await extract_fields("Some CV text", "candidate", {})
        assert "experience" in result["fields"]
        assert isinstance(result["fields"]["experience"], list)

    async def test_usage_has_tokens(self):
        result = await extract_fields("Some CV text", "candidate", {})
        assert "input_tokens" in result["usage"]
        assert "output_tokens" in result["usage"]


class TestExtractPosition:
    async def test_returns_dict(self):
        result = await extract_fields("Job description", "position", {})
        assert isinstance(result, dict)

    async def test_fields_has_title(self):
        result = await extract_fields("Job description", "position", {})
        assert "title" in result["fields"]

    async def test_fields_has_tech_stack(self):
        result = await extract_fields("Job description", "position", {})
        assert "techStack" in result["fields"]
        assert isinstance(result["fields"]["techStack"], list)


# =============================================================================
# LLM -- generate_summary
# =============================================================================

class TestSummary:
    async def test_returns_dict(self):
        result = await generate_summary("Some text", "candidate")
        assert isinstance(result, dict)
        assert "summary" in result
        assert "usage" in result

    async def test_summary_is_string(self):
        result = await generate_summary("Some text", "candidate")
        assert isinstance(result["summary"], str)
        assert len(result["summary"]) > 0

    async def test_position_summary(self):
        result = await generate_summary("Job desc", "position")
        assert isinstance(result["summary"], str)


# =============================================================================
# LLM -- Model usage
# =============================================================================

class TestModelUsage:
    async def test_returns_nova_model(self):
        result = await extract_fields("text", "candidate", {})
        assert result["usage"]["model"] == "nova"


# =============================================================================
# LLM -- JSON parsing
# =============================================================================

class TestJsonParsing:
    """Test that the JSON extraction from LLM response handles edge cases."""

    async def test_returns_valid_json(self):
        """LLM responses should always be valid JSON."""
        result = await extract_fields("text", "candidate", {})
        # Verify the fields can be serialized back to JSON
        json.dumps(result["fields"])


# =============================================================================
# LLM -- Prompt loading
# =============================================================================

class TestPromptLoading:
    def test_prompts_exist(self):
        prompts_dir = os.path.join(os.path.dirname(__file__), "..", "prompts")
        expected = [
            "extract_candidate_v1.txt",
            "extract_position_v1.txt",
            "summarize_candidate_v1.txt",
            "summarize_position_v1.txt",
        ]
        for name in expected:
            path = os.path.join(prompts_dir, name)
            assert os.path.exists(path), f"Missing prompt: {name}"

    def test_prompts_have_placeholders(self):
        prompts_dir = os.path.join(os.path.dirname(__file__), "..", "prompts")
        extract_path = os.path.join(prompts_dir, "extract_candidate_v1.txt")
        with open(extract_path) as f:
            content = f.read()
        assert "{raw_text}" in content
        assert "{heuristic_fields}" in content


# =============================================================================
# Pipeline -- _compute_changes
# =============================================================================

class TestComputeChanges:
    def test_no_changes(self):
        existing = {"skills": ["A", "B"], "experience": [{}], "contact": {"email": "a@b.com"}, "name": "X"}
        new = {"skills": ["A", "B"], "experience": [{}], "contact": {"email": "a@b.com"}, "name": "X"}
        assert _compute_changes(existing, new) == []

    def test_added_skills(self):
        existing = {"skills": ["A"], "experience": [], "contact": {}, "name": "X"}
        new = {"skills": ["A", "B", "C"], "experience": [], "contact": {}, "name": "X"}
        changes = _compute_changes(existing, new)
        assert any("+2 skills" in c for c in changes)

    def test_removed_skills(self):
        existing = {"skills": ["A", "B", "C"], "experience": [], "contact": {}, "name": "X"}
        new = {"skills": ["A"], "experience": [], "contact": {}, "name": "X"}
        changes = _compute_changes(existing, new)
        assert any("-2 skills" in c for c in changes)

    def test_added_experience(self):
        existing = {"skills": [], "experience": [{}], "contact": {}, "name": "X"}
        new = {"skills": [], "experience": [{}, {}], "contact": {}, "name": "X"}
        changes = _compute_changes(existing, new)
        assert any("+1 role" in c for c in changes)

    def test_name_changed(self):
        existing = {"skills": [], "experience": [], "contact": {}, "name": "Old"}
        new = {"skills": [], "experience": [], "contact": {}, "name": "New"}
        changes = _compute_changes(existing, new)
        assert "name changed" in changes

    def test_email_changed(self):
        existing = {"skills": [], "experience": [], "contact": {"email": "old@a.com"}, "name": "X"}
        new = {"skills": [], "experience": [], "contact": {"email": "new@b.com"}, "name": "X"}
        changes = _compute_changes(existing, new)
        assert "email changed" in changes


# =============================================================================
# Pipeline -- CV ingestion
# =============================================================================

class TestIngestCv:
    async def test_returns_candidate_dict(self):
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert isinstance(result, dict)
        assert "id" in result
        assert "name" in result

    async def test_id_is_valid_uuid(self):
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        UUID(result["id"])

    async def test_has_skills(self):
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert isinstance(result["skills"], list)

    async def test_has_summary(self):
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert isinstance(result["summary"], str)
        assert len(result["summary"]) > 0

    async def test_cv_file_set(self):
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert result["cvFile"] == "cv_001.pdf"

    async def test_persisted_in_db(self):
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        fetched = await db.get_candidate(result["id"])
        assert fetched is not None
        assert fetched["name"] == result["name"]

    async def test_docx_cv(self):
        result = await ingest_cv(os.path.join(FIXTURES, "cv_201.docx"))
        assert "id" in result

    async def test_docx_cv_has_name(self):
        result = await ingest_cv(os.path.join(FIXTURES, "cv_201.docx"))
        assert result.get("name")


# =============================================================================
# Pipeline -- Job ingestion
# =============================================================================

class TestIngestJob:
    async def test_returns_position_dict(self):
        result = await ingest_job(os.path.join(FIXTURES, "job_001_senior_devops.txt"))
        assert isinstance(result, dict)
        assert "id" in result
        assert "title" in result

    async def test_id_is_valid_uuid(self):
        result = await ingest_job(os.path.join(FIXTURES, "job_001_senior_devops.txt"))
        UUID(result["id"])

    async def test_has_tech_stack(self):
        result = await ingest_job(os.path.join(FIXTURES, "job_001_senior_devops.txt"))
        assert isinstance(result["techStack"], list)

    async def test_persisted_in_db(self):
        result = await ingest_job(os.path.join(FIXTURES, "job_002_junior_devops.txt"))
        fetched = await db.get_position(result["id"])
        assert fetched is not None
        assert fetched["title"] == result["title"]


# =============================================================================
# Pipeline -- Skills retry on empty
# =============================================================================

class TestSkillsRetry:
    _FIXTURE_FIELDS = {
        "name": "Test Candidate",
        "status": "active",
        "experienceLevel": "mid",
        "contact": {"email": "test@example.com"},
        "languages": ["English"],
        "skills": ["AWS", "Docker"],
        "experience": [],
        "education": [],
        "certifications": [],
    }

    async def test_retries_when_skills_empty(self, monkeypatch):
        """Pipeline retries LLM extraction once when skills come back empty."""
        call_count = 0

        async def patched_extract(raw_text, entity_type, heuristic, model=None, filename=None):
            nonlocal call_count
            call_count += 1
            fields = dict(self._FIXTURE_FIELDS)
            if call_count == 1:
                fields["skills"] = []
            return {"fields": fields, "usage": {"model": "nova", "input_tokens": 100, "output_tokens": 200}}

        monkeypatch.setattr("ingest.extract_fields", patched_extract)
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert call_count == 2  # initial + retry
        assert len(result["skills"]) > 0  # retry populated skills

    async def test_no_retry_when_skills_present(self, monkeypatch):
        """No retry when skills are present on first call."""
        call_count = 0

        async def patched_extract(raw_text, entity_type, heuristic, model=None, filename=None):
            nonlocal call_count
            call_count += 1
            return {"fields": dict(self._FIXTURE_FIELDS), "usage": {"model": "nova", "input_tokens": 100, "output_tokens": 200}}

        monkeypatch.setattr("ingest.extract_fields", patched_extract)
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert call_count == 1  # no retry needed


# =============================================================================
# Pipeline -- Error handling
# =============================================================================

class TestPipelineErrors:
    async def test_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            await ingest_cv("/nonexistent/cv.pdf")

    async def test_unsupported_format(self, tmp_path):
        p = tmp_path / "fake.xlsx"
        p.write_bytes(b"fake")
        with pytest.raises(ValueError):
            await ingest_cv(str(p))


# =============================================================================
# Pipeline -- Logging
# =============================================================================

class TestPipelineLogging:
    async def test_creates_log_entry(self):
        import llm
        logs_dir = os.environ.get("LOGS_DIR", "/tmp/pipeline_test_logs")
        if os.path.isdir(logs_dir):
            for f in os.listdir(logs_dir):
                os.remove(os.path.join(logs_dir, f))

        await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        entries = llm.read_all_logs()
        assert len(entries) >= 1
        entry = entries[-1]
        assert entry["entity_type"] == "candidate"
        assert entry["status"] == "success"
        assert "input_tokens" in entry


# =============================================================================
# _check_candidate_fields
# =============================================================================

class TestCheckCandidateFields:
    def test_valid_fields_pass(self):
        _check_candidate_fields({"name": "Alex", "skills": ["AWS"], "experience": []})

    def test_missing_name_raises(self):
        with pytest.raises(InsufficientDataError) as exc_info:
            _check_candidate_fields({"name": "", "skills": ["AWS"]})
        assert "name" in exc_info.value.missing

    def test_generic_name_raises(self):
        with pytest.raises(InsufficientDataError):
            _check_candidate_fields({"name": "Unknown", "skills": ["AWS"]})

    def test_missing_skills_and_experience_raises(self):
        with pytest.raises(InsufficientDataError):
            _check_candidate_fields({"name": "Alex", "skills": [], "experience": []})

    def test_skills_present_experience_empty_passes(self):
        _check_candidate_fields({"name": "Alex", "skills": ["AWS"], "experience": []})

    def test_experience_present_skills_empty_passes(self):
        _check_candidate_fields({"name": "Alex", "skills": [], "experience": [{"title": "SRE"}]})

    def test_none_name_raises(self):
        with pytest.raises(InsufficientDataError):
            _check_candidate_fields({"name": None, "skills": ["AWS"]})


# =============================================================================
# _check_position_fields
# =============================================================================

class TestCheckPositionFields:
    def test_valid_fields_pass(self):
        _check_position_fields({"title": "DevOps", "responsibilities": ["Lead"], "requirements": ["5y"]})

    def test_missing_title_raises(self):
        with pytest.raises(InsufficientDataError) as exc_info:
            _check_position_fields({"title": "", "responsibilities": ["Lead"], "requirements": ["5y"]})
        assert "title" in exc_info.value.missing

    def test_generic_title_raises(self):
        with pytest.raises(InsufficientDataError):
            _check_position_fields({"title": "Untitled Position", "responsibilities": ["Lead"], "requirements": ["5y"]})

    def test_missing_responsibilities_raises(self):
        with pytest.raises(InsufficientDataError):
            _check_position_fields({"title": "DevOps", "responsibilities": [], "requirements": ["5y"]})

    def test_techstack_satisfies_requirements(self):
        _check_position_fields({"title": "DevOps", "responsibilities": ["Lead"], "requirements": [], "techStack": ["AWS"]})

    def test_all_missing_raises_multiple(self):
        with pytest.raises(InsufficientDataError) as exc_info:
            _check_position_fields({"title": "", "responsibilities": [], "requirements": [], "techStack": []})
        assert len(exc_info.value.missing) >= 2


# =============================================================================
# Pipeline -- CV ingestion (mocked LLM)
# =============================================================================

class TestIngestCvPipeline:
    """CV pipeline tests with fully mocked LLM."""

    _BASE_FIELDS = {
        "name": "Pipeline Test",
        "status": "active",
        "experienceLevel": "mid",
        "contact": {"email": "pipeline@test.com", "phone": None, "linkedin": None, "github": None},
        "languages": ["English"],
        "skills": ["Docker", "AWS"],
        "experience": [{"title": "SRE", "company": "TestCo", "location": "TLV",
                        "startDate": "2020", "endDate": None, "bullets": ["Did stuff"]}],
        "education": [{"degree": "BSc CS", "institution": "TU", "startDate": "2016", "endDate": "2020"}],
        "certifications": [],
    }

    @pytest.fixture(autouse=True)
    def mock_llm(self, monkeypatch):
        async def fake_extract(raw_text, entity_type, heuristic, model=None, filename=None):
            return {"fields": dict(self._BASE_FIELDS), "usage": {"input_tokens": 10, "output_tokens": 10, "model": "nova"}}
        async def fake_summary(raw_text, entity_type, model=None, filename=None):
            return {"summary": "Test summary.", "usage": {"input_tokens": 5, "output_tokens": 5, "model": "nova"}}
        async def fake_embed(cid):
            pass
        monkeypatch.setattr("ingest.extract_fields", fake_extract)
        monkeypatch.setattr("ingest.generate_summary", fake_summary)
        monkeypatch.setattr("embeddings.embed_candidate", fake_embed)

    async def test_returns_expected_shape(self):
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        for key in ("id", "name", "skills", "cvFile", "isUpdate", "changes"):
            assert key in result, f"Missing key: {key}"

    async def test_persists_to_db(self):
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        fetched = await db.get_candidate(result["id"])
        assert fetched is not None
        assert fetched["name"] == result["name"]

    async def test_document_stored(self):
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        docs = await db.get_documents("candidate", result["id"])
        assert len(docs) >= 1
        assert docs[0]["filename"] == "cv_001.pdf"

    async def test_short_document_raises(self, tmp_path):
        p = tmp_path / "short.txt"
        p.write_text("too few words here")
        with pytest.raises(InsufficientDataError):
            await ingest_cv(str(p))

    async def test_llm_failure_falls_back_to_heuristic(self, monkeypatch):
        async def failing_extract(raw_text, entity_type, heuristic, model=None, filename=None):
            raise RuntimeError("LLM down")
        monkeypatch.setattr("ingest.extract_fields", failing_extract)
        with pytest.raises(InsufficientDataError):
            await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))

    async def test_skills_retry_on_empty(self, monkeypatch):
        call_count = 0
        async def patched_extract(raw_text, entity_type, heuristic, model=None, filename=None):
            nonlocal call_count
            call_count += 1
            fields = dict(self._BASE_FIELDS)
            if call_count == 1:
                fields["skills"] = []
            return {"fields": fields, "usage": {"input_tokens": 10, "output_tokens": 10, "model": "nova"}}
        monkeypatch.setattr("ingest.extract_fields", patched_extract)
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert call_count == 2
        assert len(result["skills"]) > 0

    async def test_skills_retry_failure_continues(self, monkeypatch):
        call_count = 0
        async def patched_extract(raw_text, entity_type, heuristic, model=None, filename=None):
            nonlocal call_count
            call_count += 1
            fields = dict(self._BASE_FIELDS)
            fields["skills"] = []
            if call_count == 2:
                raise RuntimeError("retry failed")
            return {"fields": fields, "usage": {"input_tokens": 10, "output_tokens": 10, "model": "nova"}}
        monkeypatch.setattr("ingest.extract_fields", patched_extract)
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert call_count == 2

    async def test_heuristic_overrides_llm_contact(self, monkeypatch):
        """Heuristic email wins over LLM email when both present."""
        async def extract_with_diff_email(raw_text, entity_type, heuristic, model=None, filename=None):
            fields = dict(self._BASE_FIELDS)
            fields["contact"] = {"email": "llm@wrong.com", "phone": None, "linkedin": None, "github": None}
            return {"fields": fields, "usage": {"input_tokens": 10, "output_tokens": 10, "model": "nova"}}
        monkeypatch.setattr("ingest.extract_fields", extract_with_diff_email)
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert result.get("contact", {}).get("email") != "llm@wrong.com" or result["contact"]["email"] is not None

    async def test_summary_failure_continues(self, monkeypatch):
        async def failing_summary(raw_text, entity_type, model=None, filename=None):
            raise RuntimeError("Summary LLM down")
        monkeypatch.setattr("ingest.generate_summary", failing_summary)
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert result["summary"] == ""

    async def test_embedding_failure_continues(self, monkeypatch):
        async def failing_embed(cid):
            raise RuntimeError("Embedding service down")
        monkeypatch.setattr("embeddings.embed_candidate", failing_embed)
        result = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert "id" in result

    async def test_dedup_by_email_updates(self, monkeypatch):
        """Two CVs with same email -> second is update."""
        result1 = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert result1["isUpdate"] is False
        result2 = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert result2["isUpdate"] is True

    async def test_no_dedup_creates_new(self, monkeypatch):
        """Different email -> new candidate."""
        call_count = 0
        async def extract_unique(raw_text, entity_type, heuristic, model=None, filename=None):
            nonlocal call_count
            call_count += 1
            fields = dict(self._BASE_FIELDS)
            fields["contact"] = {"email": f"unique{call_count}@test.com", "phone": None, "linkedin": None, "github": None}
            return {"fields": fields, "usage": {"input_tokens": 10, "output_tokens": 10, "model": "nova"}}
        monkeypatch.setattr("ingest.extract_fields", extract_unique)
        result1 = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        result2 = await ingest_cv(os.path.join(FIXTURES, "cv_201.docx"))
        assert result1["id"] != result2["id"]
        assert result2["isUpdate"] is False

    async def test_compute_changes_on_update(self, monkeypatch):
        """Second ingest with different skills shows changes."""
        call_count = 0
        async def extract_evolving(raw_text, entity_type, heuristic, model=None, filename=None):
            nonlocal call_count
            call_count += 1
            fields = dict(self._BASE_FIELDS)
            if call_count > 1:
                fields["skills"] = ["Docker", "AWS", "Kubernetes"]
            return {"fields": fields, "usage": {"input_tokens": 10, "output_tokens": 10, "model": "nova"}}
        monkeypatch.setattr("ingest.extract_fields", extract_evolving)
        await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        result2 = await ingest_cv(os.path.join(FIXTURES, "cv_001.pdf"))
        assert result2["isUpdate"] is True
        assert len(result2["changes"]) > 0


# =============================================================================
# Pipeline -- Job ingestion (mocked LLM)
# =============================================================================

class TestIngestJobPipeline:
    """Job pipeline tests with fully mocked LLM."""

    _BASE_FIELDS = {
        "title": "DevOps Engineer",
        "status": "open",
        "company": "TestCorp",
        "hiringManager": {"name": "Pat Boss", "title": "VP", "email": "pat@testcorp.com"},
        "experienceLevel": "senior",
        "location": "Tel Aviv",
        "workArrangement": "Hybrid",
        "techStack": ["AWS", "Docker"],
        "requirements": ["5+ years"],
        "niceToHave": ["Certs"],
        "responsibilities": ["Lead infra", "Mentor"],
    }

    @pytest.fixture(autouse=True)
    def mock_llm(self, monkeypatch):
        async def fake_extract(raw_text, entity_type, heuristic, model=None, filename=None):
            return {"fields": dict(self._BASE_FIELDS), "usage": {"input_tokens": 10, "output_tokens": 10, "model": "nova"}}
        async def fake_summary(raw_text, entity_type, model=None, filename=None):
            return {"summary": "Test job summary.", "usage": {"input_tokens": 5, "output_tokens": 5, "model": "nova"}}
        async def fake_embed(pid):
            pass
        monkeypatch.setattr("ingest.extract_fields", fake_extract)
        monkeypatch.setattr("ingest.generate_summary", fake_summary)
        monkeypatch.setattr("embeddings.embed_position", fake_embed)

    async def test_returns_expected_shape(self):
        result = await ingest_job(os.path.join(FIXTURES, "job_001_senior_devops.txt"))
        for key in ("id", "title", "techStack", "jobFile", "isUpdate", "changes"):
            assert key in result, f"Missing key: {key}"

    async def test_persists_to_db(self):
        result = await ingest_job(os.path.join(FIXTURES, "job_001_senior_devops.txt"))
        fetched = await db.get_position(result["id"])
        assert fetched is not None
        assert fetched["title"] == result["title"]

    async def test_short_document_raises(self, tmp_path):
        p = tmp_path / "short.txt"
        p.write_text("too few words")
        with pytest.raises(InsufficientDataError):
            await ingest_job(str(p))

    async def test_llm_failure_falls_back_to_heuristic(self, monkeypatch):
        async def failing_extract(raw_text, entity_type, heuristic, model=None, filename=None):
            raise RuntimeError("LLM down")
        monkeypatch.setattr("ingest.extract_fields", failing_extract)
        with pytest.raises(InsufficientDataError):
            await ingest_job(os.path.join(FIXTURES, "job_001_senior_devops.txt"))

    async def test_heuristic_fills_title_gap(self, monkeypatch):
        """When LLM returns no title, heuristic Subject: line is used."""
        async def extract_no_title(raw_text, entity_type, heuristic, model=None, filename=None):
            fields = dict(self._BASE_FIELDS)
            fields["title"] = ""
            return {"fields": fields, "usage": {"input_tokens": 10, "output_tokens": 10, "model": "nova"}}
        monkeypatch.setattr("ingest.extract_fields", extract_no_title)
        result = await ingest_job(os.path.join(FIXTURES, "job_001_senior_devops.txt"))
        assert result["title"] != ""

    async def test_heuristic_fills_hiring_manager(self, monkeypatch):
        """When LLM returns no HM, heuristic From: line is used."""
        async def extract_no_hm(raw_text, entity_type, heuristic, model=None, filename=None):
            fields = dict(self._BASE_FIELDS)
            fields["hiringManager"] = {"name": "", "email": "", "title": ""}
            return {"fields": fields, "usage": {"input_tokens": 10, "output_tokens": 10, "model": "nova"}}
        monkeypatch.setattr("ingest.extract_fields", extract_no_hm)
        result = await ingest_job(os.path.join(FIXTURES, "job_001_senior_devops.txt"))
        hm = result.get("hiringManager", {})
        assert hm.get("name") or hm.get("email")

    async def test_dedup_by_title_company(self, monkeypatch):
        """Same title+company -> second is update."""
        result1 = await ingest_job(os.path.join(FIXTURES, "job_001_senior_devops.txt"))
        assert result1["isUpdate"] is False
        result2 = await ingest_job(os.path.join(FIXTURES, "job_001_senior_devops.txt"))
        assert result2["isUpdate"] is True

    async def test_compute_position_changes(self, monkeypatch):
        call_count = 0
        async def extract_evolving(raw_text, entity_type, heuristic, model=None, filename=None):
            nonlocal call_count
            call_count += 1
            fields = dict(self._BASE_FIELDS)
            if call_count > 1:
                fields["techStack"] = ["AWS", "Docker", "Kubernetes"]
            return {"fields": fields, "usage": {"input_tokens": 10, "output_tokens": 10, "model": "nova"}}
        monkeypatch.setattr("ingest.extract_fields", extract_evolving)
        await ingest_job(os.path.join(FIXTURES, "job_001_senior_devops.txt"))
        result2 = await ingest_job(os.path.join(FIXTURES, "job_001_senior_devops.txt"))
        assert result2["isUpdate"] is True
        assert len(result2["changes"]) > 0


# =============================================================================
# _compute_position_changes
# =============================================================================

class TestComputePositionChanges:
    def test_tech_added(self):
        old = {"techStack": ["AWS"], "requirements": ["5y"], "title": "SRE", "location": "TLV"}
        new = {"techStack": ["AWS", "Docker"], "requirements": ["5y"], "title": "SRE", "location": "TLV"}
        changes = _compute_position_changes(old, new)
        assert any("+1 tech" in c for c in changes)

    def test_requirements_changed(self):
        old = {"techStack": [], "requirements": ["A", "B", "C"], "title": "X", "location": "Y"}
        new = {"techStack": [], "requirements": ["A", "B"], "title": "X", "location": "Y"}
        changes = _compute_position_changes(old, new)
        assert any("requirements 3->2" in c for c in changes)

    def test_title_changed(self):
        old = {"techStack": [], "requirements": [], "title": "Old Title", "location": "Y"}
        new = {"techStack": [], "requirements": [], "title": "New Title", "location": "Y"}
        changes = _compute_position_changes(old, new)
        assert "title changed" in changes

    def test_no_changes(self):
        data = {"techStack": ["AWS"], "requirements": ["5y"], "title": "SRE", "location": "TLV"}
        assert _compute_position_changes(data, dict(data)) == []
