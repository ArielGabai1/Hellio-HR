"""Chat tests -- SQL validation, SQL extraction, answer hallucination detection.

Tests the chat module's safety and correctness functions directly (no HTTP).
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

pytestmark = pytest.mark.asyncio(loop_scope="session")

from chat import validate_sql, validate_answer, _extract_sql
from chat import _format_history, _format_results, execute_readonly, MAX_HISTORY


# =============================================================================
# chat.py -- SQL validation
# =============================================================================

class TestValidateSql:
    def test_valid_select(self):
        assert validate_sql("SELECT * FROM candidates") == "SELECT * FROM candidates LIMIT 50"

    def test_strips_markdown_fences(self):
        sql = "```sql\nSELECT * FROM candidates\n```"
        assert validate_sql(sql) == "SELECT * FROM candidates LIMIT 50"

    def test_strips_trailing_semicolon(self):
        assert validate_sql("SELECT * FROM candidates;") == "SELECT * FROM candidates LIMIT 50"

    def test_rejects_insert(self):
        with pytest.raises(ValueError):
            validate_sql("INSERT INTO candidates (name) VALUES ('x')")

    def test_rejects_update(self):
        with pytest.raises(ValueError):
            validate_sql("UPDATE candidates SET name='x'")

    def test_rejects_delete(self):
        with pytest.raises(ValueError):
            validate_sql("DELETE FROM candidates")

    def test_rejects_drop(self):
        with pytest.raises(ValueError):
            validate_sql("DROP TABLE candidates")

    def test_rejects_alter(self):
        with pytest.raises(ValueError):
            validate_sql("ALTER TABLE candidates ADD COLUMN x TEXT")

    def test_rejects_create(self):
        with pytest.raises(ValueError):
            validate_sql("CREATE TABLE evil (id INT)")

    def test_rejects_truncate(self):
        with pytest.raises(ValueError):
            validate_sql("TRUNCATE candidates")

    def test_rejects_grant(self):
        with pytest.raises(ValueError):
            validate_sql("GRANT ALL ON candidates TO public")

    def test_rejects_revoke(self):
        with pytest.raises(ValueError):
            validate_sql("REVOKE ALL ON candidates FROM public")

    def test_rejects_execute(self):
        with pytest.raises(ValueError):
            validate_sql("EXECUTE my_function()")

    def test_rejects_explain(self):
        with pytest.raises(ValueError, match="must start with SELECT"):
            validate_sql("EXPLAIN SELECT * FROM candidates")

    def test_rejects_empty(self):
        with pytest.raises(ValueError):
            validate_sql("")

    def test_rejects_whitespace_only(self):
        with pytest.raises(ValueError):
            validate_sql("   ")

    def test_auto_appends_limit(self):
        result = validate_sql("SELECT * FROM candidates")
        assert "LIMIT 50" in result

    def test_preserves_existing_limit(self):
        result = validate_sql("SELECT * FROM candidates LIMIT 10")
        assert result.count("LIMIT") == 1
        assert "LIMIT 10" in result

    def test_case_insensitive_select(self):
        result = validate_sql("select * from candidates")
        assert "candidates" in result

    def test_rejects_semicolon_injection(self):
        with pytest.raises(ValueError, match="semicolon"):
            validate_sql("SELECT 1; DROP TABLE candidates")

    def test_rejects_union_to_users(self):
        with pytest.raises(ValueError, match="users"):
            validate_sql("SELECT * FROM candidates UNION ALL SELECT * FROM users")

    def test_rejects_information_schema(self):
        with pytest.raises(ValueError, match="table"):
            validate_sql("SELECT * FROM information_schema.tables")

    def test_rejects_pg_catalog(self):
        with pytest.raises(ValueError, match="table"):
            validate_sql("SELECT * FROM pg_catalog.pg_tables")

    def test_rejects_comment_injection(self):
        with pytest.raises(ValueError, match="comment"):
            validate_sql("SELECT * FROM candidates -- DROP TABLE")

    def test_rejects_block_comment(self):
        with pytest.raises(ValueError, match="comment"):
            validate_sql("SELECT * FROM candidates /* evil */")

    def test_rejects_pg_sleep(self):
        with pytest.raises(ValueError, match="function"):
            validate_sql("SELECT pg_sleep(10)")

    def test_rejects_into_outfile(self):
        with pytest.raises(ValueError, match="INTO"):
            validate_sql("SELECT * FROM candidates INTO OUTFILE '/tmp/x'")

    def test_rejects_very_long_query(self):
        with pytest.raises(ValueError, match="too long"):
            validate_sql("SELECT " + "x" * 2001)

    def test_rejects_users_table(self):
        with pytest.raises(ValueError, match="users"):
            validate_sql("SELECT * FROM users")

    def test_allows_candidates_table(self):
        result = validate_sql("SELECT * FROM candidates")
        assert "candidates" in result

    def test_allows_positions_table(self):
        result = validate_sql("SELECT * FROM positions")
        assert "positions" in result

    def test_allows_join_query(self):
        sql = "SELECT c.name, cs.skill FROM candidates c JOIN candidate_skills cs ON c.id = cs.candidate_id"
        result = validate_sql(sql)
        assert "candidates" in result

    def test_multiline_sql(self):
        sql = "SELECT *\nFROM candidates\nWHERE status = 'active'"
        result = validate_sql(sql)
        assert "candidates" in result

    def test_strips_whitespace(self):
        result = validate_sql("  \t SELECT * FROM candidates  \t ")
        assert result.startswith("SELECT")

    def test_rejects_copy(self):
        with pytest.raises(ValueError):
            validate_sql("COPY candidates TO '/tmp/dump'")

    def test_rejects_set(self):
        with pytest.raises(ValueError):
            validate_sql("SET log_statement = 'all'")

    def test_rejects_vacuum(self):
        with pytest.raises(ValueError):
            validate_sql("VACUUM candidates")

    def test_rejects_current_setting(self):
        with pytest.raises(ValueError):
            validate_sql("SELECT current_setting('log_statement') FROM candidates")

    def test_rejects_set_config(self):
        with pytest.raises(ValueError):
            validate_sql("SELECT set_config('log_statement', 'all', false) FROM candidates")

    def test_rejects_pg_terminate_backend(self):
        with pytest.raises(ValueError):
            validate_sql("SELECT pg_terminate_backend(123) FROM candidates")

    def test_rejects_quoted_users_table(self):
        with pytest.raises(ValueError, match="users"):
            validate_sql('SELECT * FROM "users"')

    def test_allows_quoted_candidates(self):
        result = validate_sql('SELECT * FROM "candidates"')
        assert "candidates" in result


class TestExtractSql:
    def test_pure_sql_unchanged(self):
        raw = "SELECT * FROM candidates"
        assert _extract_sql(raw) == "SELECT * FROM candidates"

    def test_extracts_from_text_preamble(self):
        raw = "Here is the query:\nSELECT * FROM candidates WHERE status = 'active'"
        assert _extract_sql(raw).startswith("SELECT")
        assert "candidates" in _extract_sql(raw)

    def test_extracts_from_markdown_fences(self):
        raw = "Let me help\n```sql\nSELECT name FROM candidates\n```"
        result = _extract_sql(raw)
        assert result.startswith("SELECT")
        assert "```" not in result

    def test_extracts_from_text_after_sql(self):
        raw = "Here is the SQL:\nSELECT * FROM candidates\nThis returns all candidates."
        result = _extract_sql(raw)
        assert result.startswith("SELECT")

    def test_pure_text_returns_as_is(self):
        raw = "There are 5 candidates in the database"
        assert _extract_sql(raw) == raw  # let validate_sql reject it

    def test_strips_whitespace(self):
        raw = "  \n  SELECT * FROM candidates  \n  "
        assert _extract_sql(raw).startswith("SELECT")


class TestValidateSqlApostrophe:
    def test_fixes_backslash_apostrophe(self):
        sql = r"SELECT * FROM candidates WHERE location ILIKE '%be\'er sheva%'"
        result = validate_sql(sql)
        assert "be''er sheva" in result
        assert "\\'" not in result

    def test_preserves_double_apostrophe(self):
        sql = "SELECT * FROM candidates WHERE location ILIKE '%be''er sheva%'"
        result = validate_sql(sql)
        assert "be''er sheva" in result

    def test_fixes_multiple_backslash_apostrophes(self):
        sql = r"SELECT * FROM candidates WHERE location ILIKE '%ra\'anana%' OR location ILIKE '%be\'er sheva%'"
        result = validate_sql(sql)
        assert "ra''anana" in result
        assert "be''er sheva" in result


# =============================================================================
# chat.py -- Answer hallucination validation
# =============================================================================

class TestValidateAnswer:
    def test_name_in_results_passes(self):
        rows = [{"name": "Alex Mock"}]
        cols = ["name"]
        result = validate_answer("Alex Mock is a senior engineer.", rows, cols)
        assert result["hallucination_warning"] is False

    def test_name_not_in_results_flagged(self):
        rows = [{"name": "Alex Mock"}]
        cols = ["name"]
        result = validate_answer("Jordan Sample is a senior engineer.", rows, cols)
        assert result["hallucination_warning"] is True

    def test_correct_count_passes(self):
        rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        cols = ["id"]
        result = validate_answer("There are 3 candidates.", rows, cols)
        assert result["hallucination_warning"] is False

    def test_wrong_count_flagged(self):
        rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        cols = ["id"]
        result = validate_answer("There are 5 candidates.", rows, cols)
        assert result["hallucination_warning"] is True

    def test_empty_answer(self):
        result = validate_answer("", [], [])
        assert result["hallucination_warning"] is False

    def test_generic_answer_passes(self):
        rows = [{"count": 5}]
        cols = ["count"]
        result = validate_answer("The query returned results.", rows, cols)
        assert result["hallucination_warning"] is False

    def test_empty_results_no_data_passes(self):
        result = validate_answer("No results found.", [], [])
        assert result["hallucination_warning"] is False

    def test_empty_results_invents_data_flagged(self):
        result = validate_answer("Alex Mock has 5 years of experience.", [], [])
        assert result["hallucination_warning"] is True

    def test_count_in_aggregate_result_passes(self):
        """COUNT(*) returns 1 row with value 41 -- answer says '41 candidates' should pass."""
        rows = [{"count": 41}]
        cols = ["count"]
        result = validate_answer("There are 41 candidates with certifications.", rows, cols)
        assert result["hallucination_warning"] is False

    def test_count_mismatch_with_aggregate_flagged(self):
        """Answer claims 50 but aggregate says 41 -- should flag."""
        rows = [{"count": 41}]
        cols = ["count"]
        result = validate_answer("There are 50 candidates with certifications.", rows, cols)
        assert result["hallucination_warning"] is True

    def test_location_name_not_flagged(self):
        """'Tel Aviv' looks like a proper name but is a location -- should not flag."""
        rows = [{"name": "Alex", "location": "Tel Aviv"},
                {"name": "Bob", "location": "Tel Aviv"},
                {"name": "Carol", "location": "Tel Aviv"},
                {"name": "Dana", "location": "Tel Aviv"}]
        cols = ["name", "location"]
        result = validate_answer("There are 4 candidates in Tel Aviv.", rows, cols)
        assert result["hallucination_warning"] is False

    def test_common_phrase_not_flagged(self):
        """'Based On', 'Query Results' are LLM phrasing, not names."""
        rows = [{"count": 10}]
        cols = ["count"]
        result = validate_answer("Based on the query results, there are 10 positions.", rows, cols)
        assert result["hallucination_warning"] is False

    def test_total_column_accepted(self):
        """When result has 'total' column, compare against that value."""
        rows = [{"total": 25}]
        cols = ["total"]
        result = validate_answer("There are 25 candidates.", rows, cols)
        assert result["hallucination_warning"] is False

    def test_short_name_not_flagged(self):
        """Very short capitalized phrases (< 5 chars without spaces) should not trigger."""
        rows = [{"status": "active"}]
        cols = ["status"]
        # "Go To" mid-sentence: 4 chars without spaces -> skipped by length check
        result = validate_answer("using a Go To approach for active records.", rows, cols)
        assert result["hallucination_warning"] is False

    def test_name_from_question_not_flagged(self):
        """Name mentioned in the question should not trigger hallucination warning."""
        rows = [{"name": "Git"}, {"name": "Docker"}, {"name": "Jenkins"}]
        cols = ["name"]
        result = validate_answer(
            "Aarav Hayes has the following skills: Git, Docker, Jenkins.",
            rows, cols, question="What skills does Aarav Hayes have?"
        )
        assert result["hallucination_warning"] is False

    def test_entity_title_from_question_not_flagged(self):
        """Position title mentioned in the question should not trigger warning."""
        rows = [{"experience_level": "mid"}]
        cols = ["experience_level"]
        result = validate_answer(
            "The experience level required for the Platform Engineer position is mid.",
            rows, cols, question="What is the experience level for the Platform Engineer position?"
        )
        assert result["hallucination_warning"] is False

    def test_ramat_gan_not_flagged(self):
        """Israeli city 'Ramat Gan' should be in SKIP_NAMES."""
        rows = [{"name": "Ada Montes", "status": "active"}]
        cols = ["name", "status"]
        result = validate_answer(
            "There is 1 candidate in Ramat Gan: Ada Montes, active.",
            rows, cols, question="Candidates in Ramat Gan"
        )
        assert result["hallucination_warning"] is False

    def test_genuine_hallucination_still_flagged(self):
        """Name NOT in question AND NOT in results should still flag."""
        rows = [{"name": "Git"}, {"name": "Docker"}]
        cols = ["name"]
        result = validate_answer(
            "Jordan Smith has the following skills: Git, Docker.",
            rows, cols, question="What skills does Aarav Hayes have?"
        )
        assert result["hallucination_warning"] is True


# =============================================================================
# Extended tests -- validate_sql, extract_sql, validate_answer, format helpers
# =============================================================================

class TestValidateSqlExtended:
    def test_subquery_allowed(self):
        result = validate_sql("SELECT * FROM (SELECT name FROM candidates) AS sub")
        assert "candidates" in result

    def test_cte_allowed(self):
        result = validate_sql("SELECT * FROM candidates WHERE id IN (SELECT candidate_id FROM candidate_skills)")
        assert "candidates" in result

    def test_window_function_allowed(self):
        result = validate_sql("SELECT name, ROW_NUMBER() OVER (ORDER BY name) FROM candidates")
        assert "candidates" in result

    def test_union_allowed_tables(self):
        result = validate_sql("SELECT name AS label FROM candidates UNION SELECT title AS label FROM positions")
        assert "UNION" in result

    def test_union_with_forbidden_table(self):
        with pytest.raises(ValueError, match="users"):
            validate_sql("SELECT name FROM candidates UNION SELECT username FROM users")

    def test_select_with_dangerous_function_in_column(self):
        with pytest.raises(ValueError, match="function"):
            validate_sql("SELECT pg_sleep(1), name FROM candidates")

    def test_nested_subquery_forbidden_table(self):
        with pytest.raises(ValueError, match="users"):
            validate_sql("SELECT * FROM candidates WHERE name IN (SELECT username FROM users)")

    def test_case_expression_allowed(self):
        result = validate_sql("SELECT CASE WHEN status = 'active' THEN 'yes' ELSE 'no' END FROM candidates")
        assert "CASE" in result

    def test_offset_without_limit(self):
        result = validate_sql("SELECT * FROM candidates OFFSET 5")
        assert "LIMIT 50" in result

    def test_limit_zero(self):
        result = validate_sql("SELECT * FROM candidates LIMIT 0")
        assert "LIMIT 0" in result
        assert result.count("LIMIT") == 1

    def test_lateral_join_allowed(self):
        result = validate_sql("SELECT c.name, s.skill FROM candidates c, LATERAL (SELECT skill FROM candidate_skills WHERE candidate_id = c.id) s")
        assert "candidates" in result

    def test_rejects_unknown_table(self):
        with pytest.raises(ValueError, match="Unknown table"):
            validate_sql("SELECT * FROM nonexistent_table")


class TestExtractSqlExtended:
    def test_multiple_sql_blocks_takes_first(self):
        raw = "```sql\nSELECT name FROM candidates\n```\nAlternatively:\n```sql\nSELECT * FROM positions\n```"
        result = _extract_sql(raw)
        assert "candidates" in result

    def test_numbered_option_extracts_first_select(self):
        raw = "Option 1: SELECT name FROM candidates\nOption 2: SELECT title FROM positions"
        result = _extract_sql(raw)
        assert result.startswith("SELECT")

    def test_select_in_explanatory_text(self):
        raw = "The SELECT statement retrieves data from a database table."
        result = _extract_sql(raw)
        assert "SELECT" in result

    def test_empty_markdown_fence(self):
        raw = "```\n   \n```"
        result = _extract_sql(raw)
        assert result.strip() == ""

    def test_backticks_without_sql_tag(self):
        raw = "```\nSELECT name FROM candidates\n```"
        result = _extract_sql(raw)
        assert "SELECT" in result
        assert "```" not in result


class TestValidateAnswerExtended:
    def test_zero_count_claim_passes(self):
        result = validate_answer("0 candidates matched the criteria.", [], [])
        assert result["hallucination_warning"] is False

    def test_multiple_count_claims(self):
        rows = [{"count": 5, "position_count": 3}]
        cols = ["count", "position_count"]
        result = validate_answer("There are 5 candidates and 3 positions.", rows, cols)
        assert result["hallucination_warning"] is False

    def test_possessive_name_matches(self):
        rows = [{"name": "Alex Mock", "skill": "AWS"}]
        cols = ["name", "skill"]
        result = validate_answer("Alex Mock's experience includes AWS.", rows, cols)
        assert result["hallucination_warning"] is False

    def test_partial_name_match_not_flagged(self):
        rows = [{"name": "Alex Mock"}]
        cols = ["name"]
        result = validate_answer("Alex is great at cloud computing.", rows, cols)
        assert result["hallucination_warning"] is False

    def test_unicode_name_in_results(self):
        rows = [{"name": "Eitan Levy"}]
        cols = ["name"]
        result = validate_answer("Eitan Levy has strong skills.", rows, cols)
        assert result["hallucination_warning"] is False

    def test_name_with_apostrophe(self):
        rows = [{"name": "Sean O'Brien"}]
        cols = ["name"]
        result = validate_answer("Sean is the best candidate.", rows, cols)
        assert result["hallucination_warning"] is False

    def test_empty_rows_nonempty_answer_flagged(self):
        result = validate_answer("Jordan Smith has extensive experience.", [], [])
        assert result["hallucination_warning"] is True

    def test_answer_with_no_names_or_counts_passes(self):
        rows = [{"id": 1}, {"id": 2}]
        cols = ["id"]
        result = validate_answer("The results show data about the matching records.", rows, cols)
        assert result["hallucination_warning"] is False


class TestFormatHistory:
    def test_empty_history(self):
        assert _format_history([]) == ""

    def test_single_item(self):
        result = _format_history([{"role": "user", "content": "hello"}])
        assert result == "user: hello"

    def test_truncates_to_max_history(self):
        items = [{"role": "user", "content": f"msg{i}"} for i in range(25)]
        result = _format_history(items)
        lines = result.strip().split("\n")
        assert len(lines) == MAX_HISTORY

    def test_missing_role_defaults_to_user(self):
        result = _format_history([{"content": "hi"}])
        assert result == "user: hi"

    def test_missing_content_defaults_to_empty(self):
        result = _format_history([{"role": "user"}])
        assert result == "user: "


class TestFormatResults:
    def test_empty_rows(self):
        assert _format_results([], ["name"]) == "(no results)"

    def test_single_row(self):
        result = _format_results([{"name": "Alex"}], ["name"])
        lines = result.split("\n")
        assert lines[0] == "name"
        assert lines[1] == "Alex"

    def test_caps_at_50_rows(self):
        rows = [{"n": str(i)} for i in range(60)]
        result = _format_results(rows, ["n"])
        lines = result.split("\n")
        assert len(lines) == 51  # 1 header + 50 data

    def test_none_value_in_row(self):
        result = _format_results([{"name": None}], ["name"])
        assert "None" in result

    def test_numeric_values(self):
        result = _format_results([{"count": 42, "avg": 3.14}], ["count", "avg"])
        assert "42" in result
        assert "3.14" in result


class TestExecuteReadonly:
    async def test_basic_select(self, setup_db):
        result = await execute_readonly("SELECT count(*) AS cnt FROM candidates")
        assert "columns" in result
        assert "rows" in result
        assert "rowCount" in result
        assert result["columns"] == ["cnt"]
        assert result["rowCount"] == 1

    async def test_invalid_sql_raises(self, setup_db):
        with pytest.raises(Exception):
            await execute_readonly("NOT VALID SQL")

    async def test_readonly_rejects_insert(self, setup_db):
        with pytest.raises(Exception):
            await execute_readonly("INSERT INTO candidates (name, status, experience_level, summary) VALUES ('x', 'active', 'mid', '')")

    async def test_datetime_serialized(self, setup_db):
        result = await execute_readonly("SELECT created_at FROM candidates LIMIT 1")
        val = result["rows"][0]["created_at"]
        assert isinstance(val, str)
        assert "T" in val  # ISO format

    async def test_empty_result(self, setup_db):
        result = await execute_readonly("SELECT * FROM candidates WHERE 1=0")
        assert result["rows"] == []
        assert result["rowCount"] == 0
        assert len(result["columns"]) > 0
