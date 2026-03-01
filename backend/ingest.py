import os
import re
import time

import db
from llm import extract_fields, generate_summary, log_extraction

# =============================================================================
# File parsing (from parse.py)
# =============================================================================

SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".txt"}


def parse_file(path: str) -> str:
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    ext = os.path.splitext(path)[1].lower()
    if ext not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported file format: {ext}")

    if ext == ".pdf":
        return _parse_pdf(path)
    elif ext == ".docx":
        return _parse_docx(path)
    else:
        return _parse_txt(path)


def _parse_pdf(path: str) -> str:
    import pdfplumber
    try:
        pages = []
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                words = page.extract_words()
                if not words:
                    continue
                gap_x = _find_column_gap(words, page.width)
                if gap_x is not None:
                    pages.append(_extract_columns(page, gap_x, words))
                    continue

                # Fallback: full-width header may mask columns in lower portion.
                # Try gap detection on bottom 40% of page only.
                cut_y = page.height * 0.6
                bottom_words = [w for w in words if float(w["top"]) > cut_y]
                gap_x = _find_column_gap(bottom_words, page.width, min_words=10)
                if gap_x is not None:
                    top_text = (
                        page.crop((0, 0, page.width, cut_y)).extract_text()
                        or ""
                    ).strip()
                    # Split bottom portion into columns on original page
                    h = page.height
                    left_text = (
                        page.crop((0, cut_y, gap_x, h)).extract_text() or ""
                    ).strip()
                    right_text = (
                        page.crop((gap_x, cut_y, page.width, h)).extract_text()
                        or ""
                    ).strip()
                    lc = sum(1 for w in bottom_words if float(w["x0"]) < gap_x)
                    if lc >= len(bottom_words) - lc:
                        main, sidebar = left_text, right_text
                    else:
                        main, sidebar = right_text, left_text
                    parts = top_text + "\n\n" + main
                    if sidebar:
                        parts += "\n\n[Sidebar]\n" + sidebar
                    pages.append(parts.strip())
                    continue

                text = page.extract_text()
                if text:
                    pages.append(text.strip())
        return "\n\n".join(pages)
    except Exception as e:
        raise RuntimeError(f"Failed to parse PDF: {e}") from e


def _find_column_gap(words, region_width, offset_x=0, min_words=20):
    """Find x-position of a gap between two columns within a region.

    Returns absolute x-position of gap center, or None if single-column.
    offset_x shifts word coordinates to region-local space for binning.
    """
    if len(words) < min_words:
        return None
    n_bins = 20
    bin_width = region_width / n_bins
    bins = [0] * n_bins
    for w in words:
        local_x = float(w["x0"]) - offset_x
        idx = min(max(int(local_x / bin_width), 0), n_bins - 1)
        bins[idx] += 1
    start_bin = int(n_bins * 0.2)
    end_bin = int(n_bins * 0.8)
    best_start, best_len = None, 0
    threshold = max(2, sum(bins) * 0.01)
    i = start_bin
    while i < end_bin:
        if bins[i] <= threshold:
            gap_start = i
            while i < end_bin and bins[i] <= threshold:
                i += 1
            if i - gap_start > best_len:
                best_len = i - gap_start
                best_start = gap_start
        else:
            i += 1
    if best_len < 2:
        return None
    return offset_x + (best_start + best_len / 2) * bin_width


def _extract_columns(page, gap_x, words):
    """Extract text from a multi-column page, handling 2- and 3-column layouts."""
    left = page.crop((0, 0, gap_x, page.height))
    right = page.crop((gap_x, 0, page.width, page.height))
    left_text = (left.extract_text() or "").strip()
    right_text = (right.extract_text() or "").strip()
    left_count = sum(1 for w in words if float(w["x0"]) < gap_x)

    if left_count >= len(words) - left_count:
        main_text, sidebar_text = left_text, right_text
        main_words = [w for w in words if float(w["x0"]) < gap_x]
        main_left, main_right = 0, gap_x
    else:
        main_text, sidebar_text = right_text, left_text
        main_words = [w for w in words if float(w["x0"]) >= gap_x]
        main_left, main_right = gap_x, page.width

    # Check for sub-columns within the main area (handles 3-column PDFs)
    sub_gap = _find_column_gap(
        main_words, main_right - main_left, main_left, min_words=10
    )
    if sub_gap is not None:
        sub_l = page.crop((main_left, 0, sub_gap, page.height))
        sub_r = page.crop((sub_gap, 0, main_right, page.height))
        sub_l_text = (sub_l.extract_text() or "").strip()
        sub_r_text = (sub_r.extract_text() or "").strip()
        sub_l_count = sum(1 for w in main_words if float(w["x0"]) < sub_gap)
        if sub_l_count >= len(main_words) - sub_l_count:
            main_text = sub_l_text + "\n\n" + sub_r_text
        else:
            main_text = sub_r_text + "\n\n" + sub_l_text

    if sidebar_text:
        return main_text + "\n\n[Sidebar]\n" + sidebar_text
    return main_text


def _parse_docx(path: str) -> str:
    from docx import Document
    try:
        doc = Document(path)
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
        text = "\n".join(paragraphs)
        if not text.strip():
            raise RuntimeError(f"Empty document: {path}")
        return text
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to parse DOCX: {e}") from e


def _parse_txt(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    if not text.strip():
        raise RuntimeError(f"Empty file: {path}")
    return text


# =============================================================================
# Heuristic extraction and validation (from extract.py)
# =============================================================================

# -- Regex patterns for CV contact fields --
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.\w+")
_PHONE_RE = re.compile(r"[\+]?[\d][\d\s\-\(\)\.]{6,14}\d")
_LINKEDIN_RE = re.compile(r"(?:https?://)?linkedin\.com/in/[\w-]+")
_GITHUB_RE = re.compile(r"(?:https?://)?github\.com/[\w-]+")

# -- Regex patterns for job headers --
_FROM_RE = re.compile(r"^From:\s*(.+?)\s*<([\w.+-]+@[\w.-]+\.\w+)>", re.MULTILINE)
_FROM_EMAIL_ONLY_RE = re.compile(r"^From:\s*([\w.+-]+@[\w.-]+\.\w+)\s*$", re.MULTILINE)
_SUBJECT_RE = re.compile(r"^Subject:\s*(.+)$", re.MULTILINE)
_SUBJECT_PREFIX_RE = re.compile(r"^(?:Urgent\s*-\s*|RE:\s*|FW:\s*|Fwd:\s*)", re.IGNORECASE)

# -- Valid enum values --
_CANDIDATE_STATUSES = {"active", "inactive"}
_POSITION_STATUSES = {"open", "closed"}
_EXPERIENCE_LEVELS = {"junior", "mid", "senior", "lead", "staff", "principal"}

_MAX_NAME = 200
_MAX_TITLE = 200


def heuristic_extract(raw_text: str, entity_type: str) -> dict:
    """Extract high-confidence fields via regex.

    For candidates: email, phone, linkedin, github.
    For positions: title, hm_name, hm_email (from email headers).
    Missing fields are None.
    """
    if entity_type == "candidate":
        return _extract_candidate_fields(raw_text)
    return _extract_position_fields(raw_text)


def _extract_candidate_fields(text: str) -> dict:
    email_m = _EMAIL_RE.search(text)
    phone_m = _PHONE_RE.search(text)
    linkedin_m = _LINKEDIN_RE.search(text)
    github_m = _GITHUB_RE.search(text)
    return {
        "email": email_m.group(0) if email_m else None,
        "phone": phone_m.group(0).strip() if phone_m else None,
        "linkedin": linkedin_m.group(0) if linkedin_m else None,
        "github": github_m.group(0) if github_m else None,
    }


def _extract_position_fields(text: str) -> dict:
    hm_name, hm_email, title = None, None, None

    from_m = _FROM_RE.search(text)
    if from_m:
        hm_name = from_m.group(1).strip()
        hm_email = from_m.group(2)
    else:
        from_email_m = _FROM_EMAIL_ONLY_RE.search(text)
        if from_email_m:
            hm_email = from_email_m.group(1)

    subj_m = _SUBJECT_RE.search(text)
    if subj_m:
        title = _SUBJECT_PREFIX_RE.sub("", subj_m.group(1)).strip()

    return {"title": title, "hm_name": hm_name, "hm_email": hm_email}


# =============================================================================
# Validation / sanitization
# =============================================================================

def _coerce_dates(items):
    """Coerce date fields to strings (LLM sometimes returns bare ints like 2020)."""
    for item in items:
        if isinstance(item, dict):
            for key in ("startDate", "endDate"):
                if key in item and item[key] is not None:
                    item[key] = str(item[key])


def validate_candidate(data: dict) -> tuple:
    """Validate and sanitize candidate data before DB insert.

    Returns (cleaned_data, warnings_list).
    """
    warnings = []
    clean = {}

    # Name
    name = data.get("name") or "Unknown"
    if name == "Unknown" and "name" not in data:
        warnings.append("Missing candidate name, defaulting to 'Unknown'")
    if len(name) > _MAX_NAME:
        name = name[:_MAX_NAME]
        warnings.append(f"Name truncated to {_MAX_NAME} chars")
    clean["name"] = name

    # Status
    status = data.get("status", "active")
    if status not in _CANDIDATE_STATUSES:
        warnings.append(f"Invalid status '{status}', defaulting to 'active'")
        status = "active"
    clean["status"] = status

    # Experience level
    exp = data.get("experienceLevel", "mid")
    if exp not in _EXPERIENCE_LEVELS:
        warnings.append(f"Invalid experienceLevel '{exp}', defaulting to 'mid'")
        exp = "mid"
    clean["experienceLevel"] = exp

    # Contact
    contact = data.get("contact") or {}
    if not isinstance(contact, dict):
        contact = {}
    # Re-validate URLs against regex patterns to strip corruption
    for key, pattern in (("linkedin", _LINKEDIN_RE), ("github", _GITHUB_RE)):
        val = contact.get(key)
        if val:
            m = pattern.search(val)
            contact[key] = m.group(0) if m else None
    clean["contact"] = contact

    # Summary
    clean["summary"] = str(data.get("summary") or "")

    # Array fields
    clean["skills"] = _ensure_list(data.get("skills"), warnings, "skills")
    clean["languages"] = _ensure_list(data.get("languages"), warnings, "languages")
    clean["experience"] = _ensure_list(data.get("experience"), warnings, "experience")
    clean["education"] = _ensure_list(data.get("education"), warnings, "education")

    # Coerce date fields to strings (LLM sometimes returns bare ints like 2020)
    _coerce_dates(clean["experience"])
    _coerce_dates(clean["education"])

    # Deduplicate skills (case-insensitive)
    seen = set()
    deduped = []
    for s in clean["skills"]:
        low = str(s).lower()
        if low not in seen:
            seen.add(low)
            deduped.append(str(s))
    clean["skills"] = deduped

    # Certifications -- coerce year to int (LLM may return string "2020")
    certs = _ensure_list(data.get("certifications"), warnings, "certifications")
    for cert in certs:
        if isinstance(cert, dict) and "year" in cert:
            y = cert["year"]
            if y is not None:
                try:
                    cert["year"] = int(y)
                except (ValueError, TypeError):
                    warnings.append(f"Invalid certification year '{y}', dropping")
                    cert["year"] = None
                else:
                    if cert["year"] > 2050:
                        warnings.append(f"Certification year {cert['year']} seems far-future")
    clean["certifications"] = certs

    # cvFile
    clean["cvFile"] = data.get("cvFile")

    return clean, warnings


def validate_position(data: dict) -> tuple:
    """Validate and sanitize position data before DB insert.

    Returns (cleaned_data, warnings_list).
    """
    warnings = []
    clean = {}

    # Title
    title = data.get("title") or "Untitled Position"
    if title == "Untitled Position" and "title" not in data:
        warnings.append("Missing position title, defaulting to 'Untitled Position'")
    if len(title) > _MAX_TITLE:
        title = title[:_MAX_TITLE]
        warnings.append(f"Title truncated to {_MAX_TITLE} chars")
    clean["title"] = title

    # Status
    status = data.get("status", "open")
    if status not in _POSITION_STATUSES:
        warnings.append(f"Invalid status '{status}', defaulting to 'open'")
        status = "open"
    clean["status"] = status

    # Company
    clean["company"] = str(data.get("company") or "Unknown")

    # Hiring manager
    hm = data.get("hiringManager")
    if not isinstance(hm, dict):
        hm = {}
    clean["hiringManager"] = {
        "name": hm.get("name", ""),
        "title": hm.get("title", ""),
        "email": hm.get("email", ""),
    }

    # Experience level
    exp = data.get("experienceLevel", "mid")
    if exp not in _EXPERIENCE_LEVELS:
        warnings.append(f"Invalid experienceLevel '{exp}', defaulting to 'mid'")
        exp = "mid"
    clean["experienceLevel"] = exp

    # Array fields
    clean["requirements"] = _ensure_list(data.get("requirements"), warnings, "requirements")
    clean["niceToHave"] = _ensure_list(data.get("niceToHave"), warnings, "niceToHave")
    clean["responsibilities"] = _ensure_list(data.get("responsibilities"), warnings, "responsibilities")
    clean["techStack"] = _ensure_list(data.get("techStack"), warnings, "techStack")

    # Reclassify requirements with "preferred" language as nice-to-have
    _PREFERRED_KW = ("preferred", "nice to have", "bonus", "a plus", "ideally")
    reqs = clean["requirements"]
    nth = clean["niceToHave"]
    moved = [r for r in reqs if any(kw in r.lower() for kw in _PREFERRED_KW)]
    if moved:
        clean["requirements"] = [r for r in reqs if r not in moved]
        clean["niceToHave"] = nth + moved
        warnings.append(f"Moved {len(moved)} 'preferred' items to niceToHave")

    # Scalar fields
    clean["location"] = str(data.get("location") or "")
    clean["workArrangement"] = str(data.get("workArrangement") or "")
    clean["compensation"] = str(data.get("compensation") or "")
    sal_min, sal_max = db.parse_salary(clean["compensation"])
    clean["salaryMin"] = sal_min
    clean["salaryMax"] = sal_max
    clean["timeline"] = str(data.get("timeline") or "")
    clean["jobFile"] = data.get("jobFile")

    return clean, warnings


def _ensure_list(val, warnings, field_name):
    """Coerce value to list. Wraps strings, defaults None to []."""
    if val is None:
        return []
    if isinstance(val, str):
        warnings.append(f"'{field_name}' was string, wrapped in list")
        return [val]
    if isinstance(val, list):
        return val
    return []


# =============================================================================
# Pipeline orchestration
# =============================================================================

_MIN_WORDS = 20
_GENERIC_NAMES = {"unknown", "untitled", "untitled position", "n/a", "none", ""}


class InsufficientDataError(RuntimeError):
    """Raised when extracted data is too incomplete to persist."""
    def __init__(self, missing: list[str]):
        self.missing = missing
        super().__init__(f"Insufficient data. Missing: {', '.join(missing)}")


def _check_position_fields(fields: dict) -> None:
    missing = []
    title = (fields.get("title") or "").strip()
    if not title or title.lower() in _GENERIC_NAMES:
        missing.append("title")
    if not fields.get("responsibilities"):
        missing.append("responsibilities")
    if not fields.get("requirements") and not fields.get("techStack"):
        missing.append("requirements or techStack")
    if missing:
        raise InsufficientDataError(missing)


def _check_candidate_fields(fields: dict) -> None:
    missing = []
    name = (fields.get("name") or "").strip()
    if not name or name.lower() in _GENERIC_NAMES:
        missing.append("name")
    if not fields.get("skills") and not fields.get("experience"):
        missing.append("skills or experience")
    if missing:
        raise InsufficientDataError(missing)


_CV_CONFIG = {
    "entity_type": "candidate",
    "fallback_fields": lambda h: {
        "name": "Unknown", "status": "active", "experienceLevel": "mid",
        "contact": {k: h.get(k) for k in ("email", "phone", "linkedin", "github")},
        "skills": [], "languages": [], "experience": [], "education": [], "certifications": [],
    },
    "validate_fn": validate_candidate,
    "check_fn": _check_candidate_fields,
    "file_key": "cvFile",
    "insert_fn": db.insert_candidate,
    "update_fn": db.update_candidate,
    "embed_fn": "embed_candidate",
    "changes_fn": lambda old, new: _compute_changes(old, new, "candidate"),
}

_JOB_CONFIG = {
    "entity_type": "position",
    "fallback_fields": lambda h: {
        "title": h.get("title", "Unknown"),
        "status": "open", "company": "Unknown",
        "hiringManager": {"name": h.get("hm_name", ""), "email": h.get("hm_email", "")},
        "experienceLevel": "mid", "location": "", "workArrangement": "",
        "techStack": [], "requirements": [], "niceToHave": [], "responsibilities": [],
    },
    "validate_fn": validate_position,
    "check_fn": _check_position_fields,
    "file_key": "jobFile",
    "insert_fn": db.insert_position,
    "update_fn": db.update_position,
    "embed_fn": "embed_position",
    "changes_fn": lambda old, new: _compute_changes(old, new, "position"),
}


def _merge_candidate_heuristic(fields, heuristic):
    """Heuristic wins for contact fields (verbatim from doc)."""
    contact = fields.get("contact") or {}
    for key in ("email", "phone", "linkedin", "github"):
        if heuristic.get(key):
            contact[key] = heuristic[key]
    fields["contact"] = contact


def _merge_position_heuristic(fields, heuristic):
    """LLM fields take precedence, heuristic fills gaps."""
    hm = fields.get("hiringManager") or {}
    if not hm.get("name") and heuristic.get("hm_name"):
        hm["name"] = heuristic["hm_name"]
    if not hm.get("email") and heuristic.get("hm_email"):
        hm["email"] = heuristic["hm_email"]
    fields["hiringManager"] = hm
    if not fields.get("title") and heuristic.get("title"):
        fields["title"] = heuristic["title"]


async def _find_existing_candidate(clean):
    contact_email = (clean.get("contact") or {}).get("email")
    linkedin = (clean.get("contact") or {}).get("linkedin")
    existing = await db.find_candidate_by_email(contact_email)
    if not existing and linkedin:
        existing = await db.find_candidate_by_linkedin(linkedin)
    return existing


async def _find_existing_position(clean):
    return await db.find_position_by_title_company(
        clean.get("title", ""), clean.get("company", ""))


async def _ingest_entity(file_path, cfg, merge_fn, find_existing_fn):
    """Shared 8-step ingestion pipeline for CVs and job descriptions."""
    filename = os.path.basename(file_path)
    entity_type = cfg["entity_type"]
    start = time.monotonic()
    tokens = {"input": 0, "output": 0}
    used_model = "nova"
    raw_llm_responses = []
    warnings = []

    # 1. Parse
    raw_text = parse_file(file_path)
    if len(raw_text.split()) < _MIN_WORDS:
        raise InsufficientDataError([f"document too short -- need at least {_MIN_WORDS} words"])

    # 2. Heuristic extraction
    heuristic = heuristic_extract(raw_text, entity_type)

    # 3. LLM field extraction
    status = "success"
    try:
        llm_result = await extract_fields(raw_text, entity_type, heuristic)
        fields = llm_result["fields"]
        tokens["input"] += llm_result["usage"]["input_tokens"]
        tokens["output"] += llm_result["usage"]["output_tokens"]
        used_model = llm_result["usage"]["model"]
        raw_llm_responses.append(llm_result.get("raw_response"))
    except Exception as e:
        status = "partial"
        warnings.append(f"LLM extraction failed: {e}")
        fields = cfg["fallback_fields"](heuristic)

    # 3b. Candidate-specific: retry once if skills empty
    if entity_type == "candidate" and status == "success" and not fields.get("skills"):
        try:
            retry_result = await extract_fields(raw_text, entity_type, heuristic)
            retry_skills = retry_result["fields"].get("skills", [])
            if retry_skills:
                fields["skills"] = retry_skills
                warnings.append(f"Skills empty on first try, retry got {len(retry_skills)}")
            else:
                warnings.append("Skills empty after retry")
            tokens["input"] += retry_result["usage"]["input_tokens"]
            tokens["output"] += retry_result["usage"]["output_tokens"]
        except Exception:
            warnings.append("Skills retry failed")

    # 4. Merge heuristic fields
    merge_fn(fields, heuristic)

    # 5. Validate
    fields[cfg["file_key"]] = filename
    clean, val_warnings = cfg["validate_fn"](fields)
    warnings.extend(val_warnings)
    cfg["check_fn"](clean)

    # 6. Summary
    try:
        summary_result = await generate_summary(raw_text, entity_type)
        clean["summary"] = summary_result["summary"]
        tokens["input"] += summary_result["usage"]["input_tokens"]
        tokens["output"] += summary_result["usage"]["output_tokens"]
        raw_llm_responses.append(summary_result.get("raw_response"))
    except Exception as e:
        if status == "success":
            status = "partial"
        warnings.append(f"LLM summary failed: {e}")
        clean["summary"] = ""

    # 7. Dedup + persist
    existing = await find_existing_fn(clean)
    is_update = existing is not None
    ext = os.path.splitext(filename)[1].lower()

    if is_update:
        changes = cfg["changes_fn"](existing, clean)
        entity = await cfg["update_fn"](existing["id"], clean)
        entity["isUpdate"] = True
        entity["changes"] = changes
        if entity_type == "candidate":
            entity["previousName"] = existing["name"]
    else:
        entity = await cfg["insert_fn"](clean)
        entity["isUpdate"] = False
        entity["changes"] = []
        if entity_type == "candidate":
            entity["previousName"] = None

    await db.insert_document(entity_type, entity["id"], filename,
                             file_type=ext, stored_path=file_path, raw_text=raw_text)

    # 7b. Generate embedding
    try:
        from embeddings import embed_candidate, embed_position
        embed_fn = embed_candidate if entity_type == "candidate" else embed_position
        await embed_fn(entity["id"])
    except Exception as e:
        warnings.append(f"Embedding failed: {e}")

    # 8. Log
    duration_ms = int((time.monotonic() - start) * 1000)
    log_extraction({
        "entity_type": entity_type, "filename": filename,
        "status": status, "model": used_model,
        "heuristic_fields": heuristic, "warnings": warnings,
        "input_tokens": tokens["input"], "output_tokens": tokens["output"],
        "duration_ms": duration_ms, "raw_llm_responses": raw_llm_responses,
    })

    return entity


async def ingest_cv(file_path: str) -> dict:
    """Parse, extract, validate, and persist a CV document."""
    return await _ingest_entity(file_path, _CV_CONFIG,
                                _merge_candidate_heuristic, _find_existing_candidate)


async def ingest_job(file_path: str) -> dict:
    """Parse, extract, validate, and persist a job description."""
    return await _ingest_entity(file_path, _JOB_CONFIG,
                                _merge_position_heuristic, _find_existing_position)


def _compute_changes(existing, new_data, entity_type="candidate"):
    """Compute human-readable change list between existing and new entity data."""
    changes = []

    if entity_type == "candidate":
        _diff_set(changes, existing, new_data, "skills", "skill")
        old_exp = len(existing.get("experience") or [])
        new_exp = len(new_data.get("experience") or [])
        if new_exp > old_exp:
            changes.append(f"+{new_exp - old_exp} role{'s' if (new_exp - old_exp) != 1 else ''}")
        elif new_exp < old_exp:
            changes.append(f"-{old_exp - new_exp} role{'s' if (old_exp - new_exp) != 1 else ''}")
        old_email = (existing.get("contact") or {}).get("email", "")
        new_email = (new_data.get("contact") or {}).get("email", "")
        if old_email and new_email and old_email.lower() != new_email.lower():
            changes.append("email changed")
        if existing.get("name") != new_data.get("name"):
            changes.append("name changed")
    else:
        _diff_set(changes, existing, new_data, "techStack", "tech")
        old_reqs = len(existing.get("requirements") or [])
        new_reqs = len(new_data.get("requirements") or [])
        if new_reqs != old_reqs:
            changes.append(f"requirements {old_reqs}->{new_reqs}")
        if existing.get("title") != new_data.get("title"):
            changes.append("title changed")
        if existing.get("location") != new_data.get("location"):
            changes.append("location changed")

    return changes


def _diff_set(changes, existing, new_data, key, label):
    old, new = set(existing.get(key) or []), set(new_data.get(key) or [])
    added, removed = new - old, old - new
    if added:
        changes.append(f"+{len(added)} {label}{'s' if len(added) != 1 else ''}")
    if removed:
        changes.append(f"-{len(removed)} {label}{'s' if len(removed) != 1 else ''}")


def _compute_position_changes(existing, new_data):
    """Backward-compatible wrapper for tests."""
    return _compute_changes(existing, new_data, "position")
