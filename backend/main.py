import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bcrypt
import jwt
from fastapi import Depends, FastAPI, HTTPException, Request, UploadFile, File
from fastapi.responses import FileResponse
from starlette.responses import JSONResponse

import db
from db import seed
from ingest import ingest_cv, ingest_job, InsufficientDataError
from llm import TOKEN_PRICES, compute_stats
from chat import ask as chat_ask
from embeddings import (
    suggest_candidates_for_position,
    suggest_positions_for_candidate,
    embed_candidate, embed_position,
)

JWT_SECRET = os.environ["JWT_SECRET"]
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_MINUTES = 480
FILES_ROOT = Path(os.environ.get("FILES_ROOT", "/data/CVsJobs"))


@asynccontextmanager
async def lifespan(app):
    await db.get_pool()
    await seed()
    yield
    await db.close_pool()


app = FastAPI(lifespan=lifespan)


# --- Auth helpers -------------------------------------------------------------

def create_token(username, role):
    exp = datetime.now(timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINUTES)
    return jwt.encode({"sub": username, "role": role, "exp": exp}, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_token(token):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return {"username": payload["sub"], "role": payload.get("role", "hr-viewer")}
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid token")


async def get_current_user(request: Request):
    auth = request.headers.get("Authorization")
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing token")
    claims = decode_token(auth[7:])
    user = await db.get_user(claims["username"])
    if user is None:
        raise HTTPException(401, "Invalid token")
    return user


def require_editor(user=Depends(get_current_user)):
    if user["role"] != "hr-editor":
        raise HTTPException(403, "Editor role required")
    return user


# --- Health -------------------------------------------------------------------

@app.get("/api/health")
async def health():
    return {"status": "ok"}


# --- Auth endpoints -----------------------------------------------------------

@app.post("/api/auth/login")
async def login(request: Request):
    body = await request.json()
    username = body.get("username", "")
    password = body.get("password", "")
    user = await db.get_user(username)
    if user is None or not bcrypt.checkpw(password.encode(), user["password"].encode()):
        raise HTTPException(401, "Invalid credentials")
    token = create_token(username, user["role"])
    return {"token": token, "role": user["role"]}


@app.get("/api/auth/me")
async def me(user=Depends(get_current_user)):
    return {"username": user["username"], "role": user["role"]}


# --- Candidates ---------------------------------------------------------------

@app.get("/api/candidates")
async def list_candidates(user=Depends(get_current_user)):
    return await db.get_all_candidates()


@app.get("/api/candidates/{cid}")
async def get_candidate(cid: str, user=Depends(get_current_user)):
    c = await db.get_candidate(cid)
    if c is None:
        raise HTTPException(404, "Candidate not found")
    return c


@app.get("/api/candidates/{cid}/documents")
async def get_candidate_documents(cid: str, user=Depends(get_current_user)):
    c = await db.get_candidate(cid)
    if c is None:
        raise HTTPException(404, "Candidate not found")
    docs = await db.get_documents("candidate", cid)
    return [
        {
            "id": d["id"],
            "filename": d["filename"],
            "fileType": d["file_type"],
            "createdAt": d["created_at"].isoformat() if d["created_at"] else None,
        }
        for d in sorted(docs, key=lambda x: x.get("created_at") or "", reverse=True)
    ]


# --- Positions ----------------------------------------------------------------

@app.get("/api/positions")
async def list_positions(user=Depends(get_current_user)):
    return await db.get_all_positions()


@app.get("/api/positions/{pid}")
async def get_position(pid: str, user=Depends(get_current_user)):
    p = await db.get_position(pid)
    if p is None:
        raise HTTPException(404, "Position not found")
    return p


@app.put("/api/positions/{pid}")
async def update_position(pid: str, request: Request, user=Depends(require_editor)):
    body = await request.json()
    result = await db.update_position(pid, body)
    if result is None:
        raise HTTPException(404, "Position not found")
    return result


# --- Assignments --------------------------------------------------------------

@app.post("/api/candidates/{cid}/positions/{pid}", status_code=201)
async def assign_position(cid: str, pid: str, user=Depends(require_editor)):
    try:
        await db.assign_position(cid, pid)
    except ValueError as e:
        raise HTTPException(404, str(e))
    return {"status": "assigned"}


@app.delete("/api/candidates/{cid}/positions/{pid}", status_code=204)
async def unassign_position(cid: str, pid: str, user=Depends(require_editor)):
    await db.unassign_position(cid, pid)


@app.delete("/api/candidates/{cid}", status_code=204)
async def delete_candidate(cid: str, user=Depends(require_editor)):
    try:
        await db.delete_candidate(cid)
    except ValueError:
        raise HTTPException(404, "Candidate not found")


@app.delete("/api/positions/{pid}", status_code=204)
async def delete_position(pid: str, user=Depends(require_editor)):
    try:
        await db.delete_position(pid)
    except ValueError:
        raise HTTPException(404, "Position not found")


# --- Chat ---------------------------------------------------------------------

@app.post("/api/chat")
async def chat(request: Request, user=Depends(get_current_user)):
    body = await request.json()
    question = body.get("question", "").strip()
    if not question:
        raise HTTPException(400, "Question is required")
    if len(question) > 5000:
        raise HTTPException(400, "Question too long")
    history = body.get("history", [])
    if not isinstance(history, list):
        history = []
    return await chat_ask(question, history)


# --- Suggestions (Exercise 5) ------------------------------------------------

@app.get("/api/positions/{pid}/suggestions")
async def position_suggestions(pid: str, user=Depends(get_current_user)):
    try:
        p = await db.get_position(pid)
    except ValueError:
        raise HTTPException(422, "Invalid position ID")
    if p is None:
        raise HTTPException(404, "Position not found")
    return await suggest_candidates_for_position(pid)


@app.get("/api/candidates/{cid}/suggestions")
async def candidate_suggestions(cid: str, user=Depends(get_current_user)):
    try:
        c = await db.get_candidate(cid)
    except ValueError:
        raise HTTPException(422, "Invalid candidate ID")
    if c is None:
        raise HTTPException(404, "Candidate not found")
    return await suggest_positions_for_candidate(cid)


@app.post("/api/embeddings/rebuild", status_code=200)
async def rebuild_embeddings(user=Depends(require_editor)):
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        cids = [r["id"] for r in await conn.fetch("SELECT id FROM candidates")]
        pids = [r["id"] for r in await conn.fetch("SELECT id FROM positions")]

    errors = []
    c_count, p_count = 0, 0
    for cid in cids:
        try:
            await embed_candidate(str(cid))
            c_count += 1
        except Exception as e:
            errors.append(f"candidate {cid}: {e}")
    for pid in pids:
        try:
            await embed_position(str(pid))
            p_count += 1
        except Exception as e:
            errors.append(f"position {pid}: {e}")

    return {"candidates": c_count, "positions": p_count, "errors": errors}


# --- File serving -------------------------------------------------------------

@app.get("/api/files/{path:path}")
async def serve_file(path: str, user=Depends(get_current_user)):
    file_path = FILES_ROOT / path
    if not file_path.is_file():
        raise HTTPException(404, "File not found")
    try:
        file_path.resolve().relative_to(FILES_ROOT.resolve())
    except ValueError:
        raise HTTPException(403, "Forbidden")
    return FileResponse(file_path)


# --- Ingestion ----------------------------------------------------------------

ALLOWED_CV_EXTS = {".pdf", ".docx"}
ALLOWED_JOB_EXTS = {".txt"}
MAX_UPLOAD_BYTES = 20 * 1024 * 1024  # 20 MB


async def _save_upload(file: UploadFile, allowed_exts: set, subdir: str) -> str:
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in allowed_exts:
        raise HTTPException(400, f"Unsupported format: {ext}. Allowed: {allowed_exts}")
    safe_name = os.path.basename(file.filename or "upload" + ext)
    content = await file.read()
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(400, f"File too large. Max: {MAX_UPLOAD_BYTES // (1024*1024)} MB")
    dest = FILES_ROOT / subdir / safe_name
    with open(dest, "wb") as f:
        f.write(content)
    return str(dest)


async def _run_ingest(file, allowed_exts, subdir, ingest_fn):
    dest = await _save_upload(file, allowed_exts, subdir)
    try:
        result = await ingest_fn(dest)
        return JSONResponse(content=result, status_code=200 if result.get("isUpdate") else 201)
    except InsufficientDataError as e:
        raise HTTPException(422, f"Insufficient data: missing {', '.join(e.missing)}")
    except RuntimeError as e:
        raise HTTPException(422, str(e))


@app.post("/api/ingest/cv")
async def api_ingest_cv(file: UploadFile = File(...), user=Depends(require_editor)):
    return await _run_ingest(file, ALLOWED_CV_EXTS, "cvs", ingest_cv)


@app.post("/api/ingest/job")
async def api_ingest_job(file: UploadFile = File(...), user=Depends(require_editor)):
    return await _run_ingest(file, ALLOWED_JOB_EXTS, "jobs", ingest_job)


@app.get("/api/ingest/files")
async def api_ingest_files(user=Depends(get_current_user)):
    docs = await db.get_ingested_documents()
    cvs, jobs = [], []
    for d in docs:
        entry = {
            "filename": d["filename"],
            "entityId": str(d["entity_id"]),
            "ingestedAt": d["created_at"].isoformat() if d["created_at"] else None,
        }
        if d["entity_type"] == "candidate":
            entry["candidateName"] = d["candidate_name"]
            cvs.append(entry)
        else:
            entry["positionTitle"] = d["position_title"]
            jobs.append(entry)
    return {"cvs": cvs, "jobs": jobs}


@app.get("/api/ingest/stats")
async def api_ingest_stats(user=Depends(get_current_user)):
    return compute_stats(TOKEN_PRICES)


# --- Agent State (Exercise 6) ------------------------------------------------

@app.post("/api/agent/processed-emails", status_code=201)
async def api_mark_email_processed(request: Request, user=Depends(get_current_user)):
    body = await request.json()
    try:
        result = await db.mark_email_processed(
            body["email_id"], body["email_type"],
            body["action_taken"], body.get("draft_id"),
        )
        return result
    except Exception:
        raise HTTPException(409, "Email already processed")


@app.get("/api/agent/processed-emails")
async def api_list_processed_emails(user=Depends(get_current_user)):
    return await db.list_processed_emails()


@app.get("/api/agent/processed-emails/{eid}")
async def api_check_email_processed(eid: str, user=Depends(get_current_user)):
    return {"processed": await db.is_email_processed(eid)}


@app.post("/api/agent/notifications", status_code=201)
async def api_create_notification(request: Request, user=Depends(get_current_user)):
    body = await request.json()
    return await db.create_notification(
        type=body["type"], summary=body["summary"],
        action_url=body.get("action_url"),
        related_email_id=body.get("related_email_id"),
    )


@app.get("/api/agent/notifications")
async def api_list_notifications(status: str = None, user=Depends(get_current_user)):
    return await db.list_notifications(status=status)


@app.put("/api/agent/notifications/{nid}")
async def api_update_notification(nid: int, request: Request, user=Depends(get_current_user)):
    body = await request.json()
    result = await db.update_notification_status(nid, body["status"])
    if result is None:
        raise HTTPException(404, "Notification not found")
    return result
