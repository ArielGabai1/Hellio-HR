"""Backend API auth -- cached JWT token with auto-refresh on 401."""

import os
import httpx

BACKEND_URL = os.environ.get("BACKEND_URL", "http://hellio-hr:8000")
_token = None


def _login():
    global _token
    resp = httpx.post(f"{BACKEND_URL}/api/auth/login", json={
        "username": os.environ.get("BACKEND_USERNAME", "admin"),
        "password": os.environ.get("BACKEND_PASSWORD", "admin"),
    })
    resp.raise_for_status()
    _token = resp.json()["token"]


def _get_token():
    if not _token:
        _login()
    return _token


def clear_token():
    global _token
    _token = None


def headers():
    return {"Authorization": f"Bearer {_get_token()}"}


def authed_request(method, url, **kwargs):
    """HTTP request with auto-retry on 401 (expired JWT)."""
    resp = method(url, headers=headers(), **kwargs)
    if resp.status_code == 401:
        clear_token()
        _login()
        resp = method(url, headers=headers(), **kwargs)
    return resp
