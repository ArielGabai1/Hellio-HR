const BASE = "/api";

let candidates = [];
let positions = [];
let listeners = [];

function getToken() { return localStorage.getItem("token"); }
export function setToken(t) { localStorage.setItem("token", t); }
export function clearToken() {
  localStorage.removeItem("token");
  localStorage.removeItem("role");
  localStorage.removeItem("username");
}
export function isLoggedIn() { return !!getToken(); }
export function getRole() { return localStorage.getItem("role") || "hr-viewer"; }
export function getUsername() { return localStorage.getItem("username") || ""; }

function authHeaders() {
  return { "Authorization": `Bearer ${getToken()}`, "Content-Type": "application/json" };
}

async function apiFetch(path, opts = {}) {
  opts.headers = { ...authHeaders(), ...opts.headers };
  const res = await fetch(`${BASE}${path}`, opts);
  if (res.status === 401) {
    clearToken();
    location.hash = "#/login";
    throw new Error("Unauthorized");
  }
  if (!res.ok) throw new Error(`Request failed: ${res.status}`);
  return res;
}

async function fetchCandidates() {
  const res = await apiFetch("/candidates");
  candidates = await res.json();
  return candidates;
}

async function fetchPositions() {
  const res = await apiFetch("/positions");
  positions = await res.json();
  return positions;
}

export async function init() {
  await Promise.all([fetchCandidates(), fetchPositions()]);
}

export function getCandidates() { return candidates; }
export function getPositions() { return positions; }

export function getCandidate(id) {
  return candidates.find(c => c.id === id);
}

export function getPosition(id) {
  return positions.find(p => p.id === id);
}

export async function assignPosition(candidateId, positionId) {
  await apiFetch(`/candidates/${candidateId}/positions/${positionId}`, { method: "POST" });
  // Refresh both lists to get updated junction data
  await Promise.all([fetchCandidates(), fetchPositions()]);
  notify();
}

export async function removePosition(candidateId, positionId) {
  await apiFetch(`/candidates/${candidateId}/positions/${positionId}`, { method: "DELETE" });
  await Promise.all([fetchCandidates(), fetchPositions()]);
  notify();
}

export async function updatePosition(positionId, data) {
  const res = await apiFetch(`/positions/${positionId}`, {
    method: "PUT",
    body: JSON.stringify(data),
  });
  const updated = await res.json();
  const idx = positions.findIndex(p => p.id === positionId);
  if (idx >= 0) positions[idx] = updated;
  notify();
  return updated;
}

export async function login(username, password) {
  const res = await fetch(`${BASE}/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  if (!res.ok) throw new Error("Invalid credentials");
  const data = await res.json();
  setToken(data.token);
  localStorage.setItem("role", data.role);
  localStorage.setItem("username", username);
}

export function subscribe(fn) {
  listeners.push(fn);
  return () => { listeners = listeners.filter(l => l !== fn); };
}

export function notify() {
  for (const fn of listeners) fn();
}

export async function openFile(path) {
  const res = await apiFetch(`/files/${path}`);
  const blob = await res.blob();
  window.open(URL.createObjectURL(blob), "_blank");
}

export async function uploadFile(endpoint, file) {
  const form = new FormData();
  form.append("file", file);
  const res = await fetch(`${BASE}${endpoint}`, {
    method: "POST",
    headers: { "Authorization": `Bearer ${getToken()}` },
    body: form,
  });
  if (res.status === 401) {
    clearToken();
    location.hash = "#/login";
    throw new Error("Unauthorized");
  }
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Upload failed: ${res.status}`);
  }
  const data = await res.json();
  await Promise.all([fetchCandidates(), fetchPositions()]);
  notify();
  return data;
}

export async function fetchCandidateDocuments(cid) {
  const res = await apiFetch(`/candidates/${cid}/documents`);
  return res.json();
}

export async function fetchIngestFiles() {
  const res = await apiFetch("/ingest/files");
  return res.json();
}

export async function fetchIngestStats() {
  const res = await apiFetch("/ingest/stats");
  return res.json();
}

export async function deleteEntity(type, id) {
  const endpoint = type === "candidate" ? `/candidates/${id}` : `/positions/${id}`;
  const res = await apiFetch(endpoint, { method: "DELETE" });
  if (!res.ok) throw new Error(`Delete failed: ${res.status}`);
  await Promise.all([fetchCandidates(), fetchPositions()]);
  notify();
}

export async function bulkDeleteEntities(type, ids) {
  const prefix = type === "candidate" ? "/candidates" : "/positions";
  for (const id of ids) {
    await apiFetch(`${prefix}/${id}`, { method: "DELETE" });
  }
  await Promise.all([fetchCandidates(), fetchPositions()]);
  notify();
}

export async function fetchPositionSuggestions(pid) {
  const res = await apiFetch(`/positions/${pid}/suggestions`);
  return res.json();
}

export async function fetchCandidateSuggestions(cid) {
  const res = await apiFetch(`/candidates/${cid}/suggestions`);
  return res.json();
}

// --- Agent Notifications ---

export async function fetchNotifications(status) {
  const qs = status ? `?status=${status}` : "";
  const res = await apiFetch(`/agent/notifications${qs}`);
  return res.json();
}

export async function acknowledgeNotification(id) {
  await apiFetch(`/agent/notifications/${id}`, {
    method: "PUT",
    body: JSON.stringify({ status: "acknowledged" }),
  });
}

export async function sendChat(question, history) {
  const res = await apiFetch("/chat", {
    method: "POST",
    body: JSON.stringify({ question, history }),
  });
  return res.json();
}

// Deterministic skill color: same skill name -> same color everywhere
const TAG_COUNT = 4;
function hashSkill(name) {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = ((h << 5) - h + name.charCodeAt(i)) | 0;
  return ((h % TAG_COUNT) + TAG_COUNT) % TAG_COUNT;
}
export function skillTag(name) {
  return `<span class="skill-tag" data-tag="${hashSkill(name) + 1}">${name}</span>`;
}

// HTML escaping -- safe for both content and attribute contexts
export function esc(s) {
  if (!s) return "";
  return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

