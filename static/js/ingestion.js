import { uploadFile, fetchIngestFiles, deleteEntity, bulkDeleteEntities, getRole, skillTag, esc } from "./store.js";

let activeTab = "upload";

// Module-level batch state -- survives SPA navigation
let batch = null; // { active, total, done, entityType, currentFile, resultHtml[], statusHtml }

function onBeforeUnload(e) {
  e.preventDefault();
  e.returnValue = "";
}

export function renderIngestion(container) {
  container.innerHTML = `
    <h1>Import</h1>
    <div class="ingest-tabs">
      <button class="ingest-tab${activeTab === 'upload' ? ' active' : ''}" data-tab="upload">Upload</button>
      <button class="ingest-tab${activeTab === 'inventory' ? ' active' : ''}" data-tab="inventory">Inventory</button>
    </div>
    <div id="ingest-content"></div>
  `;

  container.querySelectorAll(".ingest-tab").forEach(btn => {
    btn.addEventListener("click", () => {
      if (batch && !batch.active) batch = null;
      activeTab = btn.dataset.tab;
      renderIngestion(container);
    });
  });

  const content = container.querySelector("#ingest-content");
  if (activeTab === "upload") renderUpload(content);
  else renderInventory(content);
}

// --- Upload tab ---

function renderUpload(el) {
  if (getRole() !== "hr-editor") {
    el.innerHTML = `<div class="empty-hint">Editor role required to upload documents.</div>`;
    return;
  }

  const disableZones = batch && batch.active;

  el.innerHTML = `
    <div class="upload-dropzone${disableZones ? ' disabled' : ''}" id="cv-zone">
      <div class="upload-dropzone-inner">
        <div class="upload-dropzone-icon">
          <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="12" y1="18" x2="12" y2="12"/><polyline points="9 15 12 12 15 15"/></svg>
        </div>
        <div class="upload-dropzone-text">
          <div class="upload-dropzone-title">Upload CVs</div>
          <div class="upload-dropzone-hint">Drag and drop files here, or click to browse</div>
          <div class="upload-dropzone-formats">PDF, DOCX</div>
        </div>
      </div>
      <input type="file" accept=".pdf,.docx" class="upload-input" id="cv-input" multiple${disableZones ? ' disabled' : ''}>
    </div>
    <div class="upload-dropzone${disableZones ? ' disabled' : ''}" id="job-zone">
      <div class="upload-dropzone-inner">
        <div class="upload-dropzone-icon">
          <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 00-2-2h-4a2 2 0 00-2 2v16"/></svg>
        </div>
        <div class="upload-dropzone-text">
          <div class="upload-dropzone-title">Upload Job Descriptions</div>
          <div class="upload-dropzone-hint">Drag and drop files here, or click to browse</div>
          <div class="upload-dropzone-formats">TXT</div>
        </div>
      </div>
      <input type="file" accept=".txt" class="upload-input" id="job-input" multiple${disableZones ? ' disabled' : ''}>
    </div>
    <div id="upload-status"></div>
    <div id="upload-result"></div>
  `;

  // Restore batch progress if exists
  if (batch) {
    el.querySelector("#upload-status").innerHTML = batch.statusHtml;
    el.querySelector("#upload-result").innerHTML = batch.resultHtml.join("");
  }

  if (!disableZones) {
    wireUploadZone(el, "cv-zone", "cv-input", "/ingest/cv", "candidate");
    wireUploadZone(el, "job-zone", "job-input", "/ingest/job", "position");
  }
}

function wireUploadZone(el, zoneId, inputId, endpoint, entityType) {
  const zone = el.querySelector(`#${zoneId}`);
  const input = el.querySelector(`#${inputId}`);

  zone.addEventListener("click", (e) => {
    if (e.target !== input) input.click();
  });

  zone.addEventListener("dragover", (e) => {
    e.preventDefault();
    zone.classList.add("dragover");
  });
  zone.addEventListener("dragleave", () => zone.classList.remove("dragover"));
  zone.addEventListener("drop", (e) => {
    e.preventDefault();
    zone.classList.remove("dragover");
    if (e.dataTransfer.files.length) handleUpload(el, [...e.dataTransfer.files], endpoint, entityType);
  });

  input.addEventListener("change", () => {
    if (input.files.length) {
      const files = [...input.files];
      input.value = ""; // reset so re-selecting same file triggers change
      handleUpload(el, files, endpoint, entityType);
    }
  });
}

// Try to update the live DOM status/result elements (no-op if navigated away)
function updateLiveDOM(statusHtml, appendResultHtml) {
  const status = document.querySelector("#upload-status");
  const result = document.querySelector("#upload-result");
  if (status) status.innerHTML = statusHtml;
  if (result && appendResultHtml) result.insertAdjacentHTML("beforeend", appendResultHtml);
}

function setDropzonesDisabled(disabled) {
  document.querySelectorAll(".upload-dropzone").forEach(z => {
    z.classList.toggle("disabled", disabled);
  });
  document.querySelectorAll(".upload-input").forEach(i => {
    i.disabled = disabled;
  });
}

async function handleUpload(el, files, endpoint, entityType) {
  const total = files.length;

  batch = { active: true, total, done: 0, newCount: 0, updatedCount: 0, entityType, currentFile: "", resultHtml: [], statusHtml: "" };
  window.addEventListener("beforeunload", onBeforeUnload);
  setDropzonesDisabled(true);

  try {
    for (const file of files) {
      batch.currentFile = file.name;
      batch.statusHtml = `<div class="ingest-loading">Processing <strong>${esc(file.name)}</strong>${total > 1 ? ` (${batch.done + 1}/${total})` : ""}...</div>`;
      updateLiveDOM(batch.statusHtml, null);

      try {
        const data = await uploadFile(endpoint, file);
        batch.done++;
        if (data.isUpdate) batch.updatedCount++; else batch.newCount++;
        const card = entityType === "candidate" ? renderCandidateResult(data) : renderPositionResult(data);
        batch.resultHtml.push(card);
        batch.statusHtml = `<div class="ingest-loading">Processing${total > 1 ? ` (${batch.done}/${total} done)` : ""}...</div>`;
        updateLiveDOM(batch.statusHtml, card);
      } catch (err) {
        batch.done++;
        const errHtml = `<div class="ingest-error" style="margin-bottom: var(--space-3)"><strong>${esc(file.name)}</strong>: ${esc(err.message)}</div>`;
        batch.resultHtml.push(errHtml);
        updateLiveDOM(batch.statusHtml, errHtml);
      }
    }

    const breakdown = (batch.newCount || batch.updatedCount)
      ? ` (${batch.newCount} new, ${batch.updatedCount} updated)`
      : "";
    batch.statusHtml = `<div class="ingest-success">${batch.done} ${entityType}${batch.done !== 1 ? "s" : ""} processed${breakdown}</div>`;
    updateLiveDOM(batch.statusHtml, null);
  } finally {
    batch.active = false;
    window.removeEventListener("beforeunload", onBeforeUnload);
    setDropzonesDisabled(false);
  }
}

function renderCandidateResult(c) {
  const isUpdate = c.isUpdate === true;
  const dedupBadge = isUpdate
    ? '<span class="dedup-badge updated">Updated</span>'
    : '<span class="dedup-badge new">New</span>';
  const nameChanged = isUpdate && c.previousName && c.previousName !== c.name;
  const changeInfo = isUpdate && c.changes?.length
    ? `<div class="dedup-info">${esc(c.changes.join(", "))}${nameChanged ? ` (was: ${esc(c.previousName)})` : ""}</div>`
    : (nameChanged ? `<div class="dedup-info">was: ${esc(c.previousName)}</div>` : "");

  return `
    <div class="ingest-result${isUpdate ? ' ingest-result-updated' : ''}">
      <div class="ingest-result-header">
        <a href="#/candidates/${c.id}" class="ingest-result-title">${esc(c.name)}</a>
        <span class="status-badge ${c.status}">${c.status}</span>
        ${dedupBadge}
      </div>
      ${changeInfo}
      ${c.summary ? `<p class="ingest-result-summary">${esc(c.summary)}</p>` : ""}
      <div class="ingest-result-meta">
        ${c.contact?.email ? `<span>${esc(c.contact.email)}</span>` : ""}
        ${c.contact?.location ? `<span>${esc(c.contact.location)}</span>` : ""}
        ${c.experienceLevel ? `<span>${esc(c.experienceLevel)}</span>` : ""}
      </div>
      ${c.skills?.length ? `
        <div class="skill-tags" style="margin-top: var(--space-3)">
          ${c.skills.map(s => skillTag(s)).join("")}
        </div>
      ` : ""}
      ${c.experience?.length ? `
        <div class="section-title" style="margin-top: var(--space-4)">Experience</div>
        ${c.experience.map(e => `
          <div style="margin-bottom: var(--space-3)">
            <div style="font-weight: 600; font-size: 0.85rem">${esc(e.title)} at ${esc(e.company)}</div>
            <div style="font-size: 0.75rem; color: var(--ink-light)">${esc(e.startDate || "")} - ${esc(e.endDate || "Present")}</div>
          </div>
        `).join("")}
      ` : ""}
    </div>
  `;
}

function renderPositionResult(p) {
  return `
    <div class="ingest-result">
      <div class="ingest-result-header">
        <a href="#/positions/${p.id}" class="ingest-result-title">${esc(p.title)}</a>
        <span class="status-badge ${p.status}">${p.status}</span>
      </div>
      <div class="ingest-result-meta">
        ${p.company ? `<span>${esc(p.company)}</span>` : ""}
        ${p.location ? `<span>${esc(p.location)}</span>` : ""}
        ${p.experienceLevel ? `<span>${esc(p.experienceLevel)}</span>` : ""}
      </div>
      ${p.summary ? `<p class="ingest-result-summary">${esc(p.summary)}</p>` : ""}
      ${p.techStack?.length ? `
        <div class="skill-tags" style="margin-top: var(--space-3)">
          ${p.techStack.map(s => skillTag(s)).join("")}
        </div>
      ` : ""}
    </div>
  `;
}

// --- Inventory tab ---

function formatIL(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  return d.toLocaleString("he-IL", { timeZone: "Asia/Jerusalem", day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" });
}

function docItem(d, type) {
  const label = type === "cv" ? esc(d.candidateName) : esc(d.positionTitle);
  const href = type === "cv" ? `#/candidates/${d.entityId}` : `#/positions/${d.entityId}`;
  const canDelete = getRole() === "hr-editor";
  const deleteType = type === "cv" ? "candidate" : "position";
  return `
    <div class="ingest-file-item ingested">
      <div class="ingest-file-info">
        <div class="ingest-file-row">
          <a href="${href}" class="ingest-file-link">${label}</a>
          ${canDelete ? `<button class="ingest-delete-btn" data-type="${deleteType}" data-id="${d.entityId}" title="Delete">&#x2715;</button>` : ""}
        </div>
        <div class="ingest-file-meta">
          <span class="ingest-file-name">${esc(d.filename)}</span>
          ${d.ingestedAt ? `<span class="ingest-file-date">${formatIL(d.ingestedAt)}</span>` : ""}
        </div>
      </div>
    </div>`;
}

async function renderInventory(el) {
  el.innerHTML = `<div class="ingest-loading">Loading inventory...</div>`;
  try {
    const inv = await fetchIngestFiles();
    const canDelete = getRole() === "hr-editor";
    el.innerHTML = `
      <div class="ingest-inventory-grid">
        <div class="ingest-inventory-section">
          <div class="section-title" style="margin-top: 0">CVs <span class="ingest-count">${inv.cvs.length}</span></div>
          ${canDelete && inv.cvs.length ? '<button class="ingest-delete-all-btn" data-type="candidate">Delete All Candidates</button>' : ""}
          <div class="ingest-file-list">
            ${inv.cvs.length ? inv.cvs.map(d => docItem(d, "cv")).join("") : '<div class="empty-hint">No CVs imported yet</div>'}
          </div>
        </div>
        <div class="ingest-inventory-section">
          <div class="section-title" style="margin-top: 0">Jobs <span class="ingest-count">${inv.jobs.length}</span></div>
          ${canDelete && inv.jobs.length ? '<button class="ingest-delete-all-btn" data-type="position">Delete All Positions</button>' : ""}
          <div class="ingest-file-list">
            ${inv.jobs.length ? inv.jobs.map(d => docItem(d, "job")).join("") : '<div class="empty-hint">No Positions imported yet</div>'}
          </div>
        </div>
      </div>
    `;
    // Wire individual delete buttons
    el.querySelectorAll(".ingest-delete-btn").forEach(btn => {
      btn.addEventListener("click", async (e) => {
        e.stopPropagation();
        const { type, id } = btn.dataset;
        const name = type === "candidate" ? "candidate" : "position";
        if (!confirm(`Delete this ${name}? This cannot be undone.`)) return;
        btn.disabled = true;
        try {
          await deleteEntity(type, id);
          renderInventory(el);
        } catch (err) {
          alert(`Failed to delete: ${err.message}`);
          btn.disabled = false;
        }
      });
    });
    // Wire bulk delete buttons
    el.querySelectorAll(".ingest-delete-all-btn").forEach(btn => {
      btn.addEventListener("click", async () => {
        const type = btn.dataset.type;
        const items = type === "candidate" ? inv.cvs : inv.jobs;
        const label = type === "candidate" ? "candidates" : "positions";
        const originalText = btn.textContent;
        if (!confirm(`Delete all ${items.length} ${label}? This cannot be undone.`)) return;
        btn.disabled = true;
        btn.textContent = "Deleting...";
        try {
          await bulkDeleteEntities(type, items.map(d => d.entityId));
          renderInventory(el);
        } catch (err) {
          alert(`Failed to delete: ${err.message}`);
          btn.disabled = false;
          btn.textContent = originalText;
        }
      });
    });
  } catch (err) {
    el.innerHTML = `<div class="ingest-error">Failed to load inventory: ${esc(err.message)}</div>`;
  }
}

