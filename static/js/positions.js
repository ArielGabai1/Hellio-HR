import { getPositions, getPosition, getCandidate, getCandidates, assignPosition, removePosition, updatePosition, openFile, getRole, fetchPositionSuggestions, skillTag, esc } from "./store.js";
import { SEARCH_ICON, activeFilterCount, renderFilterPanel, updateBadge } from "./filters.js";

let filtersOpen = false;

const FILTER_DEFAULTS = {
  status: new Set(["open"]),
  level: new Set(),
  location: "",
  arrangement: "",
  techs: new Set(),
};
const filters = {
  status: new Set(["open"]),
  level: new Set(),
  location: "",
  arrangement: "",
  techs: new Set(),
};

// --- List view --------------------------------------------------------------

export function renderList(container) {
  const positions = getPositions();
  const allTechs = [...new Set(positions.flatMap(p => p.techStack))].sort();
  const levels = [...new Set(positions.map(p => p.experienceLevel))].sort();
  const locations = [...new Set(positions.map(p => p.location))].sort();
  const arrangements = [...new Set(positions.map(p => p.workArrangement))].sort();

  container.innerHTML = `
    <h1>Positions</h1>
    <div class="search-bar">
      <div class="search-wrapper">
        ${SEARCH_ICON}
        <input type="text" class="search-input" placeholder="Search by title, company, or tech...">
      </div>
      <button class="filter-toggle${filtersOpen ? ' active' : ''}">Filters${activeFilterCount(filters, FILTER_DEFAULTS) ? `<span class="filter-badge">${activeFilterCount(filters, FILTER_DEFAULTS)}</span>` : ''}</button>
    </div>
    <div id="filter-panel-slot"></div>
    <div class="card-grid" id="position-grid"></div>
  `;

  const grid = container.querySelector("#position-grid");
  const input = container.querySelector(".search-input");
  const filterBtn = container.querySelector(".filter-toggle");
  const panelSlot = container.querySelector("#filter-panel-slot");

  const fields = [
    { type: "pills", key: "status", label: "Status", values: ["open", "closed"], toggleMode: "multi-required" },
    { type: "pills", key: "level", label: "Level", values: levels },
    { type: "dropdown", key: "location", label: "Location", values: locations, placeholder: "All locations" },
    { type: "dropdown", key: "arrangement", label: "Work", values: arrangements, placeholder: "All arrangements" },
    { type: "tags", key: "techs", label: "Tech", allItems: allTechs, placeholder: "+ Add tech", inputAttr: "tech-input", suggestionsAttr: "tech-suggestions" },
  ];

  function onFilterUpdate() {
    renderPanel();
    render();
    updateBadge(filterBtn, filters, FILTER_DEFAULTS);
  }

  function renderPanel() {
    if (!filtersOpen) { panelSlot.innerHTML = ""; return; }
    renderFilterPanel(panelSlot, filters, FILTER_DEFAULTS, fields, onFilterUpdate);
  }

  function render() {
    const query = input.value.toLowerCase().trim();
    let filtered = positions;

    if (filters.status.size < 2) {
      filtered = filtered.filter(p => filters.status.has(p.status));
    }
    if (filters.level.size) {
      filtered = filtered.filter(p => filters.level.has(p.experienceLevel));
    }
    if (filters.location) {
      filtered = filtered.filter(p => p.location === filters.location);
    }
    if (filters.arrangement) {
      filtered = filtered.filter(p => p.workArrangement === filters.arrangement);
    }
    if (filters.techs.size) {
      filtered = filtered.filter(p => [...filters.techs].every(t => p.techStack.includes(t)));
    }
    if (query) {
      filtered = filtered.filter(p =>
        p.title.toLowerCase().includes(query) ||
        p.company.toLowerCase().includes(query) ||
        p.techStack.some(t => t.toLowerCase().includes(query))
      );
    }

    grid.innerHTML = filtered.map((p, i) => `
      <div class="position-card" data-id="${p.id}" tabindex="0" style="animation-delay: ${i * 40}ms">
        <div class="position-title">${p.title}</div>
        <div class="card-meta">
          <span>${p.company}</span>
          <span>${p.experienceLevel}</span>
          <span>${p.location}</span>
        </div>
        <div class="skill-tags">${p.techStack.slice(0, 5).map(t => skillTag(t)).join('')}</div>
        <div class="card-footer">
          <span class="status-badge ${p.status}">${p.status}</span>
          ${p.candidateIds.length ? `<span class="candidate-count">${p.candidateIds.length} candidate${p.candidateIds.length > 1 ? 's' : ''}</span>` : ''}
        </div>
      </div>
    `).join('');

    grid.querySelectorAll(".position-card").forEach(card => {
      card.addEventListener("click", () => {
        location.hash = `#/positions/${card.dataset.id}`;
      });
    });
  }

  input.addEventListener("input", render);
  filterBtn.addEventListener("click", () => {
    filtersOpen = !filtersOpen;
    filterBtn.classList.toggle("active", filtersOpen);
    renderPanel();
  });

  renderPanel();
  render();
}

// --- Detail view ------------------------------------------------------------

export function renderDetail(container, id) {
  const p = getPosition(id);
  if (!p) {
    container.innerHTML = `<div class="empty-state"><svg class="empty-state-icon" width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M9 9l6 6M15 9l-6 6" stroke-linecap="round"/></svg><h2>Position not found</h2><p>No position with ID ${id}</p></div>`;
    return;
  }

  const allCandidates = getCandidates();
  const availableCandidates = allCandidates.filter(c => !p.candidateIds.includes(c.id) && c.status === "active");

  container.innerHTML = `
    <a href="#/positions" class="back-link">&larr; Back to positions</a>
    <div class="position-header">
      <div class="position-detail-title">${p.title}</div>
      <div class="position-meta">
        <span>${p.company}</span>
        <span>${p.experienceLevel}</span>
        <span class="status-badge ${p.status}">${p.status}</span>
      </div>
      ${getRole() === 'hr-editor' ? '<button class="btn btn-secondary edit-position-btn" style="margin-left: auto;">Edit</button>' : ''}
    </div>

    <div class="suggestions-panel">
      <div class="section-title">Assigned Candidates</div>
      <div id="assigned-candidates"></div>
      ${getRole() === 'hr-editor' ? `
      <div class="add-candidate-row">
        <select id="add-candidate-select">
          <option value="">Add a candidate...</option>
          ${availableCandidates.map(c => `<option value="${c.id}">${c.name}</option>`).join('')}
        </select>
        <button class="btn btn-secondary" id="add-candidate-btn">Add</button>
      </div>` : ''}

      <div class="section-title">Top Candidate Suggestions</div>
      <div id="position-suggestions" data-testid="position-suggestions">
        <p class="empty-hint suggestions-loading">Loading suggestions...</p>
      </div>
    </div>

    <div class="info-grid" style="margin-bottom: var(--space-6)">
      <div>
        <div class="info-label">Location</div>
        <div class="info-value">${p.location}</div>
      </div>
      <div>
        <div class="info-label">Work Arrangement</div>
        <div class="info-value">${p.workArrangement}</div>
      </div>
      <div>
        <div class="info-label">Compensation</div>
        <div class="info-value">${p.compensation}</div>
      </div>
      <div>
        <div class="info-label">Timeline</div>
        <div class="info-value">${p.timeline}</div>
      </div>
    </div>

    <div class="section-title">Hiring Manager</div>
    <p style="font-size: 0.875rem; margin-bottom: var(--space-4)">
      ${p.hiringManager.name}, ${p.hiringManager.title}
      <br><span style="color: var(--ink-light)">${p.hiringManager.email}</span>
    </p>

    <div class="section-title">Tech Stack</div>
    <div class="skill-tags" style="margin-bottom: var(--space-4)">
      ${p.techStack.map(t => skillTag(t)).join('')}
    </div>

    <div class="section-title">Requirements</div>
    <ul class="detail-list" style="margin-bottom: var(--space-4)">
      ${p.requirements.map(r => `<li>${r}</li>`).join('')}
    </ul>

    ${p.niceToHave.length ? `
      <div class="section-title">Nice to Have</div>
      <ul class="detail-list" style="margin-bottom: var(--space-4)">
        ${p.niceToHave.map(n => `<li>${n}</li>`).join('')}
      </ul>
    ` : ''}

    <div class="section-title">Responsibilities</div>
    <ul class="detail-list" style="margin-bottom: var(--space-4)">
      ${p.responsibilities.map(r => `<li>${r}</li>`).join('')}
    </ul>

    ${p.jobFile ? `
      <div style="margin-bottom: var(--space-6)">
        <a href="/api/files/jobs/${p.jobFile}" class="btn btn-secondary job-file-link">View Original Job File</a>
      </div>
    ` : ''}
  `;

  container.querySelector(".job-file-link")?.addEventListener("click", (e) => {
    e.preventDefault();
    openFile(`jobs/${p.jobFile}`);
  });

  container.querySelector(".edit-position-btn")?.addEventListener("click", () => {
    renderEditForm(container, id);
  });

  function renderAssignedCandidates() {
    const el = container.querySelector("#assigned-candidates");
    const current = p.candidateIds.map(cid => getCandidate(cid)).filter(Boolean);
    if (!current.length) {
      el.innerHTML = `<p class="empty-hint">No candidates assigned</p>`;
      return;
    }
    const isEditor = getRole() === 'hr-editor';
    el.innerHTML = current.map(c => `
      <div class="candidate-mini-card">
        <a href="#/candidates/${c.id}">${esc(c.name)}</a>
        ${isEditor ? `<button class="btn btn-danger remove-candidate" data-cid="${c.id}">Remove</button>` : ''}
      </div>
    `).join('');
    el.querySelectorAll(".remove-candidate").forEach(btn => {
      btn.addEventListener("click", async () => {
        await removePosition(btn.dataset.cid, id);
        renderDetail(container, id);
      });
    });
  }

  renderAssignedCandidates();

  container.querySelector("#add-candidate-btn")?.addEventListener("click", async () => {
    const select = container.querySelector("#add-candidate-select");
    if (!select.value) return;
    await assignPosition(select.value, id);
    renderDetail(container, id);
  });

  // Load suggestions async
  const sugEl = container.querySelector("#position-suggestions");
  fetchPositionSuggestions(p.id).then(suggestions => {
    if (!suggestions.length) {
      sugEl.innerHTML = '<p class="empty-hint">No matching candidates found</p>';
      return;
    }
    const isEditor = getRole() === 'hr-editor';
    sugEl.innerHTML = suggestions.map(s => `
      <div class="suggestion-card" data-testid="suggestion-card">
        <div class="suggestion-header">
          <a href="#/candidates/${s.id}" class="suggestion-name">${s.name}</a>
          <div class="suggestion-header-actions">
            <span class="suggestion-score">${Math.round(s.score * 100)}%</span>
            ${isEditor ? `<button class="btn suggestion-assign-btn" data-cid="${s.id}" title="Assign">+</button>` : ''}
          </div>
        </div>
        <div class="suggestion-meta">${s.experienceLevel} &middot; ${s.location}</div>
        <div class="skill-tags">${(s.skills || []).slice(0, 5).map(sk => skillTag(sk)).join('')}</div>
        ${s.explanation ? `<div class="suggestion-explanation">${s.explanation}</div>` : ''}
      </div>
    `).join('');
    sugEl.querySelectorAll(".suggestion-assign-btn").forEach(btn => {
      btn.addEventListener("click", async (e) => {
        e.preventDefault();
        e.stopPropagation();
        btn.disabled = true;
        try {
          await assignPosition(btn.dataset.cid, id);
          renderDetail(container, id);
        } catch { btn.disabled = false; }
      });
    });
  }).catch(() => {
    sugEl.innerHTML = '<p class="empty-hint">Could not load suggestions</p>';
  });
}

// --- Edit form ---------------------------------------------------------------

function renderEditForm(container, id) {
  if (getRole() !== 'hr-editor') return renderDetail(container, id);
  const p = getPosition(id);
  if (!p) return;

  container.innerHTML = `
    <a href="#/positions/${id}" class="back-link">&larr; Cancel</a>
    <h1>Edit Position</h1>
    <form id="edit-position-form" class="edit-form">
      <div class="edit-field">
        <label for="edit-title">Title</label>
        <input id="edit-title" type="text" value="${esc(p.title)}" required>
      </div>
      <div class="edit-field">
        <label for="edit-company">Company</label>
        <input id="edit-company" type="text" value="${esc(p.company)}" required>
      </div>
      <div class="edit-row">
        <div class="edit-field">
          <label for="edit-status">Status</label>
          <select id="edit-status">
            <option value="open"${p.status === 'open' ? ' selected' : ''}>open</option>
            <option value="closed"${p.status === 'closed' ? ' selected' : ''}>closed</option>
          </select>
        </div>
        <div class="edit-field">
          <label for="edit-level">Experience Level</label>
          <input id="edit-level" type="text" value="${esc(p.experienceLevel)}">
        </div>
      </div>
      <div class="edit-row">
        <div class="edit-field">
          <label for="edit-location">Location</label>
          <input id="edit-location" type="text" value="${esc(p.location)}">
        </div>
        <div class="edit-field">
          <label for="edit-arrangement">Work Arrangement</label>
          <input id="edit-arrangement" type="text" value="${esc(p.workArrangement)}">
        </div>
      </div>
      <div class="edit-row">
        <div class="edit-field">
          <label for="edit-compensation">Compensation</label>
          <input id="edit-compensation" type="text" value="${esc(p.compensation)}">
        </div>
        <div class="edit-field">
          <label for="edit-timeline">Timeline</label>
          <input id="edit-timeline" type="text" value="${esc(p.timeline)}">
        </div>
      </div>
      <div class="edit-field">
        <label for="edit-techstack">Tech Stack (comma-separated)</label>
        <input id="edit-techstack" type="text" value="${esc(p.techStack.join(', '))}">
      </div>
      <div class="edit-field">
        <label for="edit-requirements">Requirements (one per line)</label>
        <textarea id="edit-requirements" rows="4">${esc(p.requirements.join('\n'))}</textarea>
      </div>
      <div class="edit-field">
        <label for="edit-nicetohave">Nice to Have (one per line)</label>
        <textarea id="edit-nicetohave" rows="3">${esc(p.niceToHave.join('\n'))}</textarea>
      </div>
      <div class="edit-field">
        <label for="edit-responsibilities">Responsibilities (one per line)</label>
        <textarea id="edit-responsibilities" rows="4">${esc(p.responsibilities.join('\n'))}</textarea>
      </div>
      <div id="edit-error" class="form-error" style="display:none"></div>
      <div class="edit-actions">
        <button type="submit" class="btn btn-primary">Save</button>
        <a href="#/positions/${id}" class="btn btn-secondary">Cancel</a>
      </div>
    </form>
  `;

  container.querySelector("#edit-position-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const data = {
      ...p,
      title: document.getElementById("edit-title").value,
      company: document.getElementById("edit-company").value,
      status: document.getElementById("edit-status").value,
      experienceLevel: document.getElementById("edit-level").value,
      location: document.getElementById("edit-location").value,
      workArrangement: document.getElementById("edit-arrangement").value,
      compensation: document.getElementById("edit-compensation").value,
      timeline: document.getElementById("edit-timeline").value,
      techStack: document.getElementById("edit-techstack").value.split(",").map(s => s.trim()).filter(Boolean),
      requirements: document.getElementById("edit-requirements").value.split("\n").filter(Boolean),
      niceToHave: document.getElementById("edit-nicetohave").value.split("\n").filter(Boolean),
      responsibilities: document.getElementById("edit-responsibilities").value.split("\n").filter(Boolean),
    };
    try {
      await updatePosition(id, data);
      renderDetail(container, id);
    } catch (err) {
      const errEl = document.getElementById("edit-error");
      errEl.textContent = "Failed to save changes";
      errEl.style.display = "block";
    }
  });

  container.querySelector(".back-link").addEventListener("click", (e) => {
    e.preventDefault();
    renderDetail(container, id);
  });
}

