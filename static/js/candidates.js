import { getCandidates, getCandidate, getPositions, getPosition, assignPosition, removePosition, openFile, getRole, fetchCandidateDocuments, fetchCandidateSuggestions, skillTag } from "./store.js";
import { SEARCH_ICON, activeFilterCount, renderFilterPanel, updateBadge } from "./filters.js";

let selected = new Set();
let filtersOpen = false;

const FILTER_DEFAULTS = {
  status: new Set(["active"]),
  level: new Set(),
  location: "",
  skills: new Set(),
  availability: "all",
};
const filters = {
  status: new Set(["active"]),
  level: new Set(),
  location: "",
  skills: new Set(),
  availability: "all",
};

function avatar(name, large) {
  const initials = name.split(' ').map(w => w[0]).join('').slice(0, 2);
  const hash = name.split('').reduce((a, c) => a + c.charCodeAt(0), 0);
  const variant = (hash % 5) + 1;
  return `<span class="avatar${large ? ' avatar-lg' : ''} avatar-${variant}">${initials}</span>`;
}

// --- List view --------------------------------------------------------------

export function renderList(container) {
  const candidates = getCandidates();
  const allSkills = [...new Set(candidates.flatMap(c => c.skills))].sort();
  const locations = [...new Set(candidates.map(c => c.contact.location))].sort();

  container.innerHTML = `
    <h1>Candidates</h1>
    <div class="search-bar">
      <div class="search-wrapper">
        ${SEARCH_ICON}
        <input type="text" class="search-input" placeholder="Search by name or skill...">
      </div>
      <button class="filter-toggle${filtersOpen ? ' active' : ''}">Filters${activeFilterCount(filters, FILTER_DEFAULTS) ? `<span class="filter-badge">${activeFilterCount(filters, FILTER_DEFAULTS)}</span>` : ''}</button>
    </div>
    <div id="filter-panel-slot"></div>
    <div class="card-grid" id="candidate-grid"></div>
  `;

  const grid = container.querySelector("#candidate-grid");
  const input = container.querySelector(".search-input");
  const filterBtn = container.querySelector(".filter-toggle");
  const panelSlot = container.querySelector("#filter-panel-slot");

  const fields = [
    { type: "pills", key: "status", label: "Status", values: ["active", "inactive"], toggleMode: "multi-required" },
    { type: "pills", key: "level", label: "Level", values: ["junior", "mid", "senior"] },
    { type: "dropdown", key: "location", label: "Location", values: locations, placeholder: "All locations" },
    { type: "tags", key: "skills", label: "Skills", allItems: allSkills, placeholder: "+ Add skill", inputAttr: "skill-input", suggestionsAttr: "skill-suggestions" },
    { type: "pills", key: "availability", label: "Availability", values: ["all", "available", "assigned"] },
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
    let filtered = candidates;

    if (filters.status.size < 2) {
      filtered = filtered.filter(c => filters.status.has(c.status));
    }
    if (filters.level.size) {
      filtered = filtered.filter(c => filters.level.has(c.experienceLevel));
    }
    if (filters.location) {
      filtered = filtered.filter(c => c.contact.location === filters.location);
    }
    if (filters.skills.size) {
      filtered = filtered.filter(c => [...filters.skills].every(s => c.skills.includes(s)));
    }
    if (filters.availability === "available") {
      filtered = filtered.filter(c => c.positionIds.length === 0);
    } else if (filters.availability === "assigned") {
      filtered = filtered.filter(c => c.positionIds.length > 0);
    }
    if (query) {
      filtered = filtered.filter(c =>
        c.name.toLowerCase().includes(query) ||
        c.skills.some(s => s.toLowerCase().includes(query))
      );
    }

    grid.innerHTML = filtered.map((c, i) => `
      <div class="candidate-card${selected.has(c.id) ? ' selected' : ''}" data-id="${c.id}" tabindex="0" style="animation-delay: ${i * 40}ms">
        <label class="compare-label">
          <input type="checkbox" class="compare-checkbox" data-id="${c.id}" ${selected.has(c.id) ? 'checked' : ''}
                 aria-label="Select ${c.name} for comparison">
          <span class="compare-label-text">Compare</span>
        </label>
        ${avatar(c.name)}
        <div class="candidate-card-body">
          <div class="candidate-name">${c.name}</div>
          <div class="card-meta">
            <span>${c.contact.location}</span>
            <span>${c.experienceLevel}</span>
          </div>
          <div class="candidate-summary">${c.summary}</div>
          <div class="skill-tags">${c.skills.slice(0, 4).map(s => skillTag(s)).join('')}</div>
          <div class="card-footer">
            <span class="status-badge ${c.status}">${c.status}</span>
            ${c.positionIds.length ? `<span class="candidate-count">${c.positionIds.length} position${c.positionIds.length > 1 ? 's' : ''}</span>` : ''}
          </div>
        </div>
      </div>
    `).join('');

    grid.querySelectorAll(".candidate-card").forEach(card => {
      card.addEventListener("click", () => {
        location.hash = `#/candidates/${card.dataset.id}`;
      });
    });

    grid.querySelectorAll(".compare-label").forEach(label => {
      label.addEventListener("click", (e) => e.stopPropagation());
    });

    grid.querySelectorAll(".compare-checkbox").forEach(cb => {
      cb.addEventListener("change", () => {
        const id = cb.dataset.id;
        if (cb.checked) {
          if (selected.size >= 2) {
            const first = selected.values().next().value;
            selected.delete(first);
          }
          selected.add(id);
        } else {
          selected.delete(id);
        }
        render();
        renderCompareBar();
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
  renderCompareBar();
}

// --- Compare bar ------------------------------------------------------------

function renderCompareBar() {
  let bar = document.querySelector(".compare-bar");
  if (selected.size === 0) {
    if (bar) bar.remove();
    return;
  }
  if (!bar) {
    bar = document.createElement("div");
    bar.className = "compare-bar";
    document.body.appendChild(bar);
  }

  const ids = [...selected];
  if (selected.size === 1) {
    const c = getCandidate(ids[0]);
    bar.innerHTML = `
      <span>${c.name} selected <span class="compare-count">1/2</span></span>
      <span style="color: var(--ink-faint); font-size: 0.8rem;">Select one more to compare</span>
    `;
  } else {
    const c1 = getCandidate(ids[0]);
    const c2 = getCandidate(ids[1]);
    bar.innerHTML = `
      <span>Compare: <strong>${c1.name}</strong> vs <strong>${c2.name}</strong> <span class="compare-count">2/2</span></span>
      <a href="#/candidates/compare/${ids[0]}/${ids[1]}" class="btn btn-primary">Compare</a>
    `;
  }
}

// --- Profile view -----------------------------------------------------------

export function renderProfile(container, id) {
  const c = getCandidate(id);
  if (!c) {
    container.innerHTML = `<div class="empty-state"><svg class="empty-state-icon" width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/><path d="M8 11h6" stroke-linecap="round"/></svg><h2>Candidate not found</h2><p>No candidate with ID ${id}</p></div>`;
    return;
  }

  const positions = getPositions();
  const assigned = c.positionIds.map(pid => getPosition(pid)).filter(Boolean);
  const available = positions.filter(p => !c.positionIds.includes(p.id) && p.status === "open");

  const linkedinUrl = c.contact.linkedin || '';
  const githubUrl = c.contact.github || '';
  const linkedinHref = linkedinUrl && !linkedinUrl.startsWith('http') ? `https://${linkedinUrl}` : linkedinUrl;
  const githubHref = githubUrl && !githubUrl.startsWith('http') ? `https://${githubUrl}` : githubUrl;

  container.innerHTML = `
    <a href="#/candidates" class="back-link">&larr; Back to candidates</a>
    <div class="profile-header">
      ${avatar(c.name, true)}
      <div class="profile-header-info">
        <div class="profile-name">${c.name}</div>
        <div class="profile-contact">
          <span>${c.contact.email}</span>
          <span>${c.contact.phone}</span>
          <span>${c.contact.location}</span>
          ${linkedinHref ? `<a href="${linkedinHref}" target="_blank" rel="noopener">LinkedIn</a>` : ''}
          ${githubHref ? `<a href="${githubHref}" target="_blank" rel="noopener">GitHub</a>` : ''}
        </div>
        <span class="status-badge ${c.status}">${c.status}</span>
        ${c.updatedAt ? `<div class="profile-updated">Updated ${new Date(c.updatedAt).toLocaleDateString("en-GB")}</div>` : ''}
        <div style="margin-top: var(--space-3); display: flex; gap: var(--space-2); align-items: center;">
          <a href="/api/files/cvs/${c.cvFile}" class="btn btn-primary cv-link">View CV</a>
          <button class="btn btn-secondary cv-history-toggle">CV History</button>
        </div>
        <div class="cv-version-list" id="cv-version-list" style="display: none;"></div>
      </div>
    </div>

    <div class="suggestions-panel">
      <div class="section-title">Assigned Positions</div>
      <div class="assigned-positions" id="assigned-positions"></div>
      ${getRole() === 'hr-editor' ? `
      <div class="add-position-row">
        <select id="add-position-select">
          <option value="">Add to position...</option>
          ${available.map(p => `<option value="${p.id}">${p.title} - ${p.company}</option>`).join('')}
        </select>
        <button class="btn btn-secondary" id="add-position-btn">Add</button>
      </div>` : ''}

      <div class="section-title">Suggested Positions</div>
      <div id="candidate-suggestions" data-testid="candidate-suggestions">
        <p class="empty-hint suggestions-loading">Loading suggestions...</p>
      </div>
    </div>

    <div class="section-title">Summary</div>
    <p style="font-size: 0.85rem; line-height: 1.7; color: var(--ink-light); margin-bottom: var(--space-4)">${c.summary}</p>

    <div class="section-title">Skills</div>
    <div class="skill-tags" style="margin-bottom: var(--space-4)">${c.skills.map(s => skillTag(s)).join('')}</div>

    <div class="section-title">Languages</div>
    <div class="skill-tags" style="margin-bottom: var(--space-4)">${c.languages.map(l => skillTag(l)).join('')}</div>

    <div class="section-title">Experience</div>
    ${c.experience.length ? `
      <div class="timeline">
        ${c.experience.map(exp => `
          <div class="timeline-item">
            <div class="timeline-role">${exp.title}</div>
            <div class="timeline-company">${exp.company} - ${exp.location}</div>
            <div class="timeline-dates">${exp.startDate} - ${exp.endDate || 'Present'}</div>
            <ul class="timeline-bullets">${exp.bullets.map(b => `<li>${b}</li>`).join('')}</ul>
          </div>
        `).join('')}
      </div>
    ` : `<p class="empty-hint">No experience listed</p>`}

    <div class="section-title">Education</div>
    ${c.education.length ? c.education.map(edu => `
      <div class="edu-item">
        <div class="edu-degree">${edu.degree}</div>
        <div class="edu-school">${edu.institution} (${edu.startDate} - ${edu.endDate})</div>
      </div>
    `).join('') : `<p class="empty-hint">No education listed</p>`}

    ${c.certifications.length ? `
      <div class="section-title">Certifications</div>
      ${c.certifications.map(cert => `
        <div class="cert-item">
          <div class="cert-name">${cert.name}</div>
          <div class="cert-year">${cert.year}</div>
        </div>
      `).join('')}
    ` : ''}
  `;

  function renderAssigned() {
    const el = container.querySelector("#assigned-positions");
    const current = c.positionIds.map(pid => getPosition(pid)).filter(Boolean);
    if (!current.length) {
      el.innerHTML = `<p class="empty-hint">No positions assigned</p>`;
      return;
    }
    const isEditor = getRole() === 'hr-editor';
    el.innerHTML = current.map(p => `
      <div class="assigned-position">
        <div class="assigned-position-info">
          <a href="#/positions/${p.id}">${p.title}</a>
          <div class="assigned-position-company">${p.company}</div>
        </div>
        ${isEditor ? `<button class="btn btn-danger remove-position" data-pid="${p.id}">Remove</button>` : ''}
      </div>
    `).join('');
    el.querySelectorAll(".remove-position").forEach(btn => {
      btn.addEventListener("click", async () => {
        await removePosition(id, btn.dataset.pid);
        renderProfile(container, id);
      });
    });
  }

  container.querySelector(".cv-link")?.addEventListener("click", (e) => {
    e.preventDefault();
    openFile(`cvs/${c.cvFile}`);
  });

  const historyToggle = container.querySelector(".cv-history-toggle");
  const historyList = container.querySelector("#cv-version-list");
  if (historyToggle && historyList) {
    historyToggle.addEventListener("click", async () => {
      if (historyList.style.display !== "none") {
        historyList.style.display = "none";
        return;
      }
      historyList.innerHTML = '<div style="font-size: 0.8rem; color: var(--ink-faint);">Loading...</div>';
      historyList.style.display = "block";
      try {
        const docs = await fetchCandidateDocuments(c.id);
        if (!docs.length) {
          historyList.innerHTML = '<div class="empty-hint">No documents found</div>';
          return;
        }
        historyList.innerHTML = docs.map((d, i) => `
          <div class="cv-version-item">
            <a href="#" class="cv-version-link" data-path="cvs/${d.filename}">${d.filename}</a>
            <span class="cv-version-date">${d.createdAt ? new Date(d.createdAt).toLocaleDateString("en-GB") : ""}</span>
            ${i === 0 ? '<span class="dedup-badge new">latest</span>' : ''}
          </div>
        `).join('');
        historyList.querySelectorAll(".cv-version-link").forEach(link => {
          link.addEventListener("click", (e) => {
            e.preventDefault();
            openFile(link.dataset.path);
          });
        });
      } catch {
        historyList.innerHTML = '<div class="empty-hint">Failed to load documents</div>';
      }
    });
  }

  renderAssigned();

  container.querySelector("#add-position-btn")?.addEventListener("click", async () => {
    const select = container.querySelector("#add-position-select");
    if (!select.value) return;
    await assignPosition(id, select.value);
    renderProfile(container, id);
  });

  // Load suggestions async
  const sugEl = container.querySelector("#candidate-suggestions");
  fetchCandidateSuggestions(c.id).then(suggestions => {
    if (!suggestions.length) {
      sugEl.innerHTML = '<p class="empty-hint">No matching positions found</p>';
      return;
    }
    const isEditor = getRole() === 'hr-editor';
    sugEl.innerHTML = suggestions.map(s => `
      <div class="suggestion-card" data-testid="suggestion-card">
        <div class="suggestion-header">
          <a href="#/positions/${s.id}" class="suggestion-name">${s.title}</a>
          <div class="suggestion-header-actions">
            <span class="suggestion-score">${Math.round(s.score * 100)}%</span>
            ${isEditor ? `<button class="btn suggestion-assign-btn" data-pid="${s.id}" title="Assign">+</button>` : ''}
          </div>
        </div>
        <div class="suggestion-meta">${s.company} &middot; ${s.location} &middot; ${s.experienceLevel}</div>
        <div class="skill-tags">${(s.techStack || []).slice(0, 5).map(t => skillTag(t)).join('')}</div>
        ${s.explanation ? `<div class="suggestion-explanation">${s.explanation}</div>` : ''}
      </div>
    `).join('');
    sugEl.querySelectorAll(".suggestion-assign-btn").forEach(btn => {
      btn.addEventListener("click", async (e) => {
        e.preventDefault();
        e.stopPropagation();
        btn.disabled = true;
        try {
          await assignPosition(id, btn.dataset.pid);
          renderProfile(container, id);
        } catch { btn.disabled = false; }
      });
    });
  }).catch(() => {
    sugEl.innerHTML = '<p class="empty-hint">Could not load suggestions</p>';
  });
}

// --- Compare view -----------------------------------------------------------

export function renderCompare(container, id1, id2) {
  const c1 = getCandidate(id1);
  const c2 = getCandidate(id2);
  if (!c1 || !c2) {
    container.innerHTML = `<div class="empty-state"><svg class="empty-state-icon" width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="3" width="7" height="18" rx="1"/><rect x="14" y="3" width="7" height="18" rx="1"/><path d="M8 11h8" stroke-linecap="round" stroke-dasharray="2 2"/></svg><h2>Comparison unavailable</h2><p>One or both candidates not found</p></div>`;
    return;
  }

  const sharedSet = new Set(c1.skills.filter(s => c2.skills.includes(s)));

  function skillHtml(skills) {
    return skills.map(s =>
      sharedSet.has(s) ? `<span class="skill-tag shared">${s}</span>` : skillTag(s)
    ).join('');
  }

  function colHtml(c) {
    return `
      <div class="compare-col">
        <div class="profile-name" style="font-size: 1.35rem;">${c.name}</div>
        <span class="status-badge ${c.status}">${c.status}</span>
        <div class="profile-contact" style="margin-top: var(--space-3)">
          <span>${c.contact.location}</span>
          <span>${c.contact.email}</span>
        </div>

        <p class="compare-summary">${c.summary}</p>

        ${c.languages.length ? `
          <div class="section-title">Languages</div>
          <div class="skill-tags">${c.languages.map(l => skillTag(l)).join('')}</div>
        ` : ''}

        <div class="section-title">Skills</div>
        <div class="skill-tags">${skillHtml(c.skills)}</div>

        <div class="section-title">Experience</div>
        ${c.experience.length ? `
          <div class="timeline">
            ${c.experience.map(exp => `
              <div class="timeline-item">
                <div class="timeline-role">${exp.title}</div>
                <div class="timeline-company">${exp.company}</div>
                <div class="timeline-dates">${exp.startDate} - ${exp.endDate || 'Present'}</div>
                <ul class="timeline-bullets">${exp.bullets.map(b => `<li>${b}</li>`).join('')}</ul>
              </div>
            `).join('')}
          </div>
        ` : `<p class="empty-hint">No experience listed</p>`}

        <div class="section-title">Education</div>
        ${c.education.length ? c.education.map(edu => `
          <div class="edu-item">
            <div class="edu-degree">${edu.degree}</div>
            <div class="edu-school">${edu.institution}</div>
          </div>
        `).join('') : `<p class="empty-hint">No education listed</p>`}

        ${c.certifications.length ? `
          <div class="section-title">Certifications</div>
          ${c.certifications.map(cert => `
            <div class="cert-item">
              <div class="cert-name">${cert.name}</div>
              <div class="cert-year">${cert.year}</div>
            </div>
          `).join('')}
        ` : ''}
      </div>
    `;
  }

  container.innerHTML = `
    <a href="#/candidates" class="back-link">&larr; Back to candidates</a>
    <h1>Compare Candidates</h1>
    <div style="margin-bottom: var(--space-5)">
      <span style="font-size: 0.8rem; color: var(--ink-light);">Shared skills (${sharedSet.size}) are highlighted</span>
    </div>
    <div class="compare-grid">
      ${colHtml(c1)}
      ${colHtml(c2)}
    </div>
  `;
}
