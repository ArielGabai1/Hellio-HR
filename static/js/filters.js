const SEARCH_ICON = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg>`;

// --- Filter state helpers ---------------------------------------------------

function activeFilterCount(filters, defaults) {
  let n = 0;
  for (const key of Object.keys(defaults)) {
    const cur = filters[key];
    const def = defaults[key];
    if (cur instanceof Set && def instanceof Set) {
      if (cur.size !== def.size || ![...def].every(v => cur.has(v))) n++;
    } else if (cur instanceof Set) {
      if (cur.size) n++;
    } else {
      if (cur !== def) n++;
    }
  }
  return n;
}

function resetFilters(filters, defaults) {
  for (const key of Object.keys(defaults)) {
    const def = defaults[key];
    if (def instanceof Set) {
      filters[key].clear();
      for (const v of def) filters[key].add(v);
    } else {
      filters[key] = def;
    }
  }
}

// --- Panel rendering --------------------------------------------------------

function renderFilterPanel(panelSlot, filters, defaults, fields, onUpdate) {
  const count = activeFilterCount(filters, defaults);
  panelSlot.innerHTML = `
    <div class="filter-panel">
      <div class="filter-header">
        <span style="font-size:0.8rem;font-weight:600;">Filters</span>
        ${count ? `<span class="filter-clear" data-action="clear-all">Clear all</span>` : ''}
      </div>
      ${fields.map(f => renderField(f, filters)).join('')}
    </div>
  `;
  wirePanel(panelSlot, filters, defaults, fields, onUpdate);
}

function renderField(field, filters) {
  if (field.type === "pills") {
    return `
      <div class="filter-row">
        <span class="filter-label">${field.label}</span>
        <div class="filter-pills" data-filter="${field.key}">
          ${field.values.map(v =>
            `<button class="filter-pill${isActive(filters[field.key], v) ? ' active' : ''}" data-value="${v}">${v}</button>`
          ).join('')}
        </div>
      </div>`;
  }

  if (field.type === "dropdown") {
    return `
      <div class="filter-row">
        <span class="filter-label">${field.label}</span>
        <select class="filter-dropdown" data-filter="${field.key}">
          <option value="">${field.placeholder}</option>
          ${field.values.map(v => `<option value="${v}"${filters[field.key] === v ? ' selected' : ''}>${v}</option>`).join('')}
        </select>
      </div>`;
  }

  if (field.type === "tags") {
    const set = filters[field.key];
    return `
      <div class="filter-row">
        <span class="filter-label">${field.label}</span>
        <div class="filter-tag-wrapper">
          <div class="filter-tags" data-filter="${field.key}">
            ${[...set].map(s => `<span class="filter-tag">${s}<span class="filter-tag-remove" data-skill="${s}">&times;</span></span>`).join('')}
            <input type="text" class="filter-tag-input" placeholder="${field.placeholder}" data-filter="${field.inputAttr}">
          </div>
          <div class="filter-suggestions" style="display:none" data-filter="${field.suggestionsAttr}"></div>
        </div>
      </div>`;
  }

  return '';
}

function isActive(filterVal, value) {
  if (filterVal instanceof Set) return filterVal.has(value);
  return filterVal === value;
}

// --- Panel wiring -----------------------------------------------------------

function wirePanel(panelSlot, filters, defaults, fields, onUpdate) {
  for (const field of fields) {
    if (field.type === "pills") {
      wirePills(panelSlot, filters, field, onUpdate);
    } else if (field.type === "dropdown") {
      wireDropdown(panelSlot, filters, field, onUpdate);
    } else if (field.type === "tags") {
      wireTags(panelSlot, filters, field, onUpdate);
    }
  }

  // Clear all
  const clearBtn = panelSlot.querySelector('[data-action="clear-all"]');
  if (clearBtn) {
    clearBtn.addEventListener("click", () => {
      resetFilters(filters, defaults);
      onUpdate();
    });
  }
}

function wirePills(panelSlot, filters, field, onUpdate) {
  panelSlot.querySelectorAll(`[data-filter="${field.key}"] .filter-pill`).forEach(btn => {
    btn.addEventListener("click", () => {
      const v = btn.dataset.value;
      const set = filters[field.key];
      if (set instanceof Set) {
        // Toggle behavior: at least one must remain selected for status-like pills
        if (field.toggleMode === "multi-required") {
          if (set.has(v)) {
            if (set.size > 1) set.delete(v);
          } else {
            set.add(v);
          }
        } else {
          set.has(v) ? set.delete(v) : set.add(v);
        }
      } else {
        filters[field.key] = v;
      }
      onUpdate();
    });
  });
}

function wireDropdown(panelSlot, filters, field, onUpdate) {
  const select = panelSlot.querySelector(`[data-filter="${field.key}"]`);
  if (select) {
    select.addEventListener("change", () => {
      filters[field.key] = select.value;
      onUpdate();
    });
  }
}

function wireTags(panelSlot, filters, field, onUpdate) {
  const input = panelSlot.querySelector(`[data-filter="${field.inputAttr}"]`);
  const sugBox = panelSlot.querySelector(`[data-filter="${field.suggestionsAttr}"]`);
  if (!input || !sugBox) return;

  const set = filters[field.key];
  let highlightIdx = -1;

  function showSuggestions() {
    const q = input.value.toLowerCase().trim();
    if (!q) { sugBox.style.display = "none"; return; }
    const matches = field.allItems.filter(s => s.toLowerCase().includes(q) && !set.has(s));
    if (!matches.length) { sugBox.style.display = "none"; return; }
    highlightIdx = -1;
    sugBox.style.display = "block";
    sugBox.innerHTML = matches.map(s => `<div class="filter-suggestion" data-skill="${s}">${s}</div>`).join('');
    sugBox.querySelectorAll(".filter-suggestion").forEach(el => {
      el.addEventListener("click", () => addItem(el.dataset.skill));
    });
  }

  function addItem(s) {
    set.add(s);
    input.value = "";
    sugBox.style.display = "none";
    onUpdate();
  }

  input.addEventListener("input", showSuggestions);
  input.addEventListener("keydown", (e) => {
    const items = sugBox.querySelectorAll(".filter-suggestion");
    if (e.key === "ArrowDown") {
      e.preventDefault();
      highlightIdx = Math.min(highlightIdx + 1, items.length - 1);
      updateHighlight(items);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      highlightIdx = Math.max(highlightIdx - 1, 0);
      updateHighlight(items);
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (highlightIdx >= 0 && items[highlightIdx]) addItem(items[highlightIdx].dataset.skill);
    } else if (e.key === "Escape") {
      sugBox.style.display = "none";
    }
  });

  function updateHighlight(items) {
    items.forEach((el, i) => el.classList.toggle("highlighted", i === highlightIdx));
  }

  // Remove tag buttons
  panelSlot.querySelectorAll(`[data-filter="${field.key}"] .filter-tag-remove`).forEach(btn => {
    btn.addEventListener("click", () => {
      set.delete(btn.dataset.skill);
      onUpdate();
    });
  });
}

// --- Badge ------------------------------------------------------------------

function updateBadge(filterBtn, filters, defaults) {
  const count = activeFilterCount(filters, defaults);
  const existing = filterBtn.querySelector(".filter-badge");
  if (count) {
    if (existing) {
      existing.textContent = count;
    } else {
      filterBtn.insertAdjacentHTML("beforeend", `<span class="filter-badge">${count}</span>`);
    }
  } else if (existing) {
    existing.remove();
  }
}

// --- Public API -------------------------------------------------------------

export {
  SEARCH_ICON,
  activeFilterCount,
  resetFilters,
  renderFilterPanel,
  updateBadge,
};
