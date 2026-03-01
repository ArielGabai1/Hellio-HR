import { fetchNotifications, acknowledgeNotification, esc } from "./store.js";

const TYPE_META = {
  candidate_ingested:   { label: "New Candidate",      icon: "user-plus",    color: "active" },
  candidate_updated:    { label: "Candidate Updated",   icon: "user-check",   color: "open" },
  candidate_cv_missing: { label: "CV Missing",          icon: "file-question", color: "closed" },
  position_ingested:    { label: "New Position",        icon: "briefcase",    color: "active" },
  position_updated:     { label: "Position Updated",    icon: "briefcase",    color: "open" },
  position_info_missing:{ label: "Info Missing",        icon: "alert",        color: "closed" },
  attachments_skipped:  { label: "Attachments Skipped", icon: "paperclip",    color: "closed" },
};

const ICONS = {
  "user-plus":     '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><line x1="19" y1="8" x2="19" y2="14"/><line x1="22" y1="11" x2="16" y2="11"/></svg>',
  "user-check":    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><polyline points="16 11 18 13 22 9"/></svg>',
  "file-question": '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><circle cx="12" cy="15" r="0.5" fill="currentColor"/><path d="M10 12a2 2 0 1 1 2.5 1.9V15"/></svg>',
  "briefcase":     '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/></svg>',
  "alert":         '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>',
  "paperclip":     '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.49"/></svg>',
  "bell":          '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>',
  "check":         '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>',
  "inbox":         '<svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 16 12 14 15 10 15 8 12 2 12"/><path d="M5.45 5.11L2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/></svg>',
};

function relativeTime(dateStr) {
  const d = new Date(dateStr);
  const now = new Date();
  const diffMs = now - d;
  const mins = Math.floor(diffMs / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  if (days < 7) return `${days}d ago`;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function buildActionUrl(n) {
  if (n.action_url) return n.action_url;
  // Infer from type: candidate types -> candidates list, position types -> positions list
  if (n.type?.startsWith("candidate")) return "#/candidates";
  if (n.type?.startsWith("position")) return "#/positions";
  return null;
}

function renderCard(n) {
  const meta = TYPE_META[n.type] || { label: n.type, icon: "alert", color: "closed" };
  const icon = ICONS[meta.icon] || ICONS["alert"];
  const isPending = n.status === "pending";
  const actionUrl = buildActionUrl(n);
  const reviewLink = actionUrl
    ? `<a href="${esc(actionUrl)}" class="btn btn-secondary notif-review-btn">${ICONS["check"]} Review</a>`
    : "";
  const dismissBtn = isPending
    ? `<button class="btn notif-dismiss-btn" data-id="${n.id}">Acknowledge</button>`
    : "";

  return `
    <div class="notif-card ${isPending ? "" : "notif-acknowledged"}" data-notif-id="${n.id}">
      <div class="notif-icon notif-icon-${meta.color}">${icon}</div>
      <div class="notif-body">
        <div class="notif-header">
          <span class="notif-type-badge status-badge ${meta.color}">${esc(meta.label)}</span>
          <span class="notif-time">${relativeTime(n.created_at)}</span>
        </div>
        <div class="notif-summary">${esc(n.summary)}</div>
        <div class="notif-actions">
          ${reviewLink}${dismissBtn}
        </div>
      </div>
    </div>`;
}

let currentFilter = "pending";
let currentSort = "newest";

export async function renderNotifications(container) {
  container.innerHTML = `
    <h1>Agent Activity</h1>
    <div class="notif-toolbar">
      <div class="notif-filters">
        <button class="filter-pill ${currentFilter === "pending" ? "active" : ""}" data-filter="pending">Pending</button>
        <button class="filter-pill ${currentFilter === "acknowledged" ? "active" : ""}" data-filter="acknowledged">Acknowledged</button>
        <button class="filter-pill ${currentFilter === "all" ? "active" : ""}" data-filter="all">All</button>
      </div>
      <select class="filter-dropdown notif-sort" id="notif-sort">
        <option value="newest" ${currentSort === "newest" ? "selected" : ""}>Newest first</option>
        <option value="oldest" ${currentSort === "oldest" ? "selected" : ""}>Oldest first</option>
      </select>
    </div>
    <div id="notif-list" class="notif-list"><div class="ingest-loading">Loading notifications...</div></div>
  `;

  container.querySelector(".notif-filters").addEventListener("click", async (e) => {
    const pill = e.target.closest("[data-filter]");
    if (!pill) return;
    currentFilter = pill.dataset.filter;
    container.querySelectorAll(".notif-filters .filter-pill").forEach(p =>
      p.classList.toggle("active", p.dataset.filter === currentFilter)
    );
    await loadList(container);
  });

  container.querySelector("#notif-sort").addEventListener("change", async (e) => {
    currentSort = e.target.value;
    await loadList(container);
  });

  await loadList(container);
}

async function loadList(container) {
  const listEl = container.querySelector("#notif-list");
  try {
    const status = currentFilter === "all" ? null : currentFilter;
    const items = await fetchNotifications(status);

    if (currentSort === "newest") {
      items.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
    } else if (currentSort === "oldest") {
      items.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
    }

    if (!items.length) {
      listEl.innerHTML = `
        <div class="empty-state">
          <span class="empty-state-icon">${ICONS["inbox"]}</span>
          <h2>No ${currentFilter === "all" ? "" : currentFilter + " "}notifications</h2>
          <p class="empty-hint">The agent will create notifications here when it processes emails.</p>
        </div>`;
      return;
    }

    listEl.innerHTML = items.map(renderCard).join("");

    listEl.addEventListener("click", async (e) => {
      const btn = e.target.closest(".notif-dismiss-btn");
      if (!btn) return;
      const id = btn.dataset.id;
      btn.disabled = true;
      btn.textContent = "...";
      try {
        await acknowledgeNotification(id);
        const card = listEl.querySelector(`[data-notif-id="${id}"]`);
        if (card) {
          card.style.transition = "opacity 250ms var(--ease), max-height 250ms var(--ease), padding 250ms var(--ease), margin 250ms var(--ease)";
          card.style.opacity = "0";
          card.style.maxHeight = card.offsetHeight + "px";
          card.style.overflow = "hidden";
          requestAnimationFrame(() => {
            card.style.maxHeight = "0";
            card.style.padding = "0";
            card.style.marginBottom = "0";
            card.style.borderWidth = "0";
          });
          setTimeout(() => card.remove(), 260);
        }
        // Update sidebar badge
        updateSidebarBadge();
      } catch (err) {
        btn.disabled = false;
        btn.textContent = "Acknowledge";
      }
    });
  } catch (err) {
    listEl.innerHTML = `<div class="ingest-error">Failed to load notifications.</div>`;
  }
}

// Sidebar badge -- exported for app.js to call on init and periodically
let _badgeInterval = null;

export async function updateSidebarBadge() {
  try {
    const pending = await fetchNotifications("pending");
    const count = pending.length;
    const badge = document.getElementById("notif-sidebar-count");
    if (badge) {
      badge.textContent = count || "";
      badge.classList.toggle("notif-has-pending", count > 0);
    }
  } catch {
    // Silent -- don't break the UI if agent endpoints aren't available
  }
}

export function startBadgePolling() {
  stopBadgePolling();
  updateSidebarBadge();
  _badgeInterval = setInterval(updateSidebarBadge, 30000);
}

export function stopBadgePolling() {
  if (_badgeInterval) {
    clearInterval(_badgeInterval);
    _badgeInterval = null;
  }
}

export { ICONS as NOTIF_ICONS };
