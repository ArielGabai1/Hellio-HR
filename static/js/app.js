import { init, isLoggedIn, login, clearToken, getRole, getUsername, getCandidates, getPositions } from "./store.js";
import { renderList as renderCandidateList, renderProfile, renderCompare } from "./candidates.js";
import { renderList as renderPositionList, renderDetail as renderPositionDetail } from "./positions.js";
import { renderIngestion } from "./ingestion.js";
import { renderChat, clearChatHistory } from "./chat.js";
import { renderStats } from "./stats.js";
import { renderNotifications, startBadgePolling, stopBadgePolling } from "./notifications.js";

const sidebar = document.getElementById("sidebar");
const app = document.getElementById("app");

const NAV_ICONS = {
  candidates: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>',
  positions: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/></svg>',
  ingestion: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>',
  chat: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>',
  stats: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>',
  notifications: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>',
};
const LOGOUT_ICON = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>';
const CLOSE_ICON = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>';

let chatRendered = false;

function buildSidebar() {
  const cCount = getCandidates().length;
  const pCount = getPositions().length;
  sidebar.innerHTML = `
    <div class="sidebar-brand">Hellio <span class="brand-badge">HR</span></div>
    <div class="sidebar-section-label">Navigation</div>
    <ul class="sidebar-nav">
      <li><a href="#/candidates" data-route="candidates">${NAV_ICONS.candidates} Candidates <span class="sidebar-nav-count">${cCount}</span></a></li>
      <li><a href="#/positions" data-route="positions">${NAV_ICONS.positions} Positions <span class="sidebar-nav-count">${pCount}</span></a></li>
    </ul>
    <div class="sidebar-section-label">Tools</div>
    <ul class="sidebar-nav">
      <li><a href="#/ingestion" data-route="ingestion">${NAV_ICONS.ingestion} Import</a></li>
    </ul>
    <div class="sidebar-section-label">Agent</div>
    <ul class="sidebar-nav">
      <li><a href="#/notifications" data-route="notifications">${NAV_ICONS.notifications} Activity <span class="sidebar-nav-count" id="notif-sidebar-count"></span></a></li>
    </ul>
    <div class="sidebar-footer">
      <div class="sidebar-user">
        <span class="sidebar-username">${getUsername()}</span>
        <span class="role-badge ${getRole()}">${getRole()}</span>
      </div>
      <div class="sidebar-footer-actions">
        <a href="#/stats" class="sidebar-footer-link" title="Usage Stats">${NAV_ICONS.stats} Stats</a>
        <button class="sidebar-footer-link" id="logout-btn" title="Logout">${LOGOUT_ICON} Logout</button>
      </div>
    </div>
  `;
  document.getElementById("logout-btn").addEventListener("click", () => {
    if (!confirm("Are you sure you want to log out?")) return;
    clearToken();
    clearChatHistory();
    destroyChatPanel();
    stopBadgePolling();
    location.hash = "#/login";
    renderLogin();
  });
}

// --- Chat panel ---

function createChatPanel() {
  if (document.getElementById("chat-panel")) return;

  const panel = document.createElement("div");
  panel.id = "chat-panel";
  panel.className = "chat-panel";
  panel.innerHTML = `
    <div class="chat-panel-resize" id="chat-panel-resize"></div>
    <div class="chat-panel-header">
      <div class="chat-panel-title">${NAV_ICONS.chat} Chat</div>
      <button class="chat-panel-close" id="chat-panel-close">${CLOSE_ICON}</button>
    </div>
    <div class="chat-panel-body" id="chat-panel-body"></div>
  `;
  document.body.appendChild(panel);

  // Drag-to-resize
  const handle = document.getElementById("chat-panel-resize");
  let dragging = false;
  handle.addEventListener("mousedown", (e) => {
    e.preventDefault();
    dragging = true;
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
  });
  document.addEventListener("mousemove", (e) => {
    if (!dragging) return;
    const w = Math.min(Math.max(window.innerWidth - e.clientX, 320), 900);
    panel.style.width = w + "px";
  });
  document.addEventListener("mouseup", () => {
    if (!dragging) return;
    dragging = false;
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
  });

  const fab = document.createElement("button");
  fab.id = "chat-fab";
  fab.className = "chat-fab";
  fab.title = "Open Chat";
  fab.innerHTML = NAV_ICONS.chat;
  document.body.appendChild(fab);

  fab.addEventListener("click", toggleChatPanel);
  document.getElementById("chat-panel-close").addEventListener("click", toggleChatPanel);
}

function toggleChatPanel() {
  const panel = document.getElementById("chat-panel");
  const fab = document.getElementById("chat-fab");
  if (!panel) return;

  const opening = !panel.classList.contains("open");
  panel.classList.toggle("open");
  fab.classList.toggle("hidden", opening);

  if (opening && !chatRendered) {
    renderChat(document.getElementById("chat-panel-body"));
    chatRendered = true;
  }
}

function destroyChatPanel() {
  document.getElementById("chat-panel")?.remove();
  document.getElementById("chat-fab")?.remove();
  chatRendered = false;
}

// --- Routing ---

function updateActiveLink() {
  const hash = location.hash || "#/candidates";
  sidebar.querySelectorAll(".sidebar-nav a").forEach(a => {
    a.classList.toggle("active", hash.startsWith(`#/${a.dataset.route}`));
  });
  // Stats link in footer
  const statsLink = sidebar.querySelector('.sidebar-footer-link[href="#/stats"]');
  if (statsLink) statsLink.classList.toggle("active", hash.startsWith("#/stats"));
}

function route() {
  if (!isLoggedIn()) {
    if (location.hash !== "#/login") location.hash = "#/login";
    return renderLogin();
  }

  const hash = location.hash || "#/candidates";
  if (hash === "#/login") {
    location.hash = "#/candidates";
    return;
  }

  // Backward compat: #/chat opens the panel instead of a page
  if (hash.startsWith("#/chat")) {
    const panel = document.getElementById("chat-panel");
    if (panel && !panel.classList.contains("open")) toggleChatPanel();
    location.hash = "#/candidates";
    return;
  }

  sidebar.style.display = "";
  app.style.marginLeft = "";
  app.style.maxWidth = "";
  app.style.padding = "";
  updateActiveLink();

  const bar = document.querySelector(".compare-bar");
  if (bar) bar.remove();

  app.classList.remove('page-enter');
  void app.offsetWidth;
  app.classList.add('page-enter');

  const compareMatch = hash.match(/^#\/candidates\/compare\/([\w-]+)\/([\w-]+)/);
  if (compareMatch) return renderCompare(app, compareMatch[1], compareMatch[2]);

  const profileMatch = hash.match(/^#\/candidates\/([\w-]+)$/);
  if (profileMatch) return renderProfile(app, profileMatch[1]);

  const posMatch = hash.match(/^#\/positions\/([\w-]+)$/);
  if (posMatch) return renderPositionDetail(app, posMatch[1]);

  if (hash.startsWith("#/stats")) return renderStats(app);
  if (hash.startsWith("#/notifications")) return renderNotifications(app);
  if (hash.startsWith("#/ingestion")) return renderIngestion(app);
  if (hash.startsWith("#/positions")) return renderPositionList(app);

  renderCandidateList(app);
}

function renderLogin() {
  sidebar.style.display = "none";
  app.style.marginLeft = "0";
  app.style.maxWidth = "none";
  app.style.padding = "0";
  destroyChatPanel();
  app.innerHTML = `
    <div class="login-container">
      <div class="login-card">
        <div class="login-brand">Hellio HR</div>
        <div class="login-subtitle">Sign in to your workspace</div>
        <form id="login-form">
          <div class="login-field">
            <label for="login-user">Username</label>
            <input id="login-user" type="text" autocomplete="username" placeholder="Enter username" required>
          </div>
          <div class="login-field">
            <label for="login-pass">Password</label>
            <input id="login-pass" type="password" autocomplete="current-password" placeholder="Enter password" required>
          </div>
          <div id="login-error" class="form-error" style="display:none"></div>
          <button type="submit" class="btn btn-primary login-submit">Sign in</button>
        </form>
      </div>
    </div>
  `;
  document.getElementById("login-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const user = document.getElementById("login-user").value;
    const pass = document.getElementById("login-pass").value;
    const errEl = document.getElementById("login-error");
    try {
      await login(user, pass);
      await init();
      buildSidebar();
      createChatPanel();
      startBadgePolling();
      location.hash = "#/candidates";
      route();
    } catch (err) {
      errEl.textContent = "Invalid username or password";
      errEl.style.display = "block";
    }
  });
}

async function boot() {
  if (!isLoggedIn()) {
    location.hash = "#/login";
    renderLogin();
    window.addEventListener("hashchange", route);
    return;
  }
  await init();
  buildSidebar();
  createChatPanel();
  startBadgePolling();
  if (!location.hash || location.hash === "#/login") location.hash = "#/candidates";
  route();
  window.addEventListener("hashchange", route);
}

boot();

// Keyboard: Escape closes chat panel or navigates back
document.addEventListener("keydown", (e) => {
  const tag = e.target.tagName;

  // Escape closes chat panel first, then navigates back
  if (e.key === "Escape") {
    const panel = document.getElementById("chat-panel");
    if (panel && panel.classList.contains("open")) {
      toggleChatPanel();
      return;
    }
    if (location.hash.match(/^#\/(candidates|positions)\/[\w-]+/)) {
      const section = location.hash.startsWith("#/positions") ? "positions" : "candidates";
      location.hash = `#/${section}`;
      return;
    }
  }

  if (tag === "INPUT" || tag === "SELECT" || tag === "TEXTAREA") return;

  const cards = [...document.querySelectorAll(".candidate-card, .position-card")];
  if (!cards.length) return;

  const idx = cards.indexOf(document.activeElement);

  if (e.key === "j" || e.key === "ArrowDown") {
    e.preventDefault();
    cards[idx < cards.length - 1 ? idx + 1 : 0].focus();
  } else if (e.key === "k" || e.key === "ArrowUp") {
    e.preventDefault();
    cards[idx > 0 ? idx - 1 : cards.length - 1].focus();
  } else if (e.key === "Enter" && idx >= 0) {
    cards[idx].click();
  }
});
