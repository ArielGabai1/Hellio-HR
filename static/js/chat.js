import { sendChat, skillTag, esc } from "./store.js";

let messages = [];
let loading = false;

function buildHistory() {
  return messages.map(m => ({ role: m.role, content: m.content }));
}

const SUGGESTIONS = [
  "How many open positions do we have?",
  "Show me senior candidates in Tel Aviv",
  "Which positions still need candidates?",
  "List candidates with Kubernetes experience",
];

function renderTrace(msg) {
  if (!msg.trace && !msg.sql) return "";
  const parts = [];
  if (msg.sql) {
    parts.push(`<pre class="chat-sql-block"><code class="sql-block">${esc(msg.sql)}</code></pre>`);
  }
  if (msg.trace) {
    if (msg.trace.columns && msg.trace.columns.length) {
      parts.push(`<div class="chat-trace-columns"><span class="chat-trace-label">Fields</span><div class="skill-tags">${msg.trace.columns.map(c => skillTag(c)).join("")}</div></div>`);
    }
    parts.push(`<div class="chat-trace-meta"><span class="chat-trace-label">Records found</span><span class="chat-trace-value">${msg.trace.rowCount}</span></div>`);
    if (msg.trace.rows && msg.trace.rows.length) {
      const cols = msg.trace.columns || Object.keys(msg.trace.rows[0]);
      const headerRow = cols.map(c => `<th>${esc(c)}</th>`).join("");
      const dataRows = msg.trace.rows.slice(0, 10).map(row =>
        `<tr>${cols.map(c => `<td>${esc(String(row[c] ?? ""))}</td>`).join("")}</tr>`
      ).join("");
      parts.push(`<div class="chat-trace-table-wrap"><table class="chat-trace-table"><thead><tr>${headerRow}</tr></thead><tbody>${dataRows}</tbody></table></div>`);
    }
  }
  if (msg.usage) {
    parts.push(`<div class="chat-trace-meta"><span class="chat-trace-label">Processing</span><span class="chat-trace-value">${msg.usage.input_tokens || 0} in / ${msg.usage.output_tokens || 0} out</span></div>`);
  }
  return `<details class="chat-trace"><summary class="chat-trace-summary">Details</summary><div class="chat-trace-body">${parts.join("")}</div></details>`;
}

function renderMessage(msg, idx) {
  if (msg.role === "user") {
    return `<div class="chat-msg chat-msg-user" data-idx="${idx}"><div class="chat-bubble chat-bubble-user">${esc(msg.content)}</div></div>`;
  }
  const warn = msg.hallucination_warning ? `<div class="chat-hallucination-warning">This answer may not be fully accurate -- please double-check the details below.</div>` : "";
  const errClass = msg.error ? " chat-bubble-error" : "";
  return `<div class="chat-msg chat-msg-assistant" data-idx="${idx}"><div class="chat-bubble chat-bubble-assistant${errClass}">${warn}${esc(msg.content).replace(/\n/g, "<br>")}${renderTrace(msg)}</div></div>`;
}

function renderMessages(container) {
  const list = container.querySelector(".chat-messages");
  if (!list) return;
  let html = "";
  if (!messages.length) {
    html = `<div class="chat-welcome">
      <div class="chat-welcome-icon"><svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg></div>
      <h2 class="chat-welcome-title">Ask me anything</h2>
      <p class="chat-welcome-desc">Ask questions about your candidates, positions, and hiring pipeline. I'll find the answers for you.</p>
      <div class="chat-suggestions">${SUGGESTIONS.map((s, i) => `<button class="chat-suggestion" data-idx="${i}">${esc(s)}</button>`).join("")}</div>
    </div>`;
  } else {
    html = messages.map((m, i) => renderMessage(m, i)).join("");
  }
  if (loading) {
    html += `<div class="chat-msg chat-msg-assistant"><div class="chat-bubble chat-bubble-assistant chat-loading-bubble"><span class="chat-loading-dots"><span></span><span></span><span></span></span></div></div>`;
  }
  list.innerHTML = html;
  list.scrollTop = list.scrollHeight;
}

function autoResize(textarea) {
  textarea.style.height = "auto";
  textarea.style.height = Math.min(textarea.scrollHeight, 160) + "px";
}

async function handleSend(container) {
  const input = container.querySelector(".chat-input");
  const text = input.value.trim();
  if (!text || loading) return;

  messages.push({ role: "user", content: text });
  input.value = "";
  autoResize(input);
  loading = true;
  updateSendBtn(container);
  renderMessages(container);

  try {
    const history = buildHistory().slice(0, -1);
    const data = await sendChat(text, history);
    messages.push({
      role: "assistant",
      content: data.answer,
      sql: data.sql,
      trace: data.trace,
      usage: data.usage,
      hallucination_warning: data.hallucination_warning,
    });
  } catch (err) {
    messages.push({ role: "assistant", content: `Error: ${err.message}`, error: true });
  } finally {
    loading = false;
    updateSendBtn(container);
    renderMessages(container);
  }
}

function updateSendBtn(container) {
  const btn = container.querySelector(".chat-send-btn");
  if (btn) btn.disabled = loading;
}

export function clearChatHistory() { messages = []; }

export function renderChat(container) {
  container.innerHTML = `
    <div class="chat-container">
      <div class="chat-messages"></div>
      <div class="chat-input-bar">
        <textarea class="chat-input" placeholder="Ask a question about your hiring data..." rows="1"></textarea>
        <button class="chat-send-btn btn btn-primary" ${loading ? "disabled" : ""}>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
        </button>
      </div>
    </div>
  `;

  renderMessages(container);

  const input = container.querySelector(".chat-input");
  const sendBtn = container.querySelector(".chat-send-btn");

  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend(container);
    }
  });
  input.addEventListener("input", () => autoResize(input));
  sendBtn.addEventListener("click", () => handleSend(container));

  container.querySelector(".chat-messages").addEventListener("click", (e) => {
    const btn = e.target.closest(".chat-suggestion");
    if (!btn) return;
    const idx = parseInt(btn.dataset.idx, 10);
    input.value = SUGGESTIONS[idx];
    handleSend(container);
  });
}
