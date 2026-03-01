import { fetchIngestStats, esc } from "./store.js";

export async function renderStats(container) {
  container.innerHTML = `<h1>Stats</h1><div class="ingest-loading">Loading stats...</div>`;
  try {
    const s = await fetchIngestStats();
    const hasData = s.total_extractions > 0;
    const totalCost = Object.values(s.by_model).reduce((sum, m) => sum + (m.estimated_cost_usd || 0), 0);
    container.innerHTML = `
      <h1>Ingestion Stats</h1>
      <div class="ingest-stats-grid">
        <div class="stat-card">
          <div class="stat-value">${s.total_extractions}</div>
          <div class="stat-label">Total</div>
        </div>
        <div class="stat-card stat-success">
          <div class="stat-value">${s.success}</div>
          <div class="stat-label">Success</div>
        </div>
        <div class="stat-card stat-partial">
          <div class="stat-value">${s.partial}</div>
          <div class="stat-label">Partial</div>
        </div>
        <div class="stat-card stat-failed">
          <div class="stat-value">${s.failed}</div>
          <div class="stat-label">Failed</div>
        </div>
      </div>
      ${hasData ? `
        <div class="info-grid" style="max-width: 400px">
          <div><div class="info-label">Avg Duration</div><div class="info-value">${s.avg_duration_ms}ms</div></div>
        </div>
        <div class="section-title">By Model</div>
        ${Object.entries(s.by_model).map(([model, m]) => `
          <div class="ingest-model-card">
            <div class="ingest-model-name">${esc(model)}</div>
            <div class="ingest-model-stats">
              <span>${m.count} calls</span>
              <span>${m.total_input_tokens.toLocaleString()} in</span>
              <span>${m.total_output_tokens.toLocaleString()} out</span>
              <span class="ingest-cost">$${m.estimated_cost_usd.toFixed(4)}</span>
            </div>
          </div>
        `).join("")}
        <div class="ingest-model-card" data-testid="total-cost" style="margin-top: var(--space-3); font-weight: 600">
          <div class="ingest-model-name">Total Usage Cost</div>
          <div class="ingest-model-stats">
            <span class="ingest-cost">$${totalCost.toFixed(4)}</span>
          </div>
        </div>
      ` : '<div class="empty-hint" style="margin-top: var(--space-5)">No data yet. Stats will appear after documents are processed.</div>'}
    `;
  } catch (err) {
    container.innerHTML = `<h1>Stats</h1><div class="ingest-error">Failed to load stats: ${esc(err.message)}</div>`;
  }
}
