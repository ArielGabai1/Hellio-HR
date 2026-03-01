import { test, expect } from "@playwright/test";
import fs from "fs";
import path from "path";

const ids = JSON.parse(fs.readFileSync("tests/.auth/testdata.json", "utf-8"));
const BASE = "http://localhost:80";

// Helper: login as viewer in a fresh context
async function loginAsViewer(browser) {
  const ctx = await browser.newContext({ storageState: undefined });
  const page = await ctx.newPage();
  await page.goto(BASE);
  await page.waitForSelector("#login-form", { timeout: 15000 });
  await page.fill("#login-user", "viewer");
  await page.fill("#login-pass", "viewer");
  await page.click('button[type="submit"]');
  await expect(page.locator("#app h1")).toHaveText("Candidates", { timeout: 10000 });
  return { ctx, page };
}

// Helper: login as admin in a fresh context
async function loginAsAdmin(browser) {
  const ctx = await browser.newContext({ storageState: undefined });
  const page = await ctx.newPage();
  await page.goto(BASE);
  await page.waitForSelector("#login-form", { timeout: 15000 });
  await page.fill("#login-user", "admin");
  await page.fill("#login-pass", "admin");
  await page.click('button[type="submit"]');
  await expect(page.locator("#app h1")).toHaveText("Candidates", { timeout: 10000 });
  return { ctx, page };
}

// =============================================================================
// Viewer RBAC -- Upload Restrictions
// =============================================================================

test.describe("Ingestion -- Viewer RBAC", () => {
  test("viewer sees 'editor required' message on upload tab", async ({ browser }) => {
    const { ctx, page } = await loginAsViewer(browser);
    await page.goto(`${BASE}/#/ingestion`);
    await expect(page.locator(".ingest-tab[data-tab='upload']")).toBeVisible();
    await expect(page.locator(".empty-hint")).toContainText("Editor role required");
    // Dropzones should not be present
    await expect(page.locator(".upload-dropzone")).toHaveCount(0);
    await ctx.close();
  });

  test("viewer can still access inventory tab", async ({ browser }) => {
    const { ctx, page } = await loginAsViewer(browser);
    await page.goto(`${BASE}/#/ingestion`);
    await page.click('.ingest-tab[data-tab="inventory"]');
    // Should show inventory sections (CVs and Jobs)
    await expect(page.locator(".section-title")).toHaveCount(2);
    await ctx.close();
  });

  test("viewer can access stats page via sidebar link", async ({ browser }) => {
    const { ctx, page } = await loginAsViewer(browser);
    await page.goto(`${BASE}/#/stats`);
    await expect(page.locator("#app h1")).toHaveText("Stats", { timeout: 10000 });
    await expect(page.locator(".stat-card")).toHaveCount(4, { timeout: 10000 });
    await ctx.close();
  });

  test("viewer cannot see delete buttons in inventory", async ({ browser }) => {
    const { ctx, page } = await loginAsViewer(browser);
    await page.goto(`${BASE}/#/ingestion`);
    await page.click('.ingest-tab[data-tab="inventory"]');
    await expect(page.locator(".ingest-file-item")).not.toHaveCount(0, { timeout: 5000 });
    await expect(page.locator(".ingest-delete-btn")).toHaveCount(0);
    await ctx.close();
  });

  test("viewer gets 403 when trying to upload CV via API", async ({ browser }) => {
    const { ctx, page } = await loginAsViewer(browser);
    const token = await page.evaluate(() => localStorage.getItem("token"));
    const cvPath = path.resolve("CVsJobs/cvs/cv_001.pdf");
    const content = fs.readFileSync(cvPath);
    const form = new FormData();
    form.append("file", new Blob([content]), "cv_001.pdf");
    const resp = await fetch(`${BASE}/api/ingest/cv`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
      body: form,
    });
    expect(resp.status).toBe(403);
    await ctx.close();
  });

  test("viewer gets 403 when trying to upload job via API", async ({ browser }) => {
    const { ctx, page } = await loginAsViewer(browser);
    const token = await page.evaluate(() => localStorage.getItem("token"));
    const jobPath = path.resolve("CVsJobs/jobs/job_001_senior_devops.txt");
    const content = fs.readFileSync(jobPath);
    const form = new FormData();
    form.append("file", new Blob([content]), "job_001_senior_devops.txt");
    const resp = await fetch(`${BASE}/api/ingest/job`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
      body: form,
    });
    expect(resp.status).toBe(403);
    await ctx.close();
  });
});

// =============================================================================
// Editor -- Upload UI Presence
// =============================================================================

test.describe("Ingestion -- Editor Upload UI", () => {
  test("editor sees upload dropzones", async ({ page }) => {
    await page.goto("/#/ingestion");
    await expect(page.locator(".upload-dropzone")).toHaveCount(2);
    await expect(page.locator("#cv-zone")).toBeVisible();
    await expect(page.locator("#job-zone")).toBeVisible();
  });

  test("editor sees CV dropzone with correct accept types", async ({ page }) => {
    await page.goto("/#/ingestion");
    await expect(page.locator("#cv-input")).toHaveAttribute("accept", ".pdf,.docx");
  });

  test("editor sees job dropzone with correct accept types", async ({ page }) => {
    await page.goto("/#/ingestion");
    await expect(page.locator("#job-input")).toHaveAttribute("accept", ".txt");
  });

  test("editor can see delete buttons in inventory", async ({ page }) => {
    await page.goto("/#/ingestion");
    await page.click('.ingest-tab[data-tab="inventory"]');
    await expect(page.locator(".ingest-file-item")).not.toHaveCount(0, { timeout: 5000 });
    await expect(page.locator(".ingest-delete-btn").first()).toBeVisible();
  });
});

// =============================================================================
// Tab Navigation
// =============================================================================

test.describe("Ingestion -- Tab Navigation", () => {
  test("default tab is upload", async ({ page }) => {
    await page.goto("/#/ingestion");
    await expect(page.locator('.ingest-tab[data-tab="upload"]')).toHaveClass(/active/);
  });

  test("switching to inventory tab loads inventory", async ({ page }) => {
    await page.goto("/#/ingestion");
    await page.click('.ingest-tab[data-tab="inventory"]');
    await expect(page.locator('.ingest-tab[data-tab="inventory"]')).toHaveClass(/active/);
    // Should have inventory sections
    await expect(page.locator(".ingest-inventory-grid")).toBeVisible();
  });

  test("switching back to upload from inventory works", async ({ page }) => {
    await page.goto("/#/ingestion");
    await page.click('.ingest-tab[data-tab="inventory"]');
    await page.click('.ingest-tab[data-tab="upload"]');
    await expect(page.locator(".upload-dropzone")).toHaveCount(2);
  });
});

// =============================================================================
// Inventory Tab -- Data Display
// =============================================================================

test.describe("Ingestion -- Inventory", () => {
  test("inventory shows CVs and Jobs sections", async ({ page }) => {
    await page.goto("/#/ingestion");
    await page.click('.ingest-tab[data-tab="inventory"]');
    await expect(page.locator(".ingest-inventory-section")).toHaveCount(2);
  });

  test("inventory CVs link to candidate profiles", async ({ page }) => {
    await page.goto("/#/ingestion");
    await page.click('.ingest-tab[data-tab="inventory"]');
    await expect(page.locator(".ingest-file-item").first()).toBeVisible();
    const firstLink = page.locator(".ingest-file-link").first();
    const href = await firstLink.getAttribute("href");
    expect(href).toMatch(/#\/candidates\/[\w-]+/);
  });

  test("inventory shows filenames", async ({ page }) => {
    await page.goto("/#/ingestion");
    await page.click('.ingest-tab[data-tab="inventory"]');
    await expect(page.locator(".ingest-file-name").first()).toBeVisible();
    const filename = await page.locator(".ingest-file-name").first().textContent();
    expect(filename).toBeTruthy();
  });

  test("inventory shows ingestion dates", async ({ page }) => {
    await page.goto("/#/ingestion");
    await page.click('.ingest-tab[data-tab="inventory"]');
    await expect(page.locator(".ingest-file-date").first()).toBeVisible();
  });
});

// =============================================================================
// Stats Page -- Data Display
// =============================================================================

test.describe("Stats Page", () => {
  test("stats page shows four stat cards", async ({ page }) => {
    await page.goto("/#/stats");
    await expect(page.locator("#app h1")).toHaveText("Stats", { timeout: 10000 });
    await expect(page.locator(".stat-card")).toHaveCount(4, { timeout: 10000 });
  });

  test("stats page shows total count", async ({ page }) => {
    await page.goto("/#/stats");
    const firstStat = page.locator(".stat-card").first();
    await expect(firstStat.locator(".stat-label")).toHaveText("Total", { timeout: 10000 });
    const value = await firstStat.locator(".stat-value").textContent();
    expect(parseInt(value)).toBeGreaterThanOrEqual(0);
  });

  test("stats page shows success/partial/failed breakdown", async ({ page }) => {
    await page.goto("/#/stats");
    await expect(page.locator(".stat-card")).toHaveCount(4, { timeout: 10000 });
    const labels = await page.locator(".stat-label").allTextContents();
    expect(labels).toContain("Total");
    expect(labels).toContain("Success");
    expect(labels).toContain("Partial");
    expect(labels).toContain("Failed");
  });
});

// =============================================================================
// Navigation Resilience -- Progress Persistence
// =============================================================================

test.describe("Ingestion -- Navigation Resilience", () => {
  test("navigating away and back to ingestion preserves tab choice", async ({ page }) => {
    await page.goto("/#/ingestion");
    await page.click('.ingest-tab[data-tab="inventory"]');
    await expect(page.locator(".ingest-inventory-grid")).toBeVisible();
    // Navigate to candidates and back
    await page.goto("/#/candidates");
    await expect(page.locator("#app h1")).toHaveText("Candidates");
    await page.goto("/#/ingestion");
    // Tab should be preserved (inventory was active)
    await expect(page.locator('.ingest-tab[data-tab="inventory"]')).toHaveClass(/active/);
  });

  test("upload dropzones are not disabled initially", async ({ page }) => {
    await page.goto("/#/ingestion");
    await expect(page.locator(".upload-dropzone.disabled")).toHaveCount(0);
  });

  test("file inputs are not disabled initially", async ({ page }) => {
    await page.goto("/#/ingestion");
    const cvDisabled = await page.locator("#cv-input").isDisabled();
    expect(cvDisabled).toBe(false);
    const jobDisabled = await page.locator("#job-input").isDisabled();
    expect(jobDisabled).toBe(false);
  });
});

// =============================================================================
// Dedup -- API shape validation
// =============================================================================

test.describe("Ingestion -- Dedup API", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/#/ingestion");
    await expect(page.locator("#app h1")).toHaveText("Import");
  });

  test("candidate response includes updatedAt field", async ({ page }) => {
    const token = await page.evaluate(() => localStorage.getItem("token"));
    const resp = await fetch(`${BASE}/api/candidates`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const candidates = await resp.json();
    expect(candidates.length).toBeGreaterThan(0);
    for (const c of candidates) {
      expect(c).toHaveProperty("updatedAt");
    }
  });

  test("candidate documents endpoint returns array", async ({ page }) => {
    const token = await page.evaluate(() => localStorage.getItem("token"));
    // Use first candidate from the list
    const listResp = await fetch(`${BASE}/api/candidates`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const candidates = await listResp.json();
    const cid = candidates[0].id;
    const resp = await fetch(`${BASE}/api/candidates/${cid}/documents`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(resp.status).toBe(200);
    const docs = await resp.json();
    expect(Array.isArray(docs)).toBe(true);
  });

  test("documents endpoint returns 404 for unknown candidate", async ({ page }) => {
    const token = await page.evaluate(() => localStorage.getItem("token"));
    const resp = await fetch(`${BASE}/api/candidates/00000000-0000-0000-0000-000000000000/documents`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(resp.status).toBe(404);
  });

  test("documents endpoint requires auth", async () => {
    const resp = await fetch(`${BASE}/api/candidates/00000000-0000-0000-0000-000000000000/documents`);
    expect(resp.status).toBe(401);
  });
});

// =============================================================================
// Edge Cases -- API Boundary Validation
// =============================================================================

test.describe("Ingestion -- API Edge Cases", () => {
  // Navigate first so storageState is loaded and localStorage is accessible
  test.beforeEach(async ({ page }) => {
    await page.goto("/#/ingestion");
    await expect(page.locator("#app h1")).toHaveText("Import");
  });

  test("uploading unsupported file type returns 400", async ({ page }) => {
    const token = await page.evaluate(() => localStorage.getItem("token"));
    const form = new FormData();
    form.append("file", new Blob(["fake image data"]), "photo.jpg");
    const resp = await fetch(`${BASE}/api/ingest/cv`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
      body: form,
    });
    expect(resp.status).toBe(400);
  });

  test("uploading to job endpoint with CV extension returns 400", async ({ page }) => {
    const token = await page.evaluate(() => localStorage.getItem("token"));
    const form = new FormData();
    form.append("file", new Blob(["%PDF-1.4 fake"]), "resume.pdf");
    const resp = await fetch(`${BASE}/api/ingest/job`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
      body: form,
    });
    expect(resp.status).toBe(400);
  });

  test("uploading without file returns 422", async ({ page }) => {
    const token = await page.evaluate(() => localStorage.getItem("token"));
    const resp = await fetch(`${BASE}/api/ingest/cv`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(resp.status).toBe(422);
  });

  test("inventory endpoint returns correct shape", async ({ page }) => {
    const token = await page.evaluate(() => localStorage.getItem("token"));
    const resp = await fetch(`${BASE}/api/ingest/files`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(resp.status).toBe(200);
    const data = await resp.json();
    expect(data).toHaveProperty("cvs");
    expect(data).toHaveProperty("jobs");
    expect(Array.isArray(data.cvs)).toBe(true);
    expect(Array.isArray(data.jobs)).toBe(true);
    // Each CV entry should have required fields
    if (data.cvs.length > 0) {
      const cv = data.cvs[0];
      expect(cv).toHaveProperty("filename");
      expect(cv).toHaveProperty("entityId");
      expect(cv).toHaveProperty("candidateName");
      expect(cv).toHaveProperty("ingestedAt");
    }
    // Each job entry should have required fields
    if (data.jobs.length > 0) {
      const job = data.jobs[0];
      expect(job).toHaveProperty("filename");
      expect(job).toHaveProperty("entityId");
      expect(job).toHaveProperty("positionTitle");
      expect(job).toHaveProperty("ingestedAt");
    }
  });

  test("stats endpoint returns correct shape", async ({ page }) => {
    const token = await page.evaluate(() => localStorage.getItem("token"));
    const resp = await fetch(`${BASE}/api/ingest/stats`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(resp.status).toBe(200);
    const data = await resp.json();
    expect(data).toHaveProperty("total_extractions");
    expect(data).toHaveProperty("success");
    expect(data).toHaveProperty("partial");
    expect(data).toHaveProperty("failed");
    expect(data).toHaveProperty("by_model");
    expect(typeof data.total_extractions).toBe("number");
  });

  test("no auth on ingest endpoint returns 401", async () => {
    const form = new FormData();
    form.append("file", new Blob(["%PDF-1.4 fake"]), "test.pdf");
    const resp = await fetch(`${BASE}/api/ingest/cv`, {
      method: "POST",
      body: form,
    });
    expect(resp.status).toBe(401);
  });

  test("expired token on ingest endpoint returns 401", async () => {
    const form = new FormData();
    form.append("file", new Blob(["%PDF-1.4 fake"]), "test.pdf");
    const resp = await fetch(`${BASE}/api/ingest/cv`, {
      method: "POST",
      headers: { Authorization: "Bearer expired.token.here" },
      body: form,
    });
    expect(resp.status).toBe(401);
  });
});
