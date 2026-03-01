import { test as setup, expect } from "@playwright/test";
import fs from "fs";
import path from "path";

const authFile = "tests/.auth/state.json";
const testdataFile = "tests/.auth/testdata.json";
const BASE = "http://localhost:80";

setup("authenticate and seed data", async ({ page }) => {
  // --- Login ---
  await page.goto("/");
  await expect(page.locator("#login-form")).toBeVisible();
  await page.fill("#login-user", "admin");
  await page.fill("#login-pass", "admin");
  await page.click('button[type="submit"]');
  await expect(page.locator("#app h1")).toHaveText("Candidates", { timeout: 10000 });

  const token = await page.evaluate(() => localStorage.getItem("token"));
  expect(token).toBeTruthy();
  await page.context().storageState({ path: authFile });

  // --- Ingest test data via API ---
  const authHeader = { Authorization: `Bearer ${token}` };
  const cvsDir = path.resolve("CVsJobs/cvs");
  const jobsDir = path.resolve("CVsJobs/jobs");

  // Ingest 2 CVs
  const cv1 = await ingestFile(`${BASE}/api/ingest/cv`, path.join(cvsDir, "cv_001.pdf"), authHeader);
  const cv2 = await ingestFile(`${BASE}/api/ingest/cv`, path.join(cvsDir, "cv_002.pdf"), authHeader);

  // Ingest 2 jobs
  const job1 = await ingestFile(`${BASE}/api/ingest/job`, path.join(jobsDir, "job_001_senior_devops.txt"), authHeader);
  const job2 = await ingestFile(`${BASE}/api/ingest/job`, path.join(jobsDir, "job_002_junior_devops.txt"), authHeader);

  // Create assignments: Alex -> Senior DevOps, Jordan -> Junior Cloud
  await fetch(`${BASE}/api/candidates/${cv1.id}/positions/${job1.id}`, {
    method: "POST",
    headers: authHeader,
  });
  await fetch(`${BASE}/api/candidates/${cv2.id}/positions/${job2.id}`, {
    method: "POST",
    headers: authHeader,
  });

  // Save IDs for test files
  const testdata = {
    candidateA: cv1.id,
    candidateB: cv2.id,
    positionA: job1.id,
    positionB: job2.id,
  };
  fs.mkdirSync(path.dirname(testdataFile), { recursive: true });
  fs.writeFileSync(testdataFile, JSON.stringify(testdata, null, 2));
});

async function ingestFile(url, filePath, headers) {
  const content = fs.readFileSync(filePath);
  const filename = path.basename(filePath);
  const blob = new Blob([content]);
  const form = new FormData();
  form.append("file", blob, filename);

  const resp = await fetch(url, {
    method: "POST",
    headers,
    body: form,
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Ingest failed for ${filename}: ${resp.status} ${text}`);
  }
  return resp.json();
}
