import { test, expect } from "@playwright/test";
import fs from "fs";

const ids = JSON.parse(fs.readFileSync("tests/.auth/testdata.json", "utf-8"));

test.describe("Authentication", () => {
  test("redirects to login when not authenticated", async ({ browser }) => {
    const ctx = await browser.newContext({ storageState: undefined });
    const page = await ctx.newPage();
    await page.goto("http://localhost:80/");
    await page.waitForSelector("#login-form", { timeout: 15000 });
    await expect(page.locator("#login-form")).toBeVisible();
    await ctx.close();
  });

  test("shows error for invalid credentials", async ({ browser }) => {
    const ctx = await browser.newContext({ storageState: undefined });
    const page = await ctx.newPage();
    await page.goto("http://localhost:80/");
    await page.waitForSelector("#login-form", { timeout: 15000 });
    await page.fill("#login-user", "admin");
    await page.fill("#login-pass", "wrongpass");
    await page.click('button[type="submit"]');
    await expect(page.locator("#login-error")).toBeVisible();
    await expect(page.locator("#login-error")).toContainText("Invalid");
    await ctx.close();
  });

  test("can log in with valid credentials", async ({ browser }) => {
    const ctx = await browser.newContext({ storageState: undefined });
    const page = await ctx.newPage();
    await page.goto("http://localhost:80/");
    await page.waitForSelector("#login-form", { timeout: 15000 });
    await page.fill("#login-user", "admin");
    await page.fill("#login-pass", "admin");
    await page.click('button[type="submit"]');
    await expect(page.locator("#app h1")).toHaveText("Candidates", { timeout: 10000 });
    await ctx.close();
  });

  test("logout returns to login page", async ({ page }) => {
    await page.goto("/#/candidates");
    await expect(page.locator("#app h1")).toHaveText("Candidates");
    page.on("dialog", (d) => d.accept());
    await page.click("#logout-btn");
    await expect(page.locator("#login-form")).toBeVisible({ timeout: 10000 });
  });

  test("shows current user and role badge in sidebar", async ({ page }) => {
    await page.goto("/#/candidates");
    await expect(page.locator(".sidebar-username")).toHaveText("admin");
    await expect(page.locator(".role-badge")).toHaveText("hr-editor");
  });
});

test.describe("Viewer RBAC", () => {
  test("viewer can log in", async ({ browser }) => {
    const ctx = await browser.newContext({ storageState: undefined });
    const page = await ctx.newPage();
    await page.goto("http://localhost:80/");
    await page.waitForSelector("#login-form", { timeout: 15000 });
    await page.fill("#login-user", "viewer");
    await page.fill("#login-pass", "viewer");
    await page.click('button[type="submit"]');
    await expect(page.locator("#app h1")).toHaveText("Candidates", { timeout: 10000 });
    await expect(page.locator(".role-badge")).toHaveText("hr-viewer");
    await ctx.close();
  });

  test("viewer cannot see edit button on position detail", async ({ browser }) => {
    const ctx = await browser.newContext({ storageState: undefined });
    const page = await ctx.newPage();
    await page.goto("http://localhost:80/");
    await page.waitForSelector("#login-form", { timeout: 15000 });
    await page.fill("#login-user", "viewer");
    await page.fill("#login-pass", "viewer");
    await page.click('button[type="submit"]');
    await expect(page.locator("#app h1")).toHaveText("Candidates", { timeout: 10000 });
    await page.goto(`http://localhost:80/#/positions/${ids.positionA}`);
    await expect(page.locator(".position-detail-title")).toBeVisible({ timeout: 10000 });
    await expect(page.locator(".edit-position-btn")).toHaveCount(0);
    await ctx.close();
  });

  test("viewer cannot see assignment controls on candidate profile", async ({ browser }) => {
    const ctx = await browser.newContext({ storageState: undefined });
    const page = await ctx.newPage();
    await page.goto("http://localhost:80/");
    await page.waitForSelector("#login-form", { timeout: 15000 });
    await page.fill("#login-user", "viewer");
    await page.fill("#login-pass", "viewer");
    await page.click('button[type="submit"]');
    await expect(page.locator("#app h1")).toHaveText("Candidates", { timeout: 10000 });
    await page.goto(`http://localhost:80/#/candidates/${ids.candidateA}`);
    await expect(page.locator(".profile-name")).toBeVisible({ timeout: 10000 });
    await expect(page.locator("#add-position-select")).toHaveCount(0);
    await expect(page.locator(".remove-position")).toHaveCount(0);
    await ctx.close();
  });
});
