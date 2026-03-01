import { test, expect } from "@playwright/test";
import fs from "fs";

const ids = JSON.parse(fs.readFileSync("tests/.auth/testdata.json", "utf-8"));

test.describe("Position List", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/#/positions");
  });

  test("renders page title", async ({ page }) => {
    await expect(page.locator("#app h1")).toHaveText("Positions");
  });

  test("renders open position cards by default", async ({ page }) => {
    const cards = page.locator(".position-card");
    await expect(cards).toHaveCount(2);
  });

  test("each card shows title, company, and tech stack", async ({ page }) => {
    const first = page.locator(".position-card").first();
    await expect(first.locator(".position-title")).toBeVisible();
    await expect(first.locator(".card-meta")).toBeVisible();
    const tagCount = await first.locator(".skill-tag").count();
    expect(tagCount).toBeGreaterThan(0);
  });

  test("each card shows status badge", async ({ page }) => {
    const first = page.locator(".position-card").first();
    await expect(first.locator(".status-badge")).toBeVisible();
  });

  test("clicking a card navigates to detail", async ({ page }) => {
    await page.locator(".position-card").first().click();
    await expect(page).toHaveURL(/#\/positions\/[\w-]+/);
  });

  test("search filters positions by title", async ({ page }) => {
    const search = page.locator(".search-input");
    await search.fill("Cloud");
    const cards = page.locator(".position-card");
    // Only "Junior Cloud Engineer" matches
    await expect(cards).toHaveCount(1);
  });
});

test.describe("Position Filters", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/#/positions");
    await page.locator(".filter-toggle").click();
  });

  test("filter panel opens and closes", async ({ page }) => {
    await expect(page.locator(".filter-panel")).toBeVisible();
    await page.locator(".filter-toggle").click();
    await expect(page.locator(".filter-panel")).not.toBeVisible();
  });

  test("filter by experience level", async ({ page }) => {
    await page.locator('[data-filter="level"] .filter-pill[data-value="senior"]').click();
    const cards = page.locator(".position-card");
    // open senior: position 001 = 1
    await expect(cards).toHaveCount(1);
  });

  test("filter by location", async ({ page }) => {
    await page.locator('[data-filter="location"]').selectOption("Mockville");
    const cards = page.locator(".position-card");
    // Mockville: position 001 = 1
    await expect(cards).toHaveCount(1);
  });

  test("filter by work arrangement", async ({ page }) => {
    await page.locator('[data-filter="arrangement"]').selectOption("Hybrid");
    const cards = page.locator(".position-card");
    // Hybrid: position 001 = 1
    await expect(cards).toHaveCount(1);
  });

  test("filter by tech via tag input", async ({ page }) => {
    const input = page.locator('[data-filter="tech-input"]');
    await input.fill("AWS");
    await page.locator('.filter-suggestion[data-skill="AWS"]').click();
    const cards = page.locator(".position-card");
    // Only position 001 has AWS
    await expect(cards).toHaveCount(1);
  });

  test("filter badge shows active filter count", async ({ page }) => {
    await page.locator('[data-filter="level"] .filter-pill[data-value="junior"]').click();
    await expect(page.locator(".filter-badge")).toHaveText("1");
  });

  test("clear all resets filters", async ({ page }) => {
    await page.locator('[data-filter="level"] .filter-pill[data-value="junior"]').click();
    await expect(page.locator(".position-card")).toHaveCount(1);
    await page.locator('[data-action="clear-all"]').click();
    await expect(page.locator(".position-card")).toHaveCount(2);
  });

  test("filters compose correctly", async ({ page }) => {
    await page.locator('[data-filter="level"] .filter-pill[data-value="senior"]').click();
    await page.locator('[data-filter="location"]').selectOption("Mockville");
    const cards = page.locator(".position-card");
    // senior in Mockville: position 001 = 1
    await expect(cards).toHaveCount(1);
  });
});

test.describe("Position Detail", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`/#/positions/${ids.positionA}`);
  });

  test("shows position title", async ({ page }) => {
    await expect(page.locator(".position-detail-title")).toContainText("Senior DevOps Engineer");
  });

  test("shows company and hiring manager", async ({ page }) => {
    await expect(page.locator(".position-meta")).toContainText("Acme Corp");
    await expect(page.locator("#app")).toContainText("Pat Manager");
  });

  test("shows requirements list", async ({ page }) => {
    const section = page.locator("#app");
    await expect(section).toContainText("5+ years DevOps");
  });

  test("shows tech stack tags", async ({ page }) => {
    const tags = page.locator(".skill-tags .skill-tag");
    await expect(tags.first()).toBeVisible();
  });

  test("shows location and work arrangement", async ({ page }) => {
    await expect(page.locator("#app")).toContainText("Mockville");
    await expect(page.locator("#app")).toContainText("Hybrid");
  });

  test("has back link to positions list", async ({ page }) => {
    await page.locator(".back-link").click();
    await expect(page).toHaveURL(/#\/positions$/);
  });

  test("shows not found for invalid id", async ({ page }) => {
    await page.goto("/#/positions/999");
    await expect(page.locator(".empty-state")).toBeVisible();
  });
});

test.describe("Position Editing", () => {
  test("edit button is visible on position detail", async ({ page }) => {
    await page.goto(`/#/positions/${ids.positionB}`);
    await expect(page.locator(".edit-position-btn")).toBeVisible();
  });

  test("clicking edit shows edit form", async ({ page }) => {
    await page.goto(`/#/positions/${ids.positionB}`);
    await page.click(".edit-position-btn");
    await expect(page.locator("#edit-position-form")).toBeVisible();
  });

  test("edit form is pre-filled with current values", async ({ page }) => {
    await page.goto(`/#/positions/${ids.positionB}`);
    await page.click(".edit-position-btn");
    const title = await page.inputValue("#edit-title");
    expect(title).toBeTruthy();
  });

  test("can save edits and see updated title", async ({ page }) => {
    await page.goto(`/#/positions/${ids.positionB}`);
    await page.click(".edit-position-btn");
    await page.fill("#edit-title", "Edited Cloud Role");
    await page.click('button[type="submit"]');
    await expect(page.locator(".position-detail-title")).toContainText("Edited Cloud Role");
  });

  test("cancel returns to detail without saving", async ({ page }) => {
    await page.goto(`/#/positions/${ids.positionB}`);
    const originalTitle = await page.textContent(".position-detail-title");
    await page.click(".edit-position-btn");
    await page.fill("#edit-title", "Should Not Save");
    await page.click(".back-link");
    await expect(page.locator(".position-detail-title")).toContainText(originalTitle.trim());
  });
});

test.describe("Position Suggestions", () => {
  test("position detail shows suggestions section", async ({ page }) => {
    await page.goto(`/#/positions/${ids.positionA}`);
    await page.waitForSelector("[data-testid='position-suggestions']");
    const section = page.locator("[data-testid='position-suggestions']");
    await expect(section).toBeVisible();
    await expect(section.locator(".suggestions-loading")).toHaveCount(0, { timeout: 10000 });
  });

  test("suggestion cards or empty hint visible", async ({ page }) => {
    await page.goto(`/#/positions/${ids.positionA}`);
    await page.waitForSelector("[data-testid='position-suggestions']");
    const section = page.locator("[data-testid='position-suggestions']");
    await expect(section.locator(".suggestions-loading")).toHaveCount(0, { timeout: 10000 });
    const cards = section.locator(".suggestion-card");
    const hint = section.locator(".empty-hint");
    const hasCards = await cards.count() > 0;
    const hasHint = await hint.count() > 0;
    expect(hasCards || hasHint).toBeTruthy();
  });

  test("suggestion card links to candidate", async ({ page }) => {
    await page.goto(`/#/positions/${ids.positionA}`);
    await page.waitForSelector("[data-testid='position-suggestions']");
    const section = page.locator("[data-testid='position-suggestions']");
    await expect(section.locator(".suggestions-loading")).toHaveCount(0, { timeout: 10000 });
    const card = section.locator(".suggestion-card").first();
    if (await card.count() > 0) {
      const link = card.locator(".suggestion-name");
      const href = await link.getAttribute("href");
      expect(href).toMatch(/#\/candidates\//);
    }
  });
});

test.describe("Position Suggestion Cards", () => {
  test("suggestion card shows score percentage", async ({ page }) => {
    await page.goto(`/#/positions/${ids.positionA}`);
    await page.waitForSelector("[data-testid='position-suggestions']");
    const section = page.locator("[data-testid='position-suggestions']");
    await expect(section.locator(".suggestions-loading")).toHaveCount(0, { timeout: 10000 });
    const card = section.locator(".suggestion-card").first();
    if (await card.count() > 0) {
      const score = card.locator(".suggestion-score");
      const text = await score.textContent();
      expect(text).toMatch(/\d+%/);
    }
  });

  test("suggestion card shows skill tags", async ({ page }) => {
    await page.goto(`/#/positions/${ids.positionA}`);
    await page.waitForSelector("[data-testid='position-suggestions']");
    const section = page.locator("[data-testid='position-suggestions']");
    await expect(section.locator(".suggestions-loading")).toHaveCount(0, { timeout: 10000 });
    const card = section.locator(".suggestion-card").first();
    if (await card.count() > 0) {
      const tags = card.locator(".skill-tags .skill-tag");
      const count = await tags.count();
      expect(count).toBeLessThanOrEqual(5);
    }
  });
});

test.describe.serial("Position Assignment", () => {
  test("position detail shows assigned candidates", async ({ page }) => {
    await page.goto(`/#/positions/${ids.positionA}`);
    await expect(page.locator("#app")).toContainText("Alex Mock");
  });

  test("can assign a candidate to a position", async ({ page }) => {
    await page.goto(`/#/candidates/${ids.candidateA}`);
    await expect(page.locator(".assigned-position")).toHaveCount(1);
    const select = page.locator("#add-position-select");
    await select.selectOption({ index: 1 });
    const respPromise = page.waitForResponse(resp => resp.url().includes("/positions/") && resp.request().method() === "POST");
    await page.locator("#add-position-btn").click();
    await respPromise;
    await expect(page.locator(".assigned-position")).toHaveCount(2, { timeout: 10000 });
  });

  test("can remove a position from a candidate", async ({ page }) => {
    await page.goto(`/#/candidates/${ids.candidateA}`);
    await expect(page.locator(".assigned-position").first()).toBeVisible();
    const before = await page.locator(".assigned-position").count();
    const respPromise = page.waitForResponse(resp => resp.url().includes("/positions/") && resp.request().method() === "DELETE");
    await page.locator(".remove-position").first().click();
    await respPromise;
    await expect(page.locator(".assigned-position")).toHaveCount(before - 1, { timeout: 10000 });
  });
});
