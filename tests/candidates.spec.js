import { test, expect } from "@playwright/test";
import fs from "fs";

const ids = JSON.parse(fs.readFileSync("tests/.auth/testdata.json", "utf-8"));

test.describe("Navigation", () => {
  test("defaults to candidates view", async ({ page }) => {
    await page.goto("/");
    await expect(page).toHaveURL(/#\/candidates/);
    await expect(page.locator("#app h1")).toHaveText("Candidates");
  });

  test("sidebar has nav links", async ({ page }) => {
    await page.goto("/");
    const nav = page.locator("#sidebar");
    await expect(nav.getByRole("link", { name: "Candidates" })).toBeVisible();
    await expect(nav.getByRole("link", { name: "Positions" })).toBeVisible();
  });

  test("clicking Positions nav goes to positions", async ({ page }) => {
    await page.goto("/");
    await page.locator("#sidebar").getByRole("link", { name: "Positions" }).click();
    await expect(page).toHaveURL(/#\/positions/);
  });
});

test.describe("Candidate List", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/#/candidates");
  });

  test("renders all active candidate cards", async ({ page }) => {
    const cards = page.locator(".candidate-card");
    await expect(cards).toHaveCount(1);
  });

  test("each card shows name, summary snippet, and skills", async ({ page }) => {
    const first = page.locator(".candidate-card").first();
    await expect(first.locator(".candidate-name")).toBeVisible();
    await expect(first.locator(".candidate-summary")).toBeVisible();
    const tagCount = await first.locator(".skill-tag").count();
    expect(tagCount).toBeGreaterThan(0);
  });

  test("each card shows status badge", async ({ page }) => {
    const first = page.locator(".candidate-card").first();
    await expect(first.locator(".status-badge")).toBeVisible();
  });

  test("clicking a card navigates to profile", async ({ page }) => {
    await page.locator(".candidate-card").first().click();
    await expect(page).toHaveURL(/#\/candidates\/[\w-]+/);
  });

  test("search filters candidates by name", async ({ page }) => {
    const search = page.locator(".search-input");
    await search.fill("Alex");
    const cards = page.locator(".candidate-card");
    await expect(cards).toHaveCount(1);
    await expect(cards.first().locator(".candidate-name")).toContainText("Alex");
  });

  test("search filters candidates by skill", async ({ page }) => {
    const search = page.locator(".search-input");
    await search.fill("Kubernetes");
    const cards = page.locator(".candidate-card");
    await expect(cards).toHaveCount(1);
  });

  test("search is case insensitive", async ({ page }) => {
    const search = page.locator(".search-input");
    await search.fill("alex");
    await expect(page.locator(".candidate-card")).toHaveCount(1);
  });

  test("showing all statuses displays inactive candidates", async ({ page }) => {
    await page.locator(".filter-toggle").click();
    await page.locator('[data-filter="status"] .filter-pill[data-value="inactive"]').click();
    const cards = page.locator(".candidate-card");
    await expect(cards).toHaveCount(2);
  });
});

test.describe("Candidate Filters", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/#/candidates");
    await page.locator(".filter-toggle").click();
  });

  test("filter panel opens and closes", async ({ page }) => {
    await expect(page.locator(".filter-panel")).toBeVisible();
    await page.locator(".filter-toggle").click();
    await expect(page.locator(".filter-panel")).not.toBeVisible();
  });

  test("filter by experience level", async ({ page }) => {
    await page.locator('[data-filter="level"] .filter-pill[data-value="senior"]').click();
    const cards = page.locator(".candidate-card");
    // active senior: Alex = 1
    await expect(cards).toHaveCount(1);
  });

  test("filter by location", async ({ page }) => {
    await page.locator('[data-filter="location"]').selectOption("Mockville");
    const cards = page.locator(".candidate-card");
    // active in Mockville: Alex = 1
    await expect(cards).toHaveCount(1);
  });

  test("filter by skill via tag input", async ({ page }) => {
    const input = page.locator('[data-filter="skill-input"]');
    await input.fill("AWS");
    await page.locator('.filter-suggestion[data-skill="AWS"]').click();
    const cards = page.locator(".candidate-card");
    // active with AWS: Alex = 1
    await expect(cards).toHaveCount(1);
  });

  test("filter badge shows active filter count", async ({ page }) => {
    await page.locator('[data-filter="level"] .filter-pill[data-value="junior"]').click();
    await expect(page.locator(".filter-badge")).toHaveText("1");
  });

  test("clear all resets filters", async ({ page }) => {
    await page.locator('[data-filter="level"] .filter-pill[data-value="junior"]').click();
    await expect(page.locator(".candidate-card")).toHaveCount(0);
    await page.locator('[data-action="clear-all"]').click();
    await expect(page.locator(".candidate-card")).toHaveCount(1);
  });

  test("filters compose correctly", async ({ page }) => {
    // Senior + Mockville
    await page.locator('[data-filter="level"] .filter-pill[data-value="senior"]').click();
    await page.locator('[data-filter="location"]').selectOption("Mockville");
    const cards = page.locator(".candidate-card");
    // active senior in Mockville: Alex = 1
    await expect(cards).toHaveCount(1);
    await expect(cards.first().locator(".candidate-name")).toContainText("Alex");
  });

  test("removing a skill tag updates results", async ({ page }) => {
    // Show all statuses to have 2 candidates
    await page.locator('[data-filter="status"] .filter-pill[data-value="inactive"]').click();
    await expect(page.locator(".candidate-card")).toHaveCount(2);
    const input = page.locator('[data-filter="skill-input"]');
    await input.fill("AWS");
    await page.locator('.filter-suggestion[data-skill="AWS"]').click();
    await expect(page.locator(".candidate-card")).toHaveCount(1);
    await page.locator('.filter-tag-remove[data-skill="AWS"]').click();
    await expect(page.locator(".candidate-card")).toHaveCount(2);
  });
});

test.describe("Candidate Profile", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`/#/candidates/${ids.candidateA}`);
  });

  test("shows candidate name as page title", async ({ page }) => {
    await expect(page.locator(".profile-name")).toContainText("Alex Mock");
  });

  test("shows contact information", async ({ page }) => {
    const contact = page.locator(".profile-contact");
    await expect(contact).toContainText("Mockville");
    await expect(contact).toContainText(/@/);
  });

  test("shows all skills as tags", async ({ page }) => {
    const tags = page.locator(".skill-tags .skill-tag");
    await expect(tags.first()).toBeVisible();
    expect(await tags.count()).toBeGreaterThan(3);
  });

  test("shows experience timeline", async ({ page }) => {
    await expect(page.locator(".timeline")).toBeVisible();
    await expect(page.locator(".timeline-item").first()).toBeVisible();
  });

  test("shows education section", async ({ page }) => {
    await expect(page.locator(".edu-degree").first()).toBeVisible();
  });

  test("has CV link", async ({ page }) => {
    const link = page.locator(".cv-link");
    await expect(link).toBeVisible();
    await expect(link).toHaveAttribute("href", /cv_001/);
  });

  test("has CV History toggle button", async ({ page }) => {
    await expect(page.locator(".cv-history-toggle")).toBeVisible();
  });

  test("CV History toggle shows document list", async ({ page }) => {
    await page.locator(".cv-history-toggle").click();
    await expect(page.locator("#cv-version-list")).toBeVisible();
  });

  test("shows assigned positions", async ({ page }) => {
    await expect(page.locator(".assigned-position").first()).toBeVisible();
  });

  test("has back link to candidates list", async ({ page }) => {
    await page.locator(".back-link").click();
    await expect(page).toHaveURL(/#\/candidates$/);
  });

  test("shows not found for invalid id", async ({ page }) => {
    await page.goto("/#/candidates/999");
    await expect(page.locator(".empty-state")).toBeVisible();
  });
});

test.describe("Candidate Comparison", () => {
  test("can select two candidates for comparison", async ({ page }) => {
    await page.goto("/#/candidates");
    await page.locator(".filter-toggle").click();
    await page.locator('[data-filter="status"] .filter-pill[data-value="inactive"]').click();
    const checkboxes = page.locator(".compare-checkbox");
    await checkboxes.nth(0).check();
    await checkboxes.nth(1).check();
    await expect(page.locator(".compare-bar")).toBeVisible();
  });

  test("compare bar shows selected names", async ({ page }) => {
    await page.goto("/#/candidates");
    await page.locator(".filter-toggle").click();
    await page.locator('[data-filter="status"] .filter-pill[data-value="inactive"]').click();
    const checkboxes = page.locator(".compare-checkbox");
    await checkboxes.nth(0).check();
    await checkboxes.nth(1).check();
    const bar = page.locator(".compare-bar");
    await expect(bar).toContainText("Compare");
  });

  test("compare button navigates to comparison view", async ({ page }) => {
    await page.goto("/#/candidates");
    await page.locator(".filter-toggle").click();
    await page.locator('[data-filter="status"] .filter-pill[data-value="inactive"]').click();
    const checkboxes = page.locator(".compare-checkbox");
    await checkboxes.nth(0).check();
    await checkboxes.nth(1).check();
    await page.locator(".compare-bar .btn-primary").click();
    await expect(page).toHaveURL(/#\/candidates\/compare\/[\w-]+\/[\w-]+/);
  });

  test("comparison view shows two columns", async ({ page }) => {
    await page.goto(`/#/candidates/compare/${ids.candidateA}/${ids.candidateB}`);
    const cols = page.locator(".compare-col");
    await expect(cols).toHaveCount(2);
  });

  test("comparison shows both candidate names", async ({ page }) => {
    await page.goto(`/#/candidates/compare/${ids.candidateA}/${ids.candidateB}`);
    await expect(page.locator(".compare-col").nth(0)).toContainText("Alex Mock");
    await expect(page.locator(".compare-col").nth(1)).toContainText("Jordan Sample");
  });

  test("shared skills are highlighted", async ({ page }) => {
    await page.goto(`/#/candidates/compare/${ids.candidateA}/${ids.candidateB}`);
    const shared = page.locator(".skill-tag.shared");
    await expect(shared.first()).toBeVisible();
  });

  test("comparison shows experience for both", async ({ page }) => {
    await page.goto(`/#/candidates/compare/${ids.candidateA}/${ids.candidateB}`);
    const timelines = page.locator(".timeline");
    await expect(timelines).toHaveCount(2);
  });

  test("has back link to candidates list", async ({ page }) => {
    await page.goto(`/#/candidates/compare/${ids.candidateA}/${ids.candidateB}`);
    await page.locator(".back-link").click();
    await expect(page).toHaveURL(/#\/candidates$/);
  });

  test("shows error for invalid candidate ids", async ({ page }) => {
    await page.goto("/#/candidates/compare/999/998");
    await expect(page.locator(".empty-state")).toBeVisible();
  });
});

test.describe("Candidate Suggestions", () => {
  test("candidate profile shows suggestions section", async ({ page }) => {
    await page.goto(`/#/candidates/${ids.candidateA}`);
    await page.waitForSelector("[data-testid='candidate-suggestions']");
    const section = page.locator("[data-testid='candidate-suggestions']");
    await expect(section).toBeVisible();
    await expect(section.locator(".suggestions-loading")).toHaveCount(0, { timeout: 10000 });
  });

  test("suggestion cards or empty hint visible", async ({ page }) => {
    await page.goto(`/#/candidates/${ids.candidateA}`);
    await page.waitForSelector("[data-testid='candidate-suggestions']");
    const section = page.locator("[data-testid='candidate-suggestions']");
    await expect(section.locator(".suggestions-loading")).toHaveCount(0, { timeout: 10000 });
    const cards = section.locator(".suggestion-card");
    const hint = section.locator(".empty-hint");
    const hasCards = await cards.count() > 0;
    const hasHint = await hint.count() > 0;
    expect(hasCards || hasHint).toBeTruthy();
  });

  test("suggestion card links to position", async ({ page }) => {
    await page.goto(`/#/candidates/${ids.candidateA}`);
    await page.waitForSelector("[data-testid='candidate-suggestions']");
    const section = page.locator("[data-testid='candidate-suggestions']");
    await expect(section.locator(".suggestions-loading")).toHaveCount(0, { timeout: 10000 });
    const card = section.locator(".suggestion-card").first();
    if (await card.count() > 0) {
      const link = card.locator(".suggestion-name");
      const href = await link.getAttribute("href");
      expect(href).toMatch(/#\/positions\//);
    }
  });

  test("suggestion card shows score percentage", async ({ page }) => {
    await page.goto(`/#/candidates/${ids.candidateA}`);
    await page.waitForSelector("[data-testid='candidate-suggestions']");
    const section = page.locator("[data-testid='candidate-suggestions']");
    await expect(section.locator(".suggestions-loading")).toHaveCount(0, { timeout: 10000 });
    const card = section.locator(".suggestion-card").first();
    if (await card.count() > 0) {
      const score = card.locator(".suggestion-score");
      const text = await score.textContent();
      expect(text).toMatch(/\d+%/);
    }
  });

  test("suggestion card shows explanation when available", async ({ page }) => {
    await page.goto(`/#/candidates/${ids.candidateA}`);
    await page.waitForSelector("[data-testid='candidate-suggestions']");
    const section = page.locator("[data-testid='candidate-suggestions']");
    await expect(section.locator(".suggestions-loading")).toHaveCount(0, { timeout: 10000 });
    const card = section.locator(".suggestion-card").first();
    if (await card.count() > 0) {
      const explanation = card.locator(".suggestion-explanation");
      if (await explanation.count() > 0) {
        const text = await explanation.textContent();
        expect(text.length).toBeGreaterThan(0);
      }
    }
  });
});
