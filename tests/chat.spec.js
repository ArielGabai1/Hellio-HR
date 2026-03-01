import { test, expect } from "@playwright/test";

test.describe("Chat Panel", () => {
  test("FAB button is visible on page load", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("#chat-fab")).toBeVisible();
  });

  test("clicking FAB opens chat panel", async ({ page }) => {
    await page.goto("/");
    await page.click("#chat-fab");
    await expect(page.locator("#chat-panel")).toHaveClass(/open/);
    await expect(page.locator(".chat-container")).toBeVisible();
  });

  test("close button closes panel", async ({ page }) => {
    await page.goto("/");
    await page.click("#chat-fab");
    await expect(page.locator("#chat-panel")).toHaveClass(/open/);
    await page.click("#chat-panel-close");
    await expect(page.locator("#chat-panel")).not.toHaveClass(/open/);
  });

  test("FAB hides when panel is open", async ({ page }) => {
    await page.goto("/");
    await page.click("#chat-fab");
    await expect(page.locator("#chat-fab")).toHaveClass(/hidden/);
  });

  test("FAB reappears when panel is closed", async ({ page }) => {
    await page.goto("/");
    await page.click("#chat-fab");
    await page.click("#chat-panel-close");
    await expect(page.locator("#chat-fab")).not.toHaveClass(/hidden/);
  });

  test("panel header shows Chat title", async ({ page }) => {
    await page.goto("/");
    await page.click("#chat-fab");
    await expect(page.locator(".chat-panel-title")).toContainText("Chat");
  });

  test("#/chat redirects and opens panel", async ({ page }) => {
    await page.goto("/#/chat");
    await expect(page.locator("#chat-panel")).toHaveClass(/open/, { timeout: 5000 });
    // Should redirect to candidates
    await expect(page).toHaveURL(/#\/candidates/);
  });
});

test.describe("Chat Welcome State", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await page.click("#chat-fab");
    await page.waitForSelector(".chat-container");
  });

  test("shows welcome message", async ({ page }) => {
    await expect(page.locator(".chat-welcome")).toBeVisible();
    await expect(page.locator(".chat-welcome-title")).toBeVisible();
  });

  test("shows 4 suggestion buttons", async ({ page }) => {
    const suggestions = page.locator(".chat-suggestion");
    await expect(suggestions).toHaveCount(4);
  });

  test("suggestions have expected text", async ({ page }) => {
    await expect(page.locator(".chat-suggestion").first()).toContainText("position");
  });
});

test.describe("Chat Messaging", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await page.click("#chat-fab");
    await page.waitForSelector(".chat-container");
  });

  test("typing and sending adds user message", async ({ page }) => {
    await page.fill(".chat-input", "list candidates");
    await page.click(".chat-send-btn");
    await expect(page.locator(".chat-msg-user")).toBeVisible();
    await expect(page.locator(".chat-bubble-user")).toContainText("list candidates");
  });

  test("assistant response appears after send", async ({ page }) => {
    await page.fill(".chat-input", "list open positions");
    await page.click(".chat-send-btn");
    await expect(page.locator(".chat-msg-assistant .chat-bubble-assistant")).toBeVisible({ timeout: 15000 });
  });

  test("welcome disappears after first message", async ({ page }) => {
    await expect(page.locator(".chat-welcome")).toBeVisible();
    await page.fill(".chat-input", "hello");
    await page.click(".chat-send-btn");
    await expect(page.locator(".chat-welcome")).not.toBeVisible();
  });

  test("multiple messages maintain order", async ({ page }) => {
    await page.fill(".chat-input", "first question");
    await page.click(".chat-send-btn");
    await page.locator(".chat-msg-assistant").first().waitFor({ timeout: 15000 });
    await page.fill(".chat-input", "second question");
    await page.click(".chat-send-btn");
    await expect(page.locator(".chat-msg-user")).toHaveCount(2, { timeout: 15000 });
    await expect(page.locator(".chat-msg-user").first()).toContainText("first question");
    await expect(page.locator(".chat-msg-user").last()).toContainText("second question");
  });
});

test.describe("Chat Trace", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await page.click("#chat-fab");
    await page.waitForSelector(".chat-container");
  });

  test("assistant message has collapsible trace", async ({ page }) => {
    await page.fill(".chat-input", "list open positions");
    await page.click(".chat-send-btn");
    await expect(page.locator(".chat-trace")).toBeVisible({ timeout: 15000 });
  });

  test("expanding trace shows SQL", async ({ page }) => {
    await page.fill(".chat-input", "list open positions");
    await page.click(".chat-send-btn");
    const trace = page.locator(".chat-trace").first();
    await trace.waitFor({ timeout: 15000 });
    await trace.locator("summary").click();
    await expect(page.locator(".sql-block")).toBeVisible();
  });

  test("trace shows row count", async ({ page }) => {
    await page.fill(".chat-input", "list candidates with kubernetes experience");
    await page.click(".chat-send-btn");
    const trace = page.locator(".chat-trace").first();
    await trace.waitFor({ timeout: 15000 });
    await trace.locator("summary").click();
    await expect(page.locator(".chat-trace-meta").first()).toBeVisible();
  });
});

test.describe("Chat Input Behavior", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await page.click("#chat-fab");
    await page.waitForSelector(".chat-container");
  });

  test("enter key submits message", async ({ page }) => {
    await page.fill(".chat-input", "test enter");
    await page.press(".chat-input", "Enter");
    await expect(page.locator(".chat-msg-user")).toBeVisible();
  });

  test("shift+enter adds newline", async ({ page }) => {
    await page.click(".chat-input");
    await page.keyboard.type("line1");
    await page.keyboard.press("Shift+Enter");
    await page.keyboard.type("line2");
    const val = await page.locator(".chat-input").inputValue();
    expect(val).toContain("line1");
    expect(val).toContain("line2");
    // Should NOT have sent
    await expect(page.locator(".chat-msg-user")).toHaveCount(0);
  });

  test("empty input does not submit", async ({ page }) => {
    await page.click(".chat-send-btn");
    await expect(page.locator(".chat-msg-user")).toHaveCount(0);
  });

  test("input cleared after submit", async ({ page }) => {
    await page.fill(".chat-input", "test clear");
    await page.click(".chat-send-btn");
    await expect(page.locator(".chat-input")).toHaveValue("");
  });

  test("send button disabled during loading", async ({ page }) => {
    await page.route("**/api/chat", async (route) => {
      await new Promise(r => setTimeout(r, 500));
      await route.continue();
    });
    await page.fill(".chat-input", "test loading");
    await page.click(".chat-send-btn");
    await expect(page.locator(".chat-send-btn")).toBeDisabled();
    await page.locator(".chat-msg-assistant").first().waitFor({ timeout: 15000 });
    await expect(page.locator(".chat-send-btn")).toBeEnabled();
  });

  test("loading dots appear while waiting", async ({ page }) => {
    await page.route("**/api/chat", async (route) => {
      await new Promise(r => setTimeout(r, 500));
      await route.continue();
    });
    await page.fill(".chat-input", "test loading dots");
    await page.click(".chat-send-btn");
    await expect(page.locator(".chat-loading-dots")).toBeVisible();
    await page.locator(".chat-msg-assistant .chat-bubble-assistant:not(.chat-loading-bubble)").first().waitFor({ timeout: 15000 });
    await expect(page.locator(".chat-loading-dots")).not.toBeVisible();
  });
});

test.describe("Chat Suggestion Buttons", () => {
  test("clicking suggestion sends it", async ({ page }) => {
    await page.goto("/");
    await page.click("#chat-fab");
    await page.waitForSelector(".chat-suggestion");
    await page.locator(".chat-suggestion").first().click();
    await expect(page.locator(".chat-msg-user")).toBeVisible();
  });

  test("suggestions disappear after first message", async ({ page }) => {
    await page.goto("/");
    await page.click("#chat-fab");
    await page.waitForSelector(".chat-suggestion");
    await expect(page.locator(".chat-suggestions")).toBeVisible();
    await page.locator(".chat-suggestion").first().click();
    await expect(page.locator(".chat-suggestions")).not.toBeVisible();
  });
});

test.describe("Chat State Persistence", () => {
  test("messages persist when closing and reopening panel", async ({ page }) => {
    await page.goto("/");
    await page.click("#chat-fab");
    await page.waitForSelector(".chat-container");
    await page.fill(".chat-input", "persistence test");
    await page.click(".chat-send-btn");
    await page.locator(".chat-msg-assistant").first().waitFor({ timeout: 15000 });

    // Close panel
    await page.click("#chat-panel-close");
    await expect(page.locator("#chat-panel")).not.toHaveClass(/open/);

    // Reopen panel
    await page.click("#chat-fab");
    await expect(page.locator(".chat-msg-user")).toBeVisible();
    await expect(page.locator(".chat-bubble-user")).toContainText("persistence test");
  });

  test("messages persist across page navigation", async ({ page }) => {
    await page.goto("/");
    await page.click("#chat-fab");
    await page.waitForSelector(".chat-container");
    await page.fill(".chat-input", "nav test");
    await page.click(".chat-send-btn");
    await page.locator(".chat-msg-assistant").first().waitFor({ timeout: 15000 });

    // Close panel and navigate
    await page.click("#chat-panel-close");
    await page.click('a[data-route="positions"]');
    await expect(page.locator("#app h1")).toHaveText("Positions");

    // Reopen panel
    await page.click("#chat-fab");
    await expect(page.locator(".chat-bubble-user")).toContainText("nav test");
  });
});
