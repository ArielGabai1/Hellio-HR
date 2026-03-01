import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "tests",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  use: { baseURL: "http://localhost:80" },
  projects: [
    { name: "setup", testMatch: /auth\.setup\.js/ },
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"], storageState: "tests/.auth/state.json" },
      dependencies: ["setup"],
    },
  ],
});
