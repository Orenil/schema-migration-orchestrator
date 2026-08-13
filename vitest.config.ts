import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    testTimeout: 30000,
    hookTimeout: 30000,
    // Integration suites share one real Postgres cluster/database and reset the schema between
    // tests, so test files must not run concurrently against it.
    fileParallelism: false,
  },
});
