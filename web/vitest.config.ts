import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        exclude: ['dist/**', 'node_modules/**'],
        include: ['src/**/*.spec.ts', 'test/**/*.test.ts'],
    },
});
