export function ok(condition: unknown, message = 'Assertion failed'): asserts condition {
    if (!condition) {
        throw new Error(message);
    }
}
