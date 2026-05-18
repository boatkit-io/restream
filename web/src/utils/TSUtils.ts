export function deepEqual(a: unknown, b: unknown): boolean {
    if (a === b) {
        return true;
    }
    if (typeof a !== 'object' || typeof b !== 'object' || !a || !b) {
        // Can't compare non-objects as anything other than triple-equals.
        // Functions must be reference equal to be equal, otherwise are ALWAYS unequal.
        return false;
    }
    if (Array.isArray(a)) {
        // Optimize array checks a bit
        if (!Array.isArray(b)) {
            return false;
        }
        const aLen = a.length;
        if (aLen !== b.length) {
            return false;
        }
        for (let i = 0; i < aLen; i++) {
            if (!deepEqual(a[i], b[i])) {
                return false;
            }
        }
    }
    const aKeys = Object.keys(a);
    const bKeys = Object.keys(b);
    if (aKeys.length !== bKeys.length) {
        return false;
    }
    // First check that all the keys exist in both
    for (const key of aKeys) {
        if (!(key in b)) {
            return false;
        }
    }
    // Got this far, now check that the values are deep equal
    for (const key of aKeys) {
        // @ts-expect-error deep equals
        if (!deepEqual(a[key], b[key])) {
            return false;
        }
    }
    return true;
}

export interface Deferred<T> {
    promise: Promise<T>;
    resolve: (t: T) => void;
    reject: (r: unknown) => void;
}

export function deferred<T>(): Deferred<T> {
    let resolve!: (t: T) => void;
    let reject!: (r: unknown) => void;
    const promise = new Promise<T>((res, rej) => {
        resolve = res;
        reject = rej;
    });
    return {
        promise,
        resolve,
        reject,
    };
}
