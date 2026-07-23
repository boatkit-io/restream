import { describe, expect, test } from 'vitest';

import { PartialModArray, PartialModMap, PartialValue } from './PackageRestream.js';
import { TestMapData, TestMapDataPartial } from './PackageStoretest.js';
import type { AppliablePartial } from '../utils/SerializationTypes.js';

type PartialApplySpecValue = {
    number: number;
};

class PartialApplySpecPartial implements AppliablePartial<PartialApplySpecValue> {
    constructor(private readonly number: number) {}

    applyTo(por: PartialApplySpecValue): (string | number)[][] {
        por.number = this.number;
        return [['number']];
    }
}

describe('partial apply field paths', () => {
    test('mod map deletes remove the key and report the changed path', () => {
        const target = new Map<string, PartialApplySpecValue>([
            ['engine', { number: 1 }],
            ['house', { number: 2 }],
        ]);
        const partial = PartialModMap.fromValues<string, PartialApplySpecValue, PartialApplySpecPartial>(
            new Map(),
            new Set(['engine']),
            new Map(),
            undefined,
        );

        const fields = partial.applyTo(target);

        expect(target.has('engine')).toBe(false);
        expect(target.get('house')?.number).toBe(2);
        expect(fields).toEqual([['engine']]);
    });

    test('mod map deletes apply when delete keys arrive in map form', () => {
        const target = new Map<string, PartialApplySpecValue>([
            ['engine', { number: 1 }],
            ['house', { number: 2 }],
        ]);
        const partial = PartialModMap.fromValues<string, PartialApplySpecValue, PartialApplySpecPartial>(
            new Map(),
            new Set(),
            new Map(),
            undefined,
        );
        (partial as unknown as { dataDeletes: Map<string, unknown> }).dataDeletes = new Map([['engine', undefined]]);

        const fields = partial.applyTo(target);

        expect(target.has('engine')).toBe(false);
        expect(target.get('house')?.number).toBe(2);
        expect(fields).toEqual([['engine']]);
    });

    test('mod map suppresses nested fields when the key was set', () => {
        const target = new Map<string, PartialApplySpecValue>();
        const partial = PartialModMap.fromValues<string, PartialApplySpecValue, PartialApplySpecPartial>(
            new Map([['engine', { number: 1 }]]),
            new Set(),
            new Map([['engine', new PartialApplySpecPartial(2)]]),
            undefined,
        );

        const fields = partial.applyTo(target);

        expect(target.get('engine')?.number).toBe(2);
        expect(fields).toEqual([['engine']]);
    });

    test('mod map materializes a missing parent for a generated nested partial', () => {
        const target = new Map<number, TestMapData>();
        const partial = PartialModMap.fromValues<number, TestMapData, TestMapDataPartial>(
            new Map(),
            new Set(),
            new Map([[5, TestMapDataPartial.fromValues(42)]]),
            undefined,
        );

        const fields = partial.applyTo(target);

        expect(target.get(5)?.number).toBe(42);
        expect(target.get(5)?.data).toEqual([]);
        expect(fields).toEqual([[5, 'number']]);
    });

    test('mod map preserves an existing parent while applying a generated nested partial', () => {
        const target = new Map<number, TestMapData>([
            [5, TestMapData.fromValues(1, [7, 8])],
        ]);
        const partial = PartialModMap.fromValues<number, TestMapData, TestMapDataPartial>(
            new Map(),
            new Set(),
            new Map([[5, TestMapDataPartial.fromValues(42)]]),
            undefined,
        );

        const fields = partial.applyTo(target);

        expect(target.get(5)?.number).toBe(42);
        expect(target.get(5)?.data).toEqual([7, 8]);
        expect(fields).toEqual([[5, 'number']]);
    });

    test('mod array suppresses nested fields when the index was set', () => {
        const target: PartialApplySpecValue[] = [{ number: 0 }];
        const partial = PartialModArray.fromValues<PartialApplySpecValue, PartialApplySpecPartial>(
            new Map([[0, { number: 1 }]]),
            new Map([[0, new PartialApplySpecPartial(2)]]),
        );
        partial.whole = undefined;

        const fields = partial.applyTo(target);

        expect(target[0].number).toBe(2);
        expect(fields).toEqual([[0]]);
    });

    test('mod array materializes a missing element for a generated nested partial', () => {
        const target: TestMapData[] = [];
        const partial = PartialModArray.fromValues<TestMapData, TestMapDataPartial>(
            new Map(),
            new Map([[2, TestMapDataPartial.fromValues(84)]]),
        );
        partial.whole = undefined;

        const fields = partial.applyTo(target);

        expect(target[2]?.number).toBe(84);
        expect(target[2]?.data).toEqual([]);
        expect(fields).toEqual([[2, 'number']]);
    });

    test('partial value materializes a missing parent for a generated nested partial', () => {
        const partial = PartialValue.fromValues<TestMapData|undefined, TestMapDataPartial>(
            undefined,
            TestMapDataPartial.fromValues(126),
        );

        const [target, fields] = partial.applyOnTop(undefined);

        expect(target?.number).toBe(126);
        expect(target?.data).toEqual([]);
        expect(fields).toEqual([['number']]);
    });
});
