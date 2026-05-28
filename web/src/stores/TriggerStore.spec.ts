import { StoreBase } from '@boatkit-io/resub';
import { beforeAll, describe, expect, test, vi } from 'vitest';

import TriggerStore from './TriggerStore.js';
import BinaryReader from '../utils/BinaryReader.js';
import { PartialFor } from '../utils/SerializationTypes.js';

interface TriggerStoreSpecState {
    values: Map<string, number>;
}

class TriggerStoreSpecPartial implements PartialFor<TriggerStoreSpecState> {
    applyTo(): (string | number)[][] {
        return [];
    }
}

interface TriggerStorePrivate {
    _triggerFieldUpdate(field: (string | number)[]): void;
}

class TriggerStoreSpecStore extends TriggerStore<TriggerStoreSpecState> {
    constructor(public readonly testStoreName: string) {
        super(testStoreName, {
            fromValues: () => ({ values: new Map() }),
            deserialized: (_r: BinaryReader) => ({ values: new Map() }),
        }, {
            deserialized: (_r: BinaryReader) => new TriggerStoreSpecPartial(),
        });
    }

    fireField(field: (string | number)[]): void {
        (this as unknown as TriggerStorePrivate)._triggerFieldUpdate(field);
    }
}

describe('TriggerStore keyed subscriptions', () => {
    beforeAll(() => {
        (globalThis as { __DEV__?: boolean }).__DEV__ = true;
    });

    test('enumerates registered stores and exposes their names', () => {
        const first = new TriggerStoreSpecStore(uniqueStoreName());
        const second = new TriggerStoreSpecStore(uniqueStoreName());

        expect(TriggerStore.getAllStores()).toEqual(expect.arrayContaining([first, second]));
        expect(first.getName()).toBe(first.testStoreName);
        expect(second.getName()).toBe(second.testStoreName);
    });

    test('refcounts duplicate subscriptions for the same key', () => {
        const store = new TriggerStoreSpecStore(uniqueStoreName());
        const tokenOne = store.subscribe(vi.fn(), 'values%&a');
        const tokenTwo = store.subscribe(vi.fn(), 'values%&a');

        expect(TriggerStore.getStoreSubs().filter(sub => sub.storeName === store.testStoreName)).toEqual([
            { storeName: store.testStoreName, key: 'values%&a' },
        ]);

        store.unsubscribe(tokenOne);
        expect(TriggerStore.getStoreSubs().filter(sub => sub.storeName === store.testStoreName)).toEqual([
            { storeName: store.testStoreName, key: 'values%&a' },
        ]);

        store.unsubscribe(tokenTwo);
        expect(TriggerStore.getStoreSubs().filter(sub => sub.storeName === store.testStoreName)).toEqual([]);
    });

    test('narrow updates only trigger matching keyed subscriptions', () => {
        const store = new TriggerStoreSpecStore(uniqueStoreName());
        const allCallback = vi.fn();
        const callbackA = vi.fn();
        const callbackB = vi.fn();
        const allToken = store.subscribe(allCallback, StoreBase.Key_All);
        const tokenA = store.subscribe(callbackA, 'values%&a');
        const tokenB = store.subscribe(callbackB, 'values%&b');

        store.fireField(['values', 'a']);

        expect(callbackA).toHaveBeenCalledTimes(1);
        expect(callbackB).not.toHaveBeenCalled();
        expect(allCallback).toHaveBeenCalledTimes(1);
        expect(allCallback).toHaveBeenCalledWith(['values%&a']);

        store.unsubscribe(allToken);
        store.unsubscribe(tokenA);
        store.unsubscribe(tokenB);
    });

    test('broad field updates cascade inward to nested keyed subscriptions', () => {
        const store = new TriggerStoreSpecStore(uniqueStoreName());
        const callbackA = vi.fn();
        const callbackB = vi.fn();
        const tokenA = store.subscribe(callbackA, 'values%&a');
        const tokenB = store.subscribe(callbackB, 'values%&b');

        store.fireField(['values']);

        expect(callbackA).toHaveBeenCalledTimes(1);
        expect(callbackB).toHaveBeenCalledTimes(1);

        store.unsubscribe(tokenA);
        store.unsubscribe(tokenB);
    });

    test('root updates trigger all keyed subscriptions without enumerating keys', () => {
        const store = new TriggerStoreSpecStore(uniqueStoreName());
        const callbackA = vi.fn();
        const callbackB = vi.fn();
        const tokenA = store.subscribe(callbackA, 'values%&a');
        const tokenB = store.subscribe(callbackB, 'other');

        store.fireField([]);

        expect(callbackA).toHaveBeenCalledTimes(1);
        expect(callbackA).toHaveBeenCalledWith(undefined);
        expect(callbackB).toHaveBeenCalledTimes(1);
        expect(callbackB).toHaveBeenCalledWith(undefined);

        store.unsubscribe(tokenA);
        store.unsubscribe(tokenB);
    });
});

let nextStoreID = 1;

function uniqueStoreName(): string {
    return `trigger-store-spec-${nextStoreID++}`;
}
