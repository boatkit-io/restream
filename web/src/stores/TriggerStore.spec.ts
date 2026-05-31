import { StoreBase } from '@boatkit-io/resub';
import { beforeAll, describe, expect, test, vi } from 'vitest';

import TriggerStore from './TriggerStore.js';
import BinaryReader from '../utils/BinaryReader.js';
import type {
    FieldInfo,
    PartialFor,
} from '../utils/SerializationTypes.js';
import {
    SerializationType,
    VarInfoMap,
    VarInfoPointer,
    VarInfoPrimitive,
    VarInfoStruct,
} from '../utils/SerializationTypes.js';

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

interface GeneratedDevicePGNState {
    rxCount: number;
}

class GeneratedDevicePGN {
    public static readonly fieldInfo: readonly FieldInfo[] = [
        { name: "RxCount", fieldIdx: 0, fieldID: 1, varInfo: new VarInfoPrimitive(SerializationType.Uint64, "uint") },
    ];

    static deserialized(): GeneratedDevicePGNState {
        return { rxCount: 0 };
    }
}

interface GeneratedTriggerStoreSpecState {
    devicePGNs: Map<string, GeneratedDevicePGNState>;
}

class GeneratedTriggerStoreSpecPartial implements PartialFor<GeneratedTriggerStoreSpecState> {
    applyTo(): (string | number)[][] {
        return [];
    }
}

class GeneratedTriggerStoreSpecStateType {
    public static readonly fieldInfo: readonly FieldInfo[] = [
        {
            name: "DevicePGNs",
            fieldIdx: 0,
            fieldID: 1,
            varInfo: new VarInfoMap(
                false,
                new VarInfoPrimitive(SerializationType.String),
                new VarInfoPointer(false, new VarInfoStruct("GeneratedDevicePGN", "test", GeneratedDevicePGN)),
            ),
        },
    ];

    static fromValues(): GeneratedTriggerStoreSpecState {
        return { devicePGNs: new Map() };
    }

    static deserialized(): GeneratedTriggerStoreSpecState {
        return { devicePGNs: new Map() };
    }
}

class GeneratedTriggerStoreSpecStore extends TriggerStore<GeneratedTriggerStoreSpecState> {
    constructor(public readonly testStoreName: string) {
        super(testStoreName, GeneratedTriggerStoreSpecStateType, {
            deserialized: (_r: BinaryReader) => new GeneratedTriggerStoreSpecPartial(),
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

    test('generated store subscriptions normalize field names for trigger matching and wire keys', () => {
        const store = new GeneratedTriggerStoreSpecStore(uniqueStoreName());
        const callback = vi.fn();
        const token = store.subscribe(callback, 'DevicePGNs');

        expect(TriggerStore.getStoreSubs().filter(sub => sub.storeName === store.testStoreName)).toEqual([
            { storeName: store.testStoreName, key: 'devicePGNs' },
        ]);

        store.fireField(['devicePGNs']);

        expect(callback).toHaveBeenCalledTimes(1);

        store.unsubscribe(token);
    });

    test('generated store nested keypaths normalize struct fields but preserve map keys', () => {
        const store = new GeneratedTriggerStoreSpecStore(uniqueStoreName());
        const matchingCallback = vi.fn();
        const wrongMapKeyCallback = vi.fn();
        const matchingToken = store.subscribe(matchingCallback, 'DevicePGNs%&CAN0%&RxCount');
        const wrongMapKeyToken = store.subscribe(wrongMapKeyCallback, 'DevicePGNs%&can0%&RxCount');

        expect(TriggerStore.getStoreSubs().filter(sub => sub.storeName === store.testStoreName)).toEqual([
            { storeName: store.testStoreName, key: 'devicePGNs%&CAN0%&rxCount' },
            { storeName: store.testStoreName, key: 'devicePGNs%&can0%&rxCount' },
        ]);

        store.fireField(['devicePGNs', 'CAN0', 'rxCount']);

        expect(matchingCallback).toHaveBeenCalledTimes(1);
        expect(wrongMapKeyCallback).not.toHaveBeenCalled();

        store.unsubscribe(matchingToken);
        store.unsubscribe(wrongMapKeyToken);
    });

    test('generated store equivalent field keys share one wire subscription until all aliases unsubscribe', () => {
        const store = new GeneratedTriggerStoreSpecStore(uniqueStoreName());
        const upperToken = store.subscribe(vi.fn(), 'DevicePGNs');
        const lowerToken = store.subscribe(vi.fn(), 'devicePGNs');

        expect(TriggerStore.getStoreSubs().filter(sub => sub.storeName === store.testStoreName)).toEqual([
            { storeName: store.testStoreName, key: 'devicePGNs' },
        ]);

        store.unsubscribe(upperToken);
        expect(TriggerStore.getStoreSubs().filter(sub => sub.storeName === store.testStoreName)).toEqual([
            { storeName: store.testStoreName, key: 'devicePGNs' },
        ]);

        store.unsubscribe(lowerToken);
        expect(TriggerStore.getStoreSubs().filter(sub => sub.storeName === store.testStoreName)).toEqual([]);
    });
});

let nextStoreID = 1;

function uniqueStoreName(): string {
    return `trigger-store-spec-${nextStoreID++}`;
}
