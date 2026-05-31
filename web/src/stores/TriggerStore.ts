import { StoreBase, formCompoundKey } from '@boatkit-io/resub';

import { StoreUpdateFullMessage, StoreUpdateMessage, StoreUpdateMessageKind, StoreUpdatePartialMessage } from '../websocket/SocketHelper.js';
import BinaryReader from '../utils/BinaryReader.js';
import type {
    FieldInfo,
    PartialFor,
    VarInfo,
} from '../utils/SerializationTypes.js';
import {
    VarInfoArray,
    VarInfoMap,
    VarInfoPointer,
    VarInfoStruct,
} from '../utils/SerializationTypes.js';
import SubscribableEvent from '../utils/SubscribableEvent.js';

const compoundKeyJoinerString = "%&";

type GeneratedStateType<S extends object> = {
    fromValues: () => S;
    deserialized: (r: BinaryReader) => S;
    _fieldInfo?: FieldInfo[];
};

declare global {
    var __DEV__: boolean | undefined;
}

export default abstract class TriggerStore<S extends object> extends StoreBase {
    protected static _storeMap: { [storeName: string]: TriggerStore<object> } = {};

    protected _state: S;

    static eventSubscriptionStarted = new SubscribableEvent<(storeName: string, key?: string) => void>();
    static eventSubscriptionStopped = new SubscribableEvent<(storeName: string, key?: string) => void>();
    private static _storeSubs = new Map<string, Set<string>>();

    static handleUpdateMessage(message: StoreUpdateMessage) {
        const store = this._storeMap[message.storeName];
        if (!store) {
            throw new Error('No store found for update message: ' + JSON.stringify(message));
        }

        store._processUpdateMessage(message);
    }

    static getStoreSubs() {
        const ret: { storeName: string, key: string|undefined }[] = [];
        for (const [storeName, keys] of this._storeSubs.entries()) {
            for (const key of keys) {
                ret.push({ storeName, key: key === "" ? undefined : key });
            }
        }
        return ret;
    }

    static getAllStores(): TriggerStore<object>[] {
        return Object.values(this._storeMap);
    }

    constructor(private _storeName: string, private _stateType: GeneratedStateType<S>,
        private _partialType: { deserialized: (r: BinaryReader) => PartialFor<S> }) {
        super();

        if ((typeof __DEV__ === 'undefined' || !__DEV__) && this._storeName in TriggerStore._storeMap) {
            // TODO: It's allowed to re-use a store in dev HMR
            throw new Error('Store already registered: ' + this._storeName);
        }

        TriggerStore._storeMap[this._storeName] = this;

        this._state = this._stateType.fromValues();
    }

    register(): void {
        // NOOP
    }

    getName(): string {
        return this._storeName;
    }

    // Track when the app first starts caring and last stops caring about this store, for the streaming service
    protected override _startedTrackingSub(key?: string) {
        const wireKey = this._canonicalSubscriptionKey(key);
        let keys = TriggerStore._storeSubs.get(this._storeName);
        if (!keys) {
            keys = new Set();
            TriggerStore._storeSubs.set(this._storeName, keys);
        }
        if (keys.has(wireKey)) {
            return;
        }

        keys.add(wireKey);
        TriggerStore.eventSubscriptionStarted.fire(this._storeName, wireKey === "" ? undefined : wireKey);
    }

    protected override _stoppedTrackingSub(key?: string) {
        const wireKey = this._canonicalSubscriptionKey(key);
        if (this._hasActiveSubscriptionForCanonicalKey(wireKey)) {
            return;
        }

        const keys = TriggerStore._storeSubs.get(this._storeName);
        if (!keys?.has(wireKey)) {
            throw new Error('Got _stoppedTrackingKey without _hasAnySubscriptions: ' + this._storeName + "/" + wireKey);
            return;
        }

        keys.delete(wireKey);
        if (keys.size === 0) {
            TriggerStore._storeSubs.delete(this._storeName);
        }
        TriggerStore.eventSubscriptionStopped.fire(this._storeName, wireKey === "" ? undefined : wireKey);
    }

    private _processUpdateMessage(message: StoreUpdateMessage): void {
        switch (message.kind) {
            case StoreUpdateMessageKind.Full: {
                const msgFull = message as StoreUpdateFullMessage;
                this._state = this._stateType.deserialized(new BinaryReader(msgFull.state));
                this.trigger();
                break;
            }
            case StoreUpdateMessageKind.Partial: {
                const msgPartial = message as StoreUpdatePartialMessage;
                const partial = this._partialType.deserialized(new BinaryReader(msgPartial.partial));
                const fields = partial.applyTo(this._state);
                for (const f of fields) {
                    this._triggerFieldUpdate(f);
                }
                break;
            }
            default:
                throw new Error("Unhandled message kind: " + message.kind);
                break;
        }
    }

    private _triggerFieldUpdate(field: (string | number)[]): void {
        if (field.length === 0) {
            this.trigger();
            return;
        }

        const fieldKey = this._canonicalFieldKey(field);
        const keysToTrigger = new Set<string>();
        for (const subscriptionKey of this._getSubscriptionKeys()) {
            if (subscriptionKey === StoreBase.Key_All) {
                continue;
            }
            if (subscriptionKeyAffectsField(fieldKey, this._canonicalSubscriptionKey(subscriptionKey))) {
                keysToTrigger.add(subscriptionKey);
            }
        }
        if (keysToTrigger.size === 0) {
            keysToTrigger.add(fieldKey);
        }
        this.trigger([...keysToTrigger]);
    }

    private _canonicalSubscriptionKey(key?: string): string {
        if (key === undefined || key === StoreBase.Key_All) {
            return "";
        }
        return this._canonicalFieldKey(key.split(compoundKeyJoinerString));
    }

    private _canonicalFieldKey(field: (string | number)[]): string {
        return formCompoundKey(...canonicalFieldPath(field, this._stateType._fieldInfo));
    }

    private _hasActiveSubscriptionForCanonicalKey(wireKey: string): boolean {
        for (const subscriptionKey of this._getSubscriptionKeys()) {
            if (subscriptionKey === StoreBase.Key_All) {
                if (wireKey === "") {
                    return true;
                }
                continue;
            }
            if (this._canonicalSubscriptionKey(subscriptionKey) === wireKey) {
                return true;
            }
        }
        return false;
    }
}

function subscriptionKeyAffectsField(fieldKey: string, subscriptionKey: string): boolean {
    const fieldParts = fieldKey.split(compoundKeyJoinerString);
    const subscriptionParts = subscriptionKey.split(compoundKeyJoinerString);
    const maxLen = Math.min(fieldParts.length, subscriptionParts.length);
    for (let idx = 0; idx < maxLen; idx++) {
        if (fieldParts[idx] !== subscriptionParts[idx]) {
            return false;
        }
    }
    return true;
}

function canonicalFieldPath(field: (string | number)[], rootFields: FieldInfo[] | undefined): (string | number)[] {
    const ret: (string | number)[] = [];
    let fields = rootFields;
    let valueInfo: VarInfo | undefined;

    for (let idx = 0; idx < field.length;) {
        if (fields) {
            const fieldInfo = fields.find(fi => fieldNameMatches(field[idx], fi.name));
            if (!fieldInfo) {
                ret.push(...field.slice(idx));
                break;
            }

            ret.push(clientFieldName(fieldInfo.name));
            valueInfo = fieldInfo.varInfo;
            fields = undefined;
            idx++;
            continue;
        }

        const unwrapped = unwrapPointer(valueInfo);
        if (unwrapped instanceof VarInfoMap) {
            ret.push(field[idx]);
            valueInfo = unwrapped.elemType;
            fields = fieldsForValue(valueInfo);
            idx++;
            continue;
        }
        if (unwrapped instanceof VarInfoArray) {
            ret.push(field[idx]);
            valueInfo = unwrapped.elemType;
            fields = fieldsForValue(valueInfo);
            idx++;
            continue;
        }

        fields = fieldsForValue(valueInfo);
        if (fields) {
            continue;
        }

        ret.push(...field.slice(idx));
        break;
    }

    return ret;
}

function fieldNameMatches(part: string | number, fieldName: string): boolean {
    return typeof part === "string" && clientFieldName(part) === clientFieldName(fieldName);
}

function clientFieldName(name: string): string {
    if (name === "" || name.includes("_")) {
        return name;
    }
    return name[0].toLowerCase() + name.slice(1);
}

function fieldsForValue(vi: VarInfo | undefined): FieldInfo[] | undefined {
    const unwrapped = unwrapPointer(vi);
    if (!(unwrapped instanceof VarInfoStruct)) {
        return undefined;
    }
    return (unwrapped.deserializer as { _fieldInfo?: FieldInfo[] } | undefined)?._fieldInfo;
}

function unwrapPointer(vi: VarInfo | undefined): VarInfo | undefined {
    while (vi instanceof VarInfoPointer) {
        vi = vi.subType;
    }
    return vi;
}
