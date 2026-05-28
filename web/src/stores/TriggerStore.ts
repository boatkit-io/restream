import { StoreBase, formCompoundKey } from '@boatkit-io/resub';

import { StoreUpdateFullMessage, StoreUpdateMessage, StoreUpdateMessageKind, StoreUpdatePartialMessage } from '../websocket/SocketHelper.js';
import BinaryReader from '../utils/BinaryReader.js';
import { PartialFor } from '../utils/SerializationTypes.js';
import SubscribableEvent from '../utils/SubscribableEvent.js';

const compoundKeyJoinerString = "%&";

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

    constructor(private _storeName: string, private _stateType: { fromValues: () => S, deserialized: (r: BinaryReader) => S },
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
        const wireKey = key ?? "";
        let keys = TriggerStore._storeSubs.get(this._storeName);
        if (!keys) {
            keys = new Set();
            TriggerStore._storeSubs.set(this._storeName, keys);
        }
        if (keys.has(wireKey)) {
            throw new Error('Started tracking already tracked sub: ' + this._storeName + "/" + wireKey);
        }

        keys.add(wireKey);
        TriggerStore.eventSubscriptionStarted.fire(this._storeName, key);
    }

    protected override _stoppedTrackingSub(key?: string) {
        const wireKey = key ?? "";
        const keys = TriggerStore._storeSubs.get(this._storeName);
        if (!keys?.has(wireKey)) {
            throw new Error('Got _stoppedTrackingKey without _hasAnySubscriptions: ' + this._storeName + "/" + wireKey);
            return;
        }

        keys.delete(wireKey);
        if (keys.size === 0) {
            TriggerStore._storeSubs.delete(this._storeName);
        }
        TriggerStore.eventSubscriptionStopped.fire(this._storeName, key);
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

        const fieldKey = formCompoundKey(...field);
        const keysToTrigger = new Set<string>([fieldKey]);
        for (const subscriptionKey of this._getSubscriptionKeys()) {
            if (subscriptionKey === StoreBase.Key_All) {
                continue;
            }
            if (subscriptionKeyAffectsField(fieldKey, subscriptionKey)) {
                keysToTrigger.add(subscriptionKey);
            }
        }
        this.trigger([...keysToTrigger]);
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
