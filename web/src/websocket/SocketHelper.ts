import { Socket } from 'socket.io-client';

import { Deserializable, Serializable, VarInfoStruct } from '../utils/SerializationTypes.js';
import { deferred, Deferred } from '../utils/TSUtils.js';
import BinaryWriter from '../utils/BinaryWriter.js';
import BinaryReader from '../utils/BinaryReader.js';
import TriggerStore from '../stores/TriggerStore.js';

interface RPCWaiting {
    def: Deferred<unknown>;
    responseType: Deserializable<RPCResponseStruct<unknown>>;
}

export abstract class RPCStruct<RS extends RPCResponseStruct<RT>, RT> implements Serializable {
    constructor(public readonly rpcBoundName: string, public readonly responseType: Deserializable<RS>) { }

    abstract serialize(w: BinaryWriter, vi: VarInfoStruct | undefined): void;
}

export abstract class RPCResponseStruct<RT = void> {
    public result?: RT;
    public error: string | undefined;
}

enum SocketEventNames {
    StoreUpdate = 'storeupdate',
    StoreSubscription = 'storesub',

    RPCCall = 'rpccall',
    RPCCallResponse = 'rpccallresp',
}

export enum StoreSubscriptionAction {
    Subscribe = 0,
    Unsubscribe = 1,
}

export interface StoreSubscriptionMessage {
    storeName: string;
    action: StoreSubscriptionAction;
    key?: string;
}

export enum StoreUpdateMessageKind {
    Full = 0,
    Partial = 2,
}

export interface StoreUpdateMessage {
    time: number;
    kind: StoreUpdateMessageKind;
    storeName: string;
}

// Models for the TriggerStore client/server exchange
export interface StoreUpdateFullMessage extends StoreUpdateMessage {
    kind: StoreUpdateMessageKind.Full;
    state: ArrayBuffer;
}

export interface StoreUpdatePartialMessage extends StoreUpdateMessage {
    kind: StoreUpdateMessageKind.Partial;
    partial: ArrayBuffer;
}

export interface RPCCallMessage {
    callID: number;
    methodName: string;
    request: ArrayBufferLike;
}

export interface RPCCallResponseMessage {
    callID: number;
    response?: ArrayBufferLike;
    error?: RPCCallError;
}

export interface RPCCallError {
    message: string;
    data: Record<string, unknown>;
}

export class ReStreamSocket {
    private _socket: Socket;

    private _timestampOffset = 0;
    private _authenticated = false;

    private _rpcCallID = 1;
    private _rpcCallsPending = new Map<number, RPCWaiting>();

    constructor(socket: Socket) {
        this._socket = socket;

        socket.on('disconnect', () => {
            for (const v of this._rpcCallsPending.values()) {
                v.def.reject({ message: "Server is disconnected", data: {} });
            }
            this._rpcCallsPending.clear();
            this._authenticated = false;
        });

        socket.on(SocketEventNames.StoreUpdate, (message: StoreUpdateMessage) => {
            this._timestampOffset = Date.now() - message.time;

            TriggerStore.handleUpdateMessage(message);
        });

        socket.on(SocketEventNames.RPCCallResponse, (message: RPCCallResponseMessage) => {
            const waiting = this._rpcCallsPending.get(message.callID);
            this._rpcCallsPending.delete(message.callID);

            if (!waiting) {
                alert("got binary RPC response for untracked RPC call " + message.callID);
                return;
            }

            if (!message.response || message.error) {
                waiting.def.reject(message.error?.message ?? "Server is disconnected");
                return;
            }

            const resp = waiting.responseType.deserialized(new BinaryReader(message.response), undefined);
            if (resp.error) {
                waiting.def.reject(resp.error);
            } else {
                waiting.def.resolve(resp.result);
            }
        });

        TriggerStore.eventSubscriptionStarted.subscribe((storeName, key) => {
            if (this._authenticated) {
                const message: StoreSubscriptionMessage = {
                    action: StoreSubscriptionAction.Subscribe,
                    storeName,
                    key,
                };
                this._socket.emit(SocketEventNames.StoreSubscription, message);
            }
        });

        TriggerStore.eventSubscriptionStopped.subscribe((storeName, key) => {
            if (this._authenticated) {
                const message: StoreSubscriptionMessage = {
                    action: StoreSubscriptionAction.Unsubscribe,
                    storeName,
                    key,
                };
                this._socket.emit(SocketEventNames.StoreSubscription, message);
            }
        });        
    }

    markAuthenticated() {
        this._authenticated = true;

        for (const storeSub of TriggerStore.getStoreSubs()) {
            const message: StoreSubscriptionMessage = {
                action: StoreSubscriptionAction.Subscribe,
                storeName: storeSub.storeName,
                key: storeSub.key,
            };
            this._socket.emit(SocketEventNames.StoreSubscription, message);
        }
    }

    sendRPC<RS extends RPCResponseStruct<RT>, RT>(rpcStruct: RPCStruct<RS, RT>): Promise<RT> {
        if (this._authenticated) {
            return Promise.reject(new Error("Server is disconnected"));
        }

        const w = new BinaryWriter();
        rpcStruct.serialize(w, undefined);

        const msg: RPCCallMessage = {
            callID: this._rpcCallID++,
            methodName: rpcStruct.rpcBoundName,
            request: w.getBytes().buffer,
        };

        const def = deferred<unknown>();
        const waiting: RPCWaiting = {
            def,
            responseType: rpcStruct.responseType,
        };
        this._rpcCallsPending.set(msg.callID, waiting);
     
        this._socket.emit(SocketEventNames.RPCCall, msg);

        return def.promise as Promise<RT>;
    }
}
