import { Socket } from 'socket.io-client';

import { Deserializable, Serializable, VarInfoStruct } from '../utils/SerializationTypes.js';
import { deferred, Deferred } from '../utils/TSUtils.js';
import BinaryWriter from '../utils/BinaryWriter.js';
import BinaryReader from '../utils/BinaryReader.js';
import TriggerStore from '../stores/TriggerStore.js';

const maxSocketStoreSubscriptions = 4096;
const maxSocketKeyedEventSubscriptions = 4096;
const maxSocketDataStreamSubscriptions = 64;
const maxSocketPendingRPCs = 128;
const maxSocketSessionAttachOperations = 8192;

interface RPCWaiting {
    def: Deferred<unknown>;
    responseType: Deserializable<RPCResponseStruct<unknown>>;
}

interface EventSubscription {
    eventType: EventStructType<EventStruct>;
    callback: (event: EventStruct) => void;
}

interface KeyedEventSubscriptionGroup {
    storeName: string;
    eventName: string;
    key: string;
    subscriptions: Set<EventSubscription>;
}

interface DataStreamSubscriptionGroup {
    subscriptionID: string;
    storeName: string;
    streamName: string;
    key: string;
    subscriptions: Set<DataStreamSubscription>;
    endpoint?: DataStreamEndpoint;
    error?: Error;
}

interface DataStreamSubscription {
    callback: DataStreamEndpointCallback;
}

interface SessionAttachOperation {
    name: SocketEventNames;
    message: unknown;
}

export abstract class RPCStruct<RS extends RPCResponseStruct<RT>, RT> implements Serializable {
    constructor(public readonly rpcBoundName: string, public readonly responseType: Deserializable<RS>) { }

    abstract serialize(w: BinaryWriter, vi: VarInfoStruct | undefined): void;
}

export abstract class FFRPCStruct implements Serializable {
    constructor(public readonly ffrpcBoundName: string) { }

    abstract serialize(w: BinaryWriter, vi: VarInfoStruct | undefined): void;
}

export abstract class RPCResponseStruct<RT = void> {
    public result?: RT;
    public error: string | undefined;
}

export abstract class EventStruct {
    constructor(public readonly eventBoundName: string) { }
}

export type EventStructType<ES extends EventStruct> = Deserializable<ES> & { readonly eventBoundName: string };

enum SocketEventNames {
    StoreUpdate = 'storeupdate',
    StoreSubscription = 'storesub',
    SubscriptionRejected = 'subscriptionrejected',

    Event = 'event',
    KeyedEvent = 'keyedevent',
    KeyedEventSubscription = 'keyedeventsub',
    DataStreamSubscription = 'datastreamsub',
    DataStreamEndpoint = 'datastreamendpoint',
    ViewerSessionAttach = 'viewersessionattach',
    ViewerSessionAttached = 'viewersessionattached',
    ViewerSessionClose = 'viewersessionclose',

    RPCCall = 'rpccall',
    RPCCallResponse = 'rpccallresp',
    FFRPCCall = 'ffrpc',
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

export type SubscriptionType = 'store' | 'keyedEvent' | 'dataStream';

export interface SubscriptionRejectionMessage {
    subscriptionType: SubscriptionType;
    storeName: string;
    key?: string;
    eventName?: string;
    streamName?: string;
    subscriptionID?: string;
    error: string;
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

export interface EventMessage {
    time: number;
    eventName: string;
    event: ArrayBufferLike;
}

export interface KeyedEventSubscriptionMessage {
    storeName: string;
    eventName: string;
    action: StoreSubscriptionAction;
    key: string;
}

export interface KeyedEventMessage {
    time: number;
    storeName: string;
    eventName: string;
    key: string;
    event: ArrayBufferLike;
}

export interface DataStreamSubscriptionMessage {
    subscriptionID: string;
    storeName: string;
    streamName: string;
    action: StoreSubscriptionAction;
    key: string;
}

export interface DataStreamEndpoint {
    leaseID: string;
    url: string;
    token: string;
    /** Zero means the credential lives until the parent subscription revokes it. */
    expiresAtUnixMilli: number;
    metadata?: Record<string, string>;
}

export interface DataStreamEndpointMessage {
    subscriptionID: string;
    endpoint?: DataStreamEndpoint;
    error?: string;
}

export interface ViewerSessionStoreSubscription {
    storeName: string;
    key: string;
}

export interface ViewerSessionDataStreamSubscription {
    subscriptionID: string;
    storeName: string;
    streamName: string;
    key: string;
}

export interface ViewerSessionAttachRequest {
    sessionID: string;
    storeSubscriptions: ViewerSessionStoreSubscription[];
    keyedEventSubscriptions: KeyedEventSubscriptionMessage[];
    dataStreamSubscriptions: ViewerSessionDataStreamSubscription[];
}

export interface ViewerSessionCapabilities {
    dataStreams: boolean;
}

export interface ViewerSessionAttachResponse {
    sessionID: string;
    resumed: boolean;
    capabilities: ViewerSessionCapabilities;
    rejectedSubscriptions?: SubscriptionRejectionMessage[];
    error?: string;
}

export interface ViewerSessionCloseMessage {
    sessionID: string;
}

export interface ReStreamSocketOptions {
    viewerSessions?: boolean;
    sessionAttachTimeoutMs?: number;
    /** Restricts this socket to an explicit subset of globally registered TriggerStores. */
    storeNames?: readonly string[];
    /** Excludes stores owned by another Restream socket/data plane. */
    excludedStoreNames?: readonly string[];
    /** Overrides the default DevTools warning for individually denied subscriptions. */
    onSubscriptionRejected?: (rejection: SubscriptionRejectionMessage) => void;
}

export type DataStreamEndpointCallback = (
    endpoint: DataStreamEndpoint | undefined,
    error?: Error,
) => void;

export interface RPCCallMessage {
    callID: number;
    methodName: string;
    request: ArrayBufferLike;
}

export interface FFRPCCallMessage {
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
    private _viewerSessions: boolean;
    private readonly _sessionAttachTimeoutMs: number;
    private readonly _storeNames: ReadonlySet<string> | undefined;
    private readonly _excludedStoreNames: ReadonlySet<string>;
    private readonly _onSubscriptionRejected: (rejection: SubscriptionRejectionMessage) => void;
    private _sessionID: string | undefined;
    private _sessionAttach: Deferred<void> | undefined;
    private _sessionAttachTimer: ReturnType<typeof setTimeout> | undefined;
    private _sessionAttachOperations: SessionAttachOperation[] = [];
    private _sessionFallbackAttempted = false;
    private _capabilities: ViewerSessionCapabilities = { dataStreams: false };

    private _rpcCallID = 1;
    private _dataStreamSubscriptionID = 1;
    private _rpcCallsPending = new Map<number, RPCWaiting>();
    private _eventSubscriptions = new Map<string, Set<EventSubscription>>();
    private _keyedEventSubscriptions = new Map<string, KeyedEventSubscriptionGroup>();
    private _dataStreamSubscriptions = new Map<string, DataStreamSubscriptionGroup>();

    constructor(socket: Socket, options: ReStreamSocketOptions = {}) {
        this._socket = socket;
        this._viewerSessions = options.viewerSessions ?? false;
        this._sessionAttachTimeoutMs = options.sessionAttachTimeoutMs ?? 10_000;
        this._storeNames = options.storeNames
            ? new Set(options.storeNames.map(name => name.trim()).filter(Boolean))
            : undefined;
        this._excludedStoreNames = new Set(
            options.excludedStoreNames?.map(name => name.trim()).filter(Boolean) ?? [],
        );
        this._onSubscriptionRejected = options.onSubscriptionRejected ?? warnSubscriptionRejected;

        socket.on('disconnect', () => {
            if (!this._viewerSessions) {
                for (const v of this._rpcCallsPending.values()) {
                    v.def.reject({ message: "Server is disconnected", data: {} });
                }
                this._rpcCallsPending.clear();
                for (const group of this._dataStreamSubscriptions.values()) {
                    group.endpoint = undefined;
                    group.error = new Error("Server is disconnected");
                    for (const subscription of group.subscriptions) {
                        subscription.callback(undefined, group.error);
                    }
                }
            }
            this._clearSessionAttach(new Error("Server disconnected during session attachment"));
            this._authenticated = false;
        });

        socket.on(SocketEventNames.ViewerSessionAttached, (message: ViewerSessionAttachResponse) => {
            if (!this._viewerSessions || !this._sessionAttach) {
                return;
            }
            if (message.error) {
                if (this._sessionID && !this._sessionFallbackAttempted) {
                    this._sessionFallbackAttempted = true;
                    this._sessionID = undefined;
                    this._rejectPendingRPCs("Viewer session expired");
                    for (const group of this._dataStreamSubscriptions.values()) {
                        group.endpoint = undefined;
                        group.error = undefined;
                    }
                    this._emitSessionAttach();
                    return;
                }
                this._clearSessionAttach(new Error(message.error));
                return;
            }
            if (!message.sessionID) {
                this._clearSessionAttach(new Error("Server returned an empty viewer session ID"));
                return;
            }
            for (const rejection of message.rejectedSubscriptions ?? []) {
                this._onSubscriptionRejected(rejection);
            }
            this._sessionID = message.sessionID;
            this._capabilities = message.capabilities ?? { dataStreams: false };
            this._authenticated = true;
            const attachOperations = this._sessionAttachOperations;
            this._sessionAttachOperations = [];
            const attaching = this._sessionAttach;
            this._sessionAttach = undefined;
            this._clearSessionAttachTimer();
            for (const operation of attachOperations) {
                this._socket.emit(operation.name, operation.message);
            }
            attaching.resolve(undefined);
        });

        socket.on(SocketEventNames.SubscriptionRejected, (message: SubscriptionRejectionMessage) => {
            this._onSubscriptionRejected(message);
        });

        socket.on(SocketEventNames.StoreUpdate, (message: StoreUpdateMessage) => {
            if (!this._usesStore(message.storeName)) {
                return;
            }
            this._timestampOffset = Date.now() - message.time;

            TriggerStore.handleUpdateMessage(message);
        });

        socket.on(SocketEventNames.Event, (message: EventMessage) => {
            this._timestampOffset = Date.now() - message.time;

            const subscriptions = this._eventSubscriptions.get(message.eventName);
            if (!subscriptions) {
                return;
            }

            for (const subscription of [...subscriptions]) {
                const event = subscription.eventType.deserialized(new BinaryReader(message.event), undefined);
                subscription.callback(event);
            }
        });

        socket.on(SocketEventNames.KeyedEvent, (message: KeyedEventMessage) => {
            this._timestampOffset = Date.now() - message.time;

            const identity = keyedEventSubscriptionIdentity(message.storeName, message.eventName, message.key);
            const group = this._keyedEventSubscriptions.get(identity);
            if (!group) {
                return;
            }

            for (const subscription of [...group.subscriptions]) {
                const event = subscription.eventType.deserialized(new BinaryReader(message.event), undefined);
                subscription.callback(event);
            }
        });

        socket.on(SocketEventNames.DataStreamEndpoint, (message: DataStreamEndpointMessage) => {
            const group = this._dataStreamSubscriptions.get(message.subscriptionID);
            if (!group) {
                return;
            }
            const error = message.error ? new Error(message.error) : undefined;
            group.endpoint = message.endpoint;
            group.error = error;
            for (const subscription of [...group.subscriptions]) {
                subscription.callback(message.endpoint, error);
            }
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
            if (!this._usesStore(storeName)) {
                return;
            }
            if (this._storeSubscriptions().length > maxSocketStoreSubscriptions) {
                this._socket.disconnect();
                throw new Error(`ReStream store subscription limit exceeded (${maxSocketStoreSubscriptions})`);
            }
            if (this._authenticated) {
                const message: StoreSubscriptionMessage = {
                    action: StoreSubscriptionAction.Subscribe,
                    storeName,
                    key: this._storeSubscriptionKey(storeName, key),
                };
                this._socket.emit(SocketEventNames.StoreSubscription, message);
            } else if (this._sessionAttach) {
                this._queueSessionAttachOperation(
                    SocketEventNames.StoreSubscription,
                    {
                        action: StoreSubscriptionAction.Subscribe,
                        storeName,
                        key: this._storeSubscriptionKey(storeName, key),
                    } satisfies StoreSubscriptionMessage,
                );
            }
        });

        TriggerStore.eventSubscriptionStopped.subscribe((storeName, key) => {
            if (!this._usesStore(storeName)) {
                return;
            }
            if (this._authenticated) {
                const message: StoreSubscriptionMessage = {
                    action: StoreSubscriptionAction.Unsubscribe,
                    storeName,
                    key: this._storeSubscriptionKey(storeName, key),
                };
                this._socket.emit(SocketEventNames.StoreSubscription, message);
            } else if (this._sessionAttach) {
                this._queueSessionAttachOperation(
                    SocketEventNames.StoreSubscription,
                    {
                        action: StoreSubscriptionAction.Unsubscribe,
                        storeName,
                        key: this._storeSubscriptionKey(storeName, key),
                    } satisfies StoreSubscriptionMessage,
                );
            }
        });        
    }

    markAuthenticated(): Promise<void> {
        if (this._viewerSessions) {
            if (this._sessionAttach) {
                return this._sessionAttach.promise;
            }
            this._sessionAttach = deferred<void>();
            this._sessionFallbackAttempted = false;
            this._emitSessionAttach();
            return this._sessionAttach.promise;
        }

        const storeSubscriptions = this._storeSubscriptions();
        if (storeSubscriptions.length > maxSocketStoreSubscriptions) {
            this._socket.disconnect();
            throw new Error(`ReStream store subscription limit exceeded (${maxSocketStoreSubscriptions})`);
        }
        this._authenticated = true;

        for (const storeSub of storeSubscriptions) {
            const message: StoreSubscriptionMessage = {
                action: StoreSubscriptionAction.Subscribe,
                storeName: storeSub.storeName,
                key: storeSub.key,
            };
            this._socket.emit(SocketEventNames.StoreSubscription, message);
        }

        for (const group of this._keyedEventSubscriptions.values()) {
            this._emitKeyedEventSubscription(group, StoreSubscriptionAction.Subscribe);
        }
        for (const group of this._dataStreamSubscriptions.values()) {
            group.endpoint = undefined;
            group.error = undefined;
            this._emitDataStreamSubscription(group, StoreSubscriptionAction.Subscribe);
        }
        return Promise.resolve();
    }

    private _usesStore(storeName: string): boolean {
        return !this._excludedStoreNames.has(storeName) &&
            (this._storeNames === undefined || this._storeNames.has(storeName));
    }

    private _storeSubscriptions(): { storeName: string; key: string | undefined }[] {
        return TriggerStore.getStoreSubs()
            .filter(subscription => this._usesStore(subscription.storeName));
    }

    private _storeSubscriptionKey(storeName: string, key: string | undefined): string | undefined {
        if (key === undefined) {
            return undefined;
        }
        return TriggerStore.subscriptionKeyForTransport(storeName, key);
    }

    /** Enables the optional session handshake before the first authentication. */
    enableViewerSessions(): void {
        this.setViewerSessionsEnabled(true);
    }

    /** Applies the capability negotiated for the next authenticated transport. */
    setViewerSessionsEnabled(enabled: boolean): void {
        if (this._authenticated || this._sessionAttach) {
            throw new Error("Viewer session capability must be set before authentication");
        }
        if (this._viewerSessions === enabled) {
            return;
        }
        this._viewerSessions = enabled;
        if (!enabled && this._sessionID) {
            this._sessionID = undefined;
            this._capabilities = { dataStreams: false };
            this._rejectPendingRPCs("Viewer sessions are unavailable on this server");
            for (const group of this._dataStreamSubscriptions.values()) {
                group.endpoint = undefined;
                group.error = undefined;
            }
        }
    }

    getSessionID(): string | undefined {
        return this._sessionID;
    }

    getCapabilities(): ViewerSessionCapabilities {
        return { ...this._capabilities };
    }

    closeSession(): void {
        const sessionID = this._sessionID;
        this._sessionID = undefined;
        this._authenticated = false;
        this._clearSessionAttach(new Error("Viewer session closed"));
        this._rejectPendingRPCs("Viewer session closed");
        if (!this._viewerSessions || !sessionID) {
            return;
        }
        const message: ViewerSessionCloseMessage = { sessionID };
        this._socket.emit(SocketEventNames.ViewerSessionClose, message);
    }

    sendRPC<RS extends RPCResponseStruct<RT>, RT>(rpcStruct: RPCStruct<RS, RT>): Promise<RT> {
        if (!this._authenticated) {
            return Promise.reject(new Error("Server is disconnected"));
        }
        if (this._rpcCallsPending.size >= maxSocketPendingRPCs) {
            this._socket.disconnect();
            return Promise.reject(
                new Error(`ReStream pending RPC limit exceeded (${maxSocketPendingRPCs})`),
            );
        }

        const w = new BinaryWriter();
        rpcStruct.serialize(w, undefined);

        const msg: RPCCallMessage = {
            callID: this._rpcCallID++,
            methodName: rpcStruct.rpcBoundName,
            request: w.getBytes().slice().buffer,
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

    sendFFRPC(ffrpcStruct: FFRPCStruct): void {
        if (!this._authenticated) {
            throw new Error("Server is disconnected");
        }

        const w = new BinaryWriter();
        ffrpcStruct.serialize(w, undefined);

        const msg: FFRPCCallMessage = {
            methodName: ffrpcStruct.ffrpcBoundName,
            request: w.getBytes().slice().buffer,
        };
        this._socket.emit(SocketEventNames.FFRPCCall, msg);
    }

    subscribeToEvent<ES extends EventStruct>(eventType: EventStructType<ES>, callback: (event: ES) => void): () => void {
        const eventName = eventType.eventBoundName;
        let subscriptions = this._eventSubscriptions.get(eventName);
        if (!subscriptions) {
            subscriptions = new Set();
            this._eventSubscriptions.set(eventName, subscriptions);
        }

        const subscription: EventSubscription = {
            eventType,
            callback: (event) => callback(event as ES),
        };
        subscriptions.add(subscription);

        return () => {
            subscriptions!.delete(subscription);
            if (subscriptions!.size === 0) {
                this._eventSubscriptions.delete(eventName);
            }
        };
    }

    subscribeToKeyedEvent<ES extends EventStruct>(
        storeName: string,
        eventType: EventStructType<ES>,
        key: string,
        callback: (event: ES) => void,
    ): () => void {
        if (!storeName) {
            throw new Error("Keyed event store name is required");
        }
        if (!key) {
            throw new Error("Keyed event key is required");
        }

        const eventName = eventType.eventBoundName;
        const identity = keyedEventSubscriptionIdentity(storeName, eventName, key);
        let group = this._keyedEventSubscriptions.get(identity);
        if (!group) {
            if (this._keyedEventSubscriptions.size >= maxSocketKeyedEventSubscriptions) {
                this._socket.disconnect();
                throw new Error(
                    `ReStream keyed event subscription limit exceeded (${maxSocketKeyedEventSubscriptions})`,
                );
            }
            group = {
                storeName,
                eventName,
                key,
                subscriptions: new Set(),
            };
            this._keyedEventSubscriptions.set(identity, group);
        }

        const subscription: EventSubscription = {
            eventType,
            callback: (event) => callback(event as ES),
        };
        const first = group.subscriptions.size === 0;
        group.subscriptions.add(subscription);
        if (first && this._authenticated) {
            this._emitKeyedEventSubscription(group, StoreSubscriptionAction.Subscribe);
        } else if (first && this._sessionAttach) {
            this._queueSessionAttachOperation(
                SocketEventNames.KeyedEventSubscription,
                this._keyedEventSubscriptionMessage(group, StoreSubscriptionAction.Subscribe),
            );
        }

        return () => {
            if (!group!.subscriptions.delete(subscription) || group!.subscriptions.size > 0) {
                return;
            }
            if (this._authenticated) {
                this._emitKeyedEventSubscription(group!, StoreSubscriptionAction.Unsubscribe);
            } else if (this._sessionAttach) {
                this._queueSessionAttachOperation(
                    SocketEventNames.KeyedEventSubscription,
                    this._keyedEventSubscriptionMessage(group!, StoreSubscriptionAction.Unsubscribe),
                );
            }
            this._keyedEventSubscriptions.delete(identity);
        };
    }

    subscribeToDataStream(
        storeName: string,
        streamName: string,
        key: string,
        callback: DataStreamEndpointCallback,
    ): () => void {
        if (!storeName) {
            throw new Error("Data stream store name is required");
        }
        if (!streamName) {
            throw new Error("Data stream name is required");
        }
        if (!key) {
            throw new Error("Data stream key is required");
        }

        const identity = dataStreamSubscriptionIdentity(storeName, streamName, key);
        let group = [...this._dataStreamSubscriptions.values()].find(
            (candidate) => dataStreamSubscriptionIdentity(
                candidate.storeName,
                candidate.streamName,
                candidate.key,
            ) === identity,
        );
        if (!group) {
            if (this._dataStreamSubscriptions.size >= maxSocketDataStreamSubscriptions) {
                this._socket.disconnect();
                throw new Error(
                    `ReStream data stream subscription limit exceeded (${maxSocketDataStreamSubscriptions})`,
                );
            }
            group = {
                subscriptionID: `stream-${this._dataStreamSubscriptionID++}`,
                storeName,
                streamName,
                key,
                subscriptions: new Set(),
            };
            this._dataStreamSubscriptions.set(group.subscriptionID, group);
        }

        const subscription: DataStreamSubscription = { callback };
        const first = group.subscriptions.size === 0;
        group.subscriptions.add(subscription);
        if (!first && (group.endpoint || group.error)) {
            callback(group.endpoint, group.error);
        }
        if (first && this._authenticated) {
            this._emitDataStreamSubscription(group, StoreSubscriptionAction.Subscribe);
        } else if (first && this._sessionAttach) {
            this._queueSessionAttachOperation(
                SocketEventNames.DataStreamSubscription,
                this._dataStreamSubscriptionMessage(group, StoreSubscriptionAction.Subscribe),
            );
        }

        return () => {
            if (!group!.subscriptions.delete(subscription) || group!.subscriptions.size > 0) {
                return;
            }
            if (this._authenticated) {
                this._emitDataStreamSubscription(group!, StoreSubscriptionAction.Unsubscribe);
            } else if (this._sessionAttach) {
                this._queueSessionAttachOperation(
                    SocketEventNames.DataStreamSubscription,
                    this._dataStreamSubscriptionMessage(group!, StoreSubscriptionAction.Unsubscribe),
                );
            }
            this._dataStreamSubscriptions.delete(group!.subscriptionID);
        };
    }

    private _emitSessionAttach(): void {
        if (!this._sessionAttach) {
            return;
        }
        // The authoritative manifest below includes every change made before
        // this call. Only deltas occurring while this attachment is in flight
        // need replaying after the acknowledgement.
        this._sessionAttachOperations = [];
        const request: ViewerSessionAttachRequest = {
            sessionID: this._sessionID ?? '',
            storeSubscriptions: this._storeSubscriptions().map(subscription => ({
                storeName: subscription.storeName,
                key: subscription.key ?? '',
            })),
            keyedEventSubscriptions: [...this._keyedEventSubscriptions.values()].map(group => ({
                storeName: group.storeName,
                eventName: group.eventName,
                action: StoreSubscriptionAction.Subscribe,
                key: group.key,
            })),
            dataStreamSubscriptions: [...this._dataStreamSubscriptions.values()].map(group => ({
                subscriptionID: group.subscriptionID,
                storeName: group.storeName,
                streamName: group.streamName,
                key: group.key,
            })),
        };
        this._clearSessionAttachTimer();
        this._sessionAttachTimer = setTimeout(() => {
            this._clearSessionAttach(new Error("Viewer session attachment timed out"));
        }, this._sessionAttachTimeoutMs);
        this._socket.emit(SocketEventNames.ViewerSessionAttach, request);
    }

    private _clearSessionAttach(error: Error): void {
        this._clearSessionAttachTimer();
        this._sessionAttachOperations = [];
        const attaching = this._sessionAttach;
        this._sessionAttach = undefined;
        attaching?.reject(error);
    }

    private _clearSessionAttachTimer(): void {
        if (this._sessionAttachTimer !== undefined) {
            clearTimeout(this._sessionAttachTimer);
            this._sessionAttachTimer = undefined;
        }
    }

    private _rejectPendingRPCs(message: string): void {
        for (const waiting of this._rpcCallsPending.values()) {
            waiting.def.reject({ message, data: {} });
        }
        this._rpcCallsPending.clear();
    }

    private _emitKeyedEventSubscription(
        group: KeyedEventSubscriptionGroup,
        action: StoreSubscriptionAction,
    ): void {
        this._socket.emit(
            SocketEventNames.KeyedEventSubscription,
            this._keyedEventSubscriptionMessage(group, action),
        );
    }

    private _keyedEventSubscriptionMessage(
        group: KeyedEventSubscriptionGroup,
        action: StoreSubscriptionAction,
    ): KeyedEventSubscriptionMessage {
        return {
            storeName: group.storeName,
            eventName: group.eventName,
            action,
            key: group.key,
        };
    }

    private _emitDataStreamSubscription(
        group: DataStreamSubscriptionGroup,
        action: StoreSubscriptionAction,
    ): void {
        this._socket.emit(
            SocketEventNames.DataStreamSubscription,
            this._dataStreamSubscriptionMessage(group, action),
        );
    }

    private _dataStreamSubscriptionMessage(
        group: DataStreamSubscriptionGroup,
        action: StoreSubscriptionAction,
    ): DataStreamSubscriptionMessage {
        return {
            subscriptionID: group.subscriptionID,
            storeName: group.storeName,
            streamName: group.streamName,
            action,
            key: group.key,
        };
    }

    private _queueSessionAttachOperation(name: SocketEventNames, message: unknown): void {
        if (!this._sessionAttach) {
            return;
        }
        if (this._sessionAttachOperations.length >= maxSocketSessionAttachOperations) {
            this._clearSessionAttach(new Error(
                `ReStream session attach operation limit exceeded (${maxSocketSessionAttachOperations})`,
            ));
            this._socket.disconnect();
            return;
        }
        this._sessionAttachOperations.push({ name, message });
    }
}

function warnSubscriptionRejected(rejection: SubscriptionRejectionMessage): void {
    const detail = [
        rejection.storeName,
        rejection.eventName ?? rejection.streamName,
        rejection.key,
    ].filter(Boolean).join('/');
    console.warn(
        `ReStream rejected unauthorized ${rejection.subscriptionType} subscription ${detail}: ${rejection.error}`,
    );
}

function keyedEventSubscriptionIdentity(storeName: string, eventName: string, key: string): string {
    return JSON.stringify([storeName, eventName, key]);
}

function dataStreamSubscriptionIdentity(storeName: string, streamName: string, key: string): string {
    return JSON.stringify([storeName, streamName, key]);
}
