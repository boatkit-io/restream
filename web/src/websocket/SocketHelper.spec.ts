import type { Socket } from 'socket.io-client';
import { describe, expect, test, vi } from 'vitest';

import TriggerStore from '../stores/TriggerStore.js';
import BinaryReader from '../utils/BinaryReader.js';
import BinaryWriter from '../utils/BinaryWriter.js';
import type { VarInfoStruct } from '../utils/SerializationTypes.js';
import {
    DataStreamEndpoint,
    DataStreamEndpointMessage,
    DataStreamSubscriptionMessage,
    EventStruct,
    FFRPCCallMessage,
    FFRPCStruct,
    KeyedEventMessage,
    KeyedEventSubscriptionMessage,
    ReStreamSocket,
    StoreSubscriptionAction,
    StoreUpdateMessageKind,
    SubscriptionRejectionMessage,
    ViewerSessionAttachRequest,
    ViewerSessionAttachResponse,
} from './SocketHelper.js';

class TestFFRPC extends FFRPCStruct {
    public constructor(public readonly value: number) {
        super('Radio.TransmitAudio');
    }

    public serialize(writer: BinaryWriter, _varInfo: VarInfoStruct | undefined): void {
        writer.writeUint8(this.value);
    }
}

class TestKeyedEvent extends EventStruct {
    public static readonly eventBoundName = 'Radio.Audio';

    public constructor(public readonly value: number) {
        super(TestKeyedEvent.eventBoundName);
    }

    public static deserialized(_reader: BinaryReader): TestKeyedEvent {
        return new TestKeyedEvent(42);
    }
}

interface EmittedMessage {
    name: string;
    message: unknown;
}

class FakeSocket {
    public readonly emitted: EmittedMessage[] = [];
    private readonly handlers = new Map<string, Array<(message?: never) => void>>();

    public on(name: string, callback: (message?: never) => void): this {
        const callbacks = this.handlers.get(name) ?? [];
        callbacks.push(callback);
        this.handlers.set(name, callbacks);
        return this;
    }

    public emit(name: string, message: unknown): this {
        this.emitted.push({ name, message });
        return this;
    }

    public fire(name: string, message?: unknown): void {
        for (const callback of this.handlers.get(name) ?? []) {
            callback(message as never);
        }
    }
}

function keyedSubscriptionMessages(socket: FakeSocket): KeyedEventSubscriptionMessage[] {
    return socket.emitted
        .filter(({ name }) => name === 'keyedeventsub')
        .map(({ message }) => message as KeyedEventSubscriptionMessage);
}

function dataStreamSubscriptionMessages(socket: FakeSocket): DataStreamSubscriptionMessage[] {
    return socket.emitted
        .filter(({ name }) => name === 'datastreamsub')
        .map(({ message }) => message as DataStreamSubscriptionMessage);
}

function viewerSessionAttachMessages(socket: FakeSocket): ViewerSessionAttachRequest[] {
    return socket.emitted
        .filter(({ name }) => name === 'viewersessionattach')
        .map(({ message }) => message as ViewerSessionAttachRequest);
}

function storeSubscriptionMessages(socket: FakeSocket) {
    return socket.emitted
        .filter(({ name }) => name === 'storesub')
        .map(({ message }) => message);
}

describe('ReStreamSocket store scoping', () => {
    test('partitions global store subscriptions between isolated sockets', async () => {
        const subscriptions = vi.spyOn(TriggerStore, 'getStoreSubs').mockReturnValue([
            { storeName: 'VesselInfo', key: undefined },
            { storeName: 'FollowedBoats', key: 'boats%&friend-a' },
        ]);
        const vesselSocket = new FakeSocket();
        const accountSocket = new FakeSocket();
        const vesselRestream = new ReStreamSocket(vesselSocket as unknown as Socket, {
            excludedStoreNames: ['FollowedBoats'],
        });
        const accountRestream = new ReStreamSocket(accountSocket as unknown as Socket, {
            storeNames: ['FollowedBoats'],
        });

        await vesselRestream.markAuthenticated();
        await accountRestream.markAuthenticated();

        expect(storeSubscriptionMessages(vesselSocket)).toEqual([{
            action: StoreSubscriptionAction.Subscribe,
            storeName: 'VesselInfo',
            key: undefined,
        }]);
        expect(storeSubscriptionMessages(accountSocket)).toEqual([{
            action: StoreSubscriptionAction.Subscribe,
            storeName: 'FollowedBoats',
            key: 'boats%&friend-a',
        }]);
        subscriptions.mockRestore();
    });

    test('routes store updates only through the socket that owns the store', () => {
        const handleUpdate = vi.spyOn(TriggerStore, 'handleUpdateMessage').mockImplementation(() => undefined);
        const vesselSocket = new FakeSocket();
        const accountSocket = new FakeSocket();
        new ReStreamSocket(vesselSocket as unknown as Socket, {
            excludedStoreNames: ['FollowedBoats'],
        });
        new ReStreamSocket(accountSocket as unknown as Socket, {
            storeNames: ['FollowedBoats'],
        });
        const followedUpdate = {
            time: Date.now(),
            kind: StoreUpdateMessageKind.Full,
            storeName: 'FollowedBoats',
            state: new ArrayBuffer(0),
        } as const;
        const vesselUpdate = {
            time: Date.now(),
            kind: StoreUpdateMessageKind.Full,
            storeName: 'VesselInfo',
            state: new ArrayBuffer(0),
        } as const;

        vesselSocket.fire('storeupdate', followedUpdate);
        accountSocket.fire('storeupdate', vesselUpdate);
        expect(handleUpdate).not.toHaveBeenCalled();

        vesselSocket.fire('storeupdate', vesselUpdate);
        accountSocket.fire('storeupdate', followedUpdate);
        expect(handleUpdate).toHaveBeenCalledTimes(2);
        expect(handleUpdate).toHaveBeenNthCalledWith(1, vesselUpdate);
        expect(handleUpdate).toHaveBeenNthCalledWith(2, followedUpdate);
        handleUpdate.mockRestore();
    });
});

describe('ReStreamSocket field-ID subscription keys', () => {
    test('always sends stable field-ID subscription keys', async () => {
        const subscriptions = vi.spyOn(TriggerStore, 'getStoreSubs').mockReturnValue([{
            storeName: 'VesselInfo',
            key: '~1%&3%&7',
        }]);
        const fieldIDSocket = new FakeSocket();
        const fieldIDRestream = new ReStreamSocket(fieldIDSocket as unknown as Socket);

        await fieldIDRestream.markAuthenticated();

        expect(storeSubscriptionMessages(fieldIDSocket)).toEqual([{
            action: StoreSubscriptionAction.Subscribe,
            storeName: 'VesselInfo',
            key: '~1%&3%&7',
        }]);
        subscriptions.mockRestore();
    });
});

describe('ReStreamSocket viewer sessions', () => {
    test('can be enabled after server capability negotiation but before authentication', () => {
        const socket = new FakeSocket();
        const restreamSocket = new ReStreamSocket(socket as unknown as Socket);

        restreamSocket.enableViewerSessions();
        void restreamSocket.markAuthenticated();

        expect(viewerSessionAttachMessages(socket)).toHaveLength(1);
    });

    test('falls back to legacy authentication when the negotiated server lacks sessions', async () => {
        const socket = new FakeSocket();
        const restreamSocket = new ReStreamSocket(socket as unknown as Socket, {
            viewerSessions: true,
        });

        restreamSocket.setViewerSessionsEnabled(false);
        await restreamSocket.markAuthenticated();

        expect(viewerSessionAttachMessages(socket)).toHaveLength(0);
    });

    test('keeps the session in memory and resumes without replaying individual subscriptions', async () => {
        const socket = new FakeSocket();
        const restreamSocket = new ReStreamSocket(socket as unknown as Socket, {
            viewerSessions: true,
        });
        const streamErrors: string[] = [];
        restreamSocket.subscribeToKeyedEvent(
            'IcomRadioStore',
            TestKeyedEvent,
            'radio-a',
            () => undefined,
        );
        restreamSocket.subscribeToDataStream(
            'CameraStore',
            'Camera.Video',
            'camera-a',
            (_endpoint, error) => {
                if (error) {
                    streamErrors.push(error.message);
                }
            },
        );

        const attached = restreamSocket.markAuthenticated();
        const firstAttach = viewerSessionAttachMessages(socket);
        expect(firstAttach).toHaveLength(1);
        expect(firstAttach[0]).toEqual(expect.objectContaining({
            sessionID: '',
            keyedEventSubscriptions: [
                expect.objectContaining({ key: 'radio-a' }),
            ],
            dataStreamSubscriptions: [
                expect.objectContaining({ key: 'camera-a' }),
            ],
        }));
        socket.fire('viewersessionattached', {
            sessionID: '70f183b6-932a-46c2-a38a-6bcd496d8fe8',
            resumed: false,
            capabilities: { dataStreams: true },
        } satisfies ViewerSessionAttachResponse);
        await attached;
        expect(restreamSocket.getSessionID()).toBe('70f183b6-932a-46c2-a38a-6bcd496d8fe8');
        expect(restreamSocket.getCapabilities()).toEqual({ dataStreams: true });
        expect(keyedSubscriptionMessages(socket)).toEqual([]);
        expect(dataStreamSubscriptionMessages(socket)).toEqual([]);

        socket.emitted.length = 0;
        socket.fire('disconnect');
        expect(streamErrors).toEqual([]);

        const resumed = restreamSocket.markAuthenticated();
        expect(viewerSessionAttachMessages(socket)).toEqual([
            expect.objectContaining({
                sessionID: '70f183b6-932a-46c2-a38a-6bcd496d8fe8',
            }),
        ]);
        socket.fire('viewersessionattached', {
            sessionID: '70f183b6-932a-46c2-a38a-6bcd496d8fe8',
            resumed: true,
            capabilities: { dataStreams: true },
        } satisfies ViewerSessionAttachResponse);
        await resumed;
        expect(keyedSubscriptionMessages(socket)).toEqual([]);
        expect(dataStreamSubscriptionMessages(socket)).toEqual([]);
    });

    test('explicit close forgets the in-memory session', async () => {
        const socket = new FakeSocket();
        const restreamSocket = new ReStreamSocket(socket as unknown as Socket, {
            viewerSessions: true,
        });
        const attached = restreamSocket.markAuthenticated();
        socket.fire('viewersessionattached', {
            sessionID: '46a70c24-c622-4d10-a25d-66e427b36620',
            resumed: false,
            capabilities: { dataStreams: false },
        } satisfies ViewerSessionAttachResponse);
        await attached;

        restreamSocket.closeSession();
        expect(restreamSocket.getSessionID()).toBeUndefined();
        expect(socket.emitted).toContainEqual({
            name: 'viewersessionclose',
            message: { sessionID: '46a70c24-c622-4d10-a25d-66e427b36620' },
        });
    });

    test('replays subscriptions added while the session attachment is in flight', async () => {
        const socket = new FakeSocket();
        const restreamSocket = new ReStreamSocket(socket as unknown as Socket, {
            viewerSessions: true,
        });

        const attached = restreamSocket.markAuthenticated();
        restreamSocket.subscribeToKeyedEvent(
            'IcomRadioStore',
            TestKeyedEvent,
            'radio-a',
            () => undefined,
        );
        restreamSocket.subscribeToDataStream(
            'CameraStore',
            'Camera.Video',
            'camera-a',
            () => undefined,
        );
        expect(keyedSubscriptionMessages(socket)).toEqual([]);
        expect(dataStreamSubscriptionMessages(socket)).toEqual([]);

        socket.fire('viewersessionattached', {
            sessionID: '0806513f-8b37-43d4-9397-9b4cfb518e70',
            resumed: false,
            capabilities: { dataStreams: true },
        } satisfies ViewerSessionAttachResponse);
        await attached;

        expect(keyedSubscriptionMessages(socket)).toEqual([{
            storeName: 'IcomRadioStore',
            eventName: TestKeyedEvent.eventBoundName,
            action: StoreSubscriptionAction.Subscribe,
            key: 'radio-a',
        }]);
        expect(dataStreamSubscriptionMessages(socket)).toEqual([{
            subscriptionID: 'stream-1',
            storeName: 'CameraStore',
            streamName: 'Camera.Video',
            action: StoreSubscriptionAction.Subscribe,
            key: 'camera-a',
        }]);
    });
});

describe('ReStreamSocket subscription rejections', () => {
    test('warns about denied session subscriptions without rejecting attachment', async () => {
        const socket = new FakeSocket();
        const restreamSocket = new ReStreamSocket(socket as unknown as Socket, {
            viewerSessions: true,
        });
        const warning = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
        const rejectedSubscription: SubscriptionRejectionMessage = {
            subscriptionType: 'store',
            storeName: 'CloudVesselSettings',
            key: 'viewerSlug',
            error: 'store CloudVesselSettings requires access level 2, caller has 1',
        };

        try {
            const attached = restreamSocket.markAuthenticated();
            socket.fire('viewersessionattached', {
                sessionID: 'a0b304c9-d5d0-4ce4-89a6-baf510956968',
                resumed: false,
                capabilities: { dataStreams: false },
                rejectedSubscriptions: [rejectedSubscription],
            } satisfies ViewerSessionAttachResponse);

            await expect(attached).resolves.toBeUndefined();
            expect(restreamSocket.getSessionID()).toBe('a0b304c9-d5d0-4ce4-89a6-baf510956968');
            expect(warning).toHaveBeenCalledWith(expect.stringContaining(
                'ReStream rejected unauthorized store subscription CloudVesselSettings/viewerSlug',
            ));
        } finally {
            warning.mockRestore();
        }
    });

    test('warns about a denied subscription added after authentication', async () => {
        const socket = new FakeSocket();
        const rejections: SubscriptionRejectionMessage[] = [];
        const restreamSocket = new ReStreamSocket(socket as unknown as Socket, {
            onSubscriptionRejected: rejection => rejections.push(rejection),
        });
        await restreamSocket.markAuthenticated();

        socket.fire('subscriptionrejected', {
            subscriptionType: 'keyedEvent',
            storeName: 'Log',
            eventName: 'Log.Line',
            key: 'recent',
            error: 'store Log requires access level 2, caller has 1',
        } satisfies SubscriptionRejectionMessage);

        expect(rejections).toEqual([expect.objectContaining({
            subscriptionType: 'keyedEvent',
            storeName: 'Log',
        })]);
    });
});

describe('ReStreamSocket keyed event subscriptions', () => {
    test('refcounts exact subscriptions and routes only matching events', () => {
        const socket = new FakeSocket();
        const restreamSocket = new ReStreamSocket(socket as unknown as Socket);
        const received: number[] = [];

        const unsubscribeA = restreamSocket.subscribeToKeyedEvent(
            'IcomRadioStore',
            TestKeyedEvent,
            'radio-a',
            event => received.push(event.value),
        );
        const unsubscribeB = restreamSocket.subscribeToKeyedEvent(
            'IcomRadioStore',
            TestKeyedEvent,
            'radio-a',
            event => received.push(event.value + 1),
        );

        expect(keyedSubscriptionMessages(socket)).toEqual([]);
        restreamSocket.markAuthenticated();
        expect(keyedSubscriptionMessages(socket)).toEqual([{
            storeName: 'IcomRadioStore',
            eventName: TestKeyedEvent.eventBoundName,
            action: StoreSubscriptionAction.Subscribe,
            key: 'radio-a',
        }]);

        socket.fire('keyedevent', {
            time: Date.now(),
            storeName: 'IcomRadioStore',
            eventName: TestKeyedEvent.eventBoundName,
            key: 'radio-b',
            event: new Uint8Array().buffer,
        } satisfies KeyedEventMessage);
        expect(received).toEqual([]);

        socket.fire('keyedevent', {
            time: Date.now(),
            storeName: 'IcomRadioStore',
            eventName: TestKeyedEvent.eventBoundName,
            key: 'radio-a',
            event: new Uint8Array().buffer,
        } satisfies KeyedEventMessage);
        expect(received).toEqual([42, 43]);

        unsubscribeA();
        expect(keyedSubscriptionMessages(socket)).toHaveLength(1);
        unsubscribeB();
        expect(keyedSubscriptionMessages(socket)).toEqual([
            expect.objectContaining({ action: StoreSubscriptionAction.Subscribe }),
            expect.objectContaining({ action: StoreSubscriptionAction.Unsubscribe }),
        ]);
    });

    test('replays active keyed subscriptions after authentication', () => {
        const socket = new FakeSocket();
        const restreamSocket = new ReStreamSocket(socket as unknown as Socket);

        restreamSocket.subscribeToKeyedEvent('IcomRadioStore', TestKeyedEvent, 'radio-a', () => undefined);
        restreamSocket.markAuthenticated();
        socket.emitted.length = 0;

        socket.fire('disconnect');
        restreamSocket.subscribeToKeyedEvent('IcomRadioStore', TestKeyedEvent, 'radio-b', () => undefined);
        expect(keyedSubscriptionMessages(socket)).toEqual([]);

        restreamSocket.markAuthenticated();
        expect(keyedSubscriptionMessages(socket)).toEqual([
            expect.objectContaining({ key: 'radio-a', action: StoreSubscriptionAction.Subscribe }),
            expect.objectContaining({ key: 'radio-b', action: StoreSubscriptionAction.Subscribe }),
        ]);
    });
});

describe('ReStreamSocket FFRPC calls', () => {
    test('sends a request without allocating response state', () => {
        const socket = new FakeSocket();
        const restreamSocket = new ReStreamSocket(socket as unknown as Socket);
        restreamSocket.markAuthenticated();

        expect(restreamSocket.sendFFRPC(new TestFFRPC(42))).toBeUndefined();
        expect(socket.emitted).toHaveLength(1);
        expect(socket.emitted[0]?.name).toBe('ffrpc');

        const message = socket.emitted[0]?.message as FFRPCCallMessage;
        expect(message.methodName).toBe('Radio.TransmitAudio');
        expect([...new Uint8Array(message.request)]).toEqual([42]);
    });

    test('throws synchronously when disconnected', () => {
        const socket = new FakeSocket();
        const restreamSocket = new ReStreamSocket(socket as unknown as Socket);

        expect(() => restreamSocket.sendFFRPC(new TestFFRPC(42))).toThrow('Server is disconnected');
        expect(socket.emitted).toEqual([]);
    });
});

describe('ReStreamSocket data stream subscriptions', () => {
    test('refcounts one endpoint lease and routes endpoint responses', () => {
        const socket = new FakeSocket();
        const restreamSocket = new ReStreamSocket(socket as unknown as Socket);
        const receivedA: Array<DataStreamEndpoint | undefined> = [];
        const receivedB: Array<DataStreamEndpoint | undefined> = [];
        const unsubscribeA = restreamSocket.subscribeToDataStream(
            'CameraMedia',
            'CameraMedia.Video',
            'camera-a',
            endpoint => receivedA.push(endpoint),
        );
        const unsubscribeB = restreamSocket.subscribeToDataStream(
            'CameraMedia',
            'CameraMedia.Video',
            'camera-a',
            endpoint => receivedB.push(endpoint),
        );

        restreamSocket.markAuthenticated();
        const subscriptions = dataStreamSubscriptionMessages(socket);
        expect(subscriptions).toHaveLength(1);
        expect(subscriptions[0]).toEqual(expect.objectContaining({
            storeName: 'CameraMedia',
            streamName: 'CameraMedia.Video',
            key: 'camera-a',
            action: StoreSubscriptionAction.Subscribe,
        }));

        const endpoint: DataStreamEndpoint = {
            leaseID: 'lease-a',
            url: 'wss://stream.example/viewer',
            token: 'token-a',
            expiresAtUnixMilli: 0,
        };
        socket.fire('datastreamendpoint', {
            subscriptionID: subscriptions[0]!.subscriptionID,
            endpoint,
        } satisfies DataStreamEndpointMessage);
        expect(receivedA).toEqual([endpoint]);
        expect(receivedB).toEqual([endpoint]);
        const receivedLate: Array<DataStreamEndpoint | undefined> = [];
        const unsubscribeLate = restreamSocket.subscribeToDataStream(
            'CameraMedia',
            'CameraMedia.Video',
            'camera-a',
            lateEndpoint => receivedLate.push(lateEndpoint),
        );
        expect(receivedLate).toEqual([endpoint]);

        unsubscribeA();
        expect(dataStreamSubscriptionMessages(socket)).toHaveLength(1);
        unsubscribeLate();
        expect(dataStreamSubscriptionMessages(socket)).toHaveLength(1);
        unsubscribeB();
        expect(dataStreamSubscriptionMessages(socket)).toEqual([
            expect.objectContaining({ action: StoreSubscriptionAction.Subscribe }),
            expect.objectContaining({ action: StoreSubscriptionAction.Unsubscribe }),
        ]);
    });

    test('replays active subscriptions after reconnect and reports disconnect', () => {
        const socket = new FakeSocket();
        const restreamSocket = new ReStreamSocket(socket as unknown as Socket);
        const errors: string[] = [];
        restreamSocket.subscribeToDataStream(
            'CameraMedia',
            'CameraMedia.Video',
            'camera-a',
            (_endpoint, error) => {
                if (error) {
                    errors.push(error.message);
                }
            },
        );
        restreamSocket.markAuthenticated();
        socket.emitted.length = 0;

        socket.fire('disconnect');
        expect(errors).toEqual(['Server is disconnected']);
        expect(dataStreamSubscriptionMessages(socket)).toEqual([]);

        restreamSocket.markAuthenticated();
        expect(dataStreamSubscriptionMessages(socket)).toEqual([
            expect.objectContaining({
                key: 'camera-a',
                action: StoreSubscriptionAction.Subscribe,
            }),
        ]);
    });
});
