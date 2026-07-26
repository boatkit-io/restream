import type { Socket } from 'socket.io-client';
import { describe, expect, test } from 'vitest';

import BinaryReader from '../utils/BinaryReader.js';
import {
    EventStruct,
    KeyedEventMessage,
    KeyedEventSubscriptionMessage,
    ReStreamSocket,
    StoreSubscriptionAction,
} from './SocketHelper.js';

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
