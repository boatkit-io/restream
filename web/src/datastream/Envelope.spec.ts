import { describe, expect, test } from 'vitest';

import {
    DataStreamFlags,
    DataStreamPayloadType,
    decodeDataStreamEnvelope,
} from './Envelope.js';

describe('data stream envelope decoding', () => {
    test('decodes a whole recovery frame', () => {
        const encoded = encodeTestEnvelope({
            streamID: 'CameraMedia/Video/camera-a',
            format: 'video/h264',
            payloadType: DataStreamPayloadType.Frame,
            flags: DataStreamFlags.Recovery,
            payload: new Uint8Array([1, 2, 3]),
        });
        const decoded = decodeDataStreamEnvelope(encoded);
        expect(decoded).toEqual(expect.objectContaining({
            streamID: 'CameraMedia/Video/camera-a',
            format: 'video/h264',
            generation: 1n,
            sequence: 2n,
            frameID: 3n,
            payloadType: DataStreamPayloadType.Frame,
            flags: DataStreamFlags.Recovery,
        }));
        expect([...decoded.payload]).toEqual([1, 2, 3]);
    });

    test('rejects a partial BlockSet commit', () => {
        const encoded = encodeTestEnvelope({
            streamID: 'Radar/Spokes/radar-a',
            format: 'radar-v1',
            payloadType: DataStreamPayloadType.BlockSet,
            flags: DataStreamFlags.Commit,
            firstIndex: 1,
            itemCount: 1,
            totalItemCount: 2048,
            payload: new Uint8Array([1]),
        });
        expect(() => decodeDataStreamEnvelope(encoded)).toThrow(
            'BlockSet commit must not contain a range or payload',
        );
    });

    test('rejects oversized identity strings before decoding payloads', () => {
        const encoded = encodeTestEnvelope({
            streamID: 's'.repeat(4097),
            format: 'video/h264',
            payloadType: DataStreamPayloadType.Frame,
            flags: DataStreamFlags.Recovery,
            payload: new Uint8Array([1]),
        });
        expect(() => decodeDataStreamEnvelope(encoded)).toThrow(
            'Data stream string length exceeds limit',
        );
    });
});

interface TestEnvelope {
    streamID: string;
    format: string;
    payloadType: DataStreamPayloadType;
    flags: DataStreamFlags;
    firstIndex?: number;
    itemCount?: number;
    totalItemCount?: number;
    payload: Uint8Array;
}

function encodeTestEnvelope(envelope: TestEnvelope): ArrayBuffer {
    const encoder = new TextEncoder();
    const streamID = encoder.encode(envelope.streamID);
    const format = encoder.encode(envelope.format);
    const result = new Uint8Array(60 + streamID.byteLength + format.byteLength + envelope.payload.byteLength);
    result.set(encoder.encode('RSD1'), 0);
    const view = new DataView(result.buffer);
    view.setUint8(4, envelope.payloadType);
    view.setUint8(5, envelope.flags);
    view.setBigUint64(8, 1n, true);
    view.setBigUint64(16, 2n, true);
    view.setBigInt64(24, 42n, true);
    view.setBigUint64(32, 3n, true);
    view.setUint32(40, envelope.firstIndex ?? 0, true);
    view.setUint32(44, envelope.itemCount ?? 0, true);
    view.setUint32(48, envelope.totalItemCount ?? 0, true);
    view.setUint16(52, streamID.byteLength, true);
    view.setUint16(54, format.byteLength, true);
    view.setUint32(56, envelope.payload.byteLength, true);
    let offset = 60;
    result.set(streamID, offset);
    offset += streamID.byteLength;
    result.set(format, offset);
    offset += format.byteLength;
    result.set(envelope.payload, offset);
    return result.buffer;
}
