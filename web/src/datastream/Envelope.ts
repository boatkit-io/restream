const wireHeaderBytes = 60;
const defaultMaxPayloadBytes = 32 * 1024 * 1024;
const maxStreamIDBytes = 4 * 1024;
const maxFormatBytes = 4 * 1024;
const textDecoder = new TextDecoder();

export enum DataStreamPayloadType {
    Frame = 1,
    BlockSet = 2,
}

export enum DataStreamFlags {
    Recovery = 1 << 0,
    Discontinuity = 1 << 1,
    Commit = 1 << 2,
}

const allFlags = DataStreamFlags.Recovery | DataStreamFlags.Discontinuity | DataStreamFlags.Commit;

export interface DataStreamEnvelope {
    streamID: string;
    generation: bigint;
    sequence: bigint;
    timestampUnixNano: bigint;
    payloadType: DataStreamPayloadType;
    frameID: bigint;
    flags: DataStreamFlags;
    format: string;
    firstIndex: number;
    itemCount: number;
    totalItemCount: number;
    payload: Uint8Array;
}

export function decodeDataStreamEnvelope(
    input: ArrayBufferLike,
    maxPayloadBytes = defaultMaxPayloadBytes,
): DataStreamEnvelope {
    const bytes = new Uint8Array(input);
    if (bytes.byteLength < wireHeaderBytes) {
        throw new Error("Data stream envelope is truncated");
    }
    if (String.fromCharCode(...bytes.subarray(0, 4)) !== 'RSD1') {
        throw new Error("Unsupported data stream wire format");
    }
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const payloadType = view.getUint8(4) as DataStreamPayloadType;
    const flags = view.getUint8(5) as DataStreamFlags;
    if (view.getUint16(6, true) !== 0) {
        throw new Error("Data stream reserved header bits are non-zero");
    }
    if ((flags & ~allFlags) !== 0) {
        throw new Error("Data stream envelope has unknown flags");
    }

    const streamIDLength = view.getUint16(52, true);
    const formatLength = view.getUint16(54, true);
    const payloadLength = view.getUint32(56, true);
    if (streamIDLength > maxStreamIDBytes || formatLength > maxFormatBytes) {
        throw new Error("Data stream string length exceeds limit");
    }
    if (payloadLength > maxPayloadBytes) {
        throw new Error("Data stream payload length exceeds limit");
    }
    const expectedLength = wireHeaderBytes + streamIDLength + formatLength + payloadLength;
    if (bytes.byteLength !== expectedLength) {
        throw new Error(`Data stream envelope length mismatch: have ${bytes.byteLength}, expected ${expectedLength}`);
    }

    let offset = wireHeaderBytes;
    const streamID = textDecoder.decode(bytes.subarray(offset, offset + streamIDLength));
    offset += streamIDLength;
    const format = textDecoder.decode(bytes.subarray(offset, offset + formatLength));
    offset += formatLength;
    const payload = bytes.slice(offset, offset + payloadLength);
    if (!streamID || !format) {
        throw new Error("Data stream identity and format are required");
    }

    const envelope: DataStreamEnvelope = {
        streamID,
        generation: view.getBigUint64(8, true),
        sequence: view.getBigUint64(16, true),
        timestampUnixNano: view.getBigInt64(24, true),
        payloadType,
        frameID: view.getBigUint64(32, true),
        flags,
        format,
        firstIndex: view.getUint32(40, true),
        itemCount: view.getUint32(44, true),
        totalItemCount: view.getUint32(48, true),
        payload,
    };
    validateDataStreamEnvelope(envelope);
    return envelope;
}

function validateDataStreamEnvelope(envelope: DataStreamEnvelope): void {
    if (envelope.generation === 0n || envelope.sequence === 0n || envelope.frameID === 0n) {
        throw new Error("Data stream generation, sequence, and frame ID must be non-zero");
    }
    switch (envelope.payloadType) {
        case DataStreamPayloadType.Frame:
            if ((envelope.flags & DataStreamFlags.Commit) !== 0) {
                throw new Error("Frame payloads are implicitly committed");
            }
            if (envelope.firstIndex !== 0 || envelope.itemCount !== 0 || envelope.totalItemCount !== 0) {
                throw new Error("Frame payload has BlockSet indexing");
            }
            if (envelope.payload.byteLength === 0) {
                throw new Error("Frame payload is empty");
            }
            break;
        case DataStreamPayloadType.BlockSet:
            validateBlockSet(envelope);
            break;
        default:
            throw new Error(`Unknown data stream payload type ${envelope.payloadType}`);
    }
}

function validateBlockSet(envelope: DataStreamEnvelope): void {
    if ((envelope.flags & DataStreamFlags.Recovery) !== 0) {
        throw new Error("BlockSet payload cannot be a recovery frame");
    }
    if (envelope.totalItemCount === 0) {
        throw new Error("BlockSet total item count must be non-zero");
    }
    if ((envelope.flags & DataStreamFlags.Commit) !== 0) {
        if (envelope.firstIndex !== 0 || envelope.itemCount !== 0 || envelope.payload.byteLength !== 0) {
            throw new Error("BlockSet commit must not contain a range or payload");
        }
        return;
    }
    if (envelope.itemCount === 0 ||
        envelope.firstIndex + envelope.itemCount > envelope.totalItemCount ||
        envelope.payload.byteLength === 0) {
        throw new Error("Invalid BlockSet data range");
    }
}
