import BinaryWriter from './BinaryWriter.js';
import { FieldInfo, Serializable, VarInfo, VarInfoArray, VarInfoDynamic, VarInfoMap, VarInfoPointer, VarInfoPrimitive, VarInfoStruct, varInfoFromObj } from './SerializationTypes.js';
import { SerializationType } from './SerializationTypes.js';

export function serializeField(val: unknown, fi: FieldInfo, w: BinaryWriter) {
    if (val === undefined) {
        return
    }

    w.writeUint8(fi.fieldID!);

    const w2 = new BinaryWriter();
    serializeValue(val, w2, fi.varInfo);
    const b = w2.getBytes();
    encodeUint64(b.length, w);
    w.writeBytes(b);
}

export function serializeValue(val: unknown, w: BinaryWriter, vi: VarInfo) {
    if (vi instanceof VarInfoPrimitive) {
        return serializePrimitiveValue(val, w, vi);
    } else if (vi instanceof VarInfoPointer) {
        return serializePointerValue(val, w, vi);
    } else if (vi instanceof VarInfoArray) {
        return serializeArrayValue(val as unknown[], w, vi);
    } else if (vi instanceof VarInfoMap) {
        return serializeMapValue(val as Map<unknown, unknown>, w, vi);
    } else if (vi instanceof VarInfoStruct) {
        return serializeStructValue(val, w, vi);
    } else if (vi instanceof VarInfoDynamic) {
        return serializeDynamicValue(val, w, vi);
    }
    throw new Error("unsupported varinfo type: " + vi);
}

export function serializePrimitiveValue(val: unknown, w: BinaryWriter, vi: VarInfoPrimitive) {
    switch (vi.dataType) {
        case SerializationType.Bool:
            return encodeBoolean(val as boolean, w);
        case SerializationType.String:
            return encodeString(val as string, w);
        case SerializationType.Int8:
            return encodeInt8(val as number | bigint, w);
        case SerializationType.Int16:
            return encodeInt16(val as number | bigint, w);
        case SerializationType.Int32:
            return encodeInt32(val as number | bigint, w);
        case SerializationType.Int64:
            return encodeInt64(val as number | bigint, w);
        case SerializationType.Uint8:
            return encodeUint8(val as number | bigint, w);
        case SerializationType.Uint16:
            return encodeUint16(val as number | bigint, w);
        case SerializationType.Uint32:
            return encodeUint32(val as number | bigint, w);
        case SerializationType.Uint64:
            return encodeUint64(val as number | bigint, w);
        case SerializationType.Float32:
            return encodeFloat32(val as number, w);
        case SerializationType.Float64:
            return encodeFloat64(val as number, w);
        case SerializationType.Time:
            return encodeTime(val as Date, w);
    }
    throw new Error("unsupported primitive type in serializePrimitiveValue: " + vi.dataType);
}
export function serializePackedInt(valRaw: number | bigint, w: BinaryWriter): void {
    let val = BigInt(valRaw);

    let signBit = 0;
    if (val < 0) {
        signBit = 0x40;
        val = -val;
    }

    if (val < (1n << 6n)) {
        w.writeUint8(Number(val & 0x3Fn) | signBit);
        return;
    }
    w.writeUint8(Number(val & 0x3Fn) | signBit | 0x80);
    val >>= 6n;

    if (val < (1n << 7n)) {
        w.writeUint8(Number(val & 0x7Fn));
        return;
    }
    w.writeUint8(Number(val & 0x7Fn) | 0x80);
    val >>= 7n;

    if (val < (1n << 15n)) {
        w.writeUint16(Number(val & 0x7FFFn));
        return;
    }
    w.writeUint16(Number(val & 0x7FFFn) | 0x8000);
    val >>= 15n;

    if (val < (1n << 31n)) {
        w.writeUint32(Number(val & 0x7FFFFFFFn));
        return;
    }
    w.writeUint32(Number(val & 0x7FFFFFFFn) | 0x80000000);
    val >>= 31n;

    // up to 5 bits left over
    if (val > 31) {
        throw new Error("value too large for restream serialization in serializePackedInt")
    }
    w.writeUint8(Number(val));
}

export function encodeBoolean(val: boolean, w: BinaryWriter): void {
    w.writeUint8(val ? 1 : 0);
}

export function encodeString(val: string, w: BinaryWriter): void {
    encodeUint32(val.length, w);
    w.writeStringAsBytes(val);
}

// Make this one different so we can hackily ID it in the array decode
export function encodeUint8(val: number | bigint, writer: BinaryWriter): void {
    serializePackedInt(val, writer);
}

export const encodeUint16 = serializePackedInt;
export const encodeUint32 = serializePackedInt;
export const encodeUint64 = serializePackedInt;
export const encodeInt8 = serializePackedInt;
export const encodeInt16 = serializePackedInt;
export const encodeInt32 = serializePackedInt;
export const encodeInt64 = serializePackedInt;

export function encodeFloat32(val: number, w: BinaryWriter): void {
    w.writeFloat32(val);
}

export function encodeFloat64(val: number, w: BinaryWriter): void {
    w.writeFloat64(val);
}

export function encodeTime(val: Date, w: BinaryWriter): void {
    const millis = val.getTime();
    encodeInt64(millis, w);
}

export function serializePointerValue<T>(val: T | null | undefined, w: BinaryWriter, vi: VarInfoPointer) {
    if (!vi.notNil) {
        if (val === undefined) {
            w.writeUint8(1);
            return;
        }
        if (val === null) {
            if (vi.subType instanceof VarInfoPointer) {
                w.writeUint8(0);
                serializePointerValue(undefined, w, vi.subType);
                return;
            }
            w.writeUint8(1);
            return;
        }
        w.writeUint8(0);
    } else {
        if (val === undefined || val === null) {
            throw new Error("serializePointerValue called with NotNil but value is undefined/null");
        }
    }

    serializeValue(val, w, vi.subType)
}

export function serializeArrayValue<T>(val: T[] | Uint8Array | undefined, w: BinaryWriter, vi: VarInfoArray) {
    if (!vi.notNil) {
        if (val === undefined) {
            w.writeUint8(1);
            return;
        }
        w.writeUint8(0);
    } else {
        if (val === undefined) {
            throw new Error("encodeArray called with NotNil but value is undefined");
        }
    }

    encodeUint32(val.length, w);
    if (vi.elemType instanceof VarInfoPrimitive && vi.elemType.dataType === SerializationType.Uint8 && (val instanceof Uint8Array)) {
        w.writeBytes(val);
        return;
    }

    for (let i = 0; i < val.length; i++) {
        serializeValue(val[i] as T, w, vi.elemType);
    }
}

export function serializeMapValue<K, V>(val: Map<K, V> | Set<K> | undefined, w: BinaryWriter, vi: VarInfoMap) {
    if (!vi.notNil) {
        if (val === undefined) {
            w.writeUint8(1);
            return;
        }
        w.writeUint8(0);
    } else {
        if (val === undefined) {
            throw new Error("encodeMap called with NotNil but value is undefined");
        }
    }

    encodeUint32(val.size, w);
    for (const [k, v] of val.entries()) {
        serializeValue(k, w, vi.keyType);
        if (vi.elemType != undefined) {
            serializeValue(v, w, vi.elemType);
        }
    }
}

export function serializeStructValue(v: unknown, w: BinaryWriter, vi: VarInfoStruct) {
    const vs = v as Serializable;
    if (!vs.serialize) {
        throw new Error("serializeStructValue called with non-serializable value");
    }
    vs.serialize(w, vi);
}

export function serializeDynamicValue(v: unknown, w: BinaryWriter, _: VarInfoDynamic): void {
    const vi = varInfoFromObj(v);
    w.writeBytes(vi.getSerializationData());
    serializeValue(v, w, vi);
}
