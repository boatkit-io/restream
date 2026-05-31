import BinaryReader from './BinaryReader.js';
import * as assert from './SimpleAssert.js';
import {
    FieldInfo, VarInfo, VarInfoArray, VarInfoDynamic, VarInfoMap, VarInfoPointer,
    VarInfoPrimitive, VarInfoStruct, varInfoFromReader
} from "./SerializationTypes.js";
import { SerializationType } from "./SerializationTypes.js";

export function decodeFieldMap(r: BinaryReader, fieldMap: ReadonlyMap<number, FieldInfo>): Map<number, unknown> {
    const ret = new Map<number, unknown>();
    while (!r.isEOF()) {
        const fieldID = r.readByte();
        const fieldLen = decodeUint32(r);
        const fi = fieldMap.get(fieldID);
        if (fi === undefined) {
            r.skipBytes(fieldLen);
            continue;
        }
        const subReader = r.slice(fieldLen);
        const ov = deserializeValue(subReader, fi.varInfo);
        ret.set(fieldID, ov);
    }
    return ret;
}

export function deserializeValue(r: BinaryReader, vi: VarInfo) {
    if (vi instanceof VarInfoPrimitive) {
        return deserializePrimitiveValue(r, vi);
    } else if (vi instanceof VarInfoPointer) {
        return deserializePointerValue(r, vi);
    } else if (vi instanceof VarInfoArray) {
        return deserializeArrayValue(r, vi);
    } else if (vi instanceof VarInfoMap) {
        return deserializeMapValue(r, vi);
    } else if (vi instanceof VarInfoStruct) {
        return deserializeStructValue(r, vi);
    } else if (vi instanceof VarInfoDynamic) {
        return deserializeDynamicValue(r, vi);
    }
    throw new Error("unsupported varinfo type: " + vi);
}

function deserializePrimitiveValue(r: BinaryReader, vi: VarInfoPrimitive) {
    switch (vi.dataType) {
        case SerializationType.Bool:
            return decodeBoolean(r);
        case SerializationType.String:
            return decodeString(r);
        case SerializationType.Int8:
            return decodeInt8(r);
        case SerializationType.Int16:
            return decodeInt16(r);
        case SerializationType.Int32:
            return decodeInt32(r);
        case SerializationType.Int64:
            return decodeInt64(r);
        case SerializationType.Uint8:
            return decodeUint8(r);
        case SerializationType.Uint16:
            return decodeUint16(r);
        case SerializationType.Uint32:
            return decodeUint32(r);
        case SerializationType.Uint64:
            return decodeUint64(r);
        case SerializationType.Float32:
            return decodeFloat32(r);
        case SerializationType.Float64:
            return decodeFloat64(r);
        case SerializationType.Time:
            return decodeTime(r);
    }
    throw new Error("unsupported primitive type in deserializePrimitiveValue: " + vi.dataType);
}

export function deserializePackedInt(r: BinaryReader): number {
    let ret = 0;

    let b = r.readByte();
    ret |= b & 0x3f;
    const neg = (b & 0x40) > 0;

    if ((b & 0x80) > 0) {
        b = r.readByte();
        ret |= ((b & 0x7f) << 6)

        if ((b & 0x80) > 0) {
            const w = r.readUint16();
            ret |= ((w & 0x7fff) << 13);

            if ((w & 0x8000) > 0) {
                let reta = BigInt(ret)
                const dw = BigInt(r.readUint32());
                reta |= ((dw & 0x7fffffffn) << 28n);

                if ((dw & 0x80000000n) > 0) {
                    const bi = BigInt(r.readByte());
                    reta |= bi << 59n;
                }

                ret = Number(reta);
            }
        }
    }

    return neg ? -ret : ret;
}

export function deserializePackedBigint(r: BinaryReader): bigint {
    let ret = BigInt(0);

    let b = r.readByte();
    ret |= BigInt(b & 0x3f);
    const neg = (b & 0x40) > 0;

    if ((b & 0x80) > 0) {
        b = r.readByte();
        ret |= BigInt(((b & 0x7f) << 6))

        if ((b & 0x80) > 0) {
            const w = r.readUint16();
            ret |= BigInt(((w & 0x7fff) << 13));

            if ((w & 0x8000) > 0) {
                const dw = BigInt(r.readUint32());
                ret |= ((dw & 0x7fffffffn) << 28n);

                if ((dw & 0x80000000n) > 0) {
                    const bi = BigInt(r.readByte());
                    ret |= bi << 59n;
                }
            }
        }
    }

    return neg ? -ret : ret;
}

export function decodeBoolean(r: BinaryReader): boolean {
    const bVal = r.readByte();
    assert.ok(bVal === 0 || bVal === 1, "invalid boolean value, datastream probably corrupt")
    return bVal === 1;
}

export function decodeString(r: BinaryReader): string {
    const len = decodeUint32(r);
    return r.readString(len);
}

// Make this one different so we can hackily ID it in the array decode
export function decodeUint8(r: BinaryReader): number {
    return deserializePackedInt(r);
}

export const decodeUint16 = deserializePackedInt;
export const decodeUint32 = deserializePackedInt;
export const decodeUint64 = deserializePackedInt;
export const decodeUint64Big = deserializePackedBigint;
export const decodeInt8 = deserializePackedInt;
export const decodeInt16 = deserializePackedInt;
export const decodeInt32 = deserializePackedInt;
export const decodeInt64 = deserializePackedInt;
export const decodeInt64Big = deserializePackedBigint;

export function decodeFloat32(r: BinaryReader): number {
    return r.readFloat32();
}

export function decodeFloat64(r: BinaryReader): number {
    return r.readFloat64();
}

export function decodeTime(r: BinaryReader): Date {
    const millis = Number(decodeInt64(r));
    return new Date(millis);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function deserializePointerValue(r: BinaryReader, vi: VarInfoPointer): any | null | undefined {
    if (!vi.notNil) {
        const bIsNull = r.readByte();
        assert.ok(bIsNull === 0 || bIsNull === 1, "invalid boolean pointer nullability value, datastream probably corrupt")
        if (bIsNull === 1) {
            return undefined;
        }
    }

    const val = deserializeValue(r, vi.subType);
    if (vi.subType instanceof VarInfoPointer && val === undefined) {
        return null;
    }
    return val;
}

export function deserializeArrayValue(r: BinaryReader, vi: VarInfoArray) {
    if (!vi.notNil) {
        const isNil = r.readByte();
        assert.ok(isNil === 0 || isNil === 1, "invalid boolean pointer nullability value, datastream probably corrupt")
        if (isNil == 1) {
            return undefined;
        }
    }

    const len = decodeUint32(r);
    if (vi.elemType instanceof VarInfoPrimitive && vi.elemType.dataType == SerializationType.Uint8) {
        return r.readBytes(len);
    }

    const arr = new Array(len);
    for (let i = 0; i < len; i++) {
        arr[i] = deserializeValue(r, vi.elemType);
    }
    return arr;
}

export function deserializeMapValue<K, V>(r: BinaryReader, vi: VarInfoMap): Map<K, V> | Set<K> | undefined {
    if (!vi.notNil) {
        const isNil = r.readByte();
        if (isNil == 1) {
            return undefined;
        }
    }

    const len = decodeUint32(r);
    if (vi.elemType === undefined) {
        const ret = new Set<K>();
        for (let i = 0; i < len; i++) {
            const k = deserializeValue(r, vi.keyType);
            ret.add(k);
        }
        return ret;
    } else {
        const ret = new Map<K, V>();
        for (let i = 0; i < len; i++) {
            const k = deserializeValue(r, vi.keyType);
            const v = deserializeValue(r, vi.elemType);
            ret.set(k, v);
        }
        return ret;
    }
}

export function deserializeDynamicValue(r: BinaryReader, _: VarInfoDynamic): unknown {
    const vi = varInfoFromReader(r);

    if (vi instanceof VarInfoPrimitive) {
        return deserializePrimitiveValue(r, vi);
    } else if (vi instanceof VarInfoPointer) {
        return deserializePointerValue(r, vi);
    } else if (vi instanceof VarInfoArray) {
        return deserializeArrayValue(r, vi);
    } else if (vi instanceof VarInfoMap) {
        return deserializeMapValue(r, vi);
    } else if (vi instanceof VarInfoStruct) {
        return deserializeStructValue(r, vi);
    } else if (vi instanceof VarInfoDynamic) {
        throw new Error("dynamic dynamic type during deserialize: " + vi);
    }
}

export function deserializeStructValue(r: BinaryReader, vi: VarInfoStruct) {
    return vi.deserializer!.deserialized(r, vi);
}
