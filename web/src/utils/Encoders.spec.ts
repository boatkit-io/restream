import BinaryWriter from './BinaryWriter.js';
import { describe, expect, test } from 'vitest';
import {
    encodeBoolean,
    encodeFloat32, encodeFloat64, encodeInt16, encodeInt32, encodeInt64, encodeInt8, encodeString, encodeTime, encodeUint16, encodeUint32, encodeUint64, encodeUint8, serializeDynamicValue, serializePackedInt,
    serializePointerValue,
    serializeValue
} from "./Encoders.js";
import BinaryReader from "./BinaryReader.js";
import {
    decodeBoolean,
    decodeFieldMap,
    decodeFloat32,
    decodeFloat64,
    decodeInt16,
    decodeInt32,
    decodeInt64,
    decodeInt8,
    decodeString,
    decodeTime,
    decodeUint16,
    decodeUint32,
    decodeUint64,
    decodeUint64Big, decodeUint8, deserializeDynamicValue, deserializePackedBigint,
    deserializePointerValue,
    deserializeValue
} from "./Decoders.js";
import { FieldInfo, VarInfo, VarInfoArray, VarInfoDynamic, VarInfoMap, VarInfoPointer, VarInfoPrimitive, varInfoFromObj, varInfoFromReader } from "./SerializationTypes.js";
import { SerializationType } from "./SerializationTypes.js";

import { toBeDeepCloseTo, toMatchCloseTo } from 'jest-matcher-deep-close-to';
import { PartialArray, PartialValue } from '../restream/PackageRestream.js';

expect.extend({ toBeDeepCloseTo, toMatchCloseTo });

describe('TestPackers', () => {
    // -1 gets the iv = 0 case
    for (let i = -1; i < 63; i++) {
        // check negative and positive versions
        for (let n = -1; n <= 1; n += 2) {
            let iv = BigInt(0);
            for (let h = 0; h <= i; h++) {
                iv |= BigInt(1) << BigInt(h)
            }
            iv *= BigInt(n);
            test("i " + i.toString() + ", n " + n.toString() + ": " + iv.toString(), () => {
                const w = new BinaryWriter();
                serializePackedInt(iv, w);
                const b = w.getBytes();

                if (i < 6) {
                    expect(b.length).toBe(1);
                } else if (i < 13) {
                    expect(b.length).toBe(2);
                } else if (i < 28) {
                    expect(b.length).toBe(4);
                } else if (i < 59) {
                    expect(b.length).toBe(8);
                } else {
                    expect(b.length).toBe(9);
                }

                const r = new BinaryReader(b.buffer);
                const ov = deserializePackedBigint(r);
                expect(ov).toBe(iv);
            });
        }
    }
});

describe('Fielded structs', () => {
    test('decodeFieldMap skips unknown field IDs', () => {
        const knownFieldBytes = new BinaryWriter();
        encodeString("kept", knownFieldBytes);
        const knownFieldPayload = knownFieldBytes.getBytes();

        const w = new BinaryWriter();
        w.writeUint8(99);
        serializePackedInt(3, w);
        w.writeBytes(new Uint8Array([1, 2, 3]));
        w.writeUint8(1);
        serializePackedInt(knownFieldPayload.length, w);
        w.writeBytes(knownFieldPayload);

        const fieldMap = new Map<number, FieldInfo>([
            [1, {
                name: "Known",
                fieldIdx: 0,
                fieldID: 1,
                varInfo: new VarInfoPrimitive(SerializationType.String),
            }],
        ]);

        const decoded = decodeFieldMap(new BinaryReader(w.getBytes().buffer), fieldMap);

        expect(decoded.get(1)).toBe("kept");
        expect(decoded.has(99)).toBe(false);
    });
});

describe('Primitive Isomorphism', () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const primitiveTests: [any, (v: any, w: BinaryWriter) => void, (r: BinaryReader) => any][] = [
        [false, encodeBoolean, decodeBoolean],
        [true, encodeBoolean, decodeBoolean],

        [0, encodeUint8, decodeUint8],
        [37, encodeUint8, decodeUint8],
        [0xff, encodeUint8, decodeUint8],

        [0, encodeUint16, decodeUint16],
        [37, encodeUint16, decodeUint16],
        [0xffff, encodeUint16, decodeUint16],

        [0, encodeUint32, decodeUint32],
        [27, encodeUint32, decodeUint32],
        [65535, encodeUint32, decodeUint32],
        [0xffffffff, encodeUint32, decodeUint32],

        [0, encodeUint64, decodeUint64],
        [27, encodeUint64, decodeUint64],
        [65535, encodeUint64, decodeUint64],
        [0xffffffff, encodeUint64, decodeUint64],
        [0xffffffffn, encodeUint64, decodeUint64Big],
        [BigInt(9223372036854774784n), encodeUint64, decodeUint64Big],

        [0, encodeInt8, decodeInt8],
        [37, encodeInt8, decodeInt8],
        [-50, encodeInt8, decodeInt8],
        [-1, encodeInt8, decodeInt8],

        [0, encodeInt16, decodeInt16],
        [37, encodeInt16, decodeInt16],
        [-50, encodeInt16, decodeInt16],
        [-5000, encodeInt16, decodeInt16],
        [-1, encodeInt16, decodeInt16],

        [0, encodeInt32, decodeInt32],
        [37, encodeInt32, decodeInt32],
        [-50, encodeInt32, decodeInt32],
        [-5000, encodeInt32, decodeInt32],
        [-50000000, encodeInt32, decodeInt32],
        [65535000, encodeInt32, decodeInt32],
        [-1, encodeInt32, decodeInt32],

        [0, encodeInt64, decodeInt64],
        [37, encodeInt64, decodeInt64],
        [-50, encodeInt64, decodeInt64],
        [-500000, encodeInt64, decodeInt64],
        [65535, encodeInt64, decodeInt64],
        [655350000000, encodeInt64, decodeInt64],
        [-1, encodeInt64, decodeInt64],

        ["", encodeString, decodeString],
        ["testme", encodeString, decodeString],

        [0, encodeFloat32, decodeFloat32],
        [-1, encodeFloat32, decodeFloat32],
        [1000, encodeFloat32, decodeFloat32],
        [-1.43534123, encodeFloat32, decodeFloat32],
        [14.3534123, encodeFloat32, decodeFloat32],

        [0, encodeFloat64, decodeFloat64],
        [-1, encodeFloat64, decodeFloat64],
        [1000, encodeFloat64, decodeFloat64],
        [-1.43534123, encodeFloat64, decodeFloat64],
        [14.3534123, encodeFloat64, decodeFloat64],

        [new Date(2026, 3, 5, 22, 44, 33, 106), encodeTime, decodeTime],
    ];
    primitiveTests.forEach(([val, enc, dec]) => {
        test("" + val + " " + enc.name + " " + dec.name, () => {
            // Do the prescribed encoder/decoder
            const w = new BinaryWriter();
            enc(val, w)
            const b = w.getBytes();

            const r = new BinaryReader(b.buffer);
            const ov = dec(r);
            if (enc == encodeFloat32 || enc == encodeFloat64) {
                expect(ov).toBeCloseTo(val);
            } else {
                expect(ov).toEqual(val);
            }

            // Do prescribed but wrapped in a pointer
            const w2 = new BinaryWriter();
            const vi2 = new VarInfoPointer(false, new VarInfoDynamic());
            serializePointerValue(val, w2, vi2);
            const b2 = w2.getBytes();

            const r2 = new BinaryReader(b2.buffer);
            const ov2 = deserializePointerValue(r2, vi2);
            if (enc == encodeFloat32 || enc == encodeFloat64) {
                expect(ov2).toBeCloseTo(val);
            } else {
                if (typeof val === 'bigint') {
                    expect(BigInt(ov2)).toEqual(val);
                } else {
                    expect(ov2).toEqual(val);
                }
            }

            // Now do it dynamically
            const w3 = new BinaryWriter();
            serializeDynamicValue(val, w3, new VarInfoDynamic());
            const b3 = w3.getBytes();

            const r3 = new BinaryReader(b3.buffer);
            const ov3 = deserializeDynamicValue(r3, new VarInfoDynamic());
            if (enc == encodeFloat32 || enc == encodeFloat64) {
                expect(ov3).toBeCloseTo(val);
            } else if (typeof ov3 === 'number' && typeof val === 'bigint') {
                expect(BigInt(ov3)).toEqual(val);
            } else {
                expect(ov3).toEqual(val);
            }
        });
    })
});

describe('Complex Isomorphism', () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const complexTests: [any, any | undefined, VarInfo][] = [
        [[-4, -3, -2, -1, 0, 1, 2, 3, 4], undefined, new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Int8))],
        [[-4, -3, -2, -1, 0, 1, 2, 3, 4], undefined, new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Int16))],
        [[-4, -3, -2, -1, 0, 1, 2, 3, 4], undefined, new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Int32))],
        [[-4, -3, -2, -1, 0, 1, 2, 3, 4], undefined, new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Int64))],
        [undefined, undefined, new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Int64))],

        [[0, 1, 2, 3, 4], new Uint8Array([0, 1, 2, 3, 4]), new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Uint8))],
        [[0, 1, 2, 3, 4], undefined, new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Uint16))],
        [[0, 1, 2, 3, 4], undefined, new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Uint32))],
        [[0, 1, 2, 3, 4], undefined, new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Uint64))],
        [undefined, undefined, new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Uint64))],

        [[-4.1, -3, -2, -1, 0, 0.1, 1, 2, 3, 4], undefined, new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Float32))],
        [[-4.1, -3, -2, -1, 0, 0.1, 1, 2, 3, 4], undefined, new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Float64))],
        [undefined, undefined, new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Float32))],
        [undefined, undefined, new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Float64))],

        [[0, -4, 7000, "hello", false, 3.7], undefined, new VarInfoArray(false, new VarInfoDynamic())],

        [new Set([0, 0.2, 1, 2.3, -3.4, -4]), undefined, new VarInfoMap(false, new VarInfoPrimitive(SerializationType.Float32), undefined)],
        [new Set([0, 1, 2, -4]), undefined, new VarInfoMap(false, new VarInfoPrimitive(SerializationType.Int32), undefined)],
        [undefined, undefined, new VarInfoMap(false, new VarInfoPrimitive(SerializationType.Int32), undefined)],

        [new Map([[0, "hi"], [4, "blah"], [-1, ""]]), undefined, new VarInfoMap(false, new VarInfoPrimitive(SerializationType.Int32), new VarInfoPrimitive(SerializationType.String))],
        [new Map([[0, 4], [5, 7], [9, -4], [-5, 0]]), undefined, new VarInfoMap(false, new VarInfoPrimitive(SerializationType.Int32), new VarInfoPrimitive(SerializationType.Int32))],
        [undefined, undefined, new VarInfoMap(false, new VarInfoPrimitive(SerializationType.Int32), new VarInfoPrimitive(SerializationType.Int32))],
    ];
    complexTests.forEach(([val, expectedRaw, vi]) => {
        test("" + val + " " + JSON.stringify(vi), () => {
            const expected = expectedRaw === undefined ? val : expectedRaw;

            // Check varinfo first
            const vip = varInfoFromObj(val);
            const wp = new BinaryWriter();
            wp.writeBytes(vip.getSerializationData());
            const bw = wp.getBytes();
            const vr = new BinaryReader(bw.buffer);
            const vird = varInfoFromReader(vr);
            expect(vird).toEqual(vip);

            // Do the prescribed encoder/decoder
            const w = new BinaryWriter();
            serializeValue(val, w, vi);
            const b = w.getBytes();

            const r = new BinaryReader(b.buffer);
            const ov = deserializeValue(r, vi);
            expect(ov).toBeDeepCloseTo(expected);

            // Do prescribed but wrapped in a pointer
            const w2 = new BinaryWriter();
            const vi2 = new VarInfoPointer(false, vi);
            serializeValue(val, w2, vi2);
            const b2 = w2.getBytes();

            const r2 = new BinaryReader(b2.buffer);
            const ov2 = deserializeValue(r2, vi2);
            expect(ov2).toBeDeepCloseTo(expected);

            // Now do it dynamically
            const w3 = new BinaryWriter();
            const vi3 = new VarInfoDynamic();
            serializeDynamicValue(val, w3, vi3);
            const b3 = w3.getBytes();

            const r3 = new BinaryReader(b3.buffer);
            const ov3 = deserializeDynamicValue(r3, vi3);
            expect(ov3).toBeDeepCloseTo(val);
        });
    })
});
