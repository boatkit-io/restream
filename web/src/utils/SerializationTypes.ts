import BinaryReader from './BinaryReader.js';
import BinaryWriter from './BinaryWriter.js';
import { deepEqual } from './TSUtils.js';

export enum SerializationType {
    Invalid = 0,
    Bool = 1,
    String = 2,
    Int8 = 3,
    Int16 = 4,
    Int32 = 5,
    Int64 = 6,
    Uint8 = 7,
    Uint16 = 8,
    Uint32 = 9,
    Uint64 = 10,
    Float32 = 11,
    Float64 = 12,
    Time = 13,
    Pointer = 14,
    Array = 15,
    Map = 16,
    Dynamic = 17,
    Void = 18,
}

export interface Serializable {
    serialize(w: BinaryWriter, vi: VarInfoStruct | undefined): void;
}

export interface Deserializable<T = unknown> {
    deserialized: (r: BinaryReader, vi: VarInfoStruct | undefined) => T;
    readonly fieldInfo?: readonly FieldInfo[];
}

export interface AppliablePartial<V> {
    applyTo(por: V): (string | number)[][];
}
export interface AppliableOnTopPartial<V> {
    applyOnTop(por: V): [V, (string | number)[][]];
}
export type MergeablePartial<V> = AppliablePartial<V> | AppliableOnTopPartial<V>;

export interface PartialFor<S extends object> {
    applyTo(por: S): (string | number)[][];
}

export interface VarInfo {
    getSerializationData(): Uint8Array;
    fillGenerics(argTypes: Map<string, VarInfo>): VarInfo;
}

export class VarInfoDynamic implements VarInfo {
    getSerializationData(): Uint8Array {
        return new Uint8Array([SerializationType.Dynamic]);
    }

    fillGenerics(_: Map<string, VarInfo>): VarInfo {
        return this;
    }
}

export class VarInfoPrimitive implements VarInfo {
    constructor(
        public readonly dataType: SerializationType,
        public readonly mappedType?: string,
    ) { }

    getSerializationData(): Uint8Array {
        return new Uint8Array([this.dataType]);
    }

    fillGenerics(_: Map<string, VarInfo>): VarInfo {
        return this;
    }
}

export class VarInfoPointer implements VarInfo {
    constructor(
        public readonly notNil: boolean,
        public readonly subType: VarInfo,
    ) { }

    getSerializationData(): Uint8Array {
        const sd = this.subType.getSerializationData();
        const na = new Uint8Array(1 + sd.length);
        na[0] = SerializationType.Pointer;
        na.set(sd, 1);
        return na;
    }

    fillGenerics(gm: Map<string, VarInfo>): VarInfo {
        return new VarInfoPointer(this.notNil, this.subType.fillGenerics(gm));
    }
}

export class VarInfoArray implements VarInfo {
    constructor(
        public readonly notNil: boolean,
        public readonly elemType: VarInfo,
    ) { }

    getSerializationData(): Uint8Array {
        const sd = this.elemType.getSerializationData();
        const na = new Uint8Array(1 + sd.length);
        na[0] = SerializationType.Array;
        na.set(sd, 1);
        return na;
    }

    fillGenerics(gm: Map<string, VarInfo>): VarInfo {
        return new VarInfoArray(this.notNil, this.elemType.fillGenerics(gm));
    }
}

export class VarInfoMap implements VarInfo {
    constructor(
        public readonly notNil: boolean,
        public readonly keyType: VarInfo,
        public readonly elemType: VarInfo | undefined,
    ) { }

    getSerializationData(): Uint8Array {
        const ksd = this.keyType.getSerializationData();
        const esd = this.elemType ? this.elemType.getSerializationData() : new Uint8Array([SerializationType.Void]);
        const na = new Uint8Array(1 + ksd.length + esd.length);
        na[0] = SerializationType.Map;
        na.set(ksd, 1);
        na.set(esd, 1 + ksd.length);
        return na;
    }

    fillGenerics(gm: Map<string, VarInfo>): VarInfo {
        const elemType = this.elemType ? this.elemType.fillGenerics(gm) : undefined;
        return new VarInfoMap(this.notNil, this.keyType.fillGenerics(gm), elemType);
    }
}

export class VarInfoStruct implements VarInfo {
    constructor(
        public readonly name: string,
        public readonly packageName: string,
        public readonly deserializer?: Deserializable,
        public readonly fieldList?: readonly FieldInfo[],
        public readonly genericTypes?: readonly VarInfo[],
    ) { }

    getSerializationData(): Uint8Array {
        throw new Error("dynamic GetSerializationData of structs not supported");
    }

    fillGenerics(_: Map<string, VarInfo>): VarInfo {
        return this;
    }
}

export class VarInfoGenericParam implements VarInfo {
    constructor(
        public readonly name: string,
    ) { }

    getSerializationData(): Uint8Array {
        throw new Error("dynamic GetSerializationData of structs not supported");
    }

    fillGenerics(gm: Map<string, VarInfo>): VarInfo {
        if (gm.has(this.name)) {
            return gm.get(this.name)!;
        }
        return this;
    }
}

export function varInfoFromReader(r: BinaryReader): VarInfo | undefined {
    const st = r.readByte() as SerializationType;
    switch (st) {
        case SerializationType.Bool:
        case SerializationType.String:
        case SerializationType.Int8:
        case SerializationType.Int16:
        case SerializationType.Int32:
        case SerializationType.Int64:
        case SerializationType.Uint8:
        case SerializationType.Uint16:
        case SerializationType.Uint32:
        case SerializationType.Uint64:
        case SerializationType.Float32:
        case SerializationType.Float64:
        case SerializationType.Time:
            return new VarInfoPrimitive(st);
        case SerializationType.Pointer: {
            const sub = varInfoFromReader(r);
            if (sub == undefined) {
                throw new Error("undefined varinfo for Pointer in varInfoFromReader");
            }
            return new VarInfoPointer(false, sub);
        }
        case SerializationType.Array: {
            const sub = varInfoFromReader(r);
            if (sub == undefined) {
                throw new Error("undefined varinfo for Array in varInfoFromReader");
            }
            return new VarInfoArray(false, sub);
        }
        case SerializationType.Map: {
            const key = varInfoFromReader(r);
            const elem = varInfoFromReader(r);
            if (key == undefined) {
                throw new Error("undefined varinfo for Map key in varInfoFromReader");
            }
            return new VarInfoMap(false, key, elem);
        }
        case SerializationType.Dynamic:
            return new VarInfoDynamic();
        case SerializationType.Void:
            return undefined;
        default:
            throw new Error("can't dynamically deserialize varinfo for unknown serialization type: " + st);
    }
}

export function varInfoFromObj(obj: unknown): VarInfo {
    switch (typeof obj) {
        case 'boolean':
            return new VarInfoPrimitive(SerializationType.Bool);
        case 'string':
            return new VarInfoPrimitive(SerializationType.String);
        case 'number':
            return (obj === (obj | 0)) ? new VarInfoPrimitive(SerializationType.Int64) : new VarInfoPrimitive(SerializationType.Float64);
        case 'bigint':
            return new VarInfoPrimitive(SerializationType.Int64);
        case 'undefined':
            return new VarInfoPointer(false, new VarInfoPrimitive(SerializationType.Int64));
        case 'object':
            if (obj === null) {
                return new VarInfoPointer(false, new VarInfoPrimitive(SerializationType.Int64));
            }
            if (obj instanceof Date) {
                return new VarInfoPrimitive(SerializationType.Time);
            }
            if (obj instanceof Uint8Array) {
                return new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Uint8));
            }
            if (obj instanceof Array) {
                if (obj.length === 0) {
                    return new VarInfoArray(false, new VarInfoPrimitive(SerializationType.Int64));
                }
                const firstVI = varInfoFromObj(obj[0]);
                for (let i = 1; i < obj.length; i++) {
                    if (!deepEqual(varInfoFromObj(obj[i]), firstVI)) {
                        return new VarInfoArray(false, new VarInfoDynamic());
                    }
                }
                return new VarInfoArray(false, firstVI);
            }
            if (obj instanceof Map) {
                if (obj.size === 0) {
                    return new VarInfoMap(false, new VarInfoPrimitive(SerializationType.Int64), new VarInfoPrimitive(SerializationType.Int64));
                }
                const entries = obj.entries();
                let entry = entries.next()!;
                const firstKT = varInfoFromObj(entry.value![0]);
                let keyDiff = false;
                const firstVT = varInfoFromObj(entry.value![1]);
                let valueDiff = false;
                for (; !entry.done; entry = entries.next()) {
                    if (!keyDiff && !deepEqual(varInfoFromObj(entry.value![0]), firstKT)) {
                        keyDiff = true;
                    }
                    if (!valueDiff && !deepEqual(varInfoFromObj(entry.value![1]), firstVT)) {
                        valueDiff = true;
                    }
                    if (keyDiff && valueDiff) {
                        break;
                    }
                }
                const kt = keyDiff ? new VarInfoDynamic() : firstKT;
                const vt = valueDiff ? new VarInfoDynamic() : firstVT;
                return new VarInfoMap(false, kt, vt);
            }
            if (obj instanceof Set) {
                if (obj.size === 0) {
                    return new VarInfoMap(false, new VarInfoPrimitive(SerializationType.Int64), new VarInfoPrimitive(SerializationType.Int64));
                }
                const entries = obj.entries();
                let entry = entries.next()!;
                const firstKT = varInfoFromObj(entry.value![0]);
                for (; !entry.done; entry = entries.next()) {
                    if (!deepEqual(varInfoFromObj(entry.value![0]), firstKT)) {
                        return new VarInfoMap(false, new VarInfoDynamic(), undefined);
                    }
                }
                return new VarInfoMap(false, firstKT, undefined);
            }
            // TODO: not sure if this works at all
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            return new VarInfoStruct(obj.constructor.name, "", ((obj as any).prototype as Deserializable));
        default:
            throw new Error("can't create varinfo for unknown type: " + typeof obj);
    }
}

export interface FieldInfo {
    readonly name: string;
    readonly fieldIdx: number;
    readonly fieldID?: number;
    readonly varInfo: VarInfo;
}
