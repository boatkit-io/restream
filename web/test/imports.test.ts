import { describe, expect, it } from 'vitest';

import {
    BinaryReader,
    BinaryWriter,
    PartialArray,
    ReStreamSocket,
    SerializationType,
    VarInfoPrimitive,
} from '../src/index.js';

describe('package exports', () => {
    it('resolves runtime entrypoint imports', () => {
        const writer = new BinaryWriter();
        writer.writeUint8(7);

        const reader = new BinaryReader(writer.getBytes().buffer);
        expect(reader.readUint8()).toBe(7);
        expect(new VarInfoPrimitive(SerializationType.Uint8).dataType).toBe(SerializationType.Uint8);
        expect(PartialArray.fromValues).toBeTypeOf('function');
        expect(ReStreamSocket).toBeTypeOf('function');
    });
});
