import { describe, expect, test } from 'vitest';

import BinaryReader from './BinaryReader.js';

describe('BinaryReader', () => {
    test('readASCIIString reads short byte strings without decoding overhead', () => {
        const reader = new BinaryReader(new Uint8Array([65, 66, 67, 68]).buffer);

        expect(reader.readASCIIString(2)).toBe('AB');
        expect(reader.readASCIIString(2)).toBe('CD');
        expect(reader.isEOF()).toBe(true);
    });

    test('readASCIIString decodes longer byte strings', () => {
        const text = 'ABCDEFGHIJKLMNOP';
        const reader = new BinaryReader(new TextEncoder().encode(text).buffer);

        expect(reader.readASCIIString(text.length)).toBe(text);
        expect(reader.isEOF()).toBe(true);
    });

    test('readByteView returns a subarray view without copying', () => {
        const bytes = new Uint8Array([1, 2, 3, 4]);
        const reader = new BinaryReader(bytes.buffer);

        reader.skipBytes(1);
        const view = reader.readByteView(2);

        expect(Array.from(view)).toEqual([2, 3]);
        bytes[1] = 9;
        expect(view[0]).toBe(9);
        expect(reader.readByte()).toBe(4);
    });
});
