import { describe, expect, it } from "vitest";

import BinaryWriter from "./BinaryWriter.js";

describe("BinaryWriter", () => {
    it("grows directly to fit a byte field larger than its current buffer", () => {
        const writer = new BinaryWriter(8);
        writer.writeBytes(Uint8Array.from([1, 2, 3]));
        const payload = Uint8Array.from({ length: 100_000 }, (_, index) => index % 251);

        writer.writeBytes(payload);

        const bytes = writer.getBytes();
        expect(bytes.byteLength).toBe(100_003);
        expect(Array.from(bytes.slice(0, 3))).toEqual([1, 2, 3]);
        expect(bytes.slice(3)).toEqual(payload);
    });
});
