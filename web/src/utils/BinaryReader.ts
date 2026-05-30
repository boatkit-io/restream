const ZERO_CHAR = '0'.charCodeAt(0);

const decoderUTF8 = new TextDecoder('utf8');
const decoderUTF16 = new TextDecoder('utf-16le');

export default class BinaryReader {
    private _int8Buffer: Int8Array;
    private _uint8Buffer: Uint8Array;
    constructor(private _buffer: ArrayBufferLike,
        private _offset = 0,
        private _length = _buffer.byteLength) {
        // ArrayBuffer also can take in typedarrays, which will screw up trying to cast them around.
        // Make sure we actually use an arraybuffer.
        if (this._buffer.constructor != ArrayBuffer) {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            this._buffer = (this._buffer as any).buffer;
        }
        this._int8Buffer = new Int8Array(this._buffer);
        this._uint8Buffer = new Uint8Array(this._buffer);
    }

    readBytes(length: number): Uint8Array {
        if (this._offset + length > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }
        const ret = this._buffer.slice(this._offset, this._offset + length);
        this._offset += length;
        return new Uint8Array(ret);
    }

    readByteView(length: number): Uint8Array {
        if (this._offset + length > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }
        const ret = this._uint8Buffer.subarray(this._offset, this._offset + length);
        this._offset += length;
        return ret;
    }

    readString(numChars: number): string {
        if (this._offset + numChars > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }

        const ret = decoderUTF8.decode(this._uint8Buffer.subarray(this._offset, this._offset + numChars));
        this._offset += numChars;
        return ret;
    }

    readASCIIString(numChars: number): string {
        if (this._offset + numChars > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }

        let ret: string;
        if (numChars < 16) {
            ret = '';
            for (let i = 0; i < numChars; i++) {
                ret += String.fromCharCode(this._uint8Buffer[this._offset++]!);
            }
        } else {
            ret = decoderUTF8.decode(this._uint8Buffer.subarray(this._offset, this._offset + numChars));
            this._offset += numChars;
        }
        return ret;
    }

    readWideStringToTerminator(terminator: number): [string, number] {
        for (let i = this._offset; i < this._length; i += 2) {
            const char = this._uint8Buffer[i]! | (this._uint8Buffer[i + 1]! << 8);
            if (char === terminator) {
                const ret = decoderUTF16.decode(this._uint8Buffer.subarray(this._offset, i));
                // account for (skipping) the terminator as well as the slice we just pulled directly off above
                const byteCount = (i + 2) - this._offset;
                this._offset += byteCount;
                return [ret, byteCount];
            }
        }
        throw new Error('Terminator not found');
    }

    readStringToTerminator(terminator: number): [string, number] {
        for (let i = this._offset; i < this._length; i++) {
            if (this._uint8Buffer[i] === terminator) {
                const ret = decoderUTF8.decode(this._uint8Buffer.subarray(this._offset, i));
                // account for (skipping) the terminator as well as the slice we just pulled directly off above
                const byteCount = (i + 1) - this._offset;
                this._offset += byteCount;
                return [ret, byteCount];
            }
        }
        throw new Error('Terminator not found');
    }

    readNumericString(numChars: number): number {
        if (this._offset + numChars > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }

        let o = 0;
        for (let i = 0; i < numChars; i++) {
            o *= 10;
            const char = this._uint8Buffer[this._offset++]! - ZERO_CHAR;
            if (char < 0 || char > 9) {
                throw new Error('Char out of range in readNumericString: ' + char);
            }
            o += char;
        }
        return o;
    }

    peekByte(): number {
        if (this._offset + 1 > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }

        return this._uint8Buffer[this._offset]!;
    }

    readByte(): number {
        if (this._offset + 1 > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }

        return this._uint8Buffer[this._offset++]!;
    }

    readInt8(): number {
        if (this._offset + 1 > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }
        return this._int8Buffer[this._offset++]!;
    }

    readUint8(): number {
        return this.readByte();
    }

    readInt16(): number {
        if (this._offset + 2 > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }
        const ret = this._uint8Buffer[this._offset]! + (this._int8Buffer[this._offset + 1]! << 8);
        this._offset += 2;
        return ret;
    }

    readUint16(): number {
        if (this._offset + 2 > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }
        const ret = this._uint8Buffer[this._offset]! | (this._uint8Buffer[this._offset + 1]! << 8);
        this._offset += 2;
        return ret;
    }

    readInt32(): number {
        if (this._offset + 4 > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }
        const ret = this._uint8Buffer[this._offset]! | (this._uint8Buffer[this._offset + 1]! << 8) |
            (this._uint8Buffer[this._offset + 2]! << 16) | (this._int8Buffer[this._offset + 3]! << 24);
        this._offset += 4;
        return ret;
    }

    readUint32(): number {
        if (this._offset + 4 > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }
        const ret = BigInt(this._uint8Buffer[this._offset]!) | BigInt(this._uint8Buffer[this._offset + 1]! << 8) |
            BigInt(this._uint8Buffer[this._offset + 2]! << 16) | (BigInt(this._uint8Buffer[this._offset + 3]!) << BigInt(24));
        this._offset += 4;
        return Number(ret);
    }

    readFloat32(): number {
        if (this._offset + 4 > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }
        const fa = new Float32Array(this._buffer.slice(this._offset, this._offset + 4));
        this._offset += 4;
        return fa[0]!;
    }

    readFloat64(): number {
        if (this._offset + 8 > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }
        const fa = new Float64Array(this._buffer.slice(this._offset, this._offset + 8));
        this._offset += 8;
        return fa[0]!;
    }

    slice(count: number): BinaryReader {
        if (this._offset + count > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }
        const br = new BinaryReader(this._buffer, this._offset, this._offset + count);
        this._offset += count;
        return br
    }

    skipBytes(count: number) {
        if (this._offset + count > this._length) {
            throw new Error('Overflowing BinaryReader buffer');
        }
        this._offset += count;
    }

    isEOF(): boolean {
        return this._offset === this._length;
    }
}
