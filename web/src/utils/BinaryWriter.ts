const INITIAL_LENGTH_GUESS = 512;

const encoder = new TextEncoder();

export default class BinaryWriter {
    private _buffer!: Uint8Array;
    private _offset = 0;
    private _length: number;

    constructor(suggestedLength?: number) {
        this._length = suggestedLength || INITIAL_LENGTH_GUESS;
        this._realloc();
    }

    private _realloc() {
        const oldBuf = this._buffer;
        this._buffer = new Uint8Array(this._length);
        if (oldBuf) {
            this._buffer.set(oldBuf, 0);
        }
    }

    private _ensureFits(numBytes: number): void {
        if (this._offset + numBytes >= this._length) {
            if (numBytes < this._length) {
                // Safe to double it
                this._length *= 2;
            } else {
                // Round to the next higher multiple of this._length..?
                this._length = Math.ceil((this._length + numBytes) / numBytes) * this._length;
            }
            this._realloc();
        }
    }

    writeUint8(val: number): void {
        this._ensureFits(1);
        this._buffer[this._offset++] = val;
    }

    writeUint16(val: number): void {
        this._ensureFits(2);
        this._buffer[this._offset++] = val & 0xFF;
        this._buffer[this._offset++] = (val >> 8) & 0xFF;
    }

    writeUint32(val: number): void {
        this._ensureFits(4);
        this._buffer[this._offset++] = val & 0xFF;
        this._buffer[this._offset++] = (val >> 8) & 0xFF;
        this._buffer[this._offset++] = (val >> 16) & 0xFF;
        this._buffer[this._offset++] = (val >> 24) & 0xFF;
    }

    writeFloat32(val: number): void {
        const fa = new Float32Array([val]);
        this.writeBytes(new Uint8Array(fa.buffer));
    }

    writeFloat64(val: number): void {
        const fa = new Float64Array([val]);
        this.writeBytes(new Uint8Array(fa.buffer));
    }

    writeBytes(bytes: Uint8Array): void {
        this._ensureFits(bytes.length);
        this._buffer.set(bytes, this._offset);
        this._offset += bytes.length;
    }

    writeStringAsBytes(str: string): void {
        const bytes = encoder.encode(str);
        this.writeBytes(bytes);
    }

    getLength() {
        return this._offset;
    }

    getBytes() {
        return this._buffer.subarray(0, this._offset);
    }
}
