import { readFileSync } from 'node:fs';
import { describe, expect, test } from 'vitest';

import { deserializeDynamicValue } from '../utils/Decoders';
import BinaryReader from '../utils/BinaryReader';
import { VarInfoDynamic } from '../utils/SerializationTypes';
import { mapValueToObject } from '../utils/TSUtils';

type BasicTesterFixtureRow = {
    name: string;
    bytes: number[];
    jsonStr: string;
};

const fixturePath = process.env.RESTREAM_BASIC_TESTER_DATA;
const basicTesterData: BasicTesterFixtureRow[] = fixturePath === undefined
    ? []
    : JSON.parse(readFileSync(fixturePath, 'utf8')) as BasicTesterFixtureRow[];
const describeBasicTester = fixturePath === undefined ? describe.skip : describe;

describeBasicTester('BasicTester', () => {
    for (const { name, bytes, jsonStr } of basicTesterData) {
        test(name, () => {
            const ui = new Uint8Array(bytes);
            let od: unknown = deserializeDynamicValue(new BinaryReader(ui.buffer), new VarInfoDynamic());

            let compareTo: unknown = JSON.parse(jsonStr);
            if (compareTo == null) {
                compareTo = undefined;
            }

            if (od?.constructor == Uint8Array) {
                od = Buffer.from(od).toString('base64');
            } else {
                od = mapValueToObject(od);
            }

            if (!isNaN(Number(od))) {
                expect(Number(od)).toBeCloseTo(Number(compareTo));
            } else {
                expect(od).toEqual(compareTo);
            }
        });
    }
});
