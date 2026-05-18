import { deserializeDynamicValue } from '@boatkit-io/restream';
import { BinaryReader } from '@boatkit-io/restream';
import { VarInfoDynamic } from '@boatkit-io/restream';
import { describe, expect, test } from 'vitest';

import BasicTesterData from './BasicTesterData';
import { mapValueToObject } from '../utils/TSUtils';

describe('BasicTester', () => {
    for (const [name, bytes, jsonStr] of BasicTesterData) {
        test(name as string, () => {
            const ui = new Uint8Array(bytes as number[]);
            let od: unknown = deserializeDynamicValue(new BinaryReader(ui.buffer), new VarInfoDynamic());

            let compareTo: unknown = JSON.parse(jsonStr as string);
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
