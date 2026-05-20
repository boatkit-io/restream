import { TriggerStore } from '@boatkit-io/restream';
import { AutoSubscribeStore, autoSubscribe } from '@boatkit-io/resub';

import { BoardStoreName, BoardStoreState, BoardStoreStatePartial } from '../restream/PackageMain';

@AutoSubscribeStore
class BoardStore extends TriggerStore<BoardStoreState> {
    constructor() {
        super(BoardStoreName, BoardStoreState, BoardStoreStatePartial);
    }

    @autoSubscribe
    getBoard(): string[][] {
        return (this._state.board ?? []).map((row) => row ?? []);
    }

    @autoSubscribe
    getXTurn(): boolean {
        return this._state.xTurn;
    }
}

export default new BoardStore();
