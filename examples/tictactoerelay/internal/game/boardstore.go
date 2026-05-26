package game

import (
	"errors"
	"reflect"

	"github.com/boatkit-io/restream/pkg/restream"
)

type BoardStore struct {
	storeData *restream.StoreData[BoardStoreState, *BoardStoreState, *BoardStoreStatePartial]
}

func NewBoardStore(rpcd *restream.RPCDispatcher) (*BoardStore, error) {
	s := &BoardStore{}

	initialState := &BoardStoreState{
		Board: [][]string{
			{"", "", "O"},
			{"", "X", ""},
			{"O", "", "X"},
		},
		XTurn: true,
	}

	s.storeData = restream.NewStoreData[BoardStoreState, *BoardStoreState, *BoardStoreStatePartial](s, initialState)

	rpcd.RegisterRPCHandler("PlaceToken", 1, func(x, y int) error {
		partial := &BoardStoreStatePartial{
			Board: restream.NewPartialArray[[]string](),
		}
		var newRow []string
		var xTurn bool
		s.storeData.ReadState(func(state *BoardStoreState) {
			newRow = append([]string{}, state.Board[y]...)
			xTurn = state.XTurn
		})
		if newRow[x] != "" {
			return errors.New("cell already occupied")
		}
		if xTurn {
			newRow[x] = "X"
		} else {
			newRow[x] = "O"
		}
		partial.Board.Set(y, newRow)
		partial.XTurn = restream.Ptr(!xTurn)
		s.storeData.ApplyPartial(partial)
		return nil
	}, reflect.TypeFor[PlaceTokenRequest](), reflect.TypeFor[PlaceTokenResponse]())

	return s, nil
}

// GetName is an implementation of the Store.GetName call
func (s *BoardStore) GetName() string {
	return BoardStoreName
}

// GetStoreData is an implementation of the Store.GetStoreData call
func (s *BoardStore) GetStoreData() restream.StoreDataBase {
	return s.storeData
}

// SubscribeToField implements the restream.Store interface
func (s *BoardStore) SubscribeToField(field []any, callback any) {
	s.storeData.SubscribeToField(field, callback)
}
