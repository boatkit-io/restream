package main

import (
	"github.com/boatkit-io/restream/pkg/restream"
)

type BoardStore struct {
	storeData *restream.StoreData[BoardStoreState, *BoardStoreState, *BoardStoreStatePartial]
}

func NewBoardStore() (*BoardStore, error) {
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
