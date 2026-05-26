package game

// @restream.partials
type BoardStoreState struct {
	// MAXFIELD(2)
	Board [][]string `restream:",fID=1"`
	XTurn bool       `restream:",fID=2"`
}
