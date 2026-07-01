package restream

// NewPartialModArray builds a PartialModArray
func NewPartialModArray[V any, P Partial]() *PartialModArray[V, P] {
	return &PartialModArray[V, P]{
		dataSets: map[int]V{},
		dataMods: map[int]P{},
		whole:    nil,
	}
}

// PartialModArray is a structure to help apply partials on fields of the slice's value
// @restream.serializers
type PartialModArray[V any, P Partial] struct {
	dataSets map[int]V `restream:",notnil"`
	dataMods map[int]P `restream:",notnil"`
	whole    []V
}

var _ Partial = (*PartialModArray[int, *fakePartial])(nil)

// Set will apply a set for the value of one of the keys of the array
func (p *PartialModArray[V, P]) Set(index int, value V) *PartialModArray[V, P] {
	p.ensureMaps()
	p.dataSets[index] = value
	delete(p.dataMods, index)
	return p
}

// ApplyPartial applies a partial to a value of the array referenced by the partial
func (p *PartialModArray[V, P]) ApplyPartial(index int, partial P) *PartialModArray[V, P] {
	p.ensureMaps()
	// We will apply sets/deletes, then modifies, so it _should_ just need storing the partial mods...
	if po, has := p.dataMods[index]; has {
		partial.MergeOntoPartial(po)
		p.dataMods[index] = po
	} else {
		p.dataMods[index] = partial
	}
	return p
}

// SetWhole will apply a whole set to replace the entire state
func (p *PartialModArray[V, P]) SetWhole(value []V) *PartialModArray[V, P] {
	p.ensureMaps()
	p.whole = value
	clear(p.dataMods)
	clear(p.dataSets)
	return p
}

func (p *PartialModArray[V, P]) ensureMaps() {
	if p.dataSets == nil {
		p.dataSets = map[int]V{}
	}
	if p.dataMods == nil {
		p.dataMods = map[int]P{}
	}
}

// MergeOntoPartial merges this partialarray onto another partialarray
func (p *PartialModArray[V, P]) MergeOntoPartial(por any) {
	po := por.(*PartialModArray[V, P])

	// ... feels like this might be a bit of a land mine, but deletes first, then sets..?
	if p.whole != nil {
		po.SetWhole(p.whole)
	}
	for k, v := range p.dataSets {
		po.Set(k, v)
	}
	for k, v := range p.dataMods {
		po.ApplyPartial(k, v)
	}
}

// ApplyTo applies the contents of this partialarray onto a full existing value
func (p *PartialModArray[V, PV]) ApplyTo(por any) [][]any {
	po, ok := por.(*[]V)
	if !ok {
		po = *por.(**[]V)
	}

	ret := [][]any{}
	if p.whole != nil {
		*po = p.whole
		ret = append(ret, []any{})
	}
	for k, v := range p.dataSets {
		if k < 0 {
			continue
		}
		*po = ensureSliceIndex(*po, k)
		(*po)[k] = v
		if p.whole == nil {
			ret = append(ret, []any{k})
		}
	}
	for k, pv := range p.dataMods {
		if k < 0 {
			continue
		}
		*po = ensureSliceIndex(*po, k)
		fs := pv.ApplyTo(&(*po)[k])
		if p.whole == nil {
			for _, f := range fs {
				ret = append(ret, append([]any{k}, f...))
			}
		}
	}
	return ret
}

// FilterToFields returns a new partial array containing only changes matching the requested field paths.
func (p *PartialModArray[V, PV]) FilterToFields(fields [][]any) (Partial, bool) {
	ret := NewPartialModArray[V, PV]()
	included := false

	for _, field := range fields {
		if len(field) == 0 {
			return p, true
		}

		index, ok := partialArrayIndex(field[0])
		if !ok {
			continue
		}

		if p.whole != nil {
			if index >= 0 && index < len(p.whole) {
				ret.Set(index, p.whole[index])
				included = true
			}
			continue
		}

		if value, exists := p.dataSets[index]; exists {
			ret.Set(index, value)
			included = true
			continue
		}

		partial, exists := p.dataMods[index]
		if !exists {
			continue
		}
		if len(field) == 1 {
			ret.ApplyPartial(index, partial)
			included = true
			continue
		}
		filtered, ok := FilterPartialToFields(partial, [][]any{field[1:]})
		if !ok {
			continue
		}
		ret.ApplyPartial(index, filtered)
		included = true
	}

	return ret, included
}

// NewPartialArray builds a PartialArray
func NewPartialArray[V any]() *PartialArray[V] {
	return &PartialArray[V]{
		dataSets: map[int]V{},
		whole:    nil,
	}
}

// PartialArray is a structure to help apply partials on fields of the slice's value
// @restream.serializers
type PartialArray[V any] struct {
	dataSets map[int]V `restream:",notnil"`
	whole    []V
}

var _ Partial = (*PartialArray[int])(nil)

// Set will apply a set for the value of one of the keys of the array
func (p *PartialArray[V]) Set(index int, value V) *PartialArray[V] {
	p.ensureMaps()
	p.dataSets[index] = value
	return p
}

// SetWhole will apply a whole set to replace the entire state
func (p *PartialArray[V]) SetWhole(value []V) *PartialArray[V] {
	p.ensureMaps()
	p.whole = value
	clear(p.dataSets)
	return p
}

func (p *PartialArray[V]) ensureMaps() {
	if p.dataSets == nil {
		p.dataSets = map[int]V{}
	}
}

// MergeOntoPartial merges this partialarray onto another partialarray
func (p *PartialArray[V]) MergeOntoPartial(por any) {
	po := por.(*PartialArray[V])

	// ... feels like this might be a bit of a land mine, but deletes first, then sets..?
	if p.whole != nil {
		po.SetWhole(p.whole)
	}
	for k, v := range p.dataSets {
		po.Set(k, v)
	}
}

// ApplyTo applies the contents of this partialarray onto a full existing value
func (p *PartialArray[V]) ApplyTo(por any) [][]any {
	po, ok := por.(*[]V)
	if !ok {
		po = *por.(**[]V)
	}

	ret := [][]any{}
	if p.whole != nil {
		*po = p.whole
		ret = append(ret, []any{})
	}
	for k, v := range p.dataSets {
		if k < 0 {
			continue
		}
		*po = ensureSliceIndex(*po, k)
		(*po)[k] = v
		if p.whole == nil {
			ret = append(ret, []any{k})
		}
	}
	return ret
}

func ensureSliceIndex[V any](slice []V, index int) []V {
	if index < len(slice) {
		return slice
	}
	ret := make([]V, index+1)
	copy(ret, slice)
	return ret
}

// FilterToFields returns a new partial array containing only changes matching the requested field paths.
func (p *PartialArray[V]) FilterToFields(fields [][]any) (Partial, bool) {
	ret := NewPartialArray[V]()
	included := false

	for _, field := range fields {
		if len(field) == 0 {
			return p, true
		}

		index, ok := partialArrayIndex(field[0])
		if !ok {
			continue
		}

		if p.whole != nil {
			if index >= 0 && index < len(p.whole) {
				ret.Set(index, p.whole[index])
				included = true
			}
			continue
		}

		if value, exists := p.dataSets[index]; exists {
			ret.Set(index, value)
			included = true
		}
	}

	return ret, included
}
