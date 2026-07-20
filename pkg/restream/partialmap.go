package restream

// NewPartialModMap builds a PartialMap for adding to a Partial
func NewPartialModMap[K comparable, V any, P Partial]() *PartialModMap[K, V, P] {
	return &PartialModMap[K, V, P]{
		dataSets:    map[K]V{},
		dataDeletes: map[K]struct{}{},
		dataMods:    map[K]P{},
		whole:       nil,
	}
}

// PartialModMap is a structure to help apply partials on fields of the map's value
// @restream.serializers
type PartialModMap[K comparable, V any, P Partial] struct {
	dataSets    map[K]V        `restream:",notnil"`
	dataDeletes map[K]struct{} `restream:",notnil"`
	dataMods    map[K]P        `restream:",notnil"`
	whole       map[K]V
}

var _ Partial = (*PartialModMap[int, int, *fakePartial])(nil)

// Set will apply a set for the value of one of the keys of the map
func (p *PartialModMap[K, V, PV]) Set(key K, value V) *PartialModMap[K, V, PV] {
	p.ensureMaps()
	p.dataSets[key] = value
	delete(p.dataDeletes, key)
	delete(p.dataMods, key)
	return p
}

// Delete will mark a key for deletion out of the map
func (p *PartialModMap[K, V, PV]) Delete(key K) *PartialModMap[K, V, PV] {
	p.ensureMaps()
	p.dataDeletes[key] = struct{}{}
	delete(p.dataSets, key)
	delete(p.dataMods, key)
	return p
}

// ApplyPartial applies a partial to a value of the map referenced by the partial
func (p *PartialModMap[K, V, PV]) ApplyPartial(key K, partial PV) *PartialModMap[K, V, PV] {
	p.ensureMaps()
	// We will apply sets/deletes, then modifies, so it _should_ just need storing the partial mods...
	if po, has := p.dataMods[key]; has {
		partial.MergeOntoPartial(po)
		p.dataMods[key] = po
	} else {
		p.dataMods[key] = partial
	}
	return p
}

// SetWhole will apply a whole set to replace the entire state
func (p *PartialModMap[K, V, PV]) SetWhole(value map[K]V) *PartialModMap[K, V, PV] {
	p.ensureMaps()
	p.whole = value
	clear(p.dataDeletes)
	clear(p.dataMods)
	clear(p.dataSets)
	return p
}

func (p *PartialModMap[K, V, PV]) ensureMaps() {
	if p.dataSets == nil {
		p.dataSets = map[K]V{}
	}
	if p.dataDeletes == nil {
		p.dataDeletes = map[K]struct{}{}
	}
	if p.dataMods == nil {
		p.dataMods = map[K]PV{}
	}
}

// MergeOntoPartial merges this partialmap onto another partialmap
func (p *PartialModMap[K, V, PV]) MergeOntoPartial(por any) {
	po := por.(*PartialModMap[K, V, PV])

	if p.whole != nil {
		po.SetWhole(p.whole)
	}
	// ... feels like this might be a bit of a land mine, but deletes first, then sets..?
	for k := range p.dataDeletes {
		po.Delete(k)
	}
	for k, v := range p.dataSets {
		po.Set(k, v)
	}
	for k, v := range p.dataMods {
		po.ApplyPartial(k, v)
	}
}

// PruneAgainst removes operations that would not change the target map and reports whether any remain.
func (p *PartialModMap[K, V, PV]) PruneAgainst(por any) bool {
	if p == nil {
		return false
	}
	po, ok := por.(*map[K]V)
	if !ok {
		po = *por.(**map[K]V)
	}

	if p.whole != nil && ValuesEqual(*po, p.whole) {
		p.whole = nil
	}
	if p.whole != nil {
		return true
	}
	for k, v := range p.dataSets {
		if current, exists := (*po)[k]; exists && ValuesEqual(current, v) {
			delete(p.dataSets, k)
		}
	}
	for k := range p.dataDeletes {
		if _, exists := (*po)[k]; !exists {
			delete(p.dataDeletes, k)
		}
	}
	for k, pv := range p.dataMods {
		cv := (*po)[k]
		if !PrunePartialAgainst(pv, &cv) {
			delete(p.dataMods, k)
		}
	}
	return len(p.dataSets) > 0 || len(p.dataDeletes) > 0 || len(p.dataMods) > 0
}

// ApplyTo prunes and applies the contents of this partial map.
func (p *PartialModMap[K, V, PV]) ApplyTo(por any) [][]any {
	if !p.PruneAgainst(por) {
		return [][]any{}
	}
	po, ok := por.(*map[K]V)
	if !ok {
		po = *por.(**map[K]V)
	}

	ret := [][]any{}
	if p.whole != nil {
		*po = p.whole
		ret = append(ret, []any{})
	}
	if *po == nil && (len(p.dataSets) > 0 || len(p.dataMods) > 0) {
		*po = map[K]V{}
	}
	for k, v := range p.dataSets {
		(*po)[k] = v
		if p.whole == nil {
			ret = append(ret, []any{k})
		}
	}
	for k := range p.dataDeletes {
		delete(*po, k)
		if p.whole == nil {
			ret = append(ret, []any{k})
		}
	}
	for k, pv := range p.dataMods {
		cv := (*po)[k]
		fs := pv.ApplyTo(&cv)
		if len(fs) == 0 {
			delete(p.dataMods, k)
			continue
		}
		if *po == nil {
			*po = map[K]V{}
		}
		(*po)[k] = cv
		if p.whole == nil {
			for _, f := range fs {
				ret = append(ret, append([]any{k}, f...))
			}
		}
	}
	return ret
}

// FilterToFields returns a new partial map containing only changes matching the requested field paths.
func (p *PartialModMap[K, V, PV]) FilterToFields(fields [][]any) (Partial, bool) {
	ret := NewPartialModMap[K, V, PV]()
	included := false

	for _, field := range fields {
		if len(field) == 0 {
			return p, true
		}

		key, ok := partialFieldKey[K](field[0])
		if !ok {
			continue
		}

		if p.whole != nil {
			if value, exists := p.whole[key]; exists {
				ret.Set(key, value)
			} else {
				ret.Delete(key)
			}
			included = true
			continue
		}

		if value, exists := p.dataSets[key]; exists {
			ret.Set(key, value)
			included = true
			continue
		}

		if _, exists := p.dataDeletes[key]; exists {
			ret.Delete(key)
			included = true
			continue
		}

		partial, exists := p.dataMods[key]
		if !exists {
			continue
		}
		if len(field) == 1 {
			ret.ApplyPartial(key, partial)
			included = true
			continue
		}
		filtered, ok := FilterPartialToFields(partial, [][]any{field[1:]})
		if !ok {
			continue
		}
		ret.ApplyPartial(key, filtered)
		included = true
	}

	return ret, included
}

// NewPartialMap builds a PartialMap for adding to a Partial
func NewPartialMap[K comparable, V any]() *PartialMap[K, V] {
	return &PartialMap[K, V]{
		dataSets:    map[K]V{},
		dataDeletes: map[K]struct{}{},
		whole:       nil,
	}
}

// PartialMap is a structure to help apply partials on fields of the map's value
// @restream.serializers
type PartialMap[K comparable, V any] struct {
	dataSets    map[K]V        `restream:",notnil"`
	dataDeletes map[K]struct{} `restream:",notnil"`
	whole       map[K]V
}

var _ Partial = (*PartialMap[int, int])(nil)

// Set will apply a set for the value of one of the keys of the map
func (p *PartialMap[K, V]) Set(key K, value V) *PartialMap[K, V] {
	p.ensureMaps()
	p.dataSets[key] = value
	delete(p.dataDeletes, key)
	return p
}

// Delete will mark a key for deletion out of the map
func (p *PartialMap[K, V]) Delete(key K) *PartialMap[K, V] {
	p.ensureMaps()
	p.dataDeletes[key] = struct{}{}
	delete(p.dataSets, key)
	return p
}

// SetWhole will apply a whole set to replace the entire state
func (p *PartialMap[K, V]) SetWhole(value map[K]V) *PartialMap[K, V] {
	p.ensureMaps()
	p.whole = value
	clear(p.dataDeletes)
	clear(p.dataSets)
	return p
}

func (p *PartialMap[K, V]) ensureMaps() {
	if p.dataSets == nil {
		p.dataSets = map[K]V{}
	}
	if p.dataDeletes == nil {
		p.dataDeletes = map[K]struct{}{}
	}
}

// MergeOntoPartial merges this partialmap onto another partialmap
func (p *PartialMap[K, V]) MergeOntoPartial(por any) {
	po := por.(*PartialMap[K, V])

	if p.whole != nil {
		po.SetWhole(p.whole)
	}
	// ... feels like this might be a bit of a land mine, but deletes first, then sets..?
	for k := range p.dataDeletes {
		po.Delete(k)
	}
	for k, v := range p.dataSets {
		po.Set(k, v)
	}
}

// PruneAgainst removes operations that would not change the target map and reports whether any remain.
func (p *PartialMap[K, V]) PruneAgainst(por any) bool {
	if p == nil {
		return false
	}
	po, ok := por.(*map[K]V)
	if !ok {
		po = *por.(**map[K]V)
	}

	if p.whole != nil && ValuesEqual(*po, p.whole) {
		p.whole = nil
	}
	if p.whole != nil {
		return true
	}
	for k, v := range p.dataSets {
		if current, exists := (*po)[k]; exists && ValuesEqual(current, v) {
			delete(p.dataSets, k)
		}
	}
	for k := range p.dataDeletes {
		if _, exists := (*po)[k]; !exists {
			delete(p.dataDeletes, k)
		}
	}
	return len(p.dataSets) > 0 || len(p.dataDeletes) > 0
}

// ApplyTo prunes and applies the contents of this partial map.
func (p *PartialMap[K, V]) ApplyTo(por any) [][]any {
	if !p.PruneAgainst(por) {
		return [][]any{}
	}
	po, ok := por.(*map[K]V)
	if !ok {
		po = *por.(**map[K]V)
	}

	ret := [][]any{}
	if p.whole != nil {
		*po = p.whole
		ret = append(ret, []any{})
	}
	if *po == nil && len(p.dataSets) > 0 {
		*po = map[K]V{}
	}
	for k, v := range p.dataSets {
		(*po)[k] = v
		if p.whole == nil {
			ret = append(ret, []any{k})
		}
	}
	for k := range p.dataDeletes {
		delete(*po, k)
		if p.whole == nil {
			ret = append(ret, []any{k})
		}
	}
	return ret
}

// FilterToFields returns a new partial map containing only changes matching the requested field paths.
func (p *PartialMap[K, V]) FilterToFields(fields [][]any) (Partial, bool) {
	ret := NewPartialMap[K, V]()
	included := false

	for _, field := range fields {
		if len(field) == 0 {
			return p, true
		}

		key, ok := partialFieldKey[K](field[0])
		if !ok {
			continue
		}

		if p.whole != nil {
			if value, exists := p.whole[key]; exists {
				ret.Set(key, value)
			} else {
				ret.Delete(key)
			}
			included = true
			continue
		}

		if value, exists := p.dataSets[key]; exists {
			ret.Set(key, value)
			included = true
			continue
		}

		if _, exists := p.dataDeletes[key]; exists {
			ret.Delete(key)
			included = true
		}
	}

	return ret, included
}
