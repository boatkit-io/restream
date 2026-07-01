package restream

// PartialValue is a structure to help apply partials/whole replacements to individual fields of a partial
// @restream.serializers
type PartialValue[V any, P Partial] struct {
	whole   *V
	partial *P
}

var _ Partial = (*PartialValue[*fakeStruct, *fakePartial])(nil)

// SetWhole will apply a whole replacement to the partial's field
func (p *PartialValue[V, P]) SetWhole(value *V) *PartialValue[V, P] {
	p.whole = value
	p.partial = nil
	return p
}

// ApplyPartial applies a partial to the field
func (p *PartialValue[V, P]) ApplyPartial(partial P) *PartialValue[V, P] {
	if p.partial == nil {
		p.partial = &partial
	} else {
		partial.MergeOntoPartial(*p.partial)
	}
	return p
}

// MergeOntoPartial merges this partialvalue onto another partialvalue
func (p *PartialValue[V, P]) MergeOntoPartial(por any) {
	po := por.(*PartialValue[V, P])

	if p.whole != nil {
		po.SetWhole(p.whole)
	}
	if p.partial != nil {
		po.ApplyPartial(*p.partial)
	}
}

// ApplyTo applies the contents of this partialvalue onto a full existing value
func (p *PartialValue[V, P]) ApplyTo(por any) [][]any {
	ret := [][]any{}
	if p.whole != nil {
		if po, ok := por.(*V); ok {
			*po = *p.whole
		} else {
			*por.(**V) = p.whole
		}
		ret = append(ret, []any{})
	}
	if p.partial != nil {
		reti := (*p.partial).ApplyTo(por)
		ret = append(ret, reti...)
	}
	return ret
}

// FilterToFields returns a new partial value containing only changes matching the requested field paths.
func (p *PartialValue[V, P]) FilterToFields(fields [][]any) (Partial, bool) {
	for _, field := range fields {
		if len(field) == 0 {
			return p, true
		}
	}

	if p.partial == nil {
		return nil, false
	}

	filtered, ok := FilterPartialToFields(*p.partial, fields)
	if !ok {
		return nil, false
	}
	return (&PartialValue[V, P]{}).ApplyPartial(filtered), true
}
