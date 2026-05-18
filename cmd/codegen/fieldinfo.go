//nolint:goconst
package main

import (
	"fmt"
	"go/token"
	"go/types"
	"slices"
	"strconv"
	"strings"

	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/dave/dst"
	"github.com/fatih/structtag"
	"github.com/samber/lo"
)

// restreamTag is the tag name for restream data stored on struct tags
const restreamTag = "restream"

// fieldIDSubTag is the subtag name for the field ID
const fieldIDSubTag = "fID"

// notNilSubTag is the subtag name for the not nil flag
const notNilSubTag = "notnil"

// valueNotNilSubTag is the subtag name for the value not nil flag
const valueNotNilSubTag = "valnotnil"

// getOrGenerateTagInfo is a helper to get or generate a tagInfo for a struct field, returning if it was generated
func (ft *FileTracking) getOrGenerateTagInfo(fd *dst.Field, idx int, maxFieldNum *byte) (*restream.FieldInfo, bool, error) {
	ti, err := ft.getFieldInfo(fd, idx)
	if err != nil {
		return nil, false, err
	}

	if maxFieldNum == nil && ti.FieldID == 0 {
		return nil, false, fmt.Errorf("getOrGenerateTagInfo generated changes with nil maxFieldNum")
	}

	changed := false
	if ti.FieldID == 0 {
		changed = true
		(*maxFieldNum)++
		ti.FieldID = *maxFieldNum
	}

	return ti, changed, nil
}

// getFieldInfo gets the field info for a field, without generating a new one, returning a bool to indicate if the field info was changed
func (ft *FileTracking) getFieldInfo(fd *dst.Field, idx int) (*restream.FieldInfo, error) {
	fi := &restream.FieldInfo{
		Name:     fd.Names[0].Name,
		FieldIdx: idx,
	}

	tagStr := ""
	if fd.Tag != nil {
		tagStr = strings.Trim(fd.Tag.Value, "`")
	}

	tags, err := structtag.Parse(tagStr)
	if err != nil {
		return nil, err
	}

	notNil := false
	valueNotNil := false
	if tag, err := tags.Get(restreamTag); err == nil {
		for _, opt := range tag.Options {
			parts := strings.Split(opt, "=")
			switch parts[0] {
			case fieldIDSubTag:
				tv, err := strconv.ParseUint(parts[1], 10, 8)
				if err != nil {
					return nil, err
				}
				fi.FieldID = byte(tv)
			case notNilSubTag:
				notNil = true
			case valueNotNilSubTag:
				valueNotNil = true
			}
		}
	}

	fi.VarInfo, err = ft.getVarInfo(fd.Type, notNil, valueNotNil)
	if err != nil {
		return nil, err
	}

	return fi, nil
}

// getVarInfo calcualtes a VarInfo for an AST field
func (ft *FileTracking) getVarInfo(fd dst.Expr, notNil, valueNotNil bool) (restream.VarInfo, error) {
	switch fdt := fd.(type) {
	case *dst.Ident:
		switch fdt.Name {
		case "string":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString}, nil
		case "int":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt64, MappedType: restream.Ptr("int")}, nil
		case "int8":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt8}, nil
		case "int16":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt16}, nil
		case "int32":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt32}, nil
		case "int64":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt64}, nil
		case "uint":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeUint64, MappedType: restream.Ptr("uint")}, nil
		case "uint8":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeUint8}, nil
		case "byte":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeUint8, MappedType: restream.Ptr("byte")}, nil
		case "uint16":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeUint16}, nil
		case "uint32":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeUint32}, nil
		case "uint64":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeUint64}, nil
		case "float32":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeFloat32}, nil
		case "float64":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeFloat64}, nil
		case "bool":
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeBool}, nil
		case "any":
			return &restream.VarInfoDynamic{}, nil
		default:
			// check for mapped types
			vi, err := ft.getUnderlyingVarInfo(fdt, notNil, valueNotNil)
			if err != nil {
				return nil, err
			}
			if vi != nil {
				return vi, nil
			}

			// So far, field types seem to be generic params only
			if fdt.Obj != nil && fdt.Obj.Decl != nil {
				if _, is := fdt.Obj.Decl.(*dst.Field); is {
					return &restream.VarInfoGenericParam{Name: fdt.Name}, nil
				}
			}
			// ... and everything else is a struct
			return &restream.VarInfoStruct{Name: fdt.Name, Package: ft.fPackage.Name}, nil
		}
	case *dst.InterfaceType:
		return &restream.VarInfoDynamic{}, nil
	case *dst.StarExpr:
		se, err := ft.getVarInfo(fdt.X, valueNotNil, false)
		if err != nil {
			return nil, err
		}
		return &restream.VarInfoPointer{SubType: se, NotNil: notNil}, nil
	case *dst.ArrayType:
		se, err := ft.getVarInfo(fdt.Elt, valueNotNil, false)
		if err != nil {
			return nil, err
		}
		return &restream.VarInfoArray{ElemType: se, NotNil: notNil}, nil
	case *dst.MapType:
		kt, err := ft.getVarInfo(fdt.Key, false, false)
		if err != nil {
			return nil, err
		}
		vt, err := ft.getVarInfo(fdt.Value, valueNotNil, false)
		if err != nil {
			return nil, err
		}
		return &restream.VarInfoMap{KeyType: kt, ElemType: vt, NotNil: notNil}, nil
	case *dst.SelectorExpr:
		pn := ft.getPackagedName(fdt)
		if pn == "time.Time" {
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeTime}, nil
		}

		// check for mapped types
		vi, err := ft.getUnderlyingVarInfo(fdt, notNil, valueNotNil)
		if err != nil {
			return nil, err
		}
		if vi != nil {
			return vi, nil
		}

		return &restream.VarInfoStruct{
			Name:    fdt.Sel.Name,
			Package: fdt.X.(*dst.Ident).Name,
		}, nil
	case *dst.IndexListExpr:
		visr, err := ft.getVarInfo(fdt.X, notNil, valueNotNil)
		if err != nil {
			return nil, err
		}
		vis := visr.(*restream.VarInfoStruct)
		vis.GenericTypes = lo.Map(fdt.Indices, func(ix dst.Expr, _ int) restream.VarInfo {
			vi, err := ft.getVarInfo(ix, false, false)
			if err != nil {
				panic(err)
			}
			return vi
		})
		return vis, nil
	case *dst.StructType:
		if len(fdt.Fields.List) == 0 {
			// void struct
			return nil, nil
		}
		fields := lo.Map(fdt.Fields.List, func(fd *dst.Field, idx int) restream.FieldInfo {
			fdi, err := ft.getFieldInfo(fd, idx)
			if err != nil {
				panic(err)
			}
			return *fdi
		})
		return &restream.VarInfoStruct{FieldList: fields}, nil
	default:
		return nil, fmt.Errorf("unsupported type in ft.getVarInfo: %T", fd)
	}
}

// genTagString generates a tag string for a FieldInfo
func genTagString(fd *dst.Field, ti *restream.FieldInfo) (string, error) {
	tag := &structtag.Tag{
		Key:     restreamTag,
		Name:    "",
		Options: []string{},
	}

	if ti.FieldID != 0 {
		tag.Options = append(tag.Options, fieldIDSubTag+"="+strconv.FormatUint(uint64(ti.FieldID), 10))
	}
	if ti.VarInfo.IsNotNil() {
		tag.Options = append(tag.Options, notNilSubTag)
	}
	if ti.VarInfo.IsValueNotNil() {
		tag.Options = append(tag.Options, valueNotNilSubTag)
	}

	tagStr := ""
	if fd.Tag != nil {
		tagStr = strings.Trim(fd.Tag.Value, "`")
	}

	tags, err := structtag.Parse(tagStr)
	if err != nil {
		return "", err
	}

	if err := tags.Set(tag); err != nil {
		return "", err
	}

	return "`" + tags.String() + "`", nil
}

// getTSFieldName calculates a field name to convert from golang uppercase names to javascript lowerCamelCase
func getTSFieldName(fi *restream.FieldInfo) string {
	n := fi.Name
	for i := range len(n) {
		if n[i:i+1] == strings.ToLower(n[i:i+1]) {
			if i > 1 {
				return strings.ToLower(n[0:i-1]) + n[i-1:]
			}
			return strings.ToLower(n[0:i]) + n[i:]
		}
	}
	return strings.ToLower(n)
}

// genFieldInfo generates FieldInfo structs for a list of fields from the AST
func (ft *FileTracking) genFieldInfo(fieldList []*dst.Field) ([]*restream.FieldInfo, error) {
	fields := make([]*restream.FieldInfo, len(fieldList))
	for idx, fd := range fieldList {
		fi, err := ft.getFieldInfo(fd, idx)
		if err != nil {
			return nil, err
		}
		fields[idx] = fi
	}
	return fields, nil
}

// genFieldInfoForType generates FieldInfo structs for a list of fields from the packages AST
func (pt *ProjTracking) genFieldInfoForType(t *types.TypeName) ([]*restream.FieldInfo, error) {
	st := t.Type().Underlying().(*types.Struct)
	fields := make([]*restream.FieldInfo, st.NumFields())
	for i := 0; i < st.NumFields(); i++ {
		fd := st.Field(i)
		vi, err := pt.getVarInfoForType(fd.Type())
		if err != nil {
			return nil, err
		}

		fi := &restream.FieldInfo{
			Name:     fd.Name(),
			FieldIdx: i,
			VarInfo:  vi,
		}

		fields[i] = fi
	}
	return fields, nil
}

// getVarInfoForType calcualtes the VarInfo for a packages AST type
func (pt *ProjTracking) getVarInfoForType(t types.Type) (restream.VarInfo, error) {
	switch ot := t.(type) {
	case *types.Basic:
		switch ot.Kind() {
		case types.Bool:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeBool}, nil
		case types.String:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString}, nil
		case types.Int:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt64, MappedType: restream.Ptr("int")}, nil
		case types.Int8:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt8}, nil
		case types.Int16:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt16}, nil
		case types.Int32:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt32}, nil
		case types.Int64:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt64}, nil
		case types.Uint:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeUint64, MappedType: restream.Ptr("uint")}, nil
		case types.Uint8:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeUint8}, nil
		case types.Uint16:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeUint16}, nil
		case types.Uint32:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeUint32}, nil
		case types.Uint64:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeUint64}, nil
		case types.Float32:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeFloat32}, nil
		case types.Float64:
			return &restream.VarInfoPrimitive{DataType: restream.SerializationTypeFloat64}, nil
		default:
			return nil, fmt.Errorf("unsupported basic type in getVarInfoForType: %s", ot.String())
		}
	case *types.Named:
		vi, err := pt.getVarInfoForType(ot.Underlying())
		if err != nil {
			return nil, err
		}
		switch vit := vi.(type) {
		case *restream.VarInfoPrimitive:
			vit.MappedType = restream.Ptr(ot.Obj().Pkg().Name() + "." + ot.Obj().Name())
		default:
			return nil, fmt.Errorf("unsupported mapped type in getVarInfoForType: %T", vi)
		}
		return vi, nil
	default:
		return nil, fmt.Errorf("unsupported type in getVarInfoForType: %s", t.String())
	}
}

// supportsPartials checks if a VarInfo supports partials
func (ft *FileTracking) supportsPartials(vi restream.VarInfo) bool {
	switch vit := vi.(type) {
	case *restream.VarInfoMap, *restream.VarInfoArray, *restream.VarInfoPrimitive, *restream.VarInfoDynamic:
		return false
	case *restream.VarInfoStruct:
		switch vit.Name {
		case "PartialValue", "PartialModMap", "PartialMap", "PartialModArray", "PartialArray":
			return true
		default:
			return ft.shouldBuildPartial(vit.Name)
		}
	case *restream.VarInfoPointer:
		return ft.supportsPartials(vit.SubType)
	default:
		panic(fmt.Sprintf("unhandled type in supportsPartials: %T", vi))
	}
}

// genPartialFieldInfo transforms fields into a partial -- add pointers to everything needed to make it optional,
// and convert maps/arrays to PartialMap/PartialArray, also some values to PartialValue
func (ft *FileTracking) genPartialFieldInfo(fieldList []*dst.Field) ([]*restream.FieldInfo, error) {
	partialFields := lo.Map(fieldList, func(fd *dst.Field, _ int) *dst.Field {
		var ot dst.Expr
		switch ott := fd.Type.(type) {
		case *dst.MapType:
			// Convert to PartialMap
			vi, err := ft.getVarInfo(ott.Value, false, false)
			if err != nil {
				panic(err)
			}
			partial := ft.supportsPartials(vi)

			tn := "PartialMap"
			inds := []dst.Expr{ott.Key, ott.Value}
			if partial {
				tn = "PartialModMap"
				pn := getPartialName(ott.Value)
				inds = append(inds, &dst.StarExpr{X: &dst.Ident{Name: pn}})
			}
			ot = &dst.StarExpr{X: &dst.IndexListExpr{
				X: &dst.SelectorExpr{X: &dst.Ident{Name: "restream"}, Sel: &dst.Ident{Name: tn}}, Indices: inds}}
		case *dst.ArrayType:
			// Convert to PartialArray
			vi, err := ft.getVarInfo(ott.Elt, false, false)
			if err != nil {
				panic(err)
			}
			partial := ft.supportsPartials(vi)

			tn := "PartialArray"
			inds := []dst.Expr{ott.Elt}
			if partial {
				tn = "PartialModArray"
				pn := getPartialName(ott.Elt)
				inds = append(inds, &dst.StarExpr{X: &dst.Ident{Name: pn}})
			}
			ot = &dst.StarExpr{X: &dst.IndexListExpr{
				X: &dst.SelectorExpr{X: &dst.Ident{Name: "restream"}, Sel: &dst.Ident{Name: tn}}, Indices: inds}}
		default:
			vi, err := ft.getVarInfo(ott, false, false)
			if err != nil {
				panic(err)
			}

			partial := ft.supportsPartials(vi)
			if partial {
				// Convert to PartialValue
				vt := ott
				pn := getPartialName(ott)
				ot = &dst.IndexListExpr{
					X: &dst.SelectorExpr{X: &dst.Ident{Name: "restream"}, Sel: &dst.Ident{Name: "PartialValue"}},
					Indices: []dst.Expr{
						vt,
						&dst.StarExpr{X: &dst.Ident{Name: pn}},
					},
				}
			} else {
				ot = ott
			}

			ot = &dst.StarExpr{X: ot}
		}

		// strip off notnil tag
		tag := fd.Tag
		if tag != nil {
			tagStr := strings.Trim(tag.Value, "`")
			tags, err := structtag.Parse(tagStr)
			if err != nil {
				panic(err)
			}
			if t, err := tags.Get(restreamTag); err == nil {
				if slices.Contains(t.Options, notNilSubTag) {
					t.Options = slices.DeleteFunc(t.Options, func(s string) bool {
						return s == notNilSubTag
					})
					tag = &dst.BasicLit{Kind: token.STRING, Value: "`" + tags.String() + "`"}
				}
			}
		}

		return &dst.Field{
			Names: fd.Names,
			Tag:   tag,
			Decs:  fd.Decs,
			Type:  ot,
		}
	})
	return ft.genFieldInfo(partialFields)
}

// getPackagedName gets the full name of a type (i.e. restream.PartialMap)
func (ft *FileTracking) getPackagedName(t dst.Expr) string {
	switch tt := t.(type) {
	case *dst.Ident:
		return ft.fPackage.Name + "." + tt.Name
	case *dst.SelectorExpr:
		return tt.X.(*dst.Ident).Name + "." + tt.Sel.Name
	}
	panic(fmt.Sprintf("unhandled type in getPackagedName: %T", t))
}

// getPartialName gets the name of a type with "Partial" appended (i.e. PartialMap)
func getPartialName(t dst.Expr) string {
	switch tt := t.(type) {
	case *dst.Ident:
		return tt.Name + "Partial"
	case *dst.SelectorExpr:
		return tt.Sel.Name + "Partial"
	case *dst.StarExpr:
		return getPartialName(tt.X)
	}
	panic(fmt.Sprintf("unhandled type in getPartialName: %T", t))
}
