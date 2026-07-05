// nolint:goconst
package main

import (
	"fmt"
	"go/types"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/dave/dst/decorator"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"golang.org/x/tools/go/packages"
)

// createGoStructSerializers is a helper to look through the parsed structures and generate golang-based serialization
// and partial structures (where useful)
func (ft *FileTracking) createGoStructSerializers(si StructInfo, fields []*restream.FieldInfo, partialFields []*restream.FieldInfo) error {
	// Calculate main struct serialization/deserialization functions
	if si.Fielded {
		outSerialization := createGolangStructFieldedSerializers(si, fields)
		outClone, err := ft.createGolangStructCloner(si, fields)
		if err != nil {
			return err
		}
		outSerialization += outClone
		ft.goGenEntries = append(ft.goGenEntries, fdef{name: si.Name, defs: outSerialization})
	} else {
		outSerialization := createGolangStructNonFieldedSerializers(si, fields, true)
		outClone, err := ft.createGolangStructCloner(si, fields)
		if err != nil {
			return err
		}
		outSerialization += outClone
		ft.goGenEntries = append(ft.goGenEntries, fdef{name: si.Name, defs: outSerialization})
	}

	if ft.shouldBuildPartial(si.Name) {
		if si.GenericParams != nil {
			return fmt.Errorf("generic types not supported for partials")
		}

		// Calculate partials for all main structs, including serialization/deserialization, apply, and merge functions
		outPartial := ""
		outPartial += fmt.Sprintf("// %sPartial is a partial struct for %s\n", si.Name, si.Name)
		outPartial += fmt.Sprintf("type %sPartial struct {\n", si.Name)
		for _, fi := range partialFields {
			outPartial += fmt.Sprintf("    %s %s\n", fi.Name, ft.getGolangTypeName(fi.VarInfo))
		}
		outPartial += "}" + "\n\n"

		outPartial += "// MergeOntoPartial merges this partial onto another partial\n"
		outPartial += fmt.Sprintf("func (s *%sPartial) MergeOntoPartial(por any) {\n", si.Name)
		outPartial += fmt.Sprintf("    po := por.(*%sPartial)\n", si.Name)
		for _, fi := range partialFields {
			fn := fi.Name
			outPartial += fmt.Sprintf("    if s.%s != nil {\n", fn)
			if ft.supportsPartials(fi.VarInfo) {
				outPartial += fmt.Sprintf("        if po.%s == nil { po.%s = s.%s } else { s.%s.MergeOntoPartial(po.%s) }\n",
					fn, fn, fn, fn, fn)
			} else {
				outPartial += fmt.Sprintf("        po.%s = s.%s\n", fn, fn)
			}
			outPartial += "    }\n"
		}
		outPartial += "}\n" + "\n"

		outPartial += "// ApplyTo applies this partial to the full version of the struct\n"
		outPartial += fmt.Sprintf("func (s *%sPartial) ApplyTo(por any) [][]any {\n", si.Name)
		outPartial += fmt.Sprintf("    po, ok := por.(*%s)\n", si.Name)
		outPartial += "    if !ok {\n"
		outPartial += fmt.Sprintf("        pop := por.(**%s)\n", si.Name)
		outPartial += fmt.Sprintf("        if *pop == nil { *pop = &%s{} }\n", si.Name)
		outPartial += "        po = *pop\n"
		outPartial += "    }\n"
		outPartial += "    ret := [][]any{}\n"
		for idx, pfi := range partialFields {
			fi := fields[idx]

			fn := pfi.Name
			isPartial := ft.supportsPartials(pfi.VarInfo)
			isDirectAssign := false
			switch fi.VarInfo.(type) {
			case *restream.VarInfoMap, *restream.VarInfoArray:
				isDirectAssign = true
			}

			outPartial += fmt.Sprintf("    if s.%s != nil {\n", fn)
			if isPartial {
				outPartial += fmt.Sprintf("        fs := s.%s.ApplyTo(&po.%s)\n", fn, fn)
				outPartial += "        for _, f := range fs {\n"
				outPartial += fmt.Sprintf("            ret = append(ret, append(append([]any{}, \"%s\"), f...))\n", fn)
				outPartial += "        }\n"
			} else {
				if isDirectAssign {
					outPartial += fmt.Sprintf("        po.%s = s.%s\n", fn, fn)
				} else {
					ptn := ft.getGolangTypeName(pfi.VarInfo.(*restream.VarInfoPointer).SubType)
					tn := ft.getGolangTypeName(fi.VarInfo)
					if ptn == tn {
						outPartial += fmt.Sprintf("        po.%s = *s.%s\n", fn, fn)
					} else {
						outPartial += fmt.Sprintf("        po.%s = %s(*s.%s)\n", fn, tn, fn)
					}
				}
				outPartial += fmt.Sprintf("        ret = append(ret, []any{\"%s\"})\n", fn)
			}
			outPartial += "    }\n"
		}
		outPartial += "    return ret\n"
		outPartial += "}\n"

		outPartial += "\n"
		outPartial += "// FilterToFields returns a new partial containing only changes matching the requested field paths\n"
		outPartial += fmt.Sprintf("func (s *%sPartial) FilterToFields(fields [][]any) (restream.Partial, bool) {\n", si.Name)
		outPartial += fmt.Sprintf("    ret := &%sPartial{}\n", si.Name)
		outPartial += "    included := false\n"
		outPartial += "    for _, field := range fields {\n"
		outPartial += "        if len(field) == 0 { return s, true }\n"
		outPartial += "    }\n"
		for _, pfi := range partialFields {
			fn := pfi.Name
			outPartial += fmt.Sprintf("    if s.%s != nil {\n", fn)
			outPartial += fmt.Sprintf("        childFields := restream.ChildFieldsForField(fields, \"%s\")\n", fn)
			outPartial += "        if len(childFields) > 0 {\n"
			if ft.supportsPartials(pfi.VarInfo) {
				outPartial += fmt.Sprintf("            filtered, ok := restream.FilterPartialToFields(s.%s, childFields)\n", fn)
				outPartial += "            if ok {\n"
				outPartial += fmt.Sprintf("                ret.%s = filtered\n", fn)
				outPartial += "                included = true\n"
				outPartial += "            }\n"
			} else {
				outPartial += fmt.Sprintf("            ret.%s = s.%s\n", fn, fn)
				outPartial += "            included = true\n"
			}
			outPartial += "        }\n"
			outPartial += "    }\n"
		}
		outPartial += "    return ret, included\n"
		outPartial += "}\n"

		outPartial += ft.createGolangPartialForFields(si, fields, partialFields)

		sip := si
		sip.Name += "Partial"

		outPartial += createGolangStructFieldedSerializers(sip, partialFields)

		ft.goGenEntries = append(ft.goGenEntries, fdef{name: sip.Name, defs: outPartial})
	}

	return nil
}

func (ft *FileTracking) createGolangPartialForFields(
	si StructInfo,
	fields []*restream.FieldInfo,
	partialFields []*restream.FieldInfo,
) string {
	out := "\n"
	out += "// PartialForFields returns a snapshot partial containing the requested field paths\n"
	out += fmt.Sprintf("func (s *%s) PartialForFields(fields [][]any) (restream.Partial, bool) {\n", si.Name)
	out += fmt.Sprintf("    ret := &%sPartial{}\n", si.Name)
	out += "    included := false\n"
	for _, pfi := range partialFields {
		out += fmt.Sprintf("    if partial, ok := s.partialForFields%s(restream.ChildFieldsForField(fields, %q)); ok {\n", pfi.Name, pfi.Name)
		out += fmt.Sprintf("        ret.%s = partial\n", pfi.Name)
		out += "        included = true\n"
		out += "    }\n"
	}
	out += "    return ret, included\n"
	out += "}\n\n"

	for idx, pfi := range partialFields {
		out += ft.createGolangFieldPartialForFields(si, fields[idx], pfi)
	}
	return out
}

func (ft *FileTracking) createGolangFieldPartialForFields(
	si StructInfo,
	fi *restream.FieldInfo,
	pfi *restream.FieldInfo,
) string {
	switch fi.VarInfo.(type) {
	case *restream.VarInfoMap:
		return ft.createGolangMapPartialForFields(si, fi, pfi)
	case *restream.VarInfoArray:
		return ft.createGolangArrayPartialForFields(si, fi, pfi)
	default:
		if ft.supportsPartials(pfi.VarInfo) {
			return ft.createGolangPartialValueForFields(si, fi, pfi)
		}
		return ft.createGolangDirectPartialForFields(si, fi, pfi)
	}
}

func (ft *FileTracking) createGolangDirectPartialForFields(
	si StructInfo,
	fi *restream.FieldInfo,
	pfi *restream.FieldInfo,
) string {
	retType := ft.getGolangTypeName(pfi.VarInfo)
	valueType := ft.getGolangTypeName(fi.VarInfo)
	out := fmt.Sprintf("func (s *%s) partialForFields%s(fields [][]any) (%s, bool) {\n", si.Name, fi.Name, retType)
	out += "    if len(fields) == 0 { return nil, false }\n"
	out += "    for _, field := range fields {\n"
	out += "        if len(field) == 0 {\n"
	if expr, ok := ft.golangSimpleCloneExpr(fi.VarInfo, fmt.Sprintf("s.%s", fi.Name)); ok {
		out += fmt.Sprintf("            cloned := %s\n", expr)
	} else {
		out += fmt.Sprintf("            var cloned %s\n", valueType)
		out += ft.mustGolangCloneAssign(fi.VarInfo, fmt.Sprintf("s.%s", fi.Name), "cloned", "            ", 0)
	}
	out += "            return restream.Ptr(cloned), true\n"
	out += "        }\n"
	out += "    }\n"
	out += "    return nil, false\n"
	out += "}\n\n"
	return out
}

func (ft *FileTracking) createGolangPartialValueForFields(
	si StructInfo,
	fi *restream.FieldInfo,
	pfi *restream.FieldInfo,
) string {
	retType := ft.getGolangTypeName(pfi.VarInfo)
	partialStruct := mustPartialStructInfo(pfi.VarInfo)
	valueType := ft.getGolangTypeName(partialStruct.GenericTypes[0])
	partialType := ft.getGolangTypeName(partialStruct.GenericTypes[1])
	fieldIsPointer := isPointerVarInfo(fi.VarInfo)

	out := fmt.Sprintf("func (s *%s) partialForFields%s(fields [][]any) (%s, bool) {\n", si.Name, fi.Name, retType)
	out += "    if len(fields) == 0 { return nil, false }\n"
	out += fmt.Sprintf("    ret := &restream.PartialValue[%s, %s]{}\n", valueType, partialType)
	out += "    included := false\n"
	out += "    for _, field := range fields {\n"
	out += "        if len(field) == 0 {\n"
	if expr, ok := ft.golangSimpleCloneExpr(fi.VarInfo, fmt.Sprintf("s.%s", fi.Name)); ok {
		out += fmt.Sprintf("            cloned := %s\n", expr)
	} else {
		out += fmt.Sprintf("            var cloned %s\n", ft.getGolangTypeName(fi.VarInfo))
		out += ft.mustGolangCloneAssign(fi.VarInfo, fmt.Sprintf("s.%s", fi.Name), "cloned", "            ", 0)
	}
	out += "            return ret.SetWhole(restream.Ptr(cloned)), true\n"
	out += "        }\n"
	if fieldIsPointer {
		out += fmt.Sprintf("        if s.%s == nil {\n", fi.Name)
		out += fmt.Sprintf("            var cloned %s\n", ft.getGolangTypeName(fi.VarInfo))
		out += "            ret.SetWhole(restream.Ptr(cloned))\n"
		out += "            included = true\n"
		out += "            continue\n"
		out += "        }\n"
		out += fmt.Sprintf("        partial, ok := s.%s.PartialForFields([][]any{field})\n", fi.Name)
	} else {
		out += fmt.Sprintf("        partial, ok := (&s.%s).PartialForFields([][]any{field})\n", fi.Name)
	}
	out += "        if ok {\n"
	out += fmt.Sprintf("            ret.ApplyPartial(partial.(%s))\n", partialType)
	out += "            included = true\n"
	out += "        }\n"
	out += "    }\n"
	out += "    return ret, included\n"
	out += "}\n\n"
	return out
}

func (ft *FileTracking) createGolangMapPartialForFields(
	si StructInfo,
	fi *restream.FieldInfo,
	pfi *restream.FieldInfo,
) string {
	retType := ft.getGolangTypeName(pfi.VarInfo)
	mapInfo := fi.VarInfo.(*restream.VarInfoMap)
	keyType := ft.getGolangTypeName(mapInfo.KeyType)
	valueSupportsPartials := ft.supportsPartials(mapInfo.ElemType)
	valueIsPointer := isPointerVarInfo(mapInfo.ElemType)
	partialStruct := mustPartialStructInfo(pfi.VarInfo)
	constructor := ft.partialContainerConstructor(partialStruct)

	out := fmt.Sprintf("func (s *%s) partialForFields%s(fields [][]any) (%s, bool) {\n", si.Name, fi.Name, retType)
	out += "    if len(fields) == 0 { return nil, false }\n"
	out += fmt.Sprintf("    ret := %s\n", constructor)
	out += "    included := false\n"
	out += "    for _, field := range fields {\n"
	out += "        if len(field) == 0 {\n"
	out += fmt.Sprintf("            var cloned %s\n", ft.getGolangTypeName(fi.VarInfo))
	out += ft.mustGolangCloneAssign(fi.VarInfo, fmt.Sprintf("s.%s", fi.Name), "cloned", "            ", 0)
	out += "            return ret.SetWhole(cloned), true\n"
	out += "        }\n"
	out += fmt.Sprintf("        key, ok := restream.FieldPathPartToKey[%s](field[0])\n", keyType)
	out += "        if !ok { continue }\n"
	out += fmt.Sprintf("        value, exists := s.%s[key]\n", fi.Name)
	out += "        if !exists {\n"
	out += "            ret.Delete(key)\n"
	out += "            included = true\n"
	out += "            continue\n"
	out += "        }\n"
	if valueSupportsPartials {
		partialType := ft.getGolangTypeName(partialStruct.GenericTypes[2])
		out += "        if len(field) == 1 {\n"
		if expr, ok := ft.golangSimpleCloneExpr(mapInfo.ElemType, "value"); ok {
			out += fmt.Sprintf("            ret.Set(key, %s)\n", expr)
		} else {
			out += fmt.Sprintf("            var cloned %s\n", ft.getGolangTypeName(mapInfo.ElemType))
			out += ft.mustGolangCloneAssign(mapInfo.ElemType, "value", "cloned", "            ", 0)
			out += "            ret.Set(key, cloned)\n"
		}
		out += "            included = true\n"
		out += "            continue\n"
		out += "        }\n"
		if valueIsPointer {
			out += "        if value == nil {\n"
			out += fmt.Sprintf("            var cloned %s\n", ft.getGolangTypeName(mapInfo.ElemType))
			out += "            ret.Set(key, cloned)\n"
			out += "            included = true\n"
			out += "            continue\n"
			out += "        }\n"
			out += "        partial, ok := value.PartialForFields([][]any{field[1:]})\n"
		} else {
			out += "        partial, ok := (&value).PartialForFields([][]any{field[1:]})\n"
		}
		out += "        if ok {\n"
		out += fmt.Sprintf("            ret.ApplyPartial(key, partial.(%s))\n", partialType)
		out += "            included = true\n"
		out += "        }\n"
	} else {
		if expr, ok := ft.golangSimpleCloneExpr(mapInfo.ElemType, "value"); ok {
			out += fmt.Sprintf("        ret.Set(key, %s)\n", expr)
		} else {
			out += fmt.Sprintf("        var cloned %s\n", ft.getGolangTypeName(mapInfo.ElemType))
			out += ft.mustGolangCloneAssign(mapInfo.ElemType, "value", "cloned", "        ", 0)
			out += "        ret.Set(key, cloned)\n"
		}
		out += "        included = true\n"
	}
	out += "    }\n"
	out += "    return ret, included\n"
	out += "}\n\n"
	return out
}

func (ft *FileTracking) createGolangArrayPartialForFields(
	si StructInfo,
	fi *restream.FieldInfo,
	pfi *restream.FieldInfo,
) string {
	retType := ft.getGolangTypeName(pfi.VarInfo)
	arrayInfo := fi.VarInfo.(*restream.VarInfoArray)
	valueSupportsPartials := ft.supportsPartials(arrayInfo.ElemType)
	valueIsPointer := isPointerVarInfo(arrayInfo.ElemType)
	partialStruct := mustPartialStructInfo(pfi.VarInfo)
	constructor := ft.partialContainerConstructor(partialStruct)

	out := fmt.Sprintf("func (s *%s) partialForFields%s(fields [][]any) (%s, bool) {\n", si.Name, fi.Name, retType)
	out += "    if len(fields) == 0 { return nil, false }\n"
	out += fmt.Sprintf("    ret := %s\n", constructor)
	out += "    included := false\n"
	out += "    for _, field := range fields {\n"
	out += "        if len(field) == 0 {\n"
	out += fmt.Sprintf("            var cloned %s\n", ft.getGolangTypeName(fi.VarInfo))
	out += ft.mustGolangCloneAssign(fi.VarInfo, fmt.Sprintf("s.%s", fi.Name), "cloned", "            ", 0)
	out += "            return ret.SetWhole(cloned), true\n"
	out += "        }\n"
	out += "        index, ok := restream.FieldPathPartToIndex(field[0])\n"
	out += fmt.Sprintf("        if !ok || index < 0 || index >= len(s.%s) { continue }\n", fi.Name)
	out += fmt.Sprintf("        value := s.%s[index]\n", fi.Name)
	if valueSupportsPartials {
		partialType := ft.getGolangTypeName(partialStruct.GenericTypes[1])
		out += "        if len(field) == 1 {\n"
		if expr, ok := ft.golangSimpleCloneExpr(arrayInfo.ElemType, "value"); ok {
			out += fmt.Sprintf("            ret.Set(index, %s)\n", expr)
		} else {
			out += fmt.Sprintf("            var cloned %s\n", ft.getGolangTypeName(arrayInfo.ElemType))
			out += ft.mustGolangCloneAssign(arrayInfo.ElemType, "value", "cloned", "            ", 0)
			out += "            ret.Set(index, cloned)\n"
		}
		out += "            included = true\n"
		out += "            continue\n"
		out += "        }\n"
		if valueIsPointer {
			out += "        if value == nil {\n"
			out += fmt.Sprintf("            var cloned %s\n", ft.getGolangTypeName(arrayInfo.ElemType))
			out += "            ret.Set(index, cloned)\n"
			out += "            included = true\n"
			out += "            continue\n"
			out += "        }\n"
			out += "        partial, ok := value.PartialForFields([][]any{field[1:]})\n"
		} else {
			out += "        partial, ok := (&value).PartialForFields([][]any{field[1:]})\n"
		}
		out += "        if ok {\n"
		out += fmt.Sprintf("            ret.ApplyPartial(index, partial.(%s))\n", partialType)
		out += "            included = true\n"
		out += "        }\n"
	} else {
		if expr, ok := ft.golangSimpleCloneExpr(arrayInfo.ElemType, "value"); ok {
			out += fmt.Sprintf("        ret.Set(index, %s)\n", expr)
		} else {
			out += fmt.Sprintf("        var cloned %s\n", ft.getGolangTypeName(arrayInfo.ElemType))
			out += ft.mustGolangCloneAssign(arrayInfo.ElemType, "value", "cloned", "        ", 0)
			out += "        ret.Set(index, cloned)\n"
		}
		out += "        included = true\n"
	}
	out += "    }\n"
	out += "    return ret, included\n"
	out += "}\n\n"
	return out
}

func (ft *FileTracking) partialContainerConstructor(si *restream.VarInfoStruct) string {
	switch si.Name {
	case "PartialMap":
		return fmt.Sprintf("restream.NewPartialMap[%s, %s]()",
			ft.getGolangTypeName(si.GenericTypes[0]),
			ft.getGolangTypeName(si.GenericTypes[1]))
	case "PartialModMap":
		return fmt.Sprintf("restream.NewPartialModMap[%s, %s, %s]()",
			ft.getGolangTypeName(si.GenericTypes[0]),
			ft.getGolangTypeName(si.GenericTypes[1]),
			ft.getGolangTypeName(si.GenericTypes[2]))
	case "PartialArray":
		return fmt.Sprintf("restream.NewPartialArray[%s]()",
			ft.getGolangTypeName(si.GenericTypes[0]))
	case "PartialModArray":
		return fmt.Sprintf("restream.NewPartialModArray[%s, %s]()",
			ft.getGolangTypeName(si.GenericTypes[0]),
			ft.getGolangTypeName(si.GenericTypes[1]))
	default:
		panic(fmt.Sprintf("unsupported partial container %s", si.Name))
	}
}

func mustPartialStructInfo(vi restream.VarInfo) *restream.VarInfoStruct {
	if ptr, ok := vi.(*restream.VarInfoPointer); ok {
		vi = ptr.SubType
	}
	if st, ok := vi.(*restream.VarInfoStruct); ok {
		return st
	}
	panic(fmt.Sprintf("partial field is not a struct: %T", vi))
}

func isPointerVarInfo(vi restream.VarInfo) bool {
	_, ok := vi.(*restream.VarInfoPointer)
	return ok
}

func (ft *FileTracking) createGolangStructCloner(si StructInfo, fields []*restream.FieldInfo) (string, error) {
	if si.GenericParams != nil {
		return "", nil
	}

	typeName := si.GolangNameWithParams()
	out := "\n"
	out += fmt.Sprintf("// RestreamClone returns a deep copy of this %s.\n", si.Name)
	out += fmt.Sprintf("func (s *%s) RestreamClone() *%s {\n", typeName, typeName)
	out += "    if s == nil { return nil }\n"
	out += fmt.Sprintf("    ret := &%s{}\n", typeName)
	for idx, fi := range fields {
		assignment, err := ft.golangCloneAssign(fi.VarInfo, "s."+fi.Name, "ret."+fi.Name, "    ", idx+1)
		if err != nil {
			return "", errors.Wrapf(err, "building clone for %s.%s", si.Name, fi.Name)
		}
		out += assignment
	}
	out += "    return ret\n"
	out += "}\n\n"
	return out, nil
}

func (ft *FileTracking) golangCloneAssign(
	vi restream.VarInfo,
	src string,
	dst string,
	indent string,
	depth int,
) (string, error) {
	switch vit := vi.(type) {
	case *restream.VarInfoPrimitive:
		return fmt.Sprintf("%s%s = %s\n", indent, dst, src), nil
	case *restream.VarInfoArray:
		elemType := ft.getGolangTypeName(vit.ElemType)
		idxName := fmt.Sprintf("cloneIdx%d", depth)
		valueName := fmt.Sprintf("cloneValue%d", depth)
		clonedName := fmt.Sprintf("cloned%d", depth)
		out := fmt.Sprintf("%sif %s != nil {\n", indent, src)
		out += fmt.Sprintf("%s    %s = make(%s, len(%s))\n", indent, dst, ft.getGolangTypeName(vit), src)
		if expr, ok := ft.golangSimpleCloneExpr(vit.ElemType, valueName); ok {
			if expr == valueName {
				out += fmt.Sprintf("%s    copy(%s, %s)\n", indent, dst, src)
			} else {
				out += fmt.Sprintf("%s    for %s, %s := range %s {\n", indent, idxName, valueName, src)
				out += fmt.Sprintf("%s        %s[%s] = %s\n", indent, dst, idxName, expr)
				out += fmt.Sprintf("%s    }\n", indent)
			}
		} else {
			out += fmt.Sprintf("%s    for %s, %s := range %s {\n", indent, idxName, valueName, src)
			out += fmt.Sprintf("%s        var %s %s\n", indent, clonedName, elemType)
			assignment, err := ft.golangCloneAssign(vit.ElemType, valueName, clonedName, indent+"        ", depth+1)
			if err != nil {
				return "", err
			}
			out += assignment
			out += fmt.Sprintf("%s        %s[%s] = %s\n", indent, dst, idxName, clonedName)
			out += fmt.Sprintf("%s    }\n", indent)
		}
		out += fmt.Sprintf("%s}\n", indent)
		return out, nil
	case *restream.VarInfoMap:
		keyName := fmt.Sprintf("cloneKey%d", depth)
		valueName := fmt.Sprintf("cloneValue%d", depth)
		out := fmt.Sprintf("%sif %s != nil {\n", indent, src)
		out += fmt.Sprintf("%s    %s = make(%s, len(%s))\n", indent, dst, ft.getGolangTypeName(vit), src)
		out += fmt.Sprintf("%s    for %s, %s := range %s {\n", indent, keyName, valueName, src)
		if vit.ElemType == nil {
			out += fmt.Sprintf("%s        %s[%s] = %s\n", indent, dst, keyName, valueName)
		} else {
			elemType := ft.getGolangTypeName(vit.ElemType)
			clonedName := fmt.Sprintf("cloned%d", depth)
			if expr, ok := ft.golangSimpleCloneExpr(vit.ElemType, valueName); ok {
				out += fmt.Sprintf("%s        %s[%s] = %s\n", indent, dst, keyName, expr)
			} else {
				out += fmt.Sprintf("%s        var %s %s\n", indent, clonedName, elemType)
				assignment, err := ft.golangCloneAssign(vit.ElemType, valueName, clonedName, indent+"        ", depth+1)
				if err != nil {
					return "", err
				}
				out += assignment
				out += fmt.Sprintf("%s        %s[%s] = %s\n", indent, dst, keyName, clonedName)
			}
		}
		out += fmt.Sprintf("%s    }\n", indent)
		out += fmt.Sprintf("%s}\n", indent)
		return out, nil
	case *restream.VarInfoPointer:
		elemType := ft.getGolangTypeName(vit.SubType)
		clonedName := fmt.Sprintf("cloned%d", depth)
		out := fmt.Sprintf("%sif %s != nil {\n", indent, src)
		if expr, ok := ft.golangSimpleCloneExpr(vit.SubType, fmt.Sprintf("(*%s)", src)); ok {
			out += fmt.Sprintf("%s    %s := %s\n", indent, clonedName, expr)
		} else {
			out += fmt.Sprintf("%s    var %s %s\n", indent, clonedName, elemType)
			assignment, err := ft.golangCloneAssign(vit.SubType, fmt.Sprintf("(*%s)", src), clonedName, indent+"    ", depth+1)
			if err != nil {
				return "", err
			}
			out += assignment
		}
		out += fmt.Sprintf("%s    %s = &%s\n", indent, dst, clonedName)
		out += fmt.Sprintf("%s}\n", indent)
		return out, nil
	case *restream.VarInfoStruct:
		if vit.FieldList != nil {
			out := fmt.Sprintf("%s%s = %s{}\n", indent, dst, ft.getGolangTypeName(vit))
			for _, field := range vit.FieldList {
				assignment, err := ft.golangCloneAssign(field.VarInfo, src+"."+field.Name, dst+"."+field.Name, indent, depth+1)
				if err != nil {
					return "", err
				}
				out += assignment
			}
			return out, nil
		}
		clonedName := fmt.Sprintf("cloned%d", depth)
		sourceName := fmt.Sprintf("cloneSource%d", depth)
		inputName := fmt.Sprintf("cloneInput%d", depth)
		typeName := ft.getGolangTypeName(vit)
		out := fmt.Sprintf("%s%s := %s\n", indent, inputName, src)
		out += fmt.Sprintf("%sif %s, ok := any(&%s).(interface{ RestreamClone() *%s }); ok {\n", indent, sourceName, inputName, typeName)
		out += fmt.Sprintf("%s    %s := %s.RestreamClone()\n", indent, clonedName, sourceName)
		out += fmt.Sprintf("%s    if %s != nil {\n", indent, clonedName)
		out += fmt.Sprintf("%s        %s = *%s\n", indent, dst, clonedName)
		out += fmt.Sprintf("%s    }\n", indent)
		out += fmt.Sprintf("%s} else {\n", indent)
		out += fmt.Sprintf("%s    %s = %s\n", indent, dst, inputName)
		out += fmt.Sprintf("%s}\n", indent)
		return out, nil
	case *restream.VarInfoDynamic:
		return fmt.Sprintf("%s%s = %sCloneDynamicValue(%s)\n", indent, dst, ft.golangRestreamQualifier(), src), nil
	case *restream.VarInfoGenericParam:
		return "", fmt.Errorf("generic fields cannot be generated as deep clones")
	default:
		return "", fmt.Errorf("unhandled clone type %T", vi)
	}
}

func (ft *FileTracking) golangRestreamQualifier() string {
	if ft.fPackage != nil && ft.fPackage.PkgPath == "github.com/boatkit-io/restream/pkg/restream" {
		return ""
	}
	return "restream."
}

func (ft *FileTracking) golangSimpleCloneExpr(vi restream.VarInfo, src string) (string, bool) {
	switch vi.(type) {
	case *restream.VarInfoPrimitive:
		return src, true
	default:
		return "", false
	}
}

func (ft *FileTracking) mustGolangCloneAssign(vi restream.VarInfo, src string, dst string, indent string, depth int) string {
	out, err := ft.golangCloneAssign(vi, src, dst, indent, depth)
	if err != nil {
		panic(err)
	}
	return out
}

// createGolangStructFieldedSerializers is a reusable helper to write out the serializer/deserializer functions for a fielded struct
func createGolangStructFieldedSerializers(si StructInfo, fields []*restream.FieldInfo) string {
	out := genGolangFieldInfo(si.Name, fields) + "\n"

	if si.GenericParams != nil {
		panic("generics not supported in fielded structures")
	}

	out += "// Serialize serializes this structure to a binary writer\n"
	out += fmt.Sprintf("func (s *%s) Serialize(w *binarystreams.Writer, _ *restream.VarInfoStruct) error {\n", si.GolangNameWithParams())
	out += "	wi, buf := binarystreams.NewMemoryWriter()\n"
	for idx, fi := range fields {
		ptrOrNot := ""
		if _, isStruct := fi.VarInfo.(*restream.VarInfoStruct); isStruct {
			ptrOrNot = "&"
		}
		out += fmt.Sprintf("    if err := restream.SerializeField(%ss.%s, &%sFieldInfo[%d], wi); err != nil { return err }\n",
			ptrOrNot, fi.Name, si.Name, idx)
	}
	out += "    if err := wi.Flush(); err != nil { return err }\n"
	out += "    b := buf.Bytes()\n"
	out += "    if err := restream.SerializePacked64(uint64(len(b)), w); err != nil { return err }\n"
	out += "    return w.WriteBytes(b)\n"
	out += "}" + "\n\n"

	out += "// Deserialize deserializes data from a binary reader into this struct\n"
	out += fmt.Sprintf("func (s *%s) Deserialize(r *binarystreams.Reader, _ *restream.VarInfoStruct) error {\n", si.GolangNameWithParams())
	out += "    fieldPtrs := []any{\n"
	for _, fi := range fields {
		out += fmt.Sprintf("        &s.%s,\n", fi.Name)
	}
	out += "    }" + "\n"
	out += "    sl, err := restream.DeserializePacked64[uint64](r); if err != nil { return err }\n"
	out += "    ri, err := r.Slice(int(sl)); if err != nil { return err }\n"
	out += fmt.Sprintf("    return restream.DeserializeFielded(ri, %sFieldInfo, %sFieldMap, fieldPtrs)\n", si.Name, si.Name)
	out += "}" + "\n\n"

	return out
}

// createGolangStructNonFieldedSerializers is a reusable helper to write out the serializer/deserializer functions for a non-fielded struct
func createGolangStructNonFieldedSerializers(si StructInfo, fields []*restream.FieldInfo, receiver bool) string {
	out := genGolangFieldInfo(si.Name, fields) + "\n"

	out += "\n"
	if si.GenericParams != nil {
		out += "// GetTypeArgs returns the type arguments for this struct\n"
		out += fmt.Sprintf("func (s *%s) GetTypeArgs() []restream.VarInfo {\n", si.GolangNameWithParams())
		out += "    vis := []restream.VarInfo{}\n"
		out += "    var vi restream.VarInfo\n"
		out += "    var err error\n"
		for _, gt := range si.GenericParams {
			out += fmt.Sprintf("    vi, err = restream.VarInfoFromType(reflect.TypeFor[%s]())\n", gt)
			out += "    if err != nil { panic(err) }\n"
			out += "    vis = append(vis, vi)\n"
		}
		out += "    return vis\n"
		out += "}\n"
		out += "\n"
	}

	var serStart string
	if receiver {
		out += "// Serialize serializes this structure to a binary writer\n"
		serStart = fmt.Sprintf("func (s *%s) Serialize(", si.GolangNameWithParams())
	} else {
		out += fmt.Sprintf("// Serialize%s serializes this structure to a binary writer\n", si.Name)
		serStart = fmt.Sprintf("func Serialize%s(v any, ", si.GolangNameWithParams())
	}
	if len(fields) == 0 {
		out += fmt.Sprintf("%s_ *binarystreams.Writer, _ *restream.VarInfoStruct) error {\n", serStart)
	} else {
		if si.GenericParams != nil {
			out += fmt.Sprintf("%sw *binarystreams.Writer, vi *restream.VarInfoStruct) error {\n", serStart)
			out += "    gm := map[string]restream.VarInfo{\n"
			for idx, gt := range si.GenericParams {
				out += fmt.Sprintf("        %q: vi.GenericTypes[%d],\n", gt, idx)
			}
			out += "    }\n"
		} else {
			out += fmt.Sprintf("%sw *binarystreams.Writer, _ *restream.VarInfoStruct) error {\n", serStart)
		}
		if !receiver {
			out += fmt.Sprintf("    s := v.(*%s.%s)\n", si.Package, si.GolangNameWithParams())
		}
		for idx, fi := range fields {
			sv := fmt.Sprintf("s.%s", fi.Name)
			if _, is := fi.VarInfo.(*restream.VarInfoStruct); is {
				sv = "&" + sv
			}
			if si.GenericParams != nil {
				out += fmt.Sprintf("    if err := restream.SerializeValue(%s, w, %sFieldInfo[%d].VarInfo.FillGenerics(gm)); "+
					"err != nil { return err }\n", sv, si.Name, idx)
			} else {
				out += fmt.Sprintf("    if err := restream.SerializeValue(%s, w, %sFieldInfo[%d].VarInfo); err != nil { return err }\n",
					sv, si.Name, idx)
			}
		}
	}
	out += "    return nil\n"
	out += "}" + "\n\n"

	var deserStart string
	if receiver {
		out += "// Deserialize deserializes data from a binary reader into this struct\n"
		deserStart = fmt.Sprintf("func (s *%s) Deserialize(", si.GolangNameWithParams())
	} else {
		out += fmt.Sprintf("// Deserialize%s deserializes data from a binary reader into this struct\n", si.Name)
		deserStart = fmt.Sprintf("func Deserialize%s(v any, ", si.GolangNameWithParams())
	}
	if len(fields) == 0 {
		out += fmt.Sprintf("%s_ *binarystreams.Reader, _ *restream.VarInfoStruct) error {\n", deserStart)
	} else {
		if si.GenericParams != nil {
			out += fmt.Sprintf("%sr *binarystreams.Reader, vi *restream.VarInfoStruct) error {\n", deserStart)
			out += "    gm := map[string]restream.VarInfo{\n"
			for idx, gt := range si.GenericParams {
				out += fmt.Sprintf("        %q: vi.GenericTypes[%d],\n", gt, idx)
			}
			out += "    }\n"
		} else {
			out += fmt.Sprintf("%sr *binarystreams.Reader, _ *restream.VarInfoStruct) error {\n", deserStart)
		}
		if !receiver {
			out += fmt.Sprintf("    s := v.(*%s.%s)\n", si.Package, si.GolangNameWithParams())
		}
		for idx, fi := range fields {
			if si.GenericParams != nil {
				out += fmt.Sprintf("    if err := restream.DeserializeValue(&s.%s, r, %sFieldInfo[%d].VarInfo.FillGenerics(gm)); "+
					"err != nil { return err }\n", fi.Name, si.Name, idx)
			} else {
				out += fmt.Sprintf("    if err := restream.DeserializeValue(&s.%s, r, %sFieldInfo[%d].VarInfo); err != nil { return err }\n",
					fi.Name, si.Name, idx)
			}
		}
	}
	out += "    return nil\n"
	out += "}"
	return out
}

// genGolangFieldInfo is a reusable helper to build the static fieldinfo struct for a given set of fields/a struct
func genGolangFieldInfo(structName string, fields []*restream.FieldInfo) string {
	out := fmt.Sprintf("// %sFieldInfo is the static field info for the %s struct\n", structName, structName)
	out += fmt.Sprintf("var %sFieldInfo = []restream.FieldInfo{\n", structName)
	for _, fi := range fields {
		out += "    " + fi.ToGolangString()
	}
	out += "}\n"

	if lo.SomeBy(fields, func(fi *restream.FieldInfo) bool { return fi.FieldID != 0 }) {
		out += "\n"
		out += fmt.Sprintf("// %sFieldMap is the static field map for the %s struct\n", structName, structName)
		out += fmt.Sprintf("var %sFieldMap = map[byte]*restream.FieldInfo{\n", structName)
		for idx, fi := range fields {
			out += fmt.Sprintf("    %d: &%sFieldInfo[%d],\n", fi.FieldID, structName, idx)
		}
		out += "}\n"
	}

	return out
}

// buildGolangRPCStructs is a helper to build the golang RPC structs
func (ft *FileTracking) buildGolangRPCStructs(rpcn, rpctn string, reqFields []*restream.FieldInfo, respFields []*restream.FieldInfo) error {
	out := fmt.Sprintf("// %sRequest is a request object for the %s RPC call\n", rpctn, rpcn)
	out += fmt.Sprintf("type %sRequest struct { //nolint:revive\n", rpctn)
	for _, fi := range reqFields {
		out += fmt.Sprintf("    %s %s\n", fi.Name, ft.getGolangTypeName(fi.VarInfo))
	}
	out += "}\n\n"

	sirq := StructInfo{Name: rpctn + "Request"}
	out += createGolangStructNonFieldedSerializers(sirq, reqFields, true) + "\n"

	out += fmt.Sprintf("// %sResponse is a response object for the %s RPC call\n", rpctn, rpcn)
	out += fmt.Sprintf("type %sResponse struct { //nolint:revive\n", rpctn)
	for _, fi := range respFields {
		out += fmt.Sprintf("    %s %s\n", fi.Name, ft.getGolangTypeName(fi.VarInfo))
	}
	out += "}\n\n"

	sirs := StructInfo{Name: rpctn + "Response"}
	out += createGolangStructNonFieldedSerializers(sirs, respFields, true) + "\n"

	ft.goGenEntries = append(ft.goGenEntries, fdef{name: rpctn, defs: out})

	return nil
}

// buildGolangEventStruct is a helper to build the golang event packet struct.
func (ft *FileTracking) buildGolangEventStruct(eventName, eventTypeName string, eventFields []*restream.FieldInfo) error {
	out := fmt.Sprintf("// %sEvent is an event packet object for the %s event\n", eventTypeName, eventName)
	out += fmt.Sprintf("type %sEvent struct { //nolint:revive\n", eventTypeName)
	for _, fi := range eventFields {
		out += fmt.Sprintf("    %s %s\n", fi.Name, ft.getGolangTypeName(fi.VarInfo))
	}
	out += "}\n\n"

	si := StructInfo{Name: eventTypeName + "Event"}
	out += createGolangStructNonFieldedSerializers(si, eventFields, true) + "\n"

	ft.goGenEntries = append(ft.goGenEntries, fdef{name: eventTypeName + "Event", defs: out})

	return nil
}

// createGoStoreMethods builds the boilerplate Store interface methods for a typed store.
func (ft *FileTracking) createGoStoreMethods(si StructInfo, storeName string, storeTypeExpr string) {
	constName := si.Name + "Name"

	out := fmt.Sprintf("// %s is the restream store name for %s\n", constName, si.Name)
	out += fmt.Sprintf("const %s = %q\n\n", constName, storeName)

	out += "// GetName is an implementation of the Store.GetName call\n"
	out += fmt.Sprintf("func (s *%s) GetName() string {\n", si.GolangNameWithParams())
	out += fmt.Sprintf("    return %s\n", constName)
	out += "}\n\n"

	out += "// GetStoreData is an implementation of the Store.GetStoreData call\n"
	out += fmt.Sprintf("func (s *%s) GetStoreData() restream.StoreDataBase {\n", si.GolangNameWithParams())
	out += "    return s.storeData\n"
	out += "}\n\n"

	out += "// SubscribeToField implements the restream.Store interface\n"
	out += fmt.Sprintf("func (s *%s) SubscribeToField(field []any, callback any) {\n", si.GolangNameWithParams())
	out += "    s.storeData.SubscribeToField(field, callback)\n"
	out += "}\n\n"

	out += "// GetStoreType returns the ReStream store implementation type.\n"
	out += fmt.Sprintf("func (s *%s) GetStoreType() restream.StoreType {\n", si.GolangNameWithParams())
	out += fmt.Sprintf("    return %s\n", storeTypeExpr)
	out += "}\n"

	ft.goGenEntries = append(ft.goGenEntries, fdef{name: si.Name + "Store", defs: out})
}

type relayStorePackage struct {
	packageName                string
	packageDir                 string
	generateStoreNameConstants bool
	stores                     []relayStoreFactory
}

type relayStoreFactory struct {
	storeTypeName      string
	storeName          string
	stateRef           storeStateRef
	minimumAccessLevel string
}

func (pt *ProjTracking) addRelayStoreFactory(
	ft *FileTracking,
	si StructInfo,
	storeName string,
	stateRef storeStateRef,
	minimumAccessLevel string,
) {
	pkg := pt.relayStorePackageForFile(ft)
	if pkg.generateStoreNameConstants && stateRef.Qualifier == "" {
		stateRef.Qualifier = ft.f.Name.Name
		if stateRef.PackagePath == "" && ft.fPackage != nil {
			stateRef.PackagePath = ft.fPackage.PkgPath
		}
	}

	store := relayStoreFactory{
		storeTypeName:      si.Name,
		storeName:          storeName,
		stateRef:           stateRef,
		minimumAccessLevel: minimumAccessLevel,
	}
	if lo.SomeBy(pkg.stores, func(existing relayStoreFactory) bool {
		return existing.storeTypeName == store.storeTypeName
	}) {
		return
	}
	pkg.stores = append(pkg.stores, store)
}

func (pt *ProjTracking) relayStorePackageForFile(ft *FileTracking) *relayStorePackage {
	if pt.config.GoRelayStoresDir != "" {
		key := "configured:" + pt.config.GoRelayStoresDir
		pkg := pt.relayStores[key]
		if pkg == nil {
			pkgName := pt.config.GoRelayStoresPackage
			if pkgName == "" {
				pkgName = path.Base(pt.config.GoRelayStoresDir)
			}
			pkg = &relayStorePackage{
				packageName:                pkgName,
				packageDir:                 pt.resolveProjectPath(pt.config.GoRelayStoresDir),
				generateStoreNameConstants: true,
			}
			pt.relayStores[key] = pkg
		}
		return pkg
	}

	key := ft.fPackage.PkgPath
	if key == "" {
		key = path.Dir(ft.inFile)
	}

	pkg := pt.relayStores[key]
	if pkg == nil {
		pkg = &relayStorePackage{
			packageName: ft.f.Name.Name,
			packageDir:  path.Dir(ft.inFile),
		}
		pt.relayStores[key] = pkg
	}
	return pkg
}

func (pt *ProjTracking) writeRelayStoreFactories() error {
	for _, pkg := range pt.relayStores {
		outPath := path.Join(pkg.packageDir, "relaystores_rs.go")
		if err := pt.writeGoFile(outPath, pkg.packageName, []fdef{pkg.relayStoreFactoryDef()}); err != nil {
			return err
		}
	}
	return nil
}

func (p *relayStorePackage) relayStoreFactoryDef() fdef {
	slices.SortFunc(p.stores, func(a, b relayStoreFactory) int {
		return strings.Compare(a.storeTypeName, b.storeTypeName)
	})

	imports := map[string]struct{}{}
	out := ""
	if p.generateStoreNameConstants && len(p.stores) > 0 {
		out += "const (\n"
		for _, store := range p.stores {
			out += fmt.Sprintf("    // %sName is the restream store name for %s.\n", store.storeTypeName, store.storeTypeName)
			out += fmt.Sprintf("    %sName = %q\n", store.storeTypeName, store.storeName)
		}
		out += ")\n\n"
	}

	out += "// NewRelayStores creates relay stores for all generated ReStream stores in this package.\n"
	out += "func NewRelayStores() []restream.Store {\n"
	out += "    return []restream.Store{\n"
	for _, store := range p.stores {
		stateType := store.stateRef.typeExpr()
		partialType := store.stateRef.partialTypeExpr()
		if store.stateRef.Qualifier != "" && store.stateRef.PackagePath != "" {
			imports[store.stateRef.PackagePath] = struct{}{}
		}
		out += fmt.Sprintf("        restream.NewRelayStore[%s, *%s, *%s](\n", stateType, stateType, partialType)
		out += fmt.Sprintf("            %sName,\n", store.storeTypeName)
		out += fmt.Sprintf("            &%s{},\n", stateType)
		out += fmt.Sprintf("            %s,\n", store.minimumAccessLevel)
		out += "        ),\n"
	}
	out += "    }\n"
	out += "}\n"

	return fdef{name: "NewRelayStores", defs: out, deps: lo.Keys(imports)}
}

// writeGoStructs writes out the golang generated structures
func (ft *FileTracking) writeGoStructs() error {
	fmt.Printf("Writing out golang gen at: %s\n", ft.outFile)

	return ft.pt.writeGoFile(ft.outFile, ft.f.Name.Name, ft.goGenEntries)
}

// writeGoExtraFile writes out the golang extra file with explicitly imported types/generated serializers
func (pt *ProjTracking) writeGoExtraFile() error {
	if pt.config.GoExtraFile == "" {
		return fmt.Errorf("no goExtraFile specified in config, but extra go content was generated")
	}

	outPath := pt.resolveProjectPath(pt.config.GoExtraFile)
	fmt.Printf("Writing out golang extra data at: %s\n", outPath)

	fp := strings.Split(pt.config.GoExtraFile, "/")
	pkgName := fp[len(fp)-2]
	return pt.writeGoFile(outPath, pkgName, pt.goGenEntries)
}

// writeGoFile writes out a golang file with project settings
func (pt *ProjTracking) writeGoFile(outPath, packageName string, entries []fdef) error {
	fc := fmt.Sprintf(`%s
	//nolint:lll
	package %s
	
	import (
		"time"
	
		"github.com/boatkit-io/restream/pkg/binarystreams"
	`, restreamGeneratedFileBanner, packageName)

	inReStream := packageName == "restream"

	if !inReStream {
		fc += "    \"github.com/boatkit-io/restream/pkg/restream\"\n"
	}

	goImports := append([]string{}, pt.config.GoImports...)
	for _, entry := range entries {
		goImports = append(goImports, entry.deps...)
	}
	goImports = lo.Uniq(goImports)
	slices.Sort(goImports)
	for _, imp := range goImports {
		fc += fmt.Sprintf("    \"%s\"\n", imp)
	}

	fc += ")\n\n"

	// Sort the go output list so it's consistent in the output
	slices.SortFunc(entries, func(f1 fdef, f2 fdef) int { return strings.Compare(f1.name, f2.name) })
	fc += strings.Join(lo.Map(entries, func(f fdef, _ int) string { return f.defs }), "\n")

	if inReStream {
		fc = strings.ReplaceAll(fc, "restream.", "")
	}

	if err := os.MkdirAll(path.Dir(outPath), 0o775); err != nil {
		return errors.Wrap(err, "error making golang gen dir")
	}

	if err := os.WriteFile(outPath, []byte(fc), 0o600); err != nil {
		return errors.Wrap(err, "error writing golang gen file")
	}

	if err := runGoimports(outPath); err != nil {
		return errors.Wrap(err, "error running go formatter")
	}

	return nil
}

func runGoimports(outPath string) error {
	goimportsPath, err := exec.LookPath("goimports")
	if err != nil {
		home, homeErr := os.UserHomeDir()
		if homeErr != nil {
			return err
		}
		fallback := filepath.Join(home, ".local/share/mise/installs/go-golang-org-x-tools-cmd-goimports/0.44.0/bin/goimports")
		if _, statErr := os.Stat(fallback); statErr != nil {
			return err
		}
		goimportsPath = fallback
	}
	return exec.Command(goimportsPath, "-w", outPath).Run() //nolint:gosec
}

// rewriteSourceFile re-prints-out the source golang file for input-side codegen fixes.
func (ft *FileTracking) rewriteSourceFile() error {
	fmt.Printf("Writing out updated golang source file: %s\n", ft.inFile)

	fw, err := os.Create(ft.inFile)
	if err != nil {
		return err
	}
	if err := decorator.Fprint(fw, ft.f); err != nil {
		return err
	}
	if err := fw.Close(); err != nil {
		return err
	}
	return runGoimports(ft.inFile)
}

// getGolangTypeName builds the golang type name for the expression info
func (ft *FileTracking) getGolangTypeName(vi restream.VarInfo) string {
	switch vit := vi.(type) {
	case *restream.VarInfoArray:
		return fmt.Sprintf("[]%s", ft.getGolangTypeName(vit.ElemType))
	case *restream.VarInfoDynamic:
		return "any"
	case *restream.VarInfoMap:
		return fmt.Sprintf("map[%s]%s", ft.getGolangTypeName(vit.KeyType), ft.getGolangTypeName(vit.ElemType))
	case *restream.VarInfoPointer:
		return "*" + ft.getGolangTypeName(vit.SubType)
	case *restream.VarInfoPrimitive:
		if vit.MappedType != nil {
			parts := strings.Split(*vit.MappedType, ".")
			if parts[0] == ft.fPackage.Name {
				return parts[1]
			}
			return *vit.MappedType
		}
		switch vit.DataType {
		case restream.SerializationTypeBool:
			return "bool"
		case restream.SerializationTypeInt8:
			return "int8"
		case restream.SerializationTypeInt16:
			return "int16"
		case restream.SerializationTypeInt32:
			return "int32"
		case restream.SerializationTypeInt64:
			return "int64"
		case restream.SerializationTypeUint8:
			return "uint8"
		case restream.SerializationTypeUint16:
			return "uint16"
		case restream.SerializationTypeUint32:
			return "uint32"
		case restream.SerializationTypeUint64:
			return "uint64"
		case restream.SerializationTypeFloat32:
			return "float32"
		case restream.SerializationTypeFloat64:
			return "float64"
		case restream.SerializationTypeString:
			return "string"
		case restream.SerializationTypeTime:
			return "time.Time"
		default:
			panic(fmt.Sprintf("unhandled primitive type: %d", vit.DataType))
		}
	case *restream.VarInfoStruct:
		// Structs have either field list or other named stuff
		if vit.FieldList != nil {
			fields := strings.Join(lo.Map(vit.FieldList, func(fi restream.FieldInfo, _ int) string {
				return fmt.Sprintf("%s %s", fi.Name, ft.getGolangTypeName(fi.VarInfo))
			}), ", ")
			return fmt.Sprintf("struct{%s}", fields)
		}

		n := vit.GetPackagedName(ft.fPackage.Name)
		if vit.GenericTypes != nil {
			types := strings.Join(lo.Map(vit.GenericTypes, func(gt restream.VarInfo, _ int) string { return ft.getGolangTypeName(gt) }), ", ")
			n += fmt.Sprintf("[%s]", types)
		}
		return n
	default:
		panic("unhandled type")
	}
}

// buildGolangSerializer builds the golang serializer for a given type
func (pt *ProjTracking) buildGolangSerializer(pkg *packages.Package, typeName *types.TypeName, fields []*restream.FieldInfo) {
	si := StructInfo{Name: typeName.Name(), Package: pkg.Name}
	outSerialization := createGolangStructNonFieldedSerializers(si, fields, false)
	pt.goGenEntries = append(pt.goGenEntries, fdef{name: typeName.Name(), defs: outSerialization})
}

// buildGolangSerializerLookup builds the golang serializer lookup for a given type
func (pt *ProjTracking) buildGolangSerializerLookup() {
	out := "// ReStreamExtraSerializers is a map of registered type names to their serializer/deserializer functions\n"
	out += "var ReStreamExtraSerializers = map[reflect.Type]restream.StructSerializerInfo{\n"

	for _, s := range pt.config.BuildSerializers {
		parts := strings.Split(s, "/")
		pkgName := parts[len(parts)-2]
		typeName := parts[len(parts)-1]
		out += fmt.Sprintf("    reflect.TypeFor[%s.%s](): { Serialize: Serialize%s, Deserialize: Deserialize%s },\n",
			pkgName, typeName, typeName, typeName)
	}

	out += "}\n"
	pt.goGenEntries = append(pt.goGenEntries, fdef{name: "ReStreamExtraSerializers", defs: out})
}
