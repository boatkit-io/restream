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
		ft.goGenEntries = append(ft.goGenEntries, fdef{name: si.Name, defs: outSerialization})
	} else {
		outSerialization := createGolangStructNonFieldedSerializers(si, fields, true)
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
		outPartial += fmt.Sprintf("func (s *%sPartial) ApplyTo(por any) ([][]any) {\n", si.Name)
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
		outPartial += "    return restream.ReduceFieldPaths(ret)\n"
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

		sip := si
		sip.Name += "Partial"

		outPartial += createGolangStructFieldedSerializers(sip, partialFields)

		ft.goGenEntries = append(ft.goGenEntries, fdef{name: sip.Name, defs: outPartial})
	}

	return nil
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
func (ft *FileTracking) createGoStoreMethods(si StructInfo, storeName string) {
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
	out += "}\n"

	ft.goGenEntries = append(ft.goGenEntries, fdef{name: si.Name + "Store", defs: out})
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

	for _, imp := range pt.config.GoImports {
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
