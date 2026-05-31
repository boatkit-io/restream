// nolint:goconst
package main

import (
	"bytes"
	"fmt"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/dave/dst"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"golang.org/x/tools/go/packages"
)

const (
	defaultTSRuntimeImportPath = "@boatkit-io/restream"
	tsRuntimeImportModeLocal   = "local"
	tsRuntimeImportModePackage = "package"
)

// restreamTypesToIgnore is a hardcoded list of restream types that we don't want to generate typescript for, since we're adding
// them in separately in a kinda hacky way
var restreamTypesToIgnore = []string{"SerializationType", "_SerializationTypeName"}

const restreamIgnoreAnnotation = "@restream.Ignore"

// createTSStructSerializers is an inner helper to produce Typescript-based structs and serialization helpers from the
// source structures
func (ft *FileTracking) createTSStructSerializers(si StructInfo, fields []*restream.FieldInfo, partialFields []*restream.FieldInfo) error {
	deps := map[string]struct{}{}
	for _, fi := range fields {
		ft.documentTSSamePackageTypeDeps(fi.VarInfo, deps)
	}

	// Calculate main structs and/or deserialization functions for typescript
	classDef := ft.genTSClass(si, fields, false)
	ft.tsGenEntries = append(ft.tsGenEntries, fdef{name: si.Name, defs: classDef, typ: fdefTypeOther, deps: lo.Keys(deps)})
	if tsClassUsesFieldPathReducer(si, false) {
		ft.tsGenEntries = append(ft.tsGenEntries, fdef{name: "reduceFieldPaths", defs: genTSReduceFieldPaths(), typ: fdefTypeOther})
	}

	if ft.shouldBuildPartial(si.Name) {
		sip := si
		sip.Name += "Partial"

		partialClassDef := ft.genTSClass(sip, partialFields, true)
		ft.tsGenEntries = append(ft.tsGenEntries, fdef{name: sip.Name, defs: partialClassDef, typ: fdefTypeOther, deps: lo.Keys(deps)})
		ft.tsGenEntries = append(ft.tsGenEntries, fdef{name: "reduceFieldPaths", defs: genTSReduceFieldPaths(), typ: fdefTypeOther})
	}

	return nil
}

func tsClassUsesFieldPathReducer(si StructInfo, partial bool) bool {
	if partial {
		return true
	}
	switch si.Name {
	case "PartialArray", "PartialMap", "PartialModArray", "PartialModMap", "PartialValue":
		return true
	default:
		return false
	}
}

// documentTSSamePackageTypeDeps is a helper for collecting all the typescript types that a given type depends on, for helping
// build the package file in the right order (reverse dependency order)
func (ft *FileTracking) documentTSSamePackageTypeDeps(vi restream.VarInfo, deps map[string]struct{}) {
	if vi == nil {
		return
	}
	switch v := vi.(type) {
	case *restream.VarInfoPointer:
		ft.documentTSSamePackageTypeDeps(v.SubType, deps)
	case *restream.VarInfoArray:
		ft.documentTSSamePackageTypeDeps(v.ElemType, deps)
	case *restream.VarInfoMap:
		ft.documentTSSamePackageTypeDeps(v.KeyType, deps)
		ft.documentTSSamePackageTypeDeps(v.ElemType, deps)
	case *restream.VarInfoStruct:
		if v.Package == ft.fPackage.Name {
			deps[v.Name] = struct{}{}
		}
		for _, fi := range v.FieldList {
			ft.documentTSSamePackageTypeDeps(fi.VarInfo, deps)
		}
	}
}

// genTSFieldInfo is a reusable helper to build the static fieldinfo struct for a given set of fields/a struct
func genTSFieldInfo(fields []*restream.FieldInfo) string {
	out := ""
	if len(fields) > 0 {
		out += "    public static readonly fieldInfo: readonly FieldInfo[] = [\n"
		for _, fi := range fields {
			out += "        " + fi.ToTSString() + ",\n"
		}
		out += "    ];\n"
	}

	if lo.SomeBy(fields, func(fi *restream.FieldInfo) bool { return fi.FieldID != 0 }) {
		if out != "" {
			out += "\n"
		}
		out += "    private static readonly _fieldMap: ReadonlyMap<number, FieldInfo> = new Map<number, FieldInfo>([\n"
		for idx, fi := range fields {
			out += fmt.Sprintf("        [%d, this.fieldInfo[%d]],\n", fi.FieldID, idx)
		}
		out += "    ]);\n"
	}

	return out
}

// genTSClass is a reusable helper to build the typescript class for a given struct
func (ft *FileTracking) genTSClass(si StructInfo, fields []*restream.FieldInfo, partial bool) string {
	genParams := ""
	if si.GenericParams != nil {
		genParams = genTSGenericClassSignature(si)
	}
	out := fmt.Sprintf("export class %s%s {\n", si.Name, genParams)

	for _, fi := range fields {
		out += fmt.Sprintf("    public %s!: %s;\n", getTSFieldName(fi), ft.getTSType(fi.VarInfo))
	}

	out += "\n"
	out += "    private constructor() {}\n"
	out += "\n"

	out += ft.genTSFromValuesConstructor(si, fields) + "\n"

	if si.Fielded {
		out += "    public static deserialized(r: BinaryReader) {\n"
		out += "        let fieldMap: Map<number, unknown>|undefined;\n"
		out += "        if (r) {\n"
		out += "            const sl = ReStreamDecoders.decodeUint32(r);\n"
		out += fmt.Sprintf("            fieldMap = ReStreamDecoders.decodeFieldMap(r.slice(sl), %s._fieldMap);\n", si.Name)
		out += "        }\n"
		out += fmt.Sprintf("        const o = new %s();\n", si.TSNameWithParams())

		for _, fi := range fields {
			out += fmt.Sprintf("        o.%s = fieldMap?.has(%d) ? fieldMap.get(%d) as %s : %s;\n",
				getTSFieldName(fi), fi.FieldID, fi.FieldID, ft.getTSType(fi.VarInfo), ft.getTSZeroValue(fi.VarInfo))
		}
		out += "        return o;\n"
		out += "    }" + "\n\n"

		out += "    public serialize(w: BinaryWriter, _: VarInfoStruct | undefined) {\n"
		out += "        const wi = new BinaryWriter();\n"

		for idx, fi := range fields {
			out += fmt.Sprintf("        ReStreamEncoders.serializeField(this.%s, %s.fieldInfo[%d], wi);\n",
				getTSFieldName(fi), si.Name, idx)
		}
		out += "        const b = wi.getBytes();\n"
		out += "        ReStreamEncoders.serializePackedInt(b.length, w);\n"
		out += "        w.writeBytes(b);\n"
		out += "    }" + "\n\n"
	} else {
		out += ft.genTSNonFieldedStructSerializers(si, fields) + "\n"
	}

	out += genTSFieldInfo(fields)

	if partial {
		out += "\n"
		out += fmt.Sprintf("    applyTo(por: %s): (string | number)[][] {\n", strings.TrimSuffix(si.Name, "Partial"))
		out += "        const ret: (string | number)[][] = [];\n"
		for _, fi := range fields {
			fn := getTSFieldName(fi)
			isPartial := ft.supportsPartials(fi.VarInfo)
			isApplyOnTop := false
			if fip, is := fi.VarInfo.(*restream.VarInfoPointer); is && isPartial {
				if fis, is := fip.SubType.(*restream.VarInfoStruct); is && fis.Name == "PartialValue" {
					isApplyOnTop = true
				}
			}

			if isPartial {
				if isApplyOnTop {
					out += fmt.Sprintf("        if (this.%s !== undefined) { let fs; [por.%s,fs] = this.%s.applyOnTop(por.%s); "+
						"for (const f of fs) { ret.push([\"%s\",...f]); }}\n",
						fn, fn, fn, fn, fn)
				} else {
					switch getTSPartialCollectionKind(fi.VarInfo) {
					case "array":
						out += fmt.Sprintf("        if (this.%s !== undefined) { "+
							"if (!Array.isArray(por.%s)) { por.%s = Array.from(por.%s ?? []); } "+
							"const fs = this.%s.applyTo(por.%s!); "+
							"for (const f of fs) { ret.push([\"%s\",...f]); }}\n",
							fn, fn, fn, fn, fn, fn, fn)
					case "map":
						out += fmt.Sprintf("        if (this.%s !== undefined) { "+
							"if (!(por.%s instanceof Map)) { por.%s = new Map(); } "+
							"const fs = this.%s.applyTo(por.%s!); "+
							"for (const f of fs) { ret.push([\"%s\",...f]); }}\n",
							fn, fn, fn, fn, fn, fn)
					default:
						out += fmt.Sprintf("        if (this.%s !== undefined) { const fs = this.%s.applyTo(por.%s!); "+
							"for (const f of fs) { ret.push([\"%s\",...f]); }}\n",
							fn, fn, fn, fn)
					}
				}
			} else {
				if isPointerToPointer(fi.VarInfo) {
					out += fmt.Sprintf("        if (this.%s !== undefined) { por.%s = this.%s === null ? undefined : this.%s; ret.push([\"%s\"]); }\n",
						fn, fn, fn, fn, fn)
				} else {
					out += fmt.Sprintf("        if (this.%s !== undefined) { por.%s = this.%s; ret.push([\"%s\"]); }\n",
						fn, fn, fn, fn)
				}
			}
		}
		out += "        return reduceFieldPaths(ret);\n"
		out += "    }" + "\n"
	}

	out += genTSPartialAugmentations(si)

	out += "}\n"
	return out
}

// genTSFromValuesConstructor is a reusable helper to build the typescript fromValues constructor for a given struct
func (ft *FileTracking) genTSFromValuesConstructor(si StructInfo, fields []*restream.FieldInfo) string {
	var out string
	if si.GenericParams != nil {
		out += "    public static fromValues" + genTSGenericClassSignature(si) + "(\n"
	} else {
		out += "    public static fromValues(\n"
	}
	for _, fi := range fields {
		out += fmt.Sprintf("        %s: %s = %s,\n", getTSFieldName(fi), ft.getTSType(fi.VarInfo), ft.getTSZeroValue(fi.VarInfo))
	}
	out += "    ) {\n"
	out += "        const o = new " + si.TSNameWithParams() + "();\n"
	for _, fi := range fields {
		out += fmt.Sprintf("        o.%s = %s;\n", getTSFieldName(fi), getTSFieldName(fi))
	}
	out += "        return o;\n"
	out += "    }" + "\n"
	return out
}

// genTSNonFieldedStructSerializers is a reusable helper to build the typescript serialization funcs for a non-fielded struct
func (ft *FileTracking) genTSNonFieldedStructSerializers(si StructInfo, fields []*restream.FieldInfo) string {
	out := ""
	if si.GenericParams != nil {
		out += "    public static deserialized" + genTSGenericClassSignature(si) +
			"(r: BinaryReader, vi: VarInfoStruct | undefined) {\n"
		out += "    	const gm = new Map<string,VarInfo>([\n"
		for idx, gt := range si.GenericParams {
			out += fmt.Sprintf("            [%q, vi!.genericTypes![%d]],\n", gt, idx)
		}
		out += "        ]);\n"
	} else {
		if len(fields) == 0 {
			out += "    public static deserialized(_: BinaryReader, __: VarInfoStruct | undefined) {\n"
		} else {
			out += "    public static deserialized(r: BinaryReader, _: VarInfoStruct | undefined) {\n"
		}
	}

	out += fmt.Sprintf("        const o = new %s();\n", si.TSNameWithParams())
	for idx, fi := range fields {
		if si.GenericParams != nil {
			out += fmt.Sprintf("        o.%s = ReStreamDecoders.deserializeValue(r, this.fieldInfo[%d].varInfo.fillGenerics(gm));\n",
				getTSFieldName(fi), idx)
		} else {
			out += fmt.Sprintf("        o.%s = ReStreamDecoders.deserializeValue(r, this.fieldInfo[%d].varInfo);\n", getTSFieldName(fi), idx)
		}
	}
	out += "        return o;\n"
	out += "    }" + "\n\n"

	if si.GenericParams != nil {
		out += "    public serialize(w: BinaryWriter, vi: VarInfoStruct | undefined) {\n"
		out += "    	const gm = new Map<string,VarInfo>([\n"
		for idx, gt := range si.GenericParams {
			out += fmt.Sprintf("            [%q, vi!.genericTypes![%d]],\n", gt, idx)
		}
		out += "    ]);\n"
	} else {
		if len(fields) == 0 {
			out += "    public serialize(_: BinaryWriter, __: VarInfoStruct | undefined) {\n"
		} else {
			out += "    public serialize(w: BinaryWriter, _: VarInfoStruct | undefined) {\n"
		}
	}

	for idx, fi := range fields {
		if si.GenericParams != nil {
			out += fmt.Sprintf("        ReStreamEncoders.serializeValue(this.%s, w, %s.fieldInfo[%d].varInfo.fillGenerics(gm));\n",
				getTSFieldName(fi), si.Name, idx)
		} else {
			out += fmt.Sprintf("        ReStreamEncoders.serializeValue(this.%s, w, %s.fieldInfo[%d].varInfo);\n",
				getTSFieldName(fi), si.Name, idx)
		}
	}
	out += "    }" + "\n"
	return out
}

// collectTSPackages gathers all the generated typescript blocks into go-project-wide typescript files
func (pt *ProjTracking) collectTSPackages() error {
	// Gather files into packages -- first pass pulls in what we've generated
	for _, ft := range pt.files {
		pkg := ft.fPackage
		for _, def := range ft.tsGenEntries {
			pt.addTSPackageFileEntry(pkg, def)
		}
	}

	// Second pass pulls in all referenced types
	for _, ft := range pt.files {
		for pkg, refs := range ft.tsPrimitiveImports {
			for _, ref := range refs {
				typeName := ref.Name()
				tstr, err := genTSEnumDecl(typeName, pkg)
				if err != nil {
					return err
				}
				pt.addTSPackageFileEntry(pkg, fdef{name: typeName, defs: tstr, typ: fdefTypeEnum})
			}
		}

		typeOnlyImpMap, found := pt.tsTypeOnlyImports[ft.fPackage]
		if !found {
			typeOnlyImpMap = map[*packages.Package][]string{}
			pt.tsTypeOnlyImports[ft.fPackage] = typeOnlyImpMap
		}
		impMap, found := pt.tsTypeImports[ft.fPackage]
		if !found {
			impMap = map[*packages.Package][]string{}
			pt.tsTypeImports[ft.fPackage] = impMap
		}
		for pkg, refs := range ft.tsPrimitiveImports {
			typeOnlyImpMap[pkg] = append(typeOnlyImpMap[pkg], lo.Map(refs, func(t *types.TypeName, _ int) string { return t.Name() })...)
		}
		for pkg, refs := range ft.tsStructImports {
			impMap[pkg] = append(impMap[pkg], lo.Map(refs, func(t *types.TypeName, _ int) string { return t.Name() })...)
		}
	}

	// Gather additional enums by package
	for _, enumFullName := range pt.config.AdditionalEnums {
		parts := strings.Split(enumFullName, ".")
		if len(parts) < 2 {
			return fmt.Errorf("additionalEnum passed without package name (%s)", enumFullName)
		}
		pkgURL := strings.Join(parts[:len(parts)-1], ".")
		enumName := parts[len(parts)-1]

		pkg, err := pt.getPackageForPath(pkgURL, false)
		if err != nil {
			return fmt.Errorf("no package found for %s referenced by additionalEnums: %w", enumFullName, err)
		}
		tstr, err := genTSEnumDecl(enumName, pkg)
		if err != nil {
			return err
		}
		pt.addTSPackageFileEntry(pkg, fdef{name: enumName, defs: tstr, typ: fdefTypeEnum})
	}

	return nil
}

// writeTSPackageFiles writes out the typescript file for each package
func (pt *ProjTracking) writeTSPackageFiles() error {
	testPackageFileNames := pt.tsTestPackageFileNames()
	for pkg, defs := range pt.tsPackageEntries {
		imps := []tsImport{}
		for pkgRef, pkgImps := range pt.tsTypeImports[pkg] {
			imps = append(imps, tsImport{Path: pt.tsPackageImportPath(pkgRef), Imports: lo.Uniq(pkgImps)})
		}
		for pkgRef, pkgImps := range pt.tsTypeOnlyImports[pkg] {
			imps = append(imps, tsImport{Path: pt.tsPackageImportPath(pkgRef), TypeImports: lo.Uniq(pkgImps)})
		}

		fileName := pt.tsPackageFileName(pkg, testPackageFileNames)
		if err := pt.writeTSFile(fileName, defs, imps); err != nil {
			return err
		}
	}
	return nil
}

func (pt *ProjTracking) tsTestPackageFileNames() map[*packages.Package]string {
	packagesByBaseName := map[string][]*packages.Package{}
	for pkg := range pt.tsPackageEntries {
		baseName := "Package" + toPublicName(pkg.Name)
		packagesByBaseName[baseName] = append(packagesByBaseName[baseName], pkg)
	}

	testPackageFileNames := map[*packages.Package]string{}
	for baseName, pkgs := range packagesByBaseName {
		if len(pkgs) < 2 {
			continue
		}
		for _, pkg := range pkgs {
			if isTestPackage(pkg) {
				testPackageFileNames[pkg] = baseName + "Test.ts"
			}
		}
	}
	return testPackageFileNames
}

func (pt *ProjTracking) tsPackageFileName(pkg *packages.Package, testPackageFileNames map[*packages.Package]string) string {
	if fileName, ok := testPackageFileNames[pkg]; ok {
		return fileName
	}
	return "Package" + toPublicName(pkg.Name) + ".ts"
}

// createTSPerFileData does a look through of the entire source file's consts/enums/etc. to see what else we need to
// bring over into typescript-land to support the stores/states.
func (ft *FileTracking) createTSPerFileData() error {
	// Pull any consts/type aliasing out
	for _, decl := range ft.f.Decls {
		gd, ok := decl.(*dst.GenDecl)
		if !ok || len(gd.Specs) < 1 {
			continue
		}

		if gd.Tok == token.TYPE {
			gds, ok := gd.Specs[0].(*dst.TypeSpec)
			if !ok {
				continue
			}

			_, ok = gds.Type.(*dst.Ident)
			if !ok {
				continue
			}

			typeName := gds.Name.Name

			if err := ft.genTSEnumDecl(typeName, ft.fPackage); err != nil {
				return err
			}
		}

		if gd.Tok == token.CONST {
			vs, ok := gd.Specs[0].(*dst.ValueSpec)
			if !ok {
				continue
			}

			if len(vs.Names) != 1 || len(vs.Values) != 1 {
				continue
			}
			bl, ok := vs.Values[0].(*dst.BasicLit)
			if !ok {
				continue
			}

			if slices.Contains(restreamTypesToIgnore, vs.Names[0].Name) || hasRestreamIgnoreAnnotation(gd, vs) {
				continue
			}

			decName := vs.Names[0].Name
			decStr := fmt.Sprintf("export const %s = %s;\n", decName, bl.Value)
			ft.tsGenEntries = append(ft.tsGenEntries, fdef{name: decName, defs: decStr, typ: fdefTypeEnum})
		}
	}

	return nil
}

func hasRestreamIgnoreAnnotation(gd *dst.GenDecl, vs *dst.ValueSpec) bool {
	for _, dec := range append(gd.Decorations().Start.All(), vs.Decorations().Start.All()...) {
		if strings.Contains(dec, restreamIgnoreAnnotation) {
			return true
		}
	}
	return false
}

// genTSEnumDecl builds a typescript enum/type mapping for a given const type, scoped to within a file
func (ft *FileTracking) genTSEnumDecl(typeName string, pkg *packages.Package) error {
	if slices.Contains(restreamTypesToIgnore, typeName) {
		return nil
	}
	tstr, err := genTSEnumDecl(typeName, pkg)
	if err != nil {
		return err
	}
	ft.tsGenEntries = append(ft.tsGenEntries, fdef{name: typeName, defs: tstr, typ: fdefTypeEnum})
	return nil
}

// genTSEnumDecl builds a typescript enum for a given const type in the source golang file (reusable helper in a few spots)
func genTSEnumDecl(typeName string, pkg *packages.Package) (string, error) {
	vals := map[string]string{}
	for _, pti := range pkg.TypesInfo.Defs {
		if ci, is := pti.(*types.Const); is {
			if nt, is := ci.Type().(*types.Named); is {
				if nt.Obj().Name() == typeName {
					name := strings.TrimPrefix(ci.Name(), typeName)
					vals[name] = ci.Val().ExactString()
				}
			}
		}
	}

	tn := pkg.Types.Scope().Lookup(typeName).(*types.TypeName)
	if tn == nil {
		return "", fmt.Errorf("unknown type in genTSEnumDecl: %s", typeName)
	}
	tnu := tn.Type().Underlying()
	var outType string
	switch tnu.String() {
	case "int", "uint", "uint32", "int32", "int64", "uint64", "int8", "uint8", "byte", "uint16", "int16", "float32", "float64":
		outType = "number"
	case "string":
		outType = "string"
	default:
		return "", fmt.Errorf("unhandled underlying type in genTSEnumDecl: %s", tnu.String())
	}

	if len(vals) == 0 {
		// no enum, must just be a straight typedef
		tstr := fmt.Sprintf("export type %s = %s;\n", typeName, outType)
		return tstr, nil
	}

	tstr := fmt.Sprintf("export enum %s {\n", typeName)
	valSlice := lo.MapToSlice(vals, func(k, v string) []string { return []string{k, v} })
	var erro error
	slices.SortFunc(valSlice, func(r1, r2 []string) int {
		if outType == "number" {
			r1v, err := strconv.ParseInt(r1[1], 10, 32)
			if err != nil {
				erro = fmt.Errorf("bad enum int conversion (%s): %w", r1[1], err)
			}
			r2v, err := strconv.ParseInt(r2[1], 10, 32)
			if err != nil {
				erro = fmt.Errorf("bad enum int conversion (%s): %w", r2[1], err)
			}
			return int(r1v - r2v)
		}
		return strings.Compare(r1[1], r2[1])
	})
	if erro != nil {
		return "", erro
	}
	for _, vs := range valSlice {
		tstr += fmt.Sprintf("    %s = %s,\n", vs[0], vs[1])
	}
	tstr += "}\n"
	return tstr, nil
}

// getTSType calculates the typescript type for a golang type
func (ft *FileTracking) getTSType(vi restream.VarInfo) string {
	switch vit := vi.(type) {
	case *restream.VarInfoPrimitive:
		if vit.MappedType != nil && !slices.Contains([]string{"int", "uint", "byte"}, *vit.MappedType) {
			ft.addTSTypeRef(*vit.MappedType, true)
			mtp := strings.Split(*vit.MappedType, ".")
			return mtp[len(mtp)-1]
		}
		switch vit.DataType {
		case restream.SerializationTypeBool:
			return "boolean"
		case restream.SerializationTypeString:
			return "string"
		case restream.SerializationTypeInt8, restream.SerializationTypeInt16, restream.SerializationTypeInt32, restream.SerializationTypeInt64,
			restream.SerializationTypeUint8, restream.SerializationTypeUint16, restream.SerializationTypeUint32, restream.SerializationTypeUint64,
			restream.SerializationTypeFloat32, restream.SerializationTypeFloat64:
			return "number"
		case restream.SerializationTypeTime:
			return "Date"
		default:
			panic(fmt.Sprintf("unhandled type in getTSType: %+v", vit.DataType))
		}
	case *restream.VarInfoStruct:
		ft.addTSTypeRef(vit.Package+"."+vit.Name, false)
		n := vit.Name
		if vit.GenericTypes != nil {
			genStrings := lo.Map(vit.GenericTypes, func(gt restream.VarInfo, _ int) string { return ft.getTSType(gt) })
			switch vit.Name {
			case "PartialValue":
				genStrings[1] = strings.TrimSuffix(genStrings[1], "|undefined")
			case "PartialModArray":
				genStrings[1] = strings.TrimSuffix(genStrings[1], "|undefined")
			case "PartialModMap":
				genStrings[2] = strings.TrimSuffix(genStrings[2], "|undefined")
			}
			n += "<" + strings.Join(genStrings, ", ") + ">"
		}
		return n
	case *restream.VarInfoDynamic:
		return "any"
	case *restream.VarInfoPointer:
		ut := ft.getTSType(vit.SubType)
		if vit.NotNil {
			return ut
		}
		if isPointerToPointer(vit) {
			ut = strings.TrimSuffix(ut, "|undefined")
			ut = appendTSTypeUnionMember(ut, "null")
		}
		return appendTSTypeUnionMember(ut, "undefined")
	case *restream.VarInfoArray:
		it := ft.getTSType(vit.ElemType)
		if strings.Contains(it, "|") {
			it = "(" + it + ")"
		}
		ut := it + "[]"
		if vit.NotNil {
			return ut
		}
		return ut + "|undefined"
	case *restream.VarInfoMap:
		kt := ft.getTSType(vit.KeyType)
		ut := ""
		if vit.ElemType == nil {
			ut = fmt.Sprintf("Set<%s>", kt)
		} else {
			vt := ft.getTSType(vit.ElemType)
			ut = fmt.Sprintf("Map<%s,%s>", kt, vt)
		}
		if vit.NotNil {
			return ut
		}
		return ut + "|undefined"
	case *restream.VarInfoGenericParam:
		return vit.Name
	default:
		panic(fmt.Sprintf("unhandled type in ft.getTSType: %+v", vit))
	}
}

// isPointerToPointer checks for generated partial fields where the outer pointer means field presence and the inner
// pointer is the actual value.
func isPointerToPointer(vi restream.VarInfo) bool {
	vip, ok := vi.(*restream.VarInfoPointer)
	if !ok {
		return false
	}
	_, ok = vip.SubType.(*restream.VarInfoPointer)
	return ok
}

func getTSPartialCollectionKind(vi restream.VarInfo) string {
	vip, ok := vi.(*restream.VarInfoPointer)
	if !ok {
		return ""
	}
	vis, ok := vip.SubType.(*restream.VarInfoStruct)
	if !ok || vis.Package != "restream" {
		return ""
	}
	switch vis.Name {
	case "PartialArray", "PartialModArray":
		return "array"
	case "PartialMap", "PartialModMap":
		return "map"
	default:
		return ""
	}
}

func appendTSTypeUnionMember(t, member string) string {
	if t == member || strings.Contains(t, "|"+member+"|") || strings.HasSuffix(t, "|"+member) {
		return t
	}
	return t + "|" + member
}

// getTSZeroValue gets the zero value in typescript for a golang type
func (ft *FileTracking) getTSZeroValue(vi restream.VarInfo) string {
	switch vit := vi.(type) {
	case *restream.VarInfoPrimitive:
		var rv string
		switch vit.DataType {
		case restream.SerializationTypeBool:
			rv = "false"
		case restream.SerializationTypeString:
			rv = "\"\""
		case restream.SerializationTypeInt8, restream.SerializationTypeInt16, restream.SerializationTypeInt32, restream.SerializationTypeInt64,
			restream.SerializationTypeUint8, restream.SerializationTypeUint16, restream.SerializationTypeUint32, restream.SerializationTypeUint64,
			restream.SerializationTypeFloat32, restream.SerializationTypeFloat64:
			rv = "0"
		case restream.SerializationTypeTime:
			rv = "new Date(0)"
		default:
			panic(fmt.Sprintf("unhandled type in getTSZeroValue: %+v", vit.DataType))
		}
		if vit.MappedType != nil && !slices.Contains([]string{"int", "uint"}, *vit.MappedType) {
			mtp := strings.Split(*vit.MappedType, ".")
			return rv + " as " + mtp[len(mtp)-1]
		}
		return rv
	case *restream.VarInfoStruct:
		return fmt.Sprintf("%s.fromValues()", vit.Name)
	case *restream.VarInfoDynamic, *restream.VarInfoPointer:
		return "undefined"
	case *restream.VarInfoArray:
		return "[]"
	case *restream.VarInfoMap:
		if vit.NotNil {
			if vit.ElemType == nil {
				return "new Set()"
			}
			return "new Map()"
		}
		return "undefined"
	default:
		panic(fmt.Sprintf("unhandled type in getTSZeroValue: %+v", vi))
	}
}

// buildTSRPCStructs is a helper to build the typescript RPC structs
func (ft *FileTracking) buildTSRPCStructs(rpcn, rpctn string, reqFields []*restream.FieldInfo, respFields []*restream.FieldInfo) error {
	rt := "void"
	if len(respFields) > 1 {
		rt = ft.getTSType(respFields[0].VarInfo)
	}

	outTS := fmt.Sprintf("export class %sRequest extends RPCStruct<%sResponse,%s> {\n", rpctn, rpctn, rt)
	for _, fi := range reqFields {
		outTS += fmt.Sprintf("    public %s!: %s;\n", getTSFieldName(fi), ft.getTSType(fi.VarInfo))
	}

	outTS += fmt.Sprintf("    private constructor() { super(%q, %sResponse); }\n\n", rpcn, rpctn)

	sirq := StructInfo{Name: rpctn + "Request"}
	outTS += ft.genTSFromValuesConstructor(sirq, reqFields) + "\n"
	outTS += genTSFieldInfo(reqFields)
	outTS += ft.genTSNonFieldedStructSerializers(sirq, reqFields)
	outTS += "}\n"
	ft.tsGenEntries = append(ft.tsGenEntries, fdef{name: sirq.Name, defs: outTS, typ: fdefTypeOther})

	sirs := StructInfo{
		Name: rpctn + "Response",
	}
	respOut := ft.genTSClass(sirs, respFields, false)
	ft.tsGenEntries = append(ft.tsGenEntries, fdef{name: sirs.Name, defs: respOut, typ: fdefTypeOther})

	return nil
}

// buildTSEventStruct is a helper to build the typescript event packet struct.
func (ft *FileTracking) buildTSEventStruct(eventName, eventTypeName string, eventFields []*restream.FieldInfo) error {
	si := StructInfo{Name: eventTypeName + "Event"}
	outTS := fmt.Sprintf("export class %s extends EventStruct {\n", si.Name)
	for _, fi := range eventFields {
		outTS += fmt.Sprintf("    public %s!: %s;\n", getTSFieldName(fi), ft.getTSType(fi.VarInfo))
	}

	outTS += "\n"
	outTS += fmt.Sprintf("    public static readonly eventBoundName = %q;\n", eventName)
	outTS += fmt.Sprintf("    private constructor() { super(%s.eventBoundName); }\n\n", si.Name)

	outTS += ft.genTSFromValuesConstructor(si, eventFields) + "\n"
	outTS += genTSFieldInfo(eventFields)
	outTS += ft.genTSNonFieldedStructSerializers(si, eventFields)
	outTS += "}\n"

	deps := map[string]struct{}{}
	for _, fi := range eventFields {
		ft.documentTSSamePackageTypeDeps(fi.VarInfo, deps)
	}
	ft.tsGenEntries = append(ft.tsGenEntries, fdef{name: si.Name, defs: outTS, typ: fdefTypeOther, deps: lo.Keys(deps)})

	return nil
}

// createTSStoreNameConst adds the store name constant to the generated TypeScript package output.
func (ft *FileTracking) createTSStoreNameConst(storeTypeName, storeName string) {
	constName := storeTypeName + "Name"
	decStr := fmt.Sprintf("export const %s = %q;\n", constName, storeName)
	ft.tsGenEntries = append(ft.tsGenEntries, fdef{name: constName, defs: decStr, typ: fdefTypeEnum})
}

// addTSTypeRef adds a marker for later processing that a type was referenced by a source structure, so we know to pull
// that into the output typescript later on after the main storestate structs.
func (ft *FileTracking) addTSTypeRef(typeName string, isEnum bool) {
	parts := strings.Split(typeName, ".")
	pkgName := parts[0]
	refName := parts[1]

	// Don't need to raise anything to the extra global level that's in-package
	if pkgName == ft.fPackage.Name /*|| pkgName == "restream"*/ {
		return
	}
	if pkgName == "restream" && isEnum && slices.Contains(restreamTypesToIgnore, refName) {
		return
	}

	var has bool
	var pkg *packages.Package
	if pkgName == "restream" {
		var err error
		pkg, err = ft.pt.getRestreamPackage(false)
		if err != nil {
			panic(err)
		}
	} else {
		pkg, has = ft.importLookup[pkgName]
		if !has {
			if pkgName == ft.fPackage.Name {
				pkg = ft.fPackage
			} else {
				panic("unknown package in addTSTypeRef: " + pkgName)
			}
		}
	}

	var tl []*types.TypeName
	if isEnum {
		tl, has = ft.tsPrimitiveImports[pkg]
	} else {
		tl, has = ft.tsStructImports[pkg]
	}
	if !has {
		tl = []*types.TypeName{}
	}

	if !lo.SomeBy(tl, func(tn *types.TypeName) bool { return tn.Name() == refName }) {
		rfl := pkg.Types.Scope().Lookup(refName)
		if rfl == nil {
			panic("Unable to look up " + refName + " in package " + pkg.Name)
		}
		tn, ok := rfl.(*types.TypeName)
		if tn == nil || !ok {
			panic("unknown type in addTSTypeRef: " + typeName)
		}
		tl = append(tl, tn)
		if isEnum {
			ft.tsPrimitiveImports[pkg] = tl
		} else {
			ft.tsStructImports[pkg] = tl
		}
	}
}

// buildTSSerializer is a helper to build the typescript serializer for a given BuildSerializer entry
func (pt *ProjTracking) buildTSSerializer(pkg *packages.Package, typeName *types.TypeName, fields []*restream.FieldInfo) {
	fakeFt := &FileTracking{
		pt:       pt,
		fPackage: pkg,

		tsStructImports:    map[*packages.Package][]*types.TypeName{},
		tsPrimitiveImports: map[*packages.Package][]*types.TypeName{},
	}
	si := StructInfo{Name: typeName.Name(), Package: pkg.Name}
	outSerialization := fakeFt.genTSClass(si, fields, false)
	fakeFt.tsGenEntries = append(fakeFt.tsGenEntries, fdef{name: si.Name, defs: outSerialization, typ: fdefTypeOther})
	pt.files = append(pt.files, fakeFt)
}

// addTSPackageFileEntry adds a typescript file entry to the project tracking structure
func (pt *ProjTracking) addTSPackageFileEntry(pkg *packages.Package, def fdef) {
	arr, ok := pt.tsPackageEntries[pkg]
	if !ok {
		arr = []fdef{}
	}

	if lo.SomeBy(arr, func(x fdef) bool {
		return x.name == def.name
	}) {
		return
	}

	pt.tsPackageEntries[pkg] = append(arr, def)
}

// writeTSFile is a helper to write out a typescript file
func (pt *ProjTracking) writeTSFile(filename string, entries []fdef, pkgImports []tsImport) error {
	fc := restreamGeneratedFileBanner + `
/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unused-vars */
`

	imports, err := pt.tsRuntimeImports()
	if err != nil {
		return err
	}

	imports = append(imports, pt.config.TSImports...)
	imports = append(imports, pkgImports...)

	body := ""
	// Display enums before other types, since they seem to have to be referenced...
	writtenAny := false
	for _, fdt := range []fdefType{fdefTypeEnum, fdefTypeOther} {
		if !writtenAny {
			writtenAny = true
		} else {
			body += "\n"
		}

		entriesFiltered := lo.Filter(entries, func(f fdef, _ int) bool { return f.typ == fdt })
		// Now spit out by dependencies
		outputOrder := []fdef{}
		outputted := map[string]struct{}{}
		for len(entriesFiltered) > 0 {
			// Sort the struct list so it's consistent in the output
			slices.SortFunc(entriesFiltered, func(f1 fdef, f2 fdef) int { return strings.Compare(f1.name, f2.name) })

			progress := false
			for i := 0; i < len(entriesFiltered); {
				if len(entriesFiltered[i].deps) == 0 {
					// output!
					outputted[entriesFiltered[i].name] = struct{}{}
					outputOrder = append(outputOrder, entriesFiltered[i])
					entriesFiltered = append(entriesFiltered[:i], entriesFiltered[i+1:]...)
					progress = true
					continue
				}

				entriesFiltered[i].deps = lo.Filter(entriesFiltered[i].deps, func(dep string, _ int) bool {
					_, ok := outputted[dep]
					return !ok
				})
				i++
			}
			if !progress && len(entriesFiltered) > 0 {
				// If a type has an unresolved external dependency or a cycle, keep generation moving in deterministic order.
				outputted[entriesFiltered[0].name] = struct{}{}
				outputOrder = append(outputOrder, entriesFiltered[0])
				entriesFiltered = entriesFiltered[1:]
			}
		}
		body += strings.Join(lo.Map(outputOrder, func(f fdef, _ int) string { return f.defs }), "\n")
	}

	imports = filterTSImports(imports, body)

	slices.SortFunc(imports, func(f1 tsImport, f2 tsImport) int {
		if cmp := strings.Compare(f1.Path, f2.Path); cmp != 0 {
			return cmp
		}
		return strings.Compare(tsImportSortKey(f1), tsImportSortKey(f2))
	})

	for _, imp := range imports {
		for _, stmt := range tsImportStatements(imp) {
			fc += stmt + "\n"
		}
	}

	fc += "\n"
	fc += body

	tsDir := pt.resolveProjectPath(pt.config.TSDir)
	if err := os.MkdirAll(tsDir, 0o775); err != nil {
		return errors.Wrap(err, "error making TSDir")
	}
	tsFPath := path.Join(tsDir, filename)
	fmt.Printf("Writing out TS gen at: %s\n", tsFPath)
	if err := os.WriteFile(tsFPath, []byte(fc), 0o600); err != nil {
		return errors.Wrap(err, "writing TS file")
	}

	return nil
}

func (pt *ProjTracking) tsRuntimeImports() ([]tsImport, error) {
	switch mode := pt.tsRuntimeImportMode(); mode {
	case tsRuntimeImportModeLocal:
		return []tsImport{
			{
				Path:       "../utils/Decoders.js",
				ImportRoot: "* as ReStreamDecoders",
			},
			{
				Path:       "../utils/Encoders.js",
				ImportRoot: "* as ReStreamEncoders",
			},
			{
				Path: "../utils/SerializationTypes.js",
				Imports: []string{
					"VarInfoPrimitive", "VarInfoStruct", "VarInfoGenericParam", "VarInfoPointer", "VarInfoArray", "VarInfoMap",
					"VarInfoDynamic", "SerializationType",
				},
				TypeImports: []string{"VarInfo", "FieldInfo", "AppliablePartial", "AppliableOnTopPartial"},
			},
			{
				Path:       "../utils/BinaryReader.js",
				ImportRoot: "BinaryReader",
			},
			{
				Path:       "../utils/BinaryWriter.js",
				ImportRoot: "BinaryWriter",
			},
			{
				Path:    "../websocket/SocketHelper.js",
				Imports: []string{"EventStruct", "RPCResponseStruct", "RPCStruct"},
			},
		}, nil
	case tsRuntimeImportModePackage:
		runtimeImportPath := pt.tsRuntimeImportPath()
		return []tsImport{
			{
				Path:       runtimeImportPath,
				ImportRoot: "* as ReStreamDecoders",
			},
			{
				Path:       runtimeImportPath,
				ImportRoot: "* as ReStreamEncoders",
			},
			{
				Path: runtimeImportPath,
				Imports: []string{
					"BinaryReader", "BinaryWriter", "EventStruct", "RPCResponseStruct", "RPCStruct",
					"SerializationType", "VarInfoArray", "VarInfoDynamic", "VarInfoGenericParam", "VarInfoMap",
					"VarInfoPointer", "VarInfoPrimitive", "VarInfoStruct",
				},
				TypeImports: []string{"AppliableOnTopPartial", "AppliablePartial", "FieldInfo", "VarInfo"},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown tsRuntimeImportMode %q", mode)
	}
}

func (pt *ProjTracking) tsPackageImportPath(pkgRef *packages.Package) string {
	if pt.tsRuntimeImportMode() == tsRuntimeImportModePackage && pkgRef.PkgPath == restreamPackagePath {
		return pt.tsRuntimeImportPath()
	}
	return "./Package" + toPublicName(pkgRef.Name) + ".js"
}

func isTestPackage(pkg *packages.Package) bool {
	return strings.Contains(pkg.ID, "[")
}

func (pt *ProjTracking) tsRuntimeImportMode() string {
	if pt.config.TSRuntimeImportMode == "" {
		return tsRuntimeImportModePackage
	}
	return pt.config.TSRuntimeImportMode
}

func (pt *ProjTracking) tsRuntimeImportPath() string {
	if pt.config.TSRuntimeImportPath == "" {
		return defaultTSRuntimeImportPath
	}
	return pt.config.TSRuntimeImportPath
}

func tsImportStatements(imp tsImport) []string {
	stmts := []string{}
	if imp.ImportRoot != "" {
		stmts = append(stmts, fmt.Sprintf("import %s from '%s';", imp.ImportRoot, imp.Path))
	}
	if len(imp.Imports) > 0 {
		stmts = append(stmts, fmt.Sprintf("import %s from '%s';", tsNamedImportString(imp.Imports), imp.Path))
	}
	if len(imp.TypeImports) > 0 {
		stmts = append(stmts, fmt.Sprintf("import type %s from '%s';", tsNamedImportString(imp.TypeImports), imp.Path))
	}
	return stmts
}

func filterTSImports(imports []tsImport, body string) []tsImport {
	if strings.TrimSpace(body) == "" {
		return imports
	}

	filtered := []tsImport{}
	for _, imp := range imports {
		out := imp
		if out.ImportRoot != "" && !tsIdentifierUsed(body, tsImportRootIdentifier(out.ImportRoot)) {
			out.ImportRoot = ""
		}
		out.Imports = lo.Filter(out.Imports, func(name string, _ int) bool {
			return tsIdentifierUsed(body, name)
		})
		out.TypeImports = lo.Filter(out.TypeImports, func(name string, _ int) bool {
			return tsIdentifierUsed(body, name)
		})
		if out.ImportRoot != "" || len(out.Imports) > 0 || len(out.TypeImports) > 0 {
			filtered = append(filtered, out)
		}
	}
	return filtered
}

func tsImportRootIdentifier(importRoot string) string {
	if strings.HasPrefix(importRoot, "* as ") {
		return strings.TrimPrefix(importRoot, "* as ")
	}
	fields := strings.Fields(importRoot)
	if len(fields) == 0 {
		return importRoot
	}
	return fields[len(fields)-1]
}

func tsIdentifierUsed(body, ident string) bool {
	start := 0
	for {
		idx := strings.Index(body[start:], ident)
		if idx < 0 {
			return false
		}
		idx += start
		beforeOK := idx == 0 || !isTSIdentifierChar(body[idx-1])
		after := idx + len(ident)
		afterOK := after == len(body) || !isTSIdentifierChar(body[after])
		if beforeOK && afterOK {
			return true
		}
		start = after
	}
}

func isTSIdentifierChar(ch byte) bool {
	return ch == '$' || ch == '_' || (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
}

func tsImportSortKey(imp tsImport) string {
	parts := []string{}
	if imp.ImportRoot != "" {
		parts = append(parts, "0:"+imp.ImportRoot)
	}
	if len(imp.Imports) > 0 {
		parts = append(parts, "1:"+strings.Join(imp.Imports, ","))
	}
	if len(imp.TypeImports) > 0 {
		parts = append(parts, "2:"+strings.Join(imp.TypeImports, ","))
	}
	return strings.Join(parts, "\n")
}

func tsNamedImportString(imports []string) string {
	slices.Sort(imports)
	return fmt.Sprintf("{ %s }", strings.Join(imports, ", "))
}

// lintTSFiles lints the typescript files in the project
func (pt *ProjTracking) lintTSFiles() error {
	startTime := time.Now()
	fmt.Printf("ESLinting Web output directory...\n")

	cmd := exec.Command("pnpm", "exec", "eslint", "--fix", ".")
	outb := bytes.NewBuffer(nil)
	cmd.Stdout = outb
	cmd.Dir = pt.tsLintDir()
	if err := cmd.Run(); err != nil {
		fmt.Printf("ESLint output:\n%s\n", outb.String())
		return errors.Wrap(err, "ESLinting")
	}

	fmt.Printf("ESLinting Web output directory finished, took %s\n", time.Since(startTime))

	return nil
}

func (pt *ProjTracking) tsLintDir() string {
	startDir := path.Dir(pt.resolveProjectPath(pt.config.TSDir))
	dir := startDir
	for {
		if _, err := os.Stat(path.Join(dir, "package.json")); err == nil {
			return dir
		}

		parent := path.Dir(dir)
		if parent == dir || dir == pt.projPath {
			return startDir
		}
		dir = parent
	}
}

// genTSGenericClassSignature is a reusable helper to generate the typescript generic class signature for a given struct
func genTSGenericClassSignature(si StructInfo) string {
	var out string
	switch si.Name {
	case "PartialValue":
		out = `<V, P extends AppliablePartial<V>|AppliableOnTopPartial<V>>`
	case "PartialMap":
		out = `<K extends string|number, V>`
	case "PartialModMap":
		out = `<K extends string|number, V, P extends AppliablePartial<V>|AppliableOnTopPartial<V>>`
	case "PartialArray":
		out = `<V>`
	case "PartialModArray":
		out = `<V, P extends AppliablePartial<V>|AppliableOnTopPartial<V>>`
	default:
		out = "<" + strings.Join(si.GenericParams, ",") + ">"
	}
	return out
}

func genTSReduceFieldPaths() string {
	return `function reduceFieldPaths(fields: (string | number)[][]): (string | number)[][] {
    if (fields.length < 2) {
        return fields;
    }

    const ret: (string | number)[][] = [];
    for (const field of fields) {
        let suppressed = false;
        for (let idx = 0; idx < ret.length;) {
            const existing = ret[idx];
            if (fieldPathHasPrefix(field, existing)) {
                suppressed = true;
                idx++;
                continue;
            }
            if (fieldPathHasPrefix(existing, field)) {
                ret.splice(idx, 1);
                continue;
            }
            idx++;
        }
        if (!suppressed) {
            ret.push(field);
        }
    }
    return ret;
}

function fieldPathHasPrefix(field: (string | number)[], prefix: (string | number)[]): boolean {
    if (prefix.length > field.length) {
        return false;
    }
    for (let idx = 0; idx < prefix.length; idx++) {
        if (field[idx] !== prefix[idx]) {
            return false;
        }
    }
    return true;
}
`
}

// genTSPartialAugmentations is a reusable helper to glue in any hardcoded augmentations for a given struct
func genTSPartialAugmentations(si StructInfo) string {
	var out string
	switch si.Name {
	case "PartialValue":
		out = `
    applyOnTop(por: V): [V, (string | number)[][]] {
        const ret: [V, (string | number)[][]] = [por, []];
        if (this.whole) {
            ret[0] = this.whole;
            ret[1].push([]);
        }
        if (this.partial) {
            let fs: (string | number)[][] = [];
            if ((this.partial as AppliablePartial<V>).applyTo) {
                fs = (this.partial as AppliablePartial<V>).applyTo(ret[0]);
            }
            if ((this.partial as AppliableOnTopPartial<V>).applyOnTop) {
                [ret[0], fs] = (this.partial as AppliableOnTopPartial<V>).applyOnTop(ret[0]);
            }

            if (!this.whole) {
                ret[1] = fs;
            }
        }
        ret[1] = reduceFieldPaths(ret[1]);
        return ret;
    }
`
	case "PartialModMap":
		out = `
	applyTo(por: Map<K, V>): (string | number)[][] {
		const ret: (string | number)[][] = [];
		if (this.whole) {
			por.clear();
			for (const [k, v] of this.whole) {
				por.set(k, v);
			}
			ret.push([]);
		}
		for (const [k, v] of this.dataSets) {
			por.set(k, v);
			if (!this.whole) {
				ret.push([k as string|number]);
			}
		}
		for (const k of this.dataDeletes) {
			por.delete(k);
			if (!this.whole) {
				ret.push([k as string|number]);
			}
		}
		for (const [k, pv] of this.dataMods) {
			let fs: (string | number)[][] = [];
			if ((pv as AppliablePartial<V>).applyTo) {
				fs = (pv as AppliablePartial<V>).applyTo(por.get(k)!)
			}
			if ((pv as AppliableOnTopPartial<V>).applyOnTop) {
				let nv: V;
				[nv, fs] = (pv as AppliableOnTopPartial<V>).applyOnTop(por.get(k)!)
				por.set(k, nv);
			}
			if (!this.whole) {
				for (const fss of fs) {
					ret.push([k as string|number, ...fss]);
				}
			}
		}
		return reduceFieldPaths(ret);
	}
`
	case "PartialMap":
		out = `
	applyTo(por: Map<K, V>): (string | number)[][] {
		const ret: (string | number)[][] = [];
		if (this.whole) {
			por.clear();
			for (const [k, v] of this.whole) {
				por.set(k, v);
			}
			ret.push([]);
		}
		for (const [k, v] of this.dataSets) {
			por.set(k, v);
			if (!this.whole) {
				ret.push([k as string|number]);
			}
		}
		for (const k of this.dataDeletes) {
			por.delete(k);
			if (!this.whole) {
				ret.push([k as string|number]);
			}
		}
		return reduceFieldPaths(ret);
	}
`
	case "PartialModArray":
		out = `
    applyTo(por: V[]): (string | number)[][] {
        const ret: (string | number)[][] = [];
        if (this.whole) {
            por.splice(0, por.length, ...this.whole);
            ret.push([]);
        }
        for (const [k, v] of this.dataSets) {
            por[k] = v;
            if (!this.whole) {
                ret.push([k as string|number]);
            }
        }
        for (const [k, pv] of this.dataMods) {
            let fs: (string | number)[][] = [];
            if ((pv as AppliablePartial<V>).applyTo) {
                fs = (pv as AppliablePartial<V>).applyTo(por[k]!)
            }
            if ((pv as AppliableOnTopPartial<V>).applyOnTop) {
                let nv: V;
                [nv, fs] = (pv as AppliableOnTopPartial<V>).applyOnTop(por[k])
                por[k] = nv;
            }

            if (!this.whole) {
                for (const fss of fs) {
                    ret.push([k as string|number, ...fss]);
                }
            }
        }
        return reduceFieldPaths(ret);
    }
`
	case "PartialArray":
		out = `
    applyTo(por: V[]): (string | number)[][] {
        const ret: (string | number)[][] = [];
        if (this.whole) {
            por.splice(0, por.length, ...this.whole);
            ret.push([]);
        }
        for (const [k, v] of this.dataSets) {
            por[k] = v;
            if (!this.whole) {
                ret.push([k as string|number]);
            }
        }
        return reduceFieldPaths(ret);
    }
`
	}
	return out
}
