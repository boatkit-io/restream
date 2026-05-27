package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/samber/lo"
)

// parseStructDecls is an inner helper to parse all of the declarations from the file and see what we want to codegen from it
func (ft *FileTracking) parseStructDecls() error { //nolint:gocyclo,funlen
	// Do a pass to get all dependencies of all specified structs
	// The result will be 3 lists, in increasing specificity:
	// 1. structsToProcess - all structs to build serializers/deserializers for
	// 2. fieldedStructs - all structs that have the @restream.fielded tag (this should be a subset of structsToProcess)
	// 3. partialStructs - all structs that have the @restream.partials tag (this should be a subset of fieldedStructs)
	for _, d := range ft.f.Decls {
		dt, ok := d.(*dst.GenDecl)
		if !ok {
			continue
		}
		if dt.Tok != token.TYPE || len(dt.Specs) < 1 {
			continue
		}
		s, ok := dt.Specs[0].(*dst.TypeSpec)
		if !ok {
			continue
		}
		_, ok = s.Type.(*dst.StructType)
		if !ok {
			continue
		}

		serializers := false
		fielded := false
		partial := false
		storeName := ""
		for _, dec := range dt.Decorations().Start {
			if strings.Contains(dec, "@restream.serializers") {
				serializers = true
			}
			if strings.Contains(dec, "@restream.fields") {
				serializers = true
				fielded = true
			}
			if strings.Contains(dec, "@restream.partials") {
				serializers = true
				fielded = true
				partial = true
			}
			parsedStoreName, err := parseRestreamStoreAnnotation(dec)
			if err != nil {
				return err
			}
			if parsedStoreName != "" {
				storeName = parsedStoreName
			}
		}

		if storeName != "" {
			si := StructInfo{
				Name: s.Name.Name,
			}
			if s.TypeParams != nil {
				si.GenericParams = lo.Map(s.TypeParams.List, func(t *dst.Field, _ int) string {
					return t.Names[0].Name
				})
			}
			stateRef, err := ft.pt.storeStateRefForStore(ft, s)
			if err != nil {
				return err
			}
			if err := ft.ensureStoreDataMember(s, stateRef); err != nil {
				return err
			}
			ft.createGoStoreMethods(si, storeName)
			ft.createTSStoreNameConst(si.Name, storeName)
		}

		if !serializers {
			continue
		}

		if serializers {
			ft.serializerStructs[s] = struct{}{}
		}
		if fielded {
			ft.fieldedStructs[s] = struct{}{}
		}
		if partial {
			ft.partialStructs[s] = struct{}{}
		}
		ft.walkStructDeps(s)
	}

	if len(ft.serializerStructs) == 0 {
		return nil
	}

	fmt.Printf("Structs to build serializers for: %s\n", strings.Join(lo.Map(lo.Keys(ft.serializerStructs),
		func(s *dst.TypeSpec, _ int) string {
			o := s.Name.Name
			if _, has := ft.fieldedStructs[s]; has {
				o += " (fielded)"
			}
			if _, has := ft.partialStructs[s]; has {
				o += " (partial)"
			}
			return o
		}), ", "))

	for s := range ft.serializerStructs {
		st := s.Type.(*dst.StructType)

		fielded := false
		if _, has := ft.fieldedStructs[s]; has {
			fielded = true

			if len(st.Fields.List) > 0 {
				// Get the highest field count off the struct, if it exists
				maxFieldNum := byte(0)
				dec := st.Fields.List[0].Decorations()
				decAll := dec.Start.All()
				if len(decAll) > 0 {
					res := regexp.MustCompile(`// MAXFIELD\((\d+)\)`).FindAllStringSubmatch(decAll[0], 1)
					if len(res) == 1 && len(res[0]) == 2 {
						tc, err := strconv.ParseUint(res[0][1], 10, 8)
						if err != nil {
							return err
						}
						maxFieldNum = byte(tc)
					}
				}

				// provision fieldids for all fields
				for idx, fd := range st.Fields.List {
					ti, changed, err := ft.getOrGenerateTagInfo(fd, idx, &maxFieldNum)
					if err != nil {
						return err
					}
					if changed {
						ft.inputFileDirty = true
						tagStr, err := genTagString(fd, ti)
						if err != nil {
							return err
						}
						fd.Tag = &dst.BasicLit{
							Kind:  token.STRING,
							Value: tagStr,
						}
					}
				}

				// update MAXFIELD as needed
				newMaxFieldLine := fmt.Sprintf("// MAXFIELD(%d)", maxFieldNum)
				if decAll == nil || (len(decAll) > 0 && decAll[0] != newMaxFieldLine) {
					dec.Start = []string{newMaxFieldLine}
					ft.inputFileDirty = true
				}
			}
		}

		si := StructInfo{
			Name:    s.Name.Name,
			Fielded: fielded,
		}

		if s.TypeParams != nil {
			si.GenericParams = lo.Map(s.TypeParams.List, func(t *dst.Field, _ int) string {
				return t.Names[0].Name
			})
		}

		fields, err := ft.genFieldInfo(st.Fields.List)
		if err != nil {
			return err
		}

		var partialFields []*restream.FieldInfo
		if _, ok := ft.partialStructs[s]; ok {
			partialFields, err = ft.genPartialFieldInfo(st.Fields.List)
			if err != nil {
				return err
			}
		}

		if err := ft.createGoStructSerializers(si, fields, partialFields); err != nil {
			return err
		}

		if err := ft.createTSStructSerializers(si, fields, partialFields); err != nil {
			return err
		}
	}

	return nil
}

func parseRestreamStoreAnnotation(dec string) (string, error) {
	const annotation = "@restream.store"

	idx := strings.Index(dec, annotation)
	if idx == -1 {
		return "", nil
	}

	args := strings.TrimSpace(dec[idx+len(annotation):])
	if !strings.HasPrefix(args, "(") {
		return "", fmt.Errorf("%s requires a store name in parentheses", annotation)
	}
	endIdx := strings.Index(args, ")")
	if endIdx == -1 {
		return "", fmt.Errorf("%s annotation is missing a closing parenthesis", annotation)
	}

	storeName := strings.TrimSpace(args[1:endIdx])
	if storeName == "" {
		return "", fmt.Errorf("%s requires a non-empty store name", annotation)
	}

	if unquoted, err := strconv.Unquote(storeName); err == nil {
		storeName = unquoted
	}

	return storeName, nil
}

type storeTypeDecl struct {
	ft      *FileTracking
	declIdx int
	spec    *dst.TypeSpec
}

type structTypeDecl struct {
	ft   *FileTracking
	gd   *dst.GenDecl
	spec *dst.TypeSpec
}

type storeStateRef struct {
	TypeName    string
	Qualifier   string
	PackagePath string
}

func (r storeStateRef) typeExpr() string {
	if r.Qualifier != "" {
		return r.Qualifier + "." + r.TypeName
	}
	return r.TypeName
}

func (r storeStateRef) partialTypeExpr() string {
	if r.Qualifier != "" {
		return r.Qualifier + "." + r.TypeName + "Partial"
	}
	return r.TypeName + "Partial"
}

func (r storeStateRef) displayName() string {
	if r.PackagePath != "" {
		return r.PackagePath + "." + r.TypeName
	}
	return r.typeExpr()
}

func (pt *ProjTracking) ensureStoreStateStructs() error {
	storeDecls := []storeTypeDecl{}
	for _, ft := range pt.files {
		for idx, d := range ft.f.Decls {
			gd, ok := d.(*dst.GenDecl)
			if !ok || gd.Tok != token.TYPE || len(gd.Specs) < 1 {
				continue
			}

			s, ok := gd.Specs[0].(*dst.TypeSpec)
			if !ok {
				continue
			}
			if _, ok := s.Type.(*dst.StructType); !ok {
				continue
			}

			for _, dec := range gd.Decorations().Start {
				storeName, err := parseRestreamStoreAnnotation(dec)
				if err != nil {
					return err
				}
				if storeName != "" {
					storeDecls = append(storeDecls, storeTypeDecl{ft: ft, declIdx: idx, spec: s})
					break
				}
			}
		}
	}

	for idx := len(storeDecls) - 1; idx >= 0; idx-- {
		storeDecl := storeDecls[idx]
		stateRef, err := pt.storeStateRefForStore(storeDecl.ft, storeDecl.spec)
		if err != nil {
			return err
		}

		stateFt, gd, _, exists := pt.findStructTypeSpecForStateRef(storeDecl.ft, stateRef)
		if exists {
			stateFt.ensurePartialsAnnotation(gd)
			continue
		}

		stateFt, insertIdx, err := pt.storeStateInsertTarget(storeDecl, stateRef)
		if err != nil {
			return err
		}
		stateFt.insertStoreStateStruct(stateRef.TypeName, insertIdx)
	}
	return nil
}

func (pt *ProjTracking) storeStateRefForStore(sourceFt *FileTracking, storeSpec *dst.TypeSpec) (storeStateRef, error) {
	if stateRef, ok := pt.storeStateRefs[storeSpec]; ok {
		return stateRef, nil
	}

	stateRef, err := pt.resolveStoreStateRef(sourceFt, storeSpec)
	if err != nil {
		return storeStateRef{}, err
	}
	pt.storeStateRefs[storeSpec] = stateRef
	return stateRef, nil
}

func (pt *ProjTracking) resolveStoreStateRef(sourceFt *FileTracking, storeSpec *dst.TypeSpec) (storeStateRef, error) {
	if stateRef, ok, err := sourceFt.storeDataStateRef(storeSpec); err != nil || ok {
		return stateRef, err
	}
	return pt.resolveConventionalStoreStateRef(sourceFt, storeSpec.Name.Name+"State")
}

func (ft *FileTracking) storeDataStateRef(storeSpec *dst.TypeSpec) (storeStateRef, bool, error) {
	st, ok := storeSpec.Type.(*dst.StructType)
	if !ok {
		return storeStateRef{}, false, nil
	}

	for _, fd := range st.Fields.List {
		hasStoreDataName := slices.ContainsFunc(fd.Names, func(name *dst.Ident) bool {
			return name.Name == "storeData"
		})
		if !hasStoreDataName {
			continue
		}
		return ft.storeDataStateRefFromType(fd.Type)
	}

	return storeStateRef{}, false, nil
}

func (ft *FileTracking) storeDataStateRefFromType(expr dst.Expr) (storeStateRef, bool, error) {
	expr = derefTypeExpr(expr)

	var storeDataType dst.Expr
	indices := []dst.Expr{}
	switch expr := expr.(type) {
	case *dst.IndexExpr:
		storeDataType = expr.X
		indices = append(indices, expr.Index)
	case *dst.IndexListExpr:
		storeDataType = expr.X
		indices = expr.Indices
	default:
		return storeStateRef{}, false, nil
	}

	if typeExprName(storeDataType) != "StoreData" || len(indices) == 0 {
		return storeStateRef{}, false, nil
	}

	stateRef, ok, err := ft.stateRefFromTypeExpr(indices[0])
	if err != nil || !ok {
		return storeStateRef{}, ok, err
	}
	return stateRef, true, nil
}

func (ft *FileTracking) stateRefFromTypeExpr(expr dst.Expr) (storeStateRef, bool, error) {
	expr = derefTypeExpr(expr)
	switch expr := expr.(type) {
	case *dst.Ident:
		if expr.Name == "any" || expr.Name == "interface{}" {
			return storeStateRef{}, false, nil
		}
		return storeStateRef{TypeName: expr.Name}, true, nil
	case *dst.SelectorExpr:
		qualifier, ok := expr.X.(*dst.Ident)
		if !ok {
			return storeStateRef{}, false, nil
		}
		stateRef := storeStateRef{
			TypeName:  expr.Sel.Name,
			Qualifier: qualifier.Name,
		}
		if pkg := ft.importLookup[qualifier.Name]; pkg != nil {
			stateRef.PackagePath = pkg.PkgPath
		}
		return stateRef, true, nil
	default:
		return storeStateRef{}, false, nil
	}
}

func derefTypeExpr(expr dst.Expr) dst.Expr {
	for {
		starExpr, ok := expr.(*dst.StarExpr)
		if !ok {
			return expr
		}
		expr = starExpr.X
	}
}

func typeExprName(expr dst.Expr) string {
	switch expr := expr.(type) {
	case *dst.Ident:
		return expr.Name
	case *dst.SelectorExpr:
		return expr.Sel.Name
	default:
		return ""
	}
}

func (pt *ProjTracking) resolveConventionalStoreStateRef(
	sourceFt *FileTracking,
	stateTypeName string,
) (storeStateRef, error) {
	if _, _, _, exists := pt.findStructTypeSpecInPackage(sourceFt, stateTypeName); exists {
		return storeStateRef{TypeName: stateTypeName}, nil
	}

	matches := pt.findStructTypeSpecs(stateTypeName)
	switch len(matches) {
	case 0:
		return storeStateRef{TypeName: stateTypeName}, nil
	case 1:
		match := matches[0]
		if samePackage(sourceFt, match.ft) {
			return storeStateRef{TypeName: stateTypeName}, nil
		}
		qualifier, err := sourceFt.ensurePackageImportName(match.ft)
		if err != nil {
			return storeStateRef{}, err
		}
		return storeStateRef{
			TypeName:    stateTypeName,
			Qualifier:   qualifier,
			PackagePath: match.ft.fPackage.PkgPath,
		}, nil
	default:
		packageNames := lo.Map(matches, func(match structTypeDecl, _ int) string {
			if match.ft.fPackage != nil && match.ft.fPackage.PkgPath != "" {
				return match.ft.fPackage.PkgPath
			}
			return match.ft.f.Name.Name
		})
		return storeStateRef{}, fmt.Errorf(
			"multiple %s structs found across input dirs (%s); add an explicit storeData field to choose one",
			stateTypeName,
			strings.Join(packageNames, ", "),
		)
	}
}

func (pt *ProjTracking) storeStateInsertTarget(
	storeDecl storeTypeDecl,
	stateRef storeStateRef,
) (*FileTracking, int, error) {
	if stateRef.PackagePath == "" && stateRef.Qualifier == "" {
		return storeDecl.ft, storeDecl.declIdx, nil
	}

	targetFt := pt.firstFileForStateRef(storeDecl.ft, stateRef)
	if targetFt == nil {
		return nil, 0, fmt.Errorf(
			"store state %s is referenced by %s but was not found in parsed input dirs",
			stateRef.displayName(),
			storeDecl.spec.Name.Name,
		)
	}
	return targetFt, len(targetFt.f.Decls), nil
}

func (ft *FileTracking) insertStoreStateStruct(stateTypeName string, insertIdx int) {
	stateDecl := &dst.GenDecl{
		Tok: token.TYPE,
		Specs: []dst.Spec{
			&dst.TypeSpec{
				Name: dst.NewIdent(stateTypeName),
				Type: &dst.StructType{
					Fields: &dst.FieldList{},
				},
			},
		},
	}
	stateDecl.Decorations().Start.Append("// @restream.partials")
	ft.f.Decls = slices.Insert(ft.f.Decls, insertIdx, dst.Decl(stateDecl))
	ft.inputFileDirty = true
}

func (pt *ProjTracking) findStructTypeSpecForStateRef(
	sourceFt *FileTracking,
	stateRef storeStateRef,
) (*FileTracking, *dst.GenDecl, *dst.TypeSpec, bool) {
	if stateRef.PackagePath != "" {
		return pt.findStructTypeSpecInPackagePath(stateRef.PackagePath, stateRef.TypeName)
	}

	if stateRef.Qualifier != "" {
		if pkg := sourceFt.importLookup[stateRef.Qualifier]; pkg != nil && pkg.PkgPath != "" {
			return pt.findStructTypeSpecInPackagePath(pkg.PkgPath, stateRef.TypeName)
		}

		for _, ft := range pt.files {
			if ft.f.Name.Name != stateRef.Qualifier {
				continue
			}
			gd, spec, exists := ft.findStructTypeSpec(stateRef.TypeName)
			if exists {
				return ft, gd, spec, true
			}
		}
		return nil, nil, nil, false
	}

	return pt.findStructTypeSpecInPackage(sourceFt, stateRef.TypeName)
}

func (pt *ProjTracking) findStructTypeSpecInPackage(
	sourceFt *FileTracking,
	typeName string,
) (*FileTracking, *dst.GenDecl, *dst.TypeSpec, bool) {
	for _, ft := range pt.files {
		if !samePackage(sourceFt, ft) {
			continue
		}
		gd, spec, exists := ft.findStructTypeSpec(typeName)
		if exists {
			return ft, gd, spec, true
		}
	}
	return nil, nil, nil, false
}

func (pt *ProjTracking) findStructTypeSpecInPackagePath(
	packagePath string,
	typeName string,
) (*FileTracking, *dst.GenDecl, *dst.TypeSpec, bool) {
	for _, ft := range pt.files {
		if ft.fPackage == nil || ft.fPackage.PkgPath != packagePath {
			continue
		}
		gd, spec, exists := ft.findStructTypeSpec(typeName)
		if exists {
			return ft, gd, spec, true
		}
	}
	return nil, nil, nil, false
}

func (pt *ProjTracking) findStructTypeSpecs(typeName string) []structTypeDecl {
	matches := []structTypeDecl{}
	for _, ft := range pt.files {
		gd, spec, exists := ft.findStructTypeSpec(typeName)
		if exists {
			matches = append(matches, structTypeDecl{
				ft:   ft,
				gd:   gd,
				spec: spec,
			})
		}
	}
	return matches
}

func (pt *ProjTracking) firstFileForStateRef(sourceFt *FileTracking, stateRef storeStateRef) *FileTracking {
	if stateRef.PackagePath != "" {
		for _, ft := range pt.files {
			if ft.fPackage != nil && ft.fPackage.PkgPath == stateRef.PackagePath {
				return ft
			}
		}
	}

	if stateRef.Qualifier != "" {
		if pkg := sourceFt.importLookup[stateRef.Qualifier]; pkg != nil && pkg.PkgPath != "" {
			for _, ft := range pt.files {
				if ft.fPackage != nil && ft.fPackage.PkgPath == pkg.PkgPath {
					return ft
				}
			}
		}

		for _, ft := range pt.files {
			if ft.f.Name.Name == stateRef.Qualifier {
				return ft
			}
		}
	}

	if stateRef.PackagePath == "" && stateRef.Qualifier == "" {
		return sourceFt
	}
	return nil
}

func samePackage(a, b *FileTracking) bool {
	if a.fPackage != nil && b.fPackage != nil && a.fPackage.PkgPath != "" && b.fPackage.PkgPath != "" {
		return a.fPackage.PkgPath == b.fPackage.PkgPath
	}
	return a.f.Name.Name == b.f.Name.Name
}

func (ft *FileTracking) findStructTypeSpec(typeName string) (*dst.GenDecl, *dst.TypeSpec, bool) {
	for _, d := range ft.f.Decls {
		gd, ok := d.(*dst.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		for _, spec := range gd.Specs {
			s, ok := spec.(*dst.TypeSpec)
			if !ok || s.Name.Name != typeName {
				continue
			}
			if _, ok := s.Type.(*dst.StructType); !ok {
				continue
			}
			return gd, s, true
		}
	}
	return nil, nil, false
}

func (ft *FileTracking) ensurePartialsAnnotation(gd *dst.GenDecl) {
	for _, dec := range gd.Decorations().Start {
		if strings.Contains(dec, "@restream.partials") {
			return
		}
	}
	gd.Decorations().Start.Append("// @restream.partials")
	ft.inputFileDirty = true
}

func (ft *FileTracking) ensureStoreDataMember(s *dst.TypeSpec, stateRef storeStateRef) error {
	st := s.Type.(*dst.StructType)
	restreamQualifier := ft.ensureRestreamImportName()
	storeDataType := expectedStoreDataTypeExpr(stateRef, restreamQualifier)
	storeDataExpr, err := parseDSTExpr(storeDataType)
	if err != nil {
		return err
	}
	for idx, fd := range st.Fields.List {
		nameIdx := slices.IndexFunc(fd.Names, func(name *dst.Ident) bool {
			return name.Name == "storeData"
		})
		if nameIdx == -1 {
			continue
		}

		if len(fd.Names) == 1 {
			if sameTypeExpr(fd.Type, storeDataExpr) {
				return nil
			}
			fd.Type = storeDataExpr
			ft.inputFileDirty = true
			return nil
		}

		fd.Names = slices.Delete(fd.Names, nameIdx, nameIdx+1)
		st.Fields.List = slices.Insert(st.Fields.List, idx, storeDataField(storeDataExpr))
		ft.inputFileDirty = true
		return nil
	}

	st.Fields.List = slices.Insert(st.Fields.List, 0, storeDataField(storeDataExpr))
	ft.inputFileDirty = true
	return nil
}

func storeDataField(storeDataType dst.Expr) *dst.Field {
	return &dst.Field{
		Names: []*dst.Ident{dst.NewIdent("storeData")},
		Type:  storeDataType,
	}
}

func expectedStoreDataTypeExpr(stateRef storeStateRef, restreamQualifier string) string {
	storeDataType := "StoreData"
	if restreamQualifier != "" {
		storeDataType = restreamQualifier + "." + storeDataType
	}
	stateType := stateRef.typeExpr()
	return fmt.Sprintf("*%s[%s, *%s, *%s]", storeDataType, stateType, stateType, stateRef.partialTypeExpr())
}

func sameTypeExpr(a, b dst.Expr) bool {
	if aParen, ok := a.(*dst.ParenExpr); ok {
		return sameTypeExpr(aParen.X, b)
	}
	if bParen, ok := b.(*dst.ParenExpr); ok {
		return sameTypeExpr(a, bParen.X)
	}

	switch a := a.(type) {
	case *dst.Ident:
		b, ok := b.(*dst.Ident)
		return ok && a.Name == b.Name
	case *dst.SelectorExpr:
		b, ok := b.(*dst.SelectorExpr)
		return ok && sameTypeExpr(a.X, b.X) && sameTypeExpr(a.Sel, b.Sel)
	case *dst.StarExpr:
		b, ok := b.(*dst.StarExpr)
		return ok && sameTypeExpr(a.X, b.X)
	case *dst.IndexExpr:
		bX, bIndices, ok := indexTypeExprParts(b)
		return ok && len(bIndices) == 1 && sameTypeExpr(a.X, bX) && sameTypeExpr(a.Index, bIndices[0])
	case *dst.IndexListExpr:
		bX, bIndices, ok := indexTypeExprParts(b)
		if !ok || len(a.Indices) != len(bIndices) || !sameTypeExpr(a.X, bX) {
			return false
		}
		for idx := range a.Indices {
			if !sameTypeExpr(a.Indices[idx], bIndices[idx]) {
				return false
			}
		}
		return true
	default:
		return dstExprString(a) == dstExprString(b)
	}
}

func indexTypeExprParts(expr dst.Expr) (dst.Expr, []dst.Expr, bool) {
	switch expr := expr.(type) {
	case *dst.IndexExpr:
		return expr.X, []dst.Expr{expr.Index}, true
	case *dst.IndexListExpr:
		return expr.X, expr.Indices, true
	default:
		return nil, nil, false
	}
}

func (ft *FileTracking) ensurePackageImportName(targetFt *FileTracking) (string, error) {
	if samePackage(ft, targetFt) {
		return "", nil
	}
	if targetFt.fPackage == nil || targetFt.fPackage.PkgPath == "" {
		return "", fmt.Errorf("unable to import package %s: package path is unknown", targetFt.f.Name.Name)
	}

	importPkgPath := targetFt.fPackage.PkgPath
	for _, imp := range ft.f.Imports {
		if importPath(imp) != importPkgPath {
			continue
		}
		if imp.Name != nil {
			switch imp.Name.Name {
			case ".":
				return "", nil
			case "_":
				return "", fmt.Errorf("store state package %s is imported for side effects only", importPkgPath)
			default:
				ft.importLookup[imp.Name.Name] = targetFt.fPackage
				return imp.Name.Name, nil
			}
		}
		qualifier := targetFt.f.Name.Name
		ft.importLookup[qualifier] = targetFt.fPackage
		return qualifier, nil
	}

	qualifier := targetFt.f.Name.Name
	if qualifier == "" {
		qualifier = path.Base(importPkgPath)
	}
	if existingPkg := ft.importLookup[qualifier]; existingPkg != nil && existingPkg.PkgPath != importPkgPath {
		baseQualifier := qualifier
		for idx := 2; ; idx++ {
			qualifier = fmt.Sprintf("%s%d", baseQualifier, idx)
			if ft.importLookup[qualifier] == nil {
				break
			}
		}
	}

	imp := &dst.ImportSpec{
		Path: &dst.BasicLit{
			Kind:  token.STRING,
			Value: strconv.Quote(importPkgPath),
		},
	}
	if qualifier != targetFt.f.Name.Name {
		imp.Name = dst.NewIdent(qualifier)
	}

	ft.f.Imports = append(ft.f.Imports, imp)
	for _, d := range ft.f.Decls {
		gd, ok := d.(*dst.GenDecl)
		if !ok || gd.Tok != token.IMPORT {
			continue
		}
		gd.Specs = append(gd.Specs, imp)
		ft.importLookup[qualifier] = targetFt.fPackage
		ft.inputFileDirty = true
		return qualifier, nil
	}

	ft.f.Decls = slices.Insert(ft.f.Decls, 0, dst.Decl(&dst.GenDecl{
		Tok:   token.IMPORT,
		Specs: []dst.Spec{imp},
	}))
	ft.importLookup[qualifier] = targetFt.fPackage
	ft.inputFileDirty = true
	return qualifier, nil
}

func (ft *FileTracking) ensureRestreamImportName() string {
	if ft.fPackage != nil && ft.fPackage.PkgPath == restreamPackagePath {
		return ""
	}

	for _, imp := range ft.f.Imports {
		if importPath(imp) != restreamPackagePath {
			continue
		}
		if imp.Name != nil && imp.Name.Name != "." && imp.Name.Name != "_" {
			return imp.Name.Name
		}
		return "restream"
	}

	imp := &dst.ImportSpec{
		Path: &dst.BasicLit{
			Kind:  token.STRING,
			Value: strconv.Quote(restreamPackagePath),
		},
	}
	ft.f.Imports = append(ft.f.Imports, imp)
	for _, d := range ft.f.Decls {
		gd, ok := d.(*dst.GenDecl)
		if !ok || gd.Tok != token.IMPORT {
			continue
		}
		gd.Specs = append(gd.Specs, imp)
		ft.inputFileDirty = true
		return "restream"
	}

	ft.f.Decls = slices.Insert(ft.f.Decls, 0, dst.Decl(&dst.GenDecl{
		Tok:   token.IMPORT,
		Specs: []dst.Spec{imp},
	}))
	ft.inputFileDirty = true
	return "restream"
}

func importPath(imp *dst.ImportSpec) string {
	p, err := strconv.Unquote(imp.Path.Value)
	if err != nil {
		return ""
	}
	return p
}

// parseFuncDecls is an inner helper to parse all of the RPC functions from the file and see what we want to codegen from it
func (ft *FileTracking) parseFuncDecls() error { //nolint:gocyclo,funlen
	eventRegistrations, err := ft.typedEventRegistrations()
	if err != nil {
		return err
	}

	// First pass to get all function types
	receiverLookup := map[string]*dst.FuncType{}
	for _, d := range ft.f.Decls {
		ft, has := d.(*dst.FuncDecl)
		if !has {
			continue
		}
		if ft.Recv == nil || len(ft.Recv.List) != 1 {
			continue
		}
		rt, has := ft.Recv.List[0].Type.(*dst.StarExpr)
		if !has {
			continue
		}
		rtx, has := rt.X.(*dst.Ident)
		if !has {
			continue
		}
		receiverLookup[rtx.Name+"."+ft.Name.Name] = ft.Type
	}

	generatedEventPackets := map[string]string{}

	for _, d := range ft.f.Decls {
		fd, has := d.(*dst.FuncDecl)
		if !has {
			continue
		}

		for _, stmt := range fd.Body.List {
			stt, ok := stmt.(*dst.ExprStmt)
			if !ok {
				continue
			}
			xt, ok := stt.X.(*dst.CallExpr)
			if !ok {
				continue
			}
			se, ok := xt.Fun.(*dst.SelectorExpr)
			if !ok {
				continue
			}
			sexid, ok := se.X.(*dst.Ident)
			if !ok {
				continue
			}
			if sexid.Name != "rpcd" || se.Sel.Name != "RegisterRPCHandler" {
				if se.Sel.Name != "RegisterEvent" {
					continue
				}

				eventName, err := stringLiteralValue(xt.Args[0])
				if err != nil {
					return err
				}
				eventInfo, has := eventRegistrations[eventName]
				if !has {
					return fmt.Errorf("unable to resolve event registration type for %s", eventName)
				}

				eventTypeName := strings.ReplaceAll(eventName, ".", "")

				eventPacketType := fmt.Sprintf("%sEvent", eventTypeName)
				if existingEventName, generated := generatedEventPackets[eventPacketType]; generated {
					return fmt.Errorf("event names %q and %q both generate packet type %s", existingEventName, eventName, eventPacketType)
				}

				fmt.Printf("Building Event packet for: %s\n", eventName)

				if err := ft.buildGolangEventStruct(eventName, eventTypeName, eventInfo.Fields); err != nil {
					return err
				}

				if err := ft.buildTSEventStruct(eventName, eventTypeName, eventInfo.Fields); err != nil {
					return err
				}

				generatedEventPackets[eventPacketType] = eventName

				fixed := false
				if eventInfo.NeedsAddress && !isAddressOf(xt.Args[1]) {
					fixed = true
					xt.Args[1] = &dst.UnaryExpr{Op: token.AND, X: xt.Args[1]}
				}
				if len(xt.Args) < 3 {
					fixed = true
					xt.Args = append(xt.Args, genRPCArg(eventPacketType))
				} else if !validateRPCArg(xt.Args[2], eventPacketType) {
					fixed = true
					xt.Args[2] = genRPCArg(eventPacketType)
				}

				if len(xt.Args) < 4 {
					fixed = true
					xt.Args = append(xt.Args, genReflectTypeArg(eventInfo.CallbackTypeExpr))
				} else if !validateReflectTypeArg(xt.Args[3], eventInfo.CallbackTypeExpr) {
					fixed = true
					xt.Args[3] = genReflectTypeArg(eventInfo.CallbackTypeExpr)
				}

				if fixed {
					ft.inputFileDirty = true
					fmt.Printf("Fixed event types for %s\n", eventName)
				}

				continue
			}

			rpcn, err := stringLiteralValue(xt.Args[0])
			if err != nil {
				return err
			}
			rpctn := strings.ReplaceAll(rpcn, ".", "")

			var ftt *dst.FuncType
			flt, has := xt.Args[2].(*dst.FuncLit)
			if has {
				ftt = flt.Type
			} else {
				st, has := xt.Args[2].(*dst.SelectorExpr)
				if !has {
					panic(fmt.Sprintf("Unhandled type for RPC handler: %T", xt.Args[2]))
				}

				tn := st.X.(*dst.Ident).Obj.Decl.(*dst.AssignStmt).Rhs[0].(*dst.UnaryExpr).X.(*dst.CompositeLit).Type.(*dst.Ident).Name
				ftt = receiverLookup[tn+"."+st.Sel.Name]
			}

			var respField *dst.Field
			ftp := ftt.Params
			ftr := ftt.Results
			errIdx := 0
			switch len(ftr.List) {
			case 1:
				errIdx = 0
			case 2:
				respField = ftr.List[0]
				errIdx = 1
			default:
				return fmt.Errorf("RPC handler for %s has %d many return values", rpcn, len(ftr.List))
			}
			if ftr.List[errIdx].Type.(*dst.Ident).Name != "error" {
				return fmt.Errorf("RPC handler for %s doesn't have proper return type of (error) or ([something], error)", rpcn)
			}

			fmt.Printf("Building RPC handlers for: %s\n", rpcn)

			reqFields, err := ft.genParamFieldInfo(ftp.List)
			if err != nil {
				return err
			}
			for _, fi := range reqFields {
				fi.Name = toPublicName(fi.Name)
			}

			respFieldsRaw := []*dst.Field{}
			if respField != nil {
				respFieldsRaw = append(respFieldsRaw,
					&dst.Field{Names: []*dst.Ident{dst.NewIdent("Result")}, Type: respField.Type,
						Tag: respField.Tag, Decs: respField.Decs})
			}
			respFieldsRaw = append(respFieldsRaw,
				&dst.Field{Names: []*dst.Ident{dst.NewIdent("Error")}, Type: &dst.StarExpr{X: &dst.Ident{Name: "string"}}})
			respFields, err := ft.genFieldInfo(respFieldsRaw)
			if err != nil {
				return err
			}

			if err := ft.buildGolangRPCStructs(rpcn, rpctn, reqFields, respFields); err != nil {
				return err
			}

			if err := ft.buildTSRPCStructs(rpcn, rpctn, reqFields, respFields); err != nil {
				return err
			}

			fixed := false
			if !validateRPCArg(xt.Args[3], fmt.Sprintf("%sRequest", rpctn)) {
				fixed = true
				xt.Args[3] = genRPCArg(fmt.Sprintf("%sRequest", rpctn))
			}
			if !validateRPCArg(xt.Args[4], fmt.Sprintf("%sResponse", rpctn)) {
				fixed = true
				xt.Args[4] = genRPCArg(fmt.Sprintf("%sResponse", rpctn))
			}

			if fixed {
				ft.inputFileDirty = true
				fmt.Printf("Fixed RPC handler types for %s\n", rpcn)
			}
		}
	}

	return nil
}

type eventRegistrationInfo struct {
	Fields           []*restream.FieldInfo
	CallbackTypeExpr string
	NeedsAddress     bool
}

func (ft *FileTracking) typedEventRegistrations() (map[string]eventRegistrationInfo, error) {
	ret := map[string]eventRegistrationInfo{}
	if ft.fPackage == nil || ft.fPackage.TypesInfo == nil {
		return ret, nil
	}

	var targetFile *ast.File
	for _, f := range ft.fPackage.Syntax {
		if filepath.Clean(ft.pt.fset.Position(f.Pos()).Filename) == filepath.Clean(ft.inFile) {
			targetFile = f
			break
		}
	}
	if targetFile == nil {
		return ret, nil
	}

	var walkErr error
	ast.Inspect(targetFile, func(n ast.Node) bool {
		if walkErr != nil {
			return false
		}

		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		se, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || se.Sel.Name != "RegisterEvent" {
			return true
		}
		if len(call.Args) < 2 {
			walkErr = fmt.Errorf("RegisterEvent call has %d args, expected at least 2", len(call.Args))
			return false
		}

		eventName, err := astStringLiteralValue(call.Args[0])
		if err != nil {
			walkErr = err
			return false
		}
		if _, exists := ret[eventName]; exists {
			walkErr = fmt.Errorf("duplicate RegisterEvent name %q in %s", eventName, ft.inFile)
			return false
		}

		eventType := ft.fPackage.TypesInfo.TypeOf(call.Args[1])
		signature, ok := subscribableEventSignature(eventType)
		if !ok {
			walkErr = fmt.Errorf(
				"RegisterEvent for %s must pass a subscribableevent.Event, got %s (package errors: %+v)",
				eventName, eventType, ft.fPackage.Errors,
			)
			return false
		}
		if signature.Results().Len() != 0 {
			walkErr = fmt.Errorf("RegisterEvent for %s uses an event callback type with %d return values", eventName, signature.Results().Len())
			return false
		}

		fields, err := ft.eventFieldsFromSignature(signature)
		if err != nil {
			walkErr = err
			return false
		}

		ret[eventName] = eventRegistrationInfo{
			Fields:           fields,
			CallbackTypeExpr: callbackTypeExprFromFields(ft, fields),
			NeedsAddress:     !isPointerType(eventType),
		}
		return true
	})
	if walkErr != nil {
		return nil, walkErr
	}

	return ret, nil
}

func subscribableEventSignature(t types.Type) (*types.Signature, bool) {
	if t == nil {
		return nil, false
	}
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}

	named, ok := t.(*types.Named)
	if !ok || named.Obj() == nil || named.Obj().Pkg() == nil {
		return nil, false
	}
	if named.Obj().Pkg().Path() != "github.com/boatkit-io/tugboat/pkg/subscribableevent" || named.Obj().Name() != "Event" {
		return nil, false
	}
	if named.TypeArgs() == nil || named.TypeArgs().Len() != 1 {
		return nil, false
	}

	callbackType := named.TypeArgs().At(0)
	if callbackNamed, ok := callbackType.(*types.Named); ok {
		callbackType = callbackNamed.Underlying()
	}
	signature, ok := callbackType.(*types.Signature)
	return signature, ok
}

func (ft *FileTracking) eventFieldsFromSignature(signature *types.Signature) ([]*restream.FieldInfo, error) {
	fields := []*restream.FieldInfo{}
	params := signature.Params()
	for idx := 0; idx < params.Len(); idx++ {
		param := params.At(idx)
		name := param.Name()
		if name == "" || name == "_" {
			name = fmt.Sprintf("Arg%d", idx)
		} else {
			name = toPublicName(name)
		}

		vi, err := ft.pt.getVarInfoForType(param.Type())
		if err != nil {
			return nil, err
		}

		fields = append(fields, &restream.FieldInfo{
			Name:     name,
			FieldIdx: idx,
			VarInfo:  vi,
		})
	}
	return fields, nil
}

func isPointerType(t types.Type) bool {
	_, ok := t.(*types.Pointer)
	return ok
}

func isAddressOf(expr dst.Expr) bool {
	ue, ok := expr.(*dst.UnaryExpr)
	return ok && ue.Op == token.AND
}

func callbackTypeExprFromFields(ft *FileTracking, fields []*restream.FieldInfo) string {
	params := lo.Map(fields, func(fi *restream.FieldInfo, _ int) string {
		return ft.getGolangTypeName(fi.VarInfo)
	})
	return fmt.Sprintf("func(%s)", strings.Join(params, ", "))
}

func stringLiteralValue(expr dst.Expr) (string, error) {
	bl, ok := expr.(*dst.BasicLit)
	if !ok {
		return "", fmt.Errorf("expected string literal, got %T", expr)
	}
	return strconv.Unquote(bl.Value)
}

func astStringLiteralValue(expr ast.Expr) (string, error) {
	bl, ok := expr.(*ast.BasicLit)
	if !ok {
		return "", fmt.Errorf("expected string literal, got %T", expr)
	}
	return strconv.Unquote(bl.Value)
}

// buildSerializers is a helper to build serializers for the configured BuildSerializers list
func (pt *ProjTracking) buildSerializers() error {
	if len(pt.config.BuildSerializers) == 0 {
		return nil
	}

	for _, s := range pt.config.BuildSerializers {
		parts := strings.Split(s, "/")
		pkgName := strings.Join(parts[:len(parts)-1], "/")
		pkg, err := pt.getPackageForPath(pkgName, false)
		if err != nil {
			return fmt.Errorf("unknown package in buildTSSerializers: %w", err)
		}
		typeName := parts[len(parts)-1]
		tn := pkg.Types.Scope().Lookup(typeName)
		if tn == nil {
			return fmt.Errorf("unknown type in buildSerializers: %s", s)
		}
		tno := tn.(*types.TypeName)
		fields, err := pt.genFieldInfoForType(tno)
		if err != nil {
			return err
		}

		// TODO generics support, or even detecting them to error...

		pt.buildGolangSerializer(pkg, tno, fields)
		pt.buildTSSerializer(pkg, tno, fields)
	}

	pt.buildGolangSerializerLookup()

	return nil
}

// genRPCArg generates an AST struct for the given RPC struct type
func genRPCArg(structType string) dst.Expr {
	return &dst.CallExpr{
		Fun: &dst.IndexExpr{
			Index: &dst.Ident{
				Name: structType,
			},
			X: &dst.SelectorExpr{
				X: &dst.Ident{
					Name: "reflect",
				},
				Sel: &dst.Ident{
					Name: "TypeFor",
				},
			},
		},
	}
}

// genReflectTypeArg generates an AST struct for reflect.TypeFor[typeExpr]().
func genReflectTypeArg(typeExpr string) dst.Expr {
	expr, err := parseDSTExpr(fmt.Sprintf("reflect.TypeFor[%s]()", typeExpr))
	if err != nil {
		panic(err)
	}
	return expr
}

// validateRPCArg is a helper for validating the type of the RPC arg
func validateRPCArg(arg dst.Expr, expectedType string) bool {
	ce, ok := arg.(*dst.CallExpr)
	if !ok {
		return false
	}
	if len(ce.Args) != 0 {
		return false
	}
	fun, ok := ce.Fun.(*dst.IndexExpr)
	if !ok {
		return false
	}
	fi, ok := fun.Index.(*dst.Ident)
	if !ok {
		return false
	}
	if fi.Name != expectedType {
		return false
	}
	fx, ok := fun.X.(*dst.SelectorExpr)
	if !ok {
		return false
	}
	fxn, ok := fx.X.(*dst.Ident)
	if !ok {
		return false
	}
	if fxn.Name != "reflect" {
		return false
	}
	if fx.Sel.Name != "TypeFor" {
		return false
	}
	return true
}

// validateReflectTypeArg is a helper for validating a reflect.TypeFor[typeExpr]() arg.
func validateReflectTypeArg(arg dst.Expr, expectedTypeExpr string) bool {
	return dstExprString(arg) == fmt.Sprintf("reflect.TypeFor[%s]()", expectedTypeExpr)
}

func parseDSTExpr(expr string) (dst.Expr, error) {
	f, err := decorator.Parse("package main\nvar _ = " + expr)
	if err != nil {
		return nil, err
	}

	gd := f.Decls[0].(*dst.GenDecl)
	vs := gd.Specs[0].(*dst.ValueSpec)
	return vs.Values[0], nil
}

func dstExprString(expr dst.Expr) string {
	f := &dst.File{
		Name: dst.NewIdent("main"),
		Decls: []dst.Decl{
			&dst.GenDecl{
				Tok: token.VAR,
				Specs: []dst.Spec{
					&dst.ValueSpec{
						Names:  []*dst.Ident{dst.NewIdent("_")},
						Values: []dst.Expr{expr},
					},
				},
			},
		},
	}

	var b strings.Builder
	if err := decorator.Fprint(&b, f); err != nil {
		panic(err)
	}

	out := b.String()
	out = strings.TrimPrefix(out, "package main\n\nvar _ = ")
	return strings.TrimSpace(out)
}

// walkStructDeps walks through the struct and finds all the structs that it references to add to the todo list
func (ft *FileTracking) walkStructDeps(s *dst.TypeSpec) {
	st := s.Type.(*dst.StructType)
	ft.serializerStructs[s] = struct{}{}

	// Walk the struct to find any more
	for _, fd := range st.Fields.List {
		ft.walkExprDeps(fd.Type)
	}
}

// walkExprDeps is a helper that walks through an expression-based AST node and recurses in to find any structs to walkStructDeps on
func (ft *FileTracking) walkExprDeps(fdt dst.Expr) {
	switch fdt := fdt.(type) {
	case *dst.Ident:
		if fdt.Obj == nil {
			return
		}
		if fdtodt, ok := fdt.Obj.Decl.(*dst.TypeSpec); ok {
			if _, ok := fdtodt.Type.(*dst.StructType); ok {
				ft.walkStructDeps(fdtodt)
			}
		}
	case *dst.StarExpr:
		ft.walkExprDeps(fdt.X)
	case *dst.SelectorExpr:
		ft.walkExprDeps(fdt.Sel)
	case *dst.ArrayType:
		ft.walkExprDeps(fdt.Elt)
	case *dst.MapType:
		ft.walkExprDeps(fdt.Key)
		ft.walkExprDeps(fdt.Value)
	}
}

// shouldBuildPartial is a helper for whether we want to generate a partial for this struct
func (ft *FileTracking) shouldBuildPartial(typeName string) bool {
	typeName = strings.TrimSuffix(strings.TrimPrefix(typeName, "*"), "|undefined")

	return lo.SomeBy(lo.Keys(ft.partialStructs), func(s *dst.TypeSpec) bool {
		return s.Name.Name == typeName
	})
}

// // isStruct is a quick helper for if a type is a struct
// func isStruct(i *dst.Ident) bool {
// 	if i.Obj != nil && i.Obj.Kind == dst.Typ {
// 		if _, is := i.Obj.Decl.(*dst.TypeSpec).Type.(*dst.StructType); is {
// 			return true
// 		}
// 	}
// 	return false
// }

// // isEnum is a quick helper for if a type is an enum (mapped) type
// func isEnum(i *dst.Ident) bool {
// 	if i.Obj != nil && i.Obj.Kind == dst.Typ {
// 		if _, is := i.Obj.Decl.(*dst.TypeSpec).Type.(*dst.Ident); is {
// 			return true
// 		}
// 	}
// 	return false
// }

// getEnumUnderlyingType calculates the underlying type, if any, for a given mapped type/enum
func (ft *FileTracking) getEnumUnderlyingType(dt dst.Expr) dst.Expr {
	switch dtt := dt.(type) {
	case *dst.Ident:
		if dtt.Obj != nil && dtt.Obj.Kind == dst.Typ {
			if dt, is := dtt.Obj.Decl.(*dst.TypeSpec); is {
				if dti, is := dt.Type.(*dst.Ident); is {
					return dti
				}
			}
		}
	case *dst.SelectorExpr:
		pkgName := dtt.X.(*dst.Ident).Name
		pkg := ft.importLookup[pkgName]
		if pkg != nil {
			ti := pkg.Types.Scope().Lookup(dtt.Sel.Name)
			if ti != nil {
				if tn, ok := ti.(*types.TypeName); ok {
					if bt, ok := tn.Type().Underlying().(*types.Basic); ok {
						return &dst.Ident{Name: bt.Name()}
					}
				}
			}
		}
		return nil
	}
	return nil
}

// getUnderlyingVarInfo is a helper for getting the underlying var info for a given primitive type
func (ft *FileTracking) getUnderlyingVarInfo(fdt dst.Expr, notNil, valueNotNil bool) (restream.VarInfo, error) {
	mt := ft.getEnumUnderlyingType(fdt)
	if mt != nil {
		vi, err := ft.getVarInfo(mt, notNil, valueNotNil)
		if err != nil {
			return nil, err
		}
		switch vit := vi.(type) {
		case *restream.VarInfoPrimitive:
			pn := ft.getPackagedName(fdt)
			vit.MappedType = restream.Ptr(pn)
		default:
			return nil, fmt.Errorf("unsupported mapped type in ft.getVarInfo: %T", vi)
		}
		return vi, nil
	}
	return nil, nil
}

// toPublicName is a helper to convert a field name to a public/capitalized-first-letter field
func toPublicName(name string) string {
	return strings.ToUpper(name[0:1]) + name[1:]
}

// StructInfo is a helper for storing basic information about a struct for codegen functions to use
type StructInfo struct {
	Name          string
	Package       string
	Fielded       bool
	GenericParams []string
}

// GolangNameWithParams is a helper for getting the golang name of a struct with generic params (i.e. PartialValue[V,P])
func (si *StructInfo) GolangNameWithParams() string {
	n := si.Name
	if si.GenericParams != nil {
		n += "[" + strings.Join(si.GenericParams, ",") + "]"
	}
	return n
}

// TSNameWithParams is a helper for getting the typescript name of a struct with generic params (i.e. PartialValue<V,P>)
func (si *StructInfo) TSNameWithParams() string {
	n := si.Name
	if si.GenericParams != nil {
		n += "<" + strings.Join(si.GenericParams, ",") + ">"
	}
	return n
}
