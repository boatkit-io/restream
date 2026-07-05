package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/fatih/structtag"
)

var maxFieldCommentRE = regexp.MustCompile(`//\s*MAXFIELD\((\d+)\)`)

type fieldedStructSnapshot struct {
	name         string
	maxField     byte
	fieldsByName map[string]fieldSnapshot
	fieldsByID   map[byte]fieldSnapshot
}

type fieldSnapshot struct {
	name string
	id   byte
}

func (ft *FileTracking) validateFieldedStructGitCompatibility() error {
	if len(ft.fieldedStructs) == 0 {
		return nil
	}

	current, err := ft.currentFieldedStructSnapshots()
	if err != nil {
		return err
	}
	for _, snapshot := range current {
		if err := validateFieldedStructSnapshot(snapshot); err != nil {
			return err
		}
	}

	previousContent, ok, err := ft.previousGitFileContent()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	previous, err := fieldedStructSnapshotsFromSource(ft.inFile, previousContent)
	if err != nil {
		return fmt.Errorf("parse previous git version of %s: %w", ft.inFile, err)
	}

	for structName, currentSnapshot := range current {
		previousSnapshot, ok := previous[structName]
		if !ok {
			continue
		}
		if err := validateFieldedStructCompatibility(previousSnapshot, currentSnapshot); err != nil {
			return err
		}
	}

	return nil
}

func (ft *FileTracking) currentFieldedStructSnapshots() (map[string]fieldedStructSnapshot, error) {
	snapshots := map[string]fieldedStructSnapshot{}
	for s := range ft.fieldedStructs {
		st := s.Type.(*dst.StructType)
		snapshot, err := fieldedStructSnapshotFromStruct(s.Name.Name, st)
		if err != nil {
			return nil, fmt.Errorf("fielded struct %s: %w", s.Name.Name, err)
		}
		snapshots[snapshot.name] = snapshot
	}
	return snapshots, nil
}

func fieldedStructSnapshotsFromSource(filename string, content []byte) (map[string]fieldedStructSnapshot, error) {
	f, err := decorator.ParseFile(token.NewFileSet(), filename, content, parser.AllErrors|parser.ParseComments)
	if err != nil {
		return nil, err
	}

	snapshots := map[string]fieldedStructSnapshot{}
	for _, d := range f.Decls {
		gd, ok := d.(*dst.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		if !genDeclHasFieldedRestreamAnnotation(gd) {
			continue
		}
		for _, spec := range gd.Specs {
			s, ok := spec.(*dst.TypeSpec)
			if !ok {
				continue
			}
			st, ok := s.Type.(*dst.StructType)
			if !ok {
				continue
			}
			snapshot, err := fieldedStructSnapshotFromStruct(s.Name.Name, st)
			if err != nil {
				return nil, fmt.Errorf("fielded struct %s: %w", s.Name.Name, err)
			}
			snapshots[snapshot.name] = snapshot
		}
	}

	return snapshots, nil
}

func genDeclHasFieldedRestreamAnnotation(gd *dst.GenDecl) bool {
	for _, dec := range gd.Decorations().Start {
		if strings.Contains(dec, "@restream.fields") || strings.Contains(dec, "@restream.partials") {
			return true
		}
	}
	return false
}

func fieldedStructSnapshotFromStruct(structName string, st *dst.StructType) (fieldedStructSnapshot, error) {
	maxField, err := maxFieldForStruct(st)
	if err != nil {
		return fieldedStructSnapshot{}, err
	}

	snapshot := fieldedStructSnapshot{
		name:         structName,
		maxField:     maxField,
		fieldsByName: map[string]fieldSnapshot{},
		fieldsByID:   map[byte]fieldSnapshot{},
	}

	for _, fd := range st.Fields.List {
		if len(fd.Names) == 0 {
			continue
		}
		id, err := restreamFieldID(fd)
		if err != nil {
			return fieldedStructSnapshot{}, err
		}
		field := fieldSnapshot{
			name: fd.Names[0].Name,
			id:   id,
		}
		snapshot.fieldsByName[field.name] = field
		if field.id != 0 {
			snapshot.fieldsByID[field.id] = field
		}
	}

	return snapshot, nil
}

func maxFieldForStruct(st *dst.StructType) (byte, error) {
	if st.Fields == nil || len(st.Fields.List) == 0 {
		return 0, nil
	}
	for _, dec := range st.Fields.List[0].Decorations().Start.All() {
		match := maxFieldCommentRE.FindStringSubmatch(dec)
		if len(match) != 2 {
			continue
		}
		maxField, err := strconv.ParseUint(match[1], 10, 8)
		if err != nil {
			return 0, err
		}
		return byte(maxField), nil
	}
	return 0, nil
}

func restreamFieldID(fd *dst.Field) (byte, error) {
	tagStr := ""
	if fd.Tag != nil {
		tagStr = strings.Trim(fd.Tag.Value, "`")
	}

	tags, err := structtag.Parse(tagStr)
	if err != nil {
		return 0, err
	}

	tag, err := tags.Get(restreamTag)
	if err != nil {
		return 0, nil
	}

	for _, opt := range tag.Options {
		parts := strings.Split(opt, "=")
		if parts[0] != fieldIDSubTag {
			continue
		}
		if len(parts) != 2 {
			return 0, fmt.Errorf("invalid %s tag option %q", restreamTag, opt)
		}
		id, err := strconv.ParseUint(parts[1], 10, 8)
		if err != nil {
			return 0, err
		}
		return byte(id), nil
	}

	return 0, nil
}

func validateFieldedStructSnapshot(snapshot fieldedStructSnapshot) error {
	seenIDs := map[byte]fieldSnapshot{}
	for _, field := range snapshot.fieldsByName {
		if field.id == 0 {
			return fmt.Errorf("fielded struct %s field %s is missing a restream fID", snapshot.name, field.name)
		}
		if field.id > snapshot.maxField {
			return fmt.Errorf(
				"fielded struct %s field %s has fID=%d above MAXFIELD(%d)",
				snapshot.name,
				field.name,
				field.id,
				snapshot.maxField,
			)
		}
		if previousField, ok := seenIDs[field.id]; ok {
			return fmt.Errorf(
				"fielded struct %s has duplicate fID=%d on fields %s and %s",
				snapshot.name,
				field.id,
				previousField.name,
				field.name,
			)
		}
		seenIDs[field.id] = field
	}
	return nil
}

func validateFieldedStructCompatibility(previous, current fieldedStructSnapshot) error {
	if current.maxField < previous.maxField {
		return fmt.Errorf(
			"fielded struct %s lowered MAXFIELD from %d to %d",
			current.name,
			previous.maxField,
			current.maxField,
		)
	}

	for fieldName, previousField := range previous.fieldsByName {
		if previousField.id == 0 {
			continue
		}
		currentField, ok := current.fieldsByName[fieldName]
		if !ok {
			continue
		}
		if currentField.id != previousField.id {
			return fmt.Errorf(
				"fielded struct %s changed field %s fID from %d to %d",
				current.name,
				fieldName,
				previousField.id,
				currentField.id,
			)
		}
	}

	for fieldName, currentField := range current.fieldsByName {
		if _, ok := previous.fieldsByName[fieldName]; ok {
			continue
		}
		if currentField.id <= previous.maxField {
			return fmt.Errorf(
				"fielded struct %s added field %s with fID=%d, but previous MAXFIELD was %d; new fields must use fID > previous MAXFIELD",
				current.name,
				fieldName,
				currentField.id,
				previous.maxField,
			)
		}
		if previousField, ok := previous.fieldsByID[currentField.id]; ok {
			return fmt.Errorf(
				"fielded struct %s added field %s with fID=%d, which was previously assigned to field %s",
				current.name,
				fieldName,
				currentField.id,
				previousField.name,
			)
		}
	}

	return nil
}

func (ft *FileTracking) previousGitFileContent() ([]byte, bool, error) {
	inFile, err := filepath.Abs(ft.inFile)
	if err != nil {
		return nil, false, err
	}
	if resolvedInFile, err := filepath.EvalSymlinks(inFile); err == nil {
		inFile = resolvedInFile
	}

	repoRootOut, err := exec.Command("git", "-C", filepath.Dir(inFile), "rev-parse", "--show-toplevel").Output() //nolint:gosec
	if err != nil {
		return nil, false, nil
	}
	repoRoot := strings.TrimSpace(string(repoRootOut))
	if repoRoot == "" {
		return nil, false, nil
	}
	if resolvedRepoRoot, err := filepath.EvalSymlinks(repoRoot); err == nil {
		repoRoot = resolvedRepoRoot
	}

	relPath, err := filepath.Rel(repoRoot, inFile)
	if err != nil {
		return nil, false, err
	}
	if strings.HasPrefix(relPath, ".."+string(filepath.Separator)) || relPath == ".." {
		return nil, false, nil
	}

	content, err := exec.Command("git", "-C", repoRoot, "show", "HEAD:"+filepath.ToSlash(relPath)).Output() //nolint:gosec
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return nil, false, nil
		}
		return nil, false, err
	}

	return content, true, nil
}
