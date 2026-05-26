package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

func TestGeneratedRestreamTypesDoNotRequireSourceImport(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	projectDir := t.TempDir()
	serverDir := filepath.Join(projectDir, "cmd", "server")
	if err := os.MkdirAll(serverDir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/bootstrap

go 1.26.2

require github.com/boatkit-io/restream v0.0.0

replace github.com/boatkit-io/restream => `+repoRoot+`
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(serverDir, "boardstorestate.go"), []byte(`package main

// @restream.partials
type BoardStoreState struct {
	Board   [][]string
	Player0 bool
}
`), 0644); err != nil {
		t.Fatal(err)
	}

	pt := NewProjTracking(projectDir, &restreamConfig{
		InputDirs: []string{"cmd/server"},
	})
	if err := pt.parseProject(); err != nil {
		t.Fatal(err)
	}
	for _, ft := range pt.files {
		if err := ft.Run(); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := os.Stat(filepath.Join(serverDir, "boardstorestate_rs.go")); err != nil {
		t.Fatal(err)
	}
}

func TestParseProjectIgnoresRestreamGeneratedGoFiles(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	projectDir := t.TempDir()
	serverDir := filepath.Join(projectDir, "cmd", "server")
	if err := os.MkdirAll(serverDir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/generated-filter

go 1.26.2

require github.com/boatkit-io/restream v0.0.0

replace github.com/boatkit-io/restream => `+repoRoot+`
`), 0644); err != nil {
		t.Fatal(err)
	}

	sourcePath := filepath.Join(serverDir, "model.go")
	if err := os.WriteFile(sourcePath, []byte(`package main

// @restream.fields
type Model struct {
	Count int
}
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(serverDir, "already_rs.go"), []byte(restreamGeneratedFileBanner+`
//
//nolint:lll
package main

type AlreadyGenerated struct{}
`), 0644); err != nil {
		t.Fatal(err)
	}

	pt := NewProjTracking(projectDir, &restreamConfig{
		InputDirs: []string{"cmd/server"},
	})
	if err := pt.parseProject(); err != nil {
		t.Fatal(err)
	}
	if len(pt.files) != 1 || filepath.Base(pt.files[0].inFile) != "model.go" {
		t.Fatalf("parsed files = %v, want only model.go", fileTrackingBaseNames(pt.files))
	}

	if err := pt.files[0].Run(); err != nil {
		t.Fatal(err)
	}

	out, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), restreamGeneratedFileBanner) {
		t.Fatalf("source rewrite leaked generated banner:\n%s", string(out))
	}
}

func TestConstIgnoreAnnotationSkipsTSConst(t *testing.T) {
	projectDir := t.TempDir()
	serverDir := filepath.Join(projectDir, "cmd", "server")
	if err := os.MkdirAll(serverDir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/constignore

go 1.26.2
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(serverDir, "consts.go"), []byte(`package main

// @restream.Ignore
const HiddenConst = "hidden"

const VisibleConst = "visible"
`), 0644); err != nil {
		t.Fatal(err)
	}

	pt := NewProjTracking(projectDir, &restreamConfig{
		InputDirs: []string{"cmd/server"},
	})
	if err := pt.parseProject(); err != nil {
		t.Fatal(err)
	}
	for _, ft := range pt.files {
		if err := ft.Run(); err != nil {
			t.Fatal(err)
		}
	}

	generated := ""
	for _, ft := range pt.files {
		for _, entry := range ft.tsGenEntries {
			generated += entry.defs
		}
	}
	if strings.Contains(generated, "HiddenConst") {
		t.Fatalf("ignored const was generated:\n%s", generated)
	}
	if !strings.Contains(generated, `export const VisibleConst = "visible";`) {
		t.Fatalf("visible const was not generated:\n%s", generated)
	}
}

func TestRPCRequestGenerationExpandsGroupedParams(t *testing.T) {
	projectDir := t.TempDir()
	serverDir := filepath.Join(projectDir, "cmd", "server")
	if err := os.MkdirAll(serverDir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/rpcparams

go 1.26.2
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(serverDir, "boardstore.go"), []byte(`package main

import (
	"reflect"
)

var _ = reflect.TypeFor[int]

type testDispatcher struct{}

func (*testDispatcher) RegisterRPCHandler(string, int, any, any, any) {}

func Register(rpcd *testDispatcher) {
	rpcd.RegisterRPCHandler("PlaceToken", 1, func(x, y int) error {
		return nil
	}, nil, nil)
}
`), 0644); err != nil {
		t.Fatal(err)
	}

	pt := NewProjTracking(projectDir, &restreamConfig{
		InputDirs: []string{"cmd/server"},
	})
	if err := pt.parseProject(); err != nil {
		t.Fatal(err)
	}
	for _, ft := range pt.files {
		if err := ft.Run(); err != nil {
			t.Fatal(err)
		}
	}

	out, err := os.ReadFile(filepath.Join(serverDir, "boardstore_rs.go"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)

	for _, expected := range []string{
		"X int",
		"Y int",
		`{Name: "X", FieldIdx: 0, VarInfo: &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt64, MappedType: restream.Ptr("int")}}`,
		`{Name: "Y", FieldIdx: 1, VarInfo: &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt64, MappedType: restream.Ptr("int")}}`,
		"restream.SerializeValue(s.Y, w, PlaceTokenRequestFieldInfo[1].VarInfo)",
		"restream.DeserializeValue(&s.Y, r, PlaceTokenRequestFieldInfo[1].VarInfo)",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("generated RPC request missing expected %q:\n%s", expected, got)
		}
	}
}

func TestEventGenerationExpandsGroupedParams(t *testing.T) {
	source := `package main

import (
	"reflect"

	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
)

type testDispatcher struct{}

func (*testDispatcher) RegisterEvent(string, any, ...reflect.Type) {}

type tokenPlacedCallback func(x, y int)

func Register(eventDispatcher *testDispatcher) {
	tokenPlaced := subscribableevent.NewEvent[tokenPlacedCallback]()
	eventDispatcher.RegisterEvent("TokenPlaced", tokenPlaced, nil, nil)
}

func RegisterAgain(eventDispatcher *testDispatcher) {
	tokenPlaced2 := subscribableevent.NewEvent[tokenPlacedCallback]()
	eventDispatcher.RegisterEvent("TokenPlaced2", tokenPlaced2, nil, nil)
}
`
	projectDir, serverDir, sourcePath := setupEventGenerationProject(t, source)

	pt := NewProjTracking(projectDir, &restreamConfig{
		InputDirs: []string{"cmd/server"},
	})
	if err := pt.parseProject(); err != nil {
		t.Fatal(err)
	}
	for _, ft := range pt.files {
		if err := ft.Run(); err != nil {
			t.Fatal(err)
		}
	}

	out, err := os.ReadFile(filepath.Join(serverDir, "boardstore_rs.go"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)

	for _, expected := range []string{
		"type TokenPlacedEvent struct",
		"type TokenPlaced2Event struct",
		"X int",
		"Y int",
		`{Name: "X", FieldIdx: 0, VarInfo: &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt64, MappedType: restream.Ptr("int")}}`,
		`{Name: "Y", FieldIdx: 1, VarInfo: &restream.VarInfoPrimitive{DataType: restream.SerializationTypeInt64, MappedType: restream.Ptr("int")}}`,
		"restream.SerializeValue(s.Y, w, TokenPlacedEventFieldInfo[1].VarInfo)",
		"restream.DeserializeValue(&s.Y, r, TokenPlacedEventFieldInfo[1].VarInfo)",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("generated event packet missing expected %q:\n%s", expected, got)
		}
	}
	if count := strings.Count(got, "type TokenPlacedEvent struct"); count != 1 {
		t.Fatalf("generated %d TokenPlacedEvent declarations, want 1:\n%s", count, got)
	}
	if count := strings.Count(got, "type TokenPlaced2Event struct"); count != 1 {
		t.Fatalf("generated %d TokenPlaced2Event declarations, want 1:\n%s", count, got)
	}

	sourceOut, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	rewrittenSource := string(sourceOut)
	for _, expected := range []string{
		"eventDispatcher.RegisterEvent(\"TokenPlaced\", &tokenPlaced",
		"eventDispatcher.RegisterEvent(\"TokenPlaced2\", &tokenPlaced2",
		"reflect.TypeFor[TokenPlacedEvent]()",
		"reflect.TypeFor[TokenPlaced2Event]()",
		"reflect.TypeFor[func(int, int)]()",
	} {
		if !strings.Contains(rewrittenSource, expected) {
			t.Fatalf("rewritten source missing expected %q:\n%s", expected, rewrittenSource)
		}
	}
	if count := strings.Count(rewrittenSource, "reflect.TypeFor[TokenPlacedEvent]()"); count != 1 {
		t.Fatalf("rewritten source has %d TokenPlacedEvent type args, want 1:\n%s", count, rewrittenSource)
	}
	if count := strings.Count(rewrittenSource, "reflect.TypeFor[TokenPlaced2Event]()"); count != 1 {
		t.Fatalf("rewritten source has %d TokenPlaced2Event type args, want 1:\n%s", count, rewrittenSource)
	}
}

func TestEventGenerationRejectsDuplicateNames(t *testing.T) {
	source := `package main

import (
	"reflect"

	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
)

type testDispatcher struct{}

func (*testDispatcher) RegisterEvent(string, any, ...reflect.Type) {}

type tokenPlacedCallback func(x, y int)

func Register(eventDispatcher *testDispatcher) {
	tokenPlaced := subscribableevent.NewEvent[tokenPlacedCallback]()
	eventDispatcher.RegisterEvent("TokenPlaced", tokenPlaced, nil, nil)
}

func RegisterAgain(eventDispatcher *testDispatcher) {
	tokenPlaced := subscribableevent.NewEvent[tokenPlacedCallback]()
	eventDispatcher.RegisterEvent("TokenPlaced", tokenPlaced, nil, nil)
}
`
	projectDir, _, _ := setupEventGenerationProject(t, source)

	pt := NewProjTracking(projectDir, &restreamConfig{
		InputDirs: []string{"cmd/server"},
	})
	if err := pt.parseProject(); err != nil {
		t.Fatal(err)
	}

	var gotErr error
	for _, ft := range pt.files {
		if err := ft.Run(); err != nil {
			gotErr = err
			break
		}
	}
	if gotErr == nil {
		t.Fatal("expected duplicate event registration to fail")
	}
	if !strings.Contains(gotErr.Error(), `duplicate RegisterEvent name "TokenPlaced"`) {
		t.Fatalf("duplicate event registration error = %q", gotErr)
	}
}

func setupEventGenerationProject(t *testing.T, source string) (string, string, string) {
	t.Helper()

	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	projectDir := t.TempDir()
	serverDir := filepath.Join(projectDir, "cmd", "server")
	if err := os.MkdirAll(serverDir, 0755); err != nil {
		t.Fatal(err)
	}
	tugboatDir := filepath.Join(projectDir, "tugboat")
	tugboatSubscribableDir := filepath.Join(tugboatDir, "pkg", "subscribableevent")
	if err := os.MkdirAll(tugboatSubscribableDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tugboatDir, "go.mod"), []byte(`module github.com/boatkit-io/tugboat

go 1.26.2
`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tugboatSubscribableDir, "subscribableevent.go"), []byte(`package subscribableevent

type Event[F any] struct{}

func NewEvent[F any]() Event[F] {
	return Event[F]{}
}
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/eventparams

go 1.26.2

require (
	github.com/boatkit-io/restream v0.0.0
	github.com/boatkit-io/tugboat v0.8.9
)

replace github.com/boatkit-io/restream => `+repoRoot+`
replace github.com/boatkit-io/tugboat => ./tugboat
`), 0644); err != nil {
		t.Fatal(err)
	}

	sourcePath := filepath.Join(serverDir, "boardstore.go")
	if err := os.WriteFile(sourcePath, []byte(source), 0644); err != nil {
		t.Fatal(err)
	}

	return projectDir, serverDir, sourcePath
}

func TestWriteTSFileUsesPackageRuntimeImportsByDefault(t *testing.T) {
	projectDir := t.TempDir()
	pt := NewProjTracking(projectDir, &restreamConfig{
		TSDir: "web/src/restream",
	})

	if err := pt.writeTSFile("PackageModel.ts", nil, []tsImport{
		{Path: "./PackageShared.js", Imports: []string{"SharedType"}},
	}); err != nil {
		t.Fatal(err)
	}

	out, err := os.ReadFile(filepath.Join(projectDir, "web", "src", "restream", "PackageModel.ts"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)

	for _, expected := range []string{
		"import * as ReStreamDecoders from '@boatkit-io/restream';",
		"import * as ReStreamEncoders from '@boatkit-io/restream';",
		"import { BinaryReader, BinaryWriter, EventStruct, RPCResponseStruct, RPCStruct, SerializationType, VarInfoArray, VarInfoDynamic, VarInfoGenericParam, VarInfoMap, VarInfoPointer, VarInfoPrimitive, VarInfoStruct } from '@boatkit-io/restream';",
		"import type { AppliableOnTopPartial, AppliablePartial, FieldInfo, VarInfo } from '@boatkit-io/restream';",
		"import { SharedType } from './PackageShared.js';",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("generated TypeScript missing expected import %q:\n%s", expected, got)
		}
	}

	for _, unexpected := range []string{
		"@/restream/ReStreamTypes",
		"../utils/BinaryReader.js",
		"./ReStreamTypes.js",
	} {
		if strings.Contains(got, unexpected) {
			t.Fatalf("generated TypeScript contains unexpected local runtime import %q:\n%s", unexpected, got)
		}
	}
}

func fileTrackingBaseNames(files []*FileTracking) []string {
	out := make([]string, 0, len(files))
	for _, ft := range files {
		out = append(out, filepath.Base(ft.inFile))
	}
	return out
}

func TestWriteTSFileCanUseLocalRuntimeImports(t *testing.T) {
	projectDir := t.TempDir()
	pt := NewProjTracking(projectDir, &restreamConfig{
		TSDir:               "web/src/restream",
		TSRuntimeImportMode: tsRuntimeImportModeLocal,
	})

	if err := pt.writeTSFile("PackageRestream.ts", nil, nil); err != nil {
		t.Fatal(err)
	}

	out, err := os.ReadFile(filepath.Join(projectDir, "web", "src", "restream", "PackageRestream.ts"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)

	for _, expected := range []string{
		"import * as ReStreamDecoders from '../utils/Decoders.js';",
		"import * as ReStreamEncoders from '../utils/Encoders.js';",
		"import BinaryReader from '../utils/BinaryReader.js';",
		"import BinaryWriter from '../utils/BinaryWriter.js';",
		"import { SerializationType, VarInfoArray, VarInfoDynamic, VarInfoGenericParam, VarInfoMap, VarInfoPointer, VarInfoPrimitive, VarInfoStruct } from '../utils/SerializationTypes.js';",
		"import type { AppliableOnTopPartial, AppliablePartial, FieldInfo, VarInfo } from '../utils/SerializationTypes.js';",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("generated TypeScript missing expected local import %q:\n%s", expected, got)
		}
	}

	if strings.Contains(got, "@boatkit-io/restream") {
		t.Fatalf("generated TypeScript contains package runtime import in local mode:\n%s", got)
	}
}

func TestWriteTSFileFiltersUnusedRuntimeImports(t *testing.T) {
	projectDir := t.TempDir()
	pt := NewProjTracking(projectDir, &restreamConfig{
		TSDir: "web/src/restream",
	})

	if err := pt.writeTSFile("PackageModel.ts", []fdef{
		{
			name: "Model",
			defs: "export class Model {\n    public static deserialized(r: BinaryReader) { return r; }\n    private static _fieldInfo: FieldInfo[] = [];\n}\n",
			typ:  fdefTypeOther,
		},
	}, nil); err != nil {
		t.Fatal(err)
	}

	out, err := os.ReadFile(filepath.Join(projectDir, "web", "src", "restream", "PackageModel.ts"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)

	for _, expected := range []string{
		"import { BinaryReader } from '@boatkit-io/restream';",
		"import type { FieldInfo } from '@boatkit-io/restream';",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("generated TypeScript missing expected filtered import %q:\n%s", expected, got)
		}
	}

	for _, unexpected := range []string{
		"AppliablePartial",
		"ReStreamDecoders",
		"VarInfoArray",
	} {
		if strings.Contains(got, unexpected) {
			t.Fatalf("generated TypeScript contains unexpected unused import %q:\n%s", unexpected, got)
		}
	}
}

func TestRestreamPackageImportsUseRuntimePackageForConsumers(t *testing.T) {
	projectDir := t.TempDir()
	restreamPkg := &packages.Package{
		Name:    "restream",
		PkgPath: restreamPackagePath,
	}

	consumerPT := NewProjTracking(projectDir, &restreamConfig{})
	if got := consumerPT.tsPackageImportPath(restreamPkg); got != defaultTSRuntimeImportPath {
		t.Fatalf("consumer restream package import path = %q, want %q", got, defaultTSRuntimeImportPath)
	}

	localPT := NewProjTracking(projectDir, &restreamConfig{TSRuntimeImportMode: tsRuntimeImportModeLocal})
	if got := localPT.tsPackageImportPath(restreamPkg); got != "./PackageRestream.js" {
		t.Fatalf("local restream package import path = %q, want %q", got, "./PackageRestream.js")
	}
}

func TestWriteTSPackageFilesDoesNotOverwriteNonTestPackage(t *testing.T) {
	projectDir := t.TempDir()
	pt := NewProjTracking(projectDir, &restreamConfig{
		TSDir:               "web/src/restream",
		TSRuntimeImportMode: tsRuntimeImportModeLocal,
	})
	restreamPkg := &packages.Package{
		ID:      restreamPackagePath,
		Name:    "restream",
		PkgPath: restreamPackagePath,
	}
	restreamTestPkg := &packages.Package{
		ID:      restreamPackagePath + " [github.com/boatkit-io/restream/pkg/restream.test]",
		Name:    "restream",
		PkgPath: restreamPackagePath,
	}
	pt.tsPackageEntries[restreamPkg] = []fdef{{name: "PartialModMap", defs: "export class PartialModMap {}", typ: fdefTypeOther}}
	pt.tsPackageEntries[restreamTestPkg] = []fdef{{name: "LatLong", defs: "export class LatLong {}", typ: fdefTypeOther}}

	if err := pt.writeTSPackageFiles(); err != nil {
		t.Fatal(err)
	}

	nonTestOut, err := os.ReadFile(filepath.Join(projectDir, "web", "src", "restream", "PackageRestream.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(nonTestOut), "export class PartialModMap") {
		t.Fatalf("non-test PackageRestream.ts was not preserved:\n%s", string(nonTestOut))
	}
	if strings.Contains(string(nonTestOut), "export class LatLong") {
		t.Fatalf("test entry overwrote or leaked into PackageRestream.ts:\n%s", string(nonTestOut))
	}

	testOut, err := os.ReadFile(filepath.Join(projectDir, "web", "src", "restream", "PackageRestreamTest.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(testOut), "export class LatLong") {
		t.Fatalf("test package output missing expected entry:\n%s", string(testOut))
	}
}
