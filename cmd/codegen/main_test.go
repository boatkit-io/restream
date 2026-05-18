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
		"import { BinaryReader, BinaryWriter, RPCResponseStruct, RPCStruct, SerializationType, VarInfoArray, VarInfoDynamic, VarInfoGenericParam, VarInfoMap, VarInfoPointer, VarInfoPrimitive, VarInfoStruct } from '@boatkit-io/restream';",
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
		"import { VarInfoPrimitive, VarInfoStruct, VarInfoGenericParam, VarInfoPointer, VarInfoArray, VarInfoMap, VarInfoDynamic, SerializationType } from '../utils/SerializationTypes.js';",
		"import type { VarInfo, FieldInfo, AppliablePartial, AppliableOnTopPartial } from '../utils/SerializationTypes.js';",
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
