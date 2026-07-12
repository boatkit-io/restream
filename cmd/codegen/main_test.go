package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/boatkit-io/restream/pkg/restream"
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
	generated, err := os.ReadFile(filepath.Join(serverDir, "boardstorestate_rs.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"func (s *BoardStoreState) PartialForFields(fields [][]any) (restream.Partial, bool)",
		"partialForFieldsBoard",
		"restream.NewPartialArray[[]string]()",
	} {
		if !strings.Contains(string(generated), expected) {
			t.Fatalf("generated partial snapshot support missing expected %q:\n%s", expected, string(generated))
		}
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

const BooleanConst = true

const booleanAlias = false

const BooleanAliasConst = booleanAlias
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
	if !strings.Contains(generated, `export const BooleanConst = true;`) {
		t.Fatalf("boolean const was not generated:\n%s", generated)
	}
	if !strings.Contains(generated, `export const BooleanAliasConst = false;`) {
		t.Fatalf("boolean alias const was not generated:\n%s", generated)
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

func TestStoreAnnotationGeneratesStoreBoilerplate(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	projectDir := t.TempDir()
	serverDir := filepath.Join(projectDir, "cmd", "server")
	if err := os.MkdirAll(serverDir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/storeannotation

go 1.26.5

require github.com/boatkit-io/restream v0.0.0

replace github.com/boatkit-io/restream => `+repoRoot+`
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(serverDir, "boardstore.go"), []byte(`package main

import "github.com/boatkit-io/restream/pkg/restream"

type AccessLevel = restream.AccessLevel

const AccessLevelAdmin AccessLevel = 2

// @restream.store(BoardStore)
type BoardStore struct {
	storeData any
}

func (*BoardStore) GetMinimumAccessLevel() restream.AccessLevel {
	return AccessLevelAdmin
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
	if err := pt.Run(); err != nil {
		t.Fatal(err)
	}

	out, err := os.ReadFile(filepath.Join(serverDir, "boardstore_rs.go"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)

	for _, expected := range []string{
		`const BoardStoreName = "BoardStore"`,
		"type BoardStoreStatePartial struct",
		"func (s *BoardStore) GetName() string",
		"return BoardStoreName",
		"func (s *BoardStore) GetStoreData() restream.StoreDataBase",
		"return s.storeData",
		"func (s *BoardStore) SubscribeToField(field []any, callback any)",
		"s.storeData.SubscribeToField(field, callback)",
		"func (s *BoardStore) GetStoreType() restream.StoreType",
		"return restream.StoreTypeDeviceWithRelay",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("generated store boilerplate missing expected %q:\n%s", expected, got)
		}
	}

	sourceOut, err := os.ReadFile(filepath.Join(serverDir, "boardstore.go"))
	if err != nil {
		t.Fatal(err)
	}
	rewrittenSource := string(sourceOut)
	for _, expected := range []string{
		"// @restream.partials",
		"type BoardStoreState struct",
		`"github.com/boatkit-io/restream/pkg/restream"`,
		"storeData *restream.StoreData[BoardStoreState, *BoardStoreState, *BoardStoreStatePartial]",
	} {
		if !strings.Contains(rewrittenSource, expected) {
			t.Fatalf("rewritten source missing expected %q:\n%s", expected, rewrittenSource)
		}
	}

	foundTSConst := false
	for _, ft := range pt.files {
		for _, entry := range ft.tsGenEntries {
			if entry.name == "BoardStoreName" && entry.typ == fdefTypeEnum &&
				strings.Contains(entry.defs, `export const BoardStoreName = "BoardStore";`) {
				foundTSConst = true
			}
		}
	}
	if !foundTSConst {
		t.Fatalf("store annotation did not generate TypeScript store name const")
	}

	relayOut, err := os.ReadFile(filepath.Join(serverDir, "relaystores_rs.go"))
	if err != nil {
		t.Fatal(err)
	}
	relayGenerated := string(relayOut)
	for _, expected := range []string{
		"func NewRelayStores() []restream.Store",
		"restream.NewRelayStore[BoardStoreState, *BoardStoreState, *BoardStoreStatePartial]",
		"BoardStoreName",
		"restream.AccessLevel(2)",
	} {
		if !strings.Contains(relayGenerated, expected) {
			t.Fatalf("generated relay store factory missing expected %q:\n%s", expected, relayGenerated)
		}
	}
}

func TestStoreAnnotationStoreTypesControlRelayFactory(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	projectDir := t.TempDir()
	serverDir := filepath.Join(projectDir, "cmd", "server")
	if err := os.MkdirAll(serverDir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/storeannotationtypes

go 1.26.5

require github.com/boatkit-io/restream v0.0.0

replace github.com/boatkit-io/restream => `+repoRoot+`
`), 0644); err != nil {
		t.Fatal(err)
	}

	sourcePath := filepath.Join(serverDir, "stores.go")
	if err := os.WriteFile(sourcePath, []byte(`package main

import "github.com/boatkit-io/restream/pkg/restream"

// @restream.partials
type DeviceRelayState struct{}

// @restream.partials
type DeviceNoRelayState struct{}

// @restream.partials
type DeviceCloudImplState struct{}

	// @restream.partials
	type DeviceAndCloudState struct{}

	// @restream.partials
	type DeviceCloudSourceState struct{}

	// @restream.partials
	type CloudImplOfDeviceState struct{}

	// @restream.partials
	type CloudSourceForDeviceState struct{}

	// @restream.partials
	type CloudOnlyState struct{}

// @restream.store(RelayStore, DeviceWithRelay)
type DeviceRelay struct {
	storeData *restream.StoreData[DeviceRelayState, *DeviceRelayState, *DeviceRelayStatePartial]
}

// @restream.store(NoRelayStore, DeviceWithNoRelay)
type DeviceNoRelay struct {
	storeData *restream.StoreData[DeviceNoRelayState, *DeviceNoRelayState, *DeviceNoRelayStatePartial]
}

// @restream.store(CloudImplStore, DeviceWithCloudImpl)
type DeviceCloudImpl struct {
	storeData *restream.StoreData[DeviceCloudImplState, *DeviceCloudImplState, *DeviceCloudImplStatePartial]
}

	// @restream.store(DeviceAndCloudStore, DeviceAndCloud)
	type DeviceAndCloud struct {
		storeData *restream.StoreData[DeviceAndCloudState, *DeviceAndCloudState, *DeviceAndCloudStatePartial]
	}

	// @restream.store(CloudSourceStore, DeviceWithCloudSource)
	type DeviceCloudSource struct {
		storeData *restream.StoreData[DeviceCloudSourceState, *DeviceCloudSourceState, *DeviceCloudSourceStatePartial]
	}

	// @restream.store(CloudImplOfDeviceStore, CloudImplOfDevice)
	type CloudImplOfDevice struct {
		storeData *restream.StoreData[CloudImplOfDeviceState, *CloudImplOfDeviceState, *CloudImplOfDeviceStatePartial]
	}

	// @restream.store(CloudSourceForDeviceStore, CloudSourceForDevice)
	type CloudSourceForDevice struct {
		storeData *restream.StoreData[CloudSourceForDeviceState, *CloudSourceForDeviceState, *CloudSourceForDeviceStatePartial]
	}

	// @restream.store(CloudOnlyStore, CloudOnly)
	type CloudOnly struct {
		storeData *restream.StoreData[CloudOnlyState, *CloudOnlyState, *CloudOnlyStatePartial]
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
	if err := pt.Run(); err != nil {
		t.Fatal(err)
	}

	generated, err := os.ReadFile(filepath.Join(serverDir, "stores_rs.go"))
	if err != nil {
		t.Fatal(err)
	}
	storeGenerated := string(generated)
	for _, expected := range []string{
		"return restream.StoreTypeDeviceWithRelay",
		"return restream.StoreTypeDeviceWithNoRelay",
		"return restream.StoreTypeDeviceWithCloudImpl",
		"return restream.StoreTypeDeviceAndCloud",
		"return restream.StoreTypeDeviceWithCloudSource",
		"return restream.StoreTypeCloudImplOfDevice",
		"return restream.StoreTypeCloudSourceForDevice",
		"return restream.StoreTypeCloudOnly",
	} {
		if !strings.Contains(storeGenerated, expected) {
			t.Fatalf("generated store source missing expected %q:\n%s", expected, storeGenerated)
		}
	}

	relayOut, err := os.ReadFile(filepath.Join(serverDir, "relaystores_rs.go"))
	if err != nil {
		t.Fatal(err)
	}
	relayGenerated := string(relayOut)
	if !strings.Contains(relayGenerated, "restream.NewRelayStore[DeviceRelayState, *DeviceRelayState, *DeviceRelayStatePartial]") {
		t.Fatalf("generated relay factory missing DeviceWithRelay store:\n%s", relayGenerated)
	}
	if !strings.Contains(relayGenerated, "restream.NewCloudSourceForDeviceStore[DeviceCloudSourceState, *DeviceCloudSourceState, *DeviceCloudSourceStatePartial]") {
		t.Fatalf("generated relay factory missing DeviceWithCloudSource store:\n%s", relayGenerated)
	}
	for _, unexpected := range []string{
		"DeviceNoRelayName",
		"DeviceCloudImplName",
		"DeviceAndCloudName",
		"CloudImplOfDeviceName",
		"CloudSourceForDeviceName",
		"CloudOnlyName",
	} {
		if strings.Contains(relayGenerated, unexpected) {
			t.Fatalf("generated relay factory included non-relay store %q:\n%s", unexpected, relayGenerated)
		}
	}
}

func TestStoreAnnotationRelayFactoryResolvesImportedMinimumAccessConstant(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	projectDir := t.TempDir()
	serverDir := filepath.Join(projectDir, "cmd", "server")
	authDir := filepath.Join(projectDir, "internal", "auth")
	for _, dir := range []string{serverDir, authDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/importedaccess

go 1.26.5

require github.com/boatkit-io/restream v0.0.0

replace github.com/boatkit-io/restream => `+repoRoot+`
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(authDir, "access.go"), []byte(`package auth

type AccessLevel int

const AccessLevelAdmin AccessLevel = 7
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(serverDir, "boardstore.go"), []byte(`package main

import (
	"example.com/importedaccess/internal/auth"
	"github.com/boatkit-io/restream/pkg/restream"
)

// @restream.store(BoardStore)
type BoardStore struct {
	storeData any
}

func (*BoardStore) GetMinimumAccessLevel() restream.AccessLevel {
	return restream.AccessLevel(auth.AccessLevelAdmin)
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
	if err := pt.Run(); err != nil {
		t.Fatal(err)
	}

	relayOut, err := os.ReadFile(filepath.Join(serverDir, "relaystores_rs.go"))
	if err != nil {
		t.Fatal(err)
	}
	relayGenerated := string(relayOut)
	if !strings.Contains(relayGenerated, "restream.AccessLevel(7)") {
		t.Fatalf("generated relay store factory did not resolve imported access constant:\n%s", relayGenerated)
	}
	if strings.Contains(relayGenerated, "AccessLevelAdmin") || strings.Contains(relayGenerated, "importedaccess/internal/auth") {
		t.Fatalf("generated relay store factory should hardcode resolved access value without importing caller auth package:\n%s", relayGenerated)
	}
}

func TestStoreAnnotationAddsMissingStoreDataMember(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	projectDir := t.TempDir()
	serverDir := filepath.Join(projectDir, "cmd", "server")
	if err := os.MkdirAll(serverDir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/storeannotationmissing

go 1.26.2

require github.com/boatkit-io/restream v0.0.0

replace github.com/boatkit-io/restream => `+repoRoot+`
`), 0644); err != nil {
		t.Fatal(err)
	}

	sourcePath := filepath.Join(serverDir, "boardstore.go")
	if err := os.WriteFile(sourcePath, []byte(`package main

type BoardStoreState struct {
	Value string
}

// @restream.store(BoardStore)
type BoardStore struct {
	other int
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

	out, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)
	for _, expected := range []string{
		"// @restream.partials",
		"Value string",
		"storeData *restream.StoreData[BoardStoreState, *BoardStoreState, *BoardStoreStatePartial]",
		"other",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("rewritten source missing expected %q:\n%s", expected, got)
		}
	}

	generated, err := os.ReadFile(filepath.Join(serverDir, "boardstore_rs.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(generated), "type BoardStoreStatePartial struct") {
		t.Fatalf("generated source missing BoardStoreStatePartial:\n%s", string(generated))
	}
}

func TestStoreAnnotationFindsReferencedStateInAnotherPackage(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	projectDir := t.TempDir()
	storeImplsDir := filepath.Join(projectDir, "internal", "storeimpls")
	storeStatesDir := filepath.Join(projectDir, "internal", "storestates")
	for _, dir := range []string{storeImplsDir, storeStatesDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/crossstore

go 1.26.5

require github.com/boatkit-io/restream v0.0.0

replace github.com/boatkit-io/restream => `+repoRoot+`
`), 0644); err != nil {
		t.Fatal(err)
	}

	storeSourcePath := filepath.Join(storeImplsDir, "boardstore.go")
	if err := os.WriteFile(storeSourcePath, []byte(`package storeimpls

import (
	"example.com/crossstore/internal/storestates"
	"github.com/boatkit-io/restream/pkg/restream"
)

// @restream.store(BoardStore)
type BoardStore struct {
	storeData *restream.StoreData[storestates.BoardStoreState, *storestates.BoardStoreState, *storestates.BoardStoreStatePartial]
}
`), 0644); err != nil {
		t.Fatal(err)
	}

	stateSourcePath := filepath.Join(storeStatesDir, "boardstorestate.go")
	if err := os.WriteFile(stateSourcePath, []byte(`package storestates

type BoardStoreState struct {
	Value string
}
`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(storeStatesDir, "boardstorestate_rs.go"), []byte(restreamGeneratedFileBanner+`
package storestates

import (
	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/restream"
)

type BoardStoreStatePartial struct{}

func (*BoardStoreState) Serialize(*binarystreams.Writer, *restream.VarInfoStruct) error { return nil }
func (*BoardStoreState) Deserialize(*binarystreams.Reader, *restream.VarInfoStruct) error { return nil }
func (*BoardStoreStatePartial) Serialize(*binarystreams.Writer, *restream.VarInfoStruct) error { return nil }
func (*BoardStoreStatePartial) Deserialize(*binarystreams.Reader, *restream.VarInfoStruct) error { return nil }
func (*BoardStoreStatePartial) MergeOntoPartial(any) {}
func (*BoardStoreStatePartial) ApplyTo(any) [][]any { return nil }
`), 0644); err != nil {
		t.Fatal(err)
	}

	pt := NewProjTracking(projectDir, &restreamConfig{
		InputDirs: []string{"internal/storeimpls", "internal/storestates"},
	})
	if err := pt.parseProject(); err != nil {
		t.Fatal(err)
	}
	for _, ft := range pt.files {
		if err := ft.Run(); err != nil {
			t.Fatal(err)
		}
	}
	if err := pt.Run(); err != nil {
		t.Fatal(err)
	}

	storeOut, err := os.ReadFile(storeSourcePath)
	if err != nil {
		t.Fatal(err)
	}
	rewrittenStore := string(storeOut)
	for _, expected := range []string{
		`"example.com/crossstore/internal/storestates"`,
		"storeData *restream.StoreData[storestates.BoardStoreState, *storestates.BoardStoreState, *storestates.BoardStoreStatePartial]",
	} {
		if !strings.Contains(rewrittenStore, expected) {
			t.Fatalf("rewritten store source missing expected %q:\n%s", expected, rewrittenStore)
		}
	}
	if strings.Contains(rewrittenStore, "type BoardStoreState struct") {
		t.Fatalf("store implementation package should not get a duplicate state struct:\n%s", rewrittenStore)
	}

	stateOut, err := os.ReadFile(stateSourcePath)
	if err != nil {
		t.Fatal(err)
	}
	rewrittenState := string(stateOut)
	for _, expected := range []string{
		"// @restream.partials",
		"Value string",
		`restream:",fID=1"`,
	} {
		if !strings.Contains(rewrittenState, expected) {
			t.Fatalf("rewritten state source missing expected %q:\n%s", expected, rewrittenState)
		}
	}

	storeGenerated, err := os.ReadFile(filepath.Join(storeImplsDir, "boardstore_rs.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(storeGenerated), `const BoardStoreName = "BoardStore"`) {
		t.Fatalf("generated store source missing BoardStoreName:\n%s", string(storeGenerated))
	}
	relayGenerated, err := os.ReadFile(filepath.Join(storeImplsDir, "relaystores_rs.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(relayGenerated), "restream.NewRelayStore[storestates.BoardStoreState, *storestates.BoardStoreState, *storestates.BoardStoreStatePartial]") {
		t.Fatalf("generated relay store source missing cross-package state type:\n%s", string(relayGenerated))
	}

	stateGenerated, err := os.ReadFile(filepath.Join(storeStatesDir, "boardstorestate_rs.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stateGenerated), "type BoardStoreStatePartial struct") {
		t.Fatalf("generated state source missing BoardStoreStatePartial:\n%s", string(stateGenerated))
	}
}

func TestRelayStoreFactoryCanBeGeneratedToConfiguredPackage(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	projectDir := t.TempDir()
	storeImplsDir := filepath.Join(projectDir, "internal", "storeimpls")
	storeStatesDir := filepath.Join(projectDir, "internal", "storestates")
	relayStoresDir := filepath.Join(projectDir, "internal", "relaystores")
	for _, dir := range []string{storeImplsDir, storeStatesDir, relayStoresDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/relayconfig

go 1.26.5

require github.com/boatkit-io/restream v0.0.0

replace github.com/boatkit-io/restream => `+repoRoot+`
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(storeImplsDir, "boardstore.go"), []byte(`package storeimpls

import (
	"example.com/relayconfig/internal/storestates"
	"github.com/boatkit-io/restream/pkg/restream"
)

// @restream.store(BoardStore)
type BoardStore struct {
	storeData *restream.StoreData[storestates.BoardStoreState, *storestates.BoardStoreState, *storestates.BoardStoreStatePartial]
}
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(storeStatesDir, "boardstorestate.go"), []byte(`package storestates

type BoardStoreState struct {
	Value string
}
`), 0644); err != nil {
		t.Fatal(err)
	}

	pt := NewProjTracking(projectDir, &restreamConfig{
		InputDirs:        []string{"internal/storeimpls", "internal/storestates"},
		GoRelayStoresDir: "internal/relaystores",
	})
	if err := pt.parseProject(); err != nil {
		t.Fatal(err)
	}
	for _, ft := range pt.files {
		if err := ft.Run(); err != nil {
			t.Fatal(err)
		}
	}
	if err := pt.Run(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(storeImplsDir, "relaystores_rs.go")); !os.IsNotExist(err) {
		t.Fatalf("store implementation package relaystores_rs.go err = %v, want not exist", err)
	}

	relayOut, err := os.ReadFile(filepath.Join(relayStoresDir, "relaystores_rs.go"))
	if err != nil {
		t.Fatal(err)
	}
	relayGenerated := string(relayOut)
	for _, expected := range []string{
		"package relaystores",
		`"example.com/relayconfig/internal/storestates"`,
		`BoardStoreName = "BoardStore"`,
		"restream.NewRelayStore[storestates.BoardStoreState, *storestates.BoardStoreState, *storestates.BoardStoreStatePartial]",
		"BoardStoreName",
	} {
		if !strings.Contains(relayGenerated, expected) {
			t.Fatalf("configured relay store output missing expected %q:\n%s", expected, relayGenerated)
		}
	}
}

func TestStoreAnnotationPreservesCorrectStoreDataFormatting(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	projectDir := t.TempDir()
	serverDir := filepath.Join(projectDir, "cmd", "server")
	if err := os.MkdirAll(serverDir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/storeannotationformat

go 1.26.5

require github.com/boatkit-io/restream v0.0.0

replace github.com/boatkit-io/restream => `+repoRoot+`
`), 0644); err != nil {
		t.Fatal(err)
	}

	sourcePath := filepath.Join(serverDir, "boardstore.go")
	if err := os.WriteFile(sourcePath, []byte(`package main

import "github.com/boatkit-io/restream/pkg/restream"

// @restream.partials
type BoardStoreState struct {
	Value string
}

// @restream.store(BoardStore)
type BoardStore struct {
	storeData *restream.StoreData[
		BoardStoreState,
		*BoardStoreState,
		*BoardStoreStatePartial,
	]
}
`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(serverDir, "boardstorestate_rs.go"), []byte(restreamGeneratedFileBanner+`
package main

import (
	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/restream"
)

type BoardStoreStatePartial struct{}

func (*BoardStoreState) Serialize(*binarystreams.Writer, *restream.VarInfoStruct) error { return nil }
func (*BoardStoreState) Deserialize(*binarystreams.Reader, *restream.VarInfoStruct) error { return nil }
func (*BoardStoreStatePartial) Serialize(*binarystreams.Writer, *restream.VarInfoStruct) error { return nil }
func (*BoardStoreStatePartial) Deserialize(*binarystreams.Reader, *restream.VarInfoStruct) error { return nil }
func (*BoardStoreStatePartial) MergeOntoPartial(any) {}
func (*BoardStoreStatePartial) ApplyTo(any) [][]any { return nil }
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

	out, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)
	expected := "storeData *restream.StoreData[\n\t\tBoardStoreState,\n\t\t*BoardStoreState,\n\t\t*BoardStoreStatePartial,\n\t]"
	if !strings.Contains(got, expected) {
		t.Fatalf("rewritten source did not preserve multiline storeData formatting:\n%s", got)
	}
	if strings.Contains(got, "storeData *restream.StoreData[BoardStoreState, *BoardStoreState, *BoardStoreStatePartial]") {
		t.Fatalf("rewritten source collapsed multiline storeData formatting:\n%s", got)
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
			defs: "export class Model {\n    public static deserialized(r: BinaryReader) { return r; }\n    public static readonly fieldInfo: readonly FieldInfo[] = [];\n}\n",
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

func TestWriteTSFileOrdersTransitiveDependencies(t *testing.T) {
	projectDir := t.TempDir()
	pt := NewProjTracking(projectDir, &restreamConfig{
		TSDir: "web/src/restream",
	})

	if err := pt.writeTSFile("PackageModel.ts", []fdef{
		{
			name: "ModelA",
			defs: "export class ModelA {}\n",
			typ:  fdefTypeOther,
			deps: []string{"ModelB"},
		},
		{
			name: "ModelB",
			defs: "export class ModelB {}\n",
			typ:  fdefTypeOther,
			deps: []string{"ModelC"},
		},
		{
			name: "ModelC",
			defs: "export class ModelC {}\n",
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

	modelAIndex := strings.Index(got, "export class ModelA")
	modelBIndex := strings.Index(got, "export class ModelB")
	modelCIndex := strings.Index(got, "export class ModelC")
	if modelAIndex == -1 || modelBIndex == -1 || modelCIndex == -1 {
		t.Fatalf("generated TypeScript missing expected classes:\n%s", got)
	}
	if modelCIndex >= modelBIndex || modelBIndex >= modelAIndex {
		t.Fatalf("generated TypeScript order = C:%d B:%d A:%d, want C before B before A:\n%s",
			modelCIndex, modelBIndex, modelAIndex, got)
	}
}

func TestGenTSFieldInfoUsesPublicReadonlyMetadata(t *testing.T) {
	got := genTSFieldInfo([]*restream.FieldInfo{
		{
			Name:     "Name",
			FieldIdx: 0,
			FieldID:  7,
			VarInfo:  &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString},
		},
	})

	for _, expected := range []string{
		"public static readonly fieldInfo: readonly FieldInfo[] = [",
		"{name: \"Name\", fieldIdx: 0, fieldID: 7",
		"private static readonly _fieldMap: ReadonlyMap<number, FieldInfo> = new Map<number, FieldInfo>([",
		"[7, this.fieldInfo[0]],",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("generated TypeScript field info missing expected %q:\n%s", expected, got)
		}
	}

	for _, unexpected := range []string{
		"_fieldInfo",
		"private static fieldInfo",
	} {
		if strings.Contains(got, unexpected) {
			t.Fatalf("generated TypeScript field info contains unexpected %q:\n%s", unexpected, got)
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

func TestFieldedStructGitCompatibilityRejectsReusedPreviousFieldID(t *testing.T) {
	projectDir, serverDir := setupFieldCompatibilityGitProject(t)
	sourcePath := filepath.Join(serverDir, "model.go")

	if err := os.WriteFile(sourcePath, []byte(`package main

// @restream.fields
type Model struct {
	// MAXFIELD(2)
	Name    string `+"`restream:\",fID=1\"`"+`
	Enabled bool   `+"`restream:\",fID=2\"`"+`
}
`), 0644); err != nil {
		t.Fatal(err)
	}

	err := runFieldCompatibilityCodegenProject(projectDir)
	if err == nil {
		t.Fatal("expected reused previous field ID to fail")
	}
	if !strings.Contains(err.Error(), "added field Enabled with fID=2") {
		t.Fatalf("unexpected error for reused field ID: %v", err)
	}
}

func TestFieldedStructGitCompatibilityAllowsNewFieldAbovePreviousMax(t *testing.T) {
	projectDir, serverDir := setupFieldCompatibilityGitProject(t)
	sourcePath := filepath.Join(serverDir, "model.go")

	if err := os.WriteFile(sourcePath, []byte(`package main

// @restream.fields
type Model struct {
	// MAXFIELD(2)
	Name    string `+"`restream:\",fID=1\"`"+`
	Count   int    `+"`restream:\",fID=2\"`"+`
	Enabled bool   `+"`restream:\",fID=3\"`"+`
}
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := runFieldCompatibilityCodegenProject(projectDir); err != nil {
		t.Fatal(err)
	}

	sourceOut, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sourceOut), "// MAXFIELD(3)") {
		t.Fatalf("source MAXFIELD was not advanced for manually assigned fID:\n%s", string(sourceOut))
	}
}

func TestFieldedStructGitCompatibilityRejectsChangedExistingFieldID(t *testing.T) {
	projectDir, serverDir := setupFieldCompatibilityGitProject(t)
	sourcePath := filepath.Join(serverDir, "model.go")

	if err := os.WriteFile(sourcePath, []byte(`package main

// @restream.fields
type Model struct {
	// MAXFIELD(3)
	Name  string `+"`restream:\",fID=3\"`"+`
	Count int    `+"`restream:\",fID=2\"`"+`
}
`), 0644); err != nil {
		t.Fatal(err)
	}

	err := runFieldCompatibilityCodegenProject(projectDir)
	if err == nil {
		t.Fatal("expected changed existing field ID to fail")
	}
	if !strings.Contains(err.Error(), "changed field Name fID from 1 to 3") {
		t.Fatalf("unexpected error for changed field ID: %v", err)
	}
}

func setupFieldCompatibilityGitProject(t *testing.T) (string, string) {
	t.Helper()
	requireGit(t)

	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	projectDir := t.TempDir()
	serverDir := filepath.Join(projectDir, "cmd", "server")
	if err := os.MkdirAll(serverDir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/fieldcompat

go 1.26.5

require github.com/boatkit-io/restream v0.0.0

replace github.com/boatkit-io/restream => `+repoRoot+`
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(serverDir, "model.go"), []byte(`package main

// @restream.fields
type Model struct {
	// MAXFIELD(2)
	Name  string `+"`restream:\",fID=1\"`"+`
	Count int    `+"`restream:\",fID=2\"`"+`
}
`), 0644); err != nil {
		t.Fatal(err)
	}

	runGit(t, projectDir, "init")
	runGit(t, projectDir, "add", ".")
	runGit(t, projectDir, "-c", "user.name=Codegen Test", "-c", "user.email=codegen@example.com", "commit", "-m", "baseline")

	return projectDir, serverDir
}

func requireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	fullArgs := append([]string{"-C", dir}, args...)
	cmd := exec.Command("git", fullArgs...) //nolint:gosec
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, string(out))
	}
}

func runFieldCompatibilityCodegenProject(projectDir string) error {
	pt := NewProjTracking(projectDir, &restreamConfig{
		InputDirs: []string{"cmd/server"},
	})
	if err := pt.parseProject(); err != nil {
		return err
	}
	for _, ft := range pt.files {
		if err := ft.Run(); err != nil {
			return err
		}
	}
	return nil
}
