package main

import (
	"fmt"
	"os"
	"path"

	"gopkg.in/yaml.v3"
)

// restreamConfig holds config for restream's codegen
type restreamConfig struct {
	InputDirs            []string   `yaml:"inputDirs"`
	TSDir                string     `yaml:"tsDir"`
	TSImports            []tsImport `yaml:"tsImports"`
	TSRuntimeImportMode  string     `yaml:"tsRuntimeImportMode"`
	TSRuntimeImportPath  string     `yaml:"tsRuntimeImportPath"`
	GoImports            []string   `yaml:"goImports"`
	AdditionalEnums      []string   `yaml:"additionalEnums"`
	BuildSerializers     []string   `yaml:"buildSerializers"`
	GoExtraFile          string     `yaml:"goExtraFile"`
	GoRelayStoresDir     string     `yaml:"goRelayStoresDir"`
	GoRelayStoresPackage string     `yaml:"goRelayStoresPackage"`
}

// tsImport is a struct for holding a list of typescripts imports for a given path
type tsImport struct {
	Imports     []string `yaml:"imports"`
	TypeImports []string `yaml:"typeImports"`
	ImportRoot  string   `yaml:"importRoot"`
	Path        string   `yaml:"path"`
}

// loadConfig loads the generator config from the restream.yaml file
func loadConfig(dir string) (*restreamConfig, error) {
	config := restreamConfig{}
	yf, err := os.ReadFile(path.Join(dir, "restream.yaml"))
	if err != nil {
		return nil, fmt.Errorf("error loading restream.yaml: %w", err)
	}
	if err := yaml.Unmarshal(yf, &config); err != nil {
		return nil, fmt.Errorf("error parsing restream.yaml: %w", err)
	}

	return &config, nil
}
