package sibylla

import (
	"log"

	"gopkg.in/yaml.v3"
)

type Configuration struct {
	BarchartPath string `yaml:"barchartPath"`
	GobPath string `yaml:"gobPath"`
	CutoffDate SerializableDate `yaml:"cutoffDate"`
	OverwriteArchives bool `yaml:"overwriteArchives"`
	QuantileTransform bool `yaml:"quantileTransform"`
	QuantileBufferSize int `yaml:"quantileBufferSize"`
	QuantileStride int `yaml:"quantileStride"`
	FontPath string `yaml:"fontPath"`
	FontName string `yaml:"fontName"`
	WebPath string `yaml:"webPath"`
	TempPath string `yaml:"tempPath"`
	IconPath string `yaml:"iconPath"`
	ProfilerAddress *string `yaml:"profilerAddress"`
}

const configurationPath = "configuration/configuration.yaml"
const assetsPath = "configuration/assets.yaml"

var loadedConfiguration bool
var configuration *Configuration
var assets *[]Asset

func loadConfiguration() {
	if loadedConfiguration {
		return
	}
	loadBaseConfiguration()
	loadAssets()
	loadedConfiguration = true
}

func loadBaseConfiguration() {
	if configuration != nil {
		panic("Base configuration had already been loaded")
	}
	yamlData := readFile(configurationPath)
	configuration = new(Configuration)
	err := yaml.Unmarshal(yamlData, configuration)
	if err != nil {
		log.Fatal("Failed to unmarshal YAML:", err)
	}
}

func loadAssets() {
	if assets != nil {
		panic("Assets had already been loaded")
	}
	yamlData := readFile(assetsPath)
	assets = new([]Asset)
	err := yaml.Unmarshal(yamlData, assets)
	if err != nil {
		log.Fatal("Failed to unmarshal YAML:", err)
	}
}