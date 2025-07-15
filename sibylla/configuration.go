package sibylla

import (
	"log"
	"time"

	"gopkg.in/yaml.v3"
)

type Configuration struct {
	BarchartPath string `yaml:"barchartPath"`
	GobPath string `yaml:"gobPath"`
	CutoffDate SerializableDate `yaml:"cutoffDate"`
	OverwriteArchives bool `yaml:"overwriteArchives"`
	FontPath string `yaml:"fontPath"`
	FontName string `yaml:"fontName"`
	WebPath string `yaml:"webPath"`
	TempPath string `yaml:"tempPath"`
	IconPath string `yaml:"iconPath"`
}

type SerializableDate struct {
	time.Time
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

func (d *SerializableDate) UnmarshalYAML(value *yaml.Node) error {
	date, err := getDate(value.Value)
	if err != nil {
		return err
	}
	d.Time = date
	return nil
}
