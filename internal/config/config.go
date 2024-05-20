package config

import (
	"gopkg.in/yaml.v3"
	"os"
)

type (
	Config struct {
		Engine  EngineConfig  `yaml:"engine"`
		Network NetworkConfig `yaml:"network"`
		Logging LoggingConfig `yaml:"logging"`
	}

	EngineConfig struct {
		Type string `yaml:"type"`
	}

	NetworkConfig struct {
		Address        string `yaml:"address"`
		MaxConnections int    `yaml:"max_connections"`
	}

	LoggingConfig struct {
		Level  string `yaml:"level"`
		Output string `yaml:"output"`
	}
)

var defConfig = &Config{
	Engine: EngineConfig{
		Type: "in_memory",
	},
	Network: NetworkConfig{
		Address:        "0.0.0.0:3223",
		MaxConnections: 1,
	},
	Logging: LoggingConfig{
		Level:  "info",
		Output: "/log/output.log",
	},
}

func Get(path string) (*Config, error) {
	config := defConfig

	if path == "" {
		return config, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return config, nil
}
