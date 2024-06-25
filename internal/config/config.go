package config

import (
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

type (
	Config struct {
		Engine      EngineConfig      `yaml:"engine"`
		Replication ReplicationConfig `yaml:"replication"`
		Network     NetworkConfig     `yaml:"network"`
		Logging     LoggingConfig     `yaml:"logging"`
		WAL         WALConfig         `yaml:"wal"`
	}

	EngineConfig struct {
		Type string `yaml:"type"`
	}

	ReplicationConfig struct {
		Host         string        `yaml:"host"`
		Cluster      []string      `yaml:"cluster"`
		SyncInterval time.Duration `yaml:"sync_interval"`
	}

	NetworkConfig struct {
		Address        string `yaml:"address"`
		MaxConnections int    `yaml:"max_connections"`
	}

	LoggingConfig struct {
		Level  string `yaml:"level"`
		Output string `yaml:"output"`
	}

	WALConfig struct {
		FlushingBatchSize    int           `yaml:"flushing_batch_size"`
		FlushingBatchTimeout time.Duration `yaml:"flushing_batch_timeout"`
		MaxSegmentSize       string        `yaml:"max_segment_size"`
		DataDir              string        `yaml:"data_dir"`
	}
)

var defConfig = &Config{
	Engine: EngineConfig{
		Type: "in_memory",
	},
	Network: NetworkConfig{
		Address:        "0.0.0.0:3223",
		MaxConnections: 10,
	},
	Replication: ReplicationConfig{
		Host:         ":7777",
		Cluster:      []string{":7777", ":7778"},
		SyncInterval: 10 * time.Second,
	},
	Logging: LoggingConfig{
		Level:  "info",
		Output: "/log/output.log",
	},
	WAL: WALConfig{
		FlushingBatchSize:    100,
		FlushingBatchTimeout: time.Millisecond * 10,
		MaxSegmentSize:       "10MB",
		DataDir:              "data",
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
