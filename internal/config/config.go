package config

import (
	"flag"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Cache  CacheConfig  `yaml:"cache"`
	Engine EngineConfig `yaml:"engine"`
}

type CacheConfig struct {
	Limit uint64 `yaml:"limit"`
}

type EngineConfig struct {
	Type     string          `yaml:"type"`
	Network  TCPServerConfig `yaml:"network"`
	Logging  LoggingConfig   `yaml:"logging"`
	Snapshot SnapshotConfig  `yaml:"snapshot"`
	WAl      WALConfig       `yaml:"wal"`
}

type WALConfig struct {
	FlushingBatchSize    int64         `yaml:"flushing_batch_size"`
	FlushingBatchTimeout time.Duration `yaml:"flushing_batch_timeout"`
	MaxSegmentSize       int64         `yaml:"max_segment_size"`
	DataDir              string        `yaml:"data_directory"`
}

type SaveRule struct {
	Seconds time.Duration `yaml:"seconds"`
	Changes int           `yaml:"changes"`
}
type SnapshotConfig struct {
	Save       []SaveRule `yaml:"save"`
	DBFileName string     `yaml:"dbfilename"`
	DataDir    string     `yaml:"data_directory"`
}

type TCPServerConfig struct {
	Address        string        `yaml:"address"`
	MaxConnections int           `yaml:"max_connections"`
	MaxMessageSize int           `yaml:"max_message_size"`
	IdleTimeout    time.Duration `yaml:"idle_timeout"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Output string `yaml:"output"`
}

func MustLoad() *Config {
	path := fetchConfigPath()
	if path == "" {
		panic("config path is empty")
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		panic("config path does not exists: " + path)
	}
	var cfg Config

	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		panic("failed to read config: " + err.Error())
	}
	return &cfg
}

func fetchConfigPath() string {
	var res string

	flag.StringVar(&res, "config", "./config.yaml", "path to config file")
	flag.Parse()

	if res == "" {
		res = os.Getenv("CONFIG_PATH")
	}
	return res
}
