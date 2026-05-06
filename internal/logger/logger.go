package logger

import (
	"in-memory-key-value-db/internal/config"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func SetupLogger(cfg *config.Config) (*zap.Logger, error) {
	level := parseLevel(cfg.Engine.Logging.Level)

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "ts"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encoder := zapcore.NewJSONEncoder(encoderCfg)

	writer, err := buildWriter(cfg.Engine.Logging.Output)
	if err != nil {
		return nil, err
	}

	core := zapcore.NewCore(encoder, writer, level)

	// logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	logger := zap.New(core, zap.AddCaller())
	return logger, nil
}

func parseLevel(level string) zapcore.Level {
	switch level {
	case "info":
		return zapcore.InfoLevel
	case "debug":
		return zapcore.DebugLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

func buildWriter(path string) (zapcore.WriteSyncer, error) {
	if path == "" || path == "stdout" {
		return zapcore.AddSync(os.Stdout), nil
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return zapcore.AddSync(file), nil
}
