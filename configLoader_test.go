package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
	"time"
)

var expectedConfig = Config{
	redisDsn:      "REDIS:6379",
	kafkaHost:     "KAFKA:9999",
	kafkaTimeout:  time.Second * 10,
	kafkaAttempts: 0,
}

func TestLoadConfigFromEnvVars(t *testing.T) {
	t.Run("FromEnvVars", func(t *testing.T) {
		_ = os.Setenv("REDIS_DSN", expectedConfig.redisDsn)
		_ = os.Setenv("KAFKA_HOST", expectedConfig.kafkaHost)
		_ = os.Setenv("KAFKA_TIMEOUT", strconv.Itoa(int(expectedConfig.kafkaTimeout.Seconds())))

		config, err := loadConfig("")

		assert.NoErrorf(t, err, "got unexpected error %s", err)
		assertConfig(t, expectedConfig, config)
		assert.Equalf(t, expectedConfig, config, "Expected for %v, actual: %v", expectedConfig, config)
	})

	t.Run("FromFile", func(t *testing.T) {
		var envFileContent string

		envFileContent += fmt.Sprintf("KAFKA_HOST=%s\n", expectedConfig.kafkaHost)
		envFileContent += fmt.Sprintf("REDIS_DSN=%s\n", expectedConfig.redisDsn)

		testEnvFilename := "TestLoadConfigFromFile.env"
		err := os.WriteFile(testEnvFilename, []byte(envFileContent), 0644)
		defer os.Remove(testEnvFilename)
		assert.NoErrorf(t, err, "got unexpected while write file %s error %s", testEnvFilename, err)

		config, err := loadConfig(testEnvFilename)

		assert.NoErrorf(t, err, "got unexpected error %s", err)
		assertConfig(t, expectedConfig, config)
		assert.Equalf(t, expectedConfig, config, "Expected for %v, actual: %v", expectedConfig, config)
	})

	t.Run("EmptyConfig", func(t *testing.T) {
		_ = os.Setenv("REDIS_DSN", "")
		_ = os.Setenv("KAFKA_HOST", "")
		_ = os.Setenv("KAFKA_TIMEOUT", "")

		config, err := loadConfig("")

		assert.Error(t, err, "loadConfig() should exit with error, actual error is nil")

		assert.Emptyf(
			t, config.redisDsn,
			"Expected for empty config.secondaryDekanatDbDSN, actual %s", config.redisDsn,
		)
		assert.Emptyf(
			t, config.kafkaHost,
			"Expected for empty config.kafkaHost, actual %s", config.kafkaHost,
		)

		os.Setenv("REDIS_DSN", "dummy-not-empty")
		config, err = loadConfig("")

		assert.Error(t, err, "loadConfig() should exit with error, actual error is nil")
		assert.Equalf(
			t, "empty KAFKA_HOST", err.Error(),
			"Expected for error with empty SECONDARY_DEKANAT_DB_DSN, actual: %s", err.Error(),
		)
		assert.Emptyf(
			t, config.kafkaHost,
			"Expected for empty config.kafkaHost, actual %s", config.kafkaHost,
		)
	})

	t.Run("NotExistConfigFile", func(t *testing.T) {
		os.Setenv("REDIS_DSN", "")
		os.Setenv("KAFKA_HOST", "")

		config, err := loadConfig("not-exists.env")

		assert.Error(t, err, "loadConfig() should exit with error, actual error is nil")
		assert.Equalf(
			t, "Error loading not-exists.env file: open not-exists.env: no such file or directory", err.Error(),
			"Expected for not exist file error, actual: %s", err.Error(),
		)
		assert.Emptyf(
			t, config.kafkaHost,
			"Expected for empty config.kafkaHost, actual %s", config.kafkaHost,
		)
	})
}

func assertConfig(t *testing.T, expected Config, actual Config) {
	assert.Equal(t, expected.kafkaHost, actual.kafkaHost)
	assert.Equal(t, expected.redisDsn, actual.redisDsn)
}
