package main

import (
	"bytes"
	"errors"
	"fmt"
	victoriaMetricsInit "github.com/kneu-messenger-pigeon/victoria-metrics-init"
	"github.com/stretchr/testify/assert"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRunApp(t *testing.T) {
	t.Run("Run with mock config", func(t *testing.T) {
		// Create and start mock Kafka server
		mockServer := NewMockKafkaServer(t)
		defer mockServer.Close()

		_ = os.Setenv("KAFKA_HOST", MOCK_KAFKA_SERVER_ADDR)
		_ = os.Setenv("REDIS_DSN", expectedConfig.redisDsn)
		_ = os.Setenv("KAFKA_TIMEOUT", strconv.Itoa(int(expectedConfig.kafkaTimeout.Seconds())))

		var out bytes.Buffer

		running := true
		var appErr error
		go func() {
			appErr = runApp(&out)
			running = false
		}()

		runtime.Gosched()

		expectedStartedMessageCount := ConnectorPoolSize + 2
		maxEndTime := time.Now().Add(time.Second * 30)

		ticker := time.NewTicker(time.Second)
		for _ = range ticker.C {
			if !running {
				break
			}

			if time.Now().After(maxEndTime) {
				break
			}

			if strings.Count(out.String(), "connector started") >= expectedStartedMessageCount {
				break
			}

			if strings.Contains(out.String(), "error") || strings.Contains(out.String(), "fail") || strings.Contains(out.String(), "fatal") {
				fmt.Println("found error")
				break
			}
		}
		ticker.Stop()

		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
		runtime.Gosched()
		time.Sleep(time.Second)

		assert.NoError(t, appErr)
		runtime.Gosched()

		outputString := out.String()
		fmt.Println(outputString)

		assert.Contains(t, outputString, "*main.KafkaToRedisMetaEventsConnector connector started")
		assert.Contains(t, outputString, "*main.LessonWriter connector started")
		assert.Contains(t, outputString, "*main.DisciplineWriter connector started")
		assert.Contains(t, outputString, "*main.ScoreWriter connector started")
		assert.Equal(t, 2, strings.Count(outputString, "*main.LessonWriter connector started"))
		assert.Equal(t, 2, strings.Count(outputString, "*main.ScoreWriter connector started"))
		assert.False(t, running)

		assert.Equal(t, "storage-writer", victoriaMetricsInit.LastInstance)
	})

	t.Run("Run with wrong env file", func(t *testing.T) {
		previousWd, err := os.Getwd()
		assert.NoErrorf(t, err, "Failed to get working dir: %s", err)
		tmpDir := os.TempDir() + "/secondary-db-watcher-run-dir"
		tmpEnvFilepath := tmpDir + "/.env"

		defer func() {
			_ = os.Chdir(previousWd)
			_ = os.Remove(tmpEnvFilepath)
			_ = os.Remove(tmpDir)
		}()

		if _, err := os.Stat(tmpDir); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(tmpDir, os.ModePerm)
			assert.NoErrorf(t, err, "Failed to create tmp dir %s: %s", tmpDir, err)
		}
		if _, err := os.Stat(tmpEnvFilepath); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(tmpEnvFilepath, os.ModePerm)
			assert.NoErrorf(t, err, "Failed to create tmp  %s/.env: %s", tmpDir, err)
		}

		err = os.Chdir(tmpDir)
		assert.NoErrorf(t, err, "Failed to change working dir: %s", err)

		var out bytes.Buffer
		err = runApp(&out)
		assert.Error(t, err, "Expected for error")
		assert.Containsf(
			t, err.Error(), "Error loading .env file",
			"Expected for Load config error, got: %s", err,
		)
	})

	t.Run("Run with wrong redis driver", func(t *testing.T) {
		_ = os.Setenv("REDIS_DSN", "//")
		defer os.Unsetenv("REDIS_DSN")

		var out bytes.Buffer
		err := runApp(&out)

		expectedError := errors.New("redis: invalid URL scheme: ")

		assert.Error(t, err, "Expected for error")
		assert.Equal(t, expectedError, err, "Expected for another error, got %s", err)
	})
}

func TestHandleExitError(t *testing.T) {
	t.Run("Handle exit error", func(t *testing.T) {
		var actualExitCode int
		var out bytes.Buffer

		testCases := map[error]int{
			errors.New("dummy error"): ExitCodeMainError,
			nil:                       0,
		}

		for err, expectedCode := range testCases {
			out.Reset()
			actualExitCode = handleExitError(&out, err)

			assert.Equalf(
				t, expectedCode, actualExitCode,
				"Expect handleExitError(%v) = %d, actual: %d",
				err, expectedCode, actualExitCode,
			)
			if err == nil {
				assert.Empty(t, out.String(), "Error is not empty")
			} else {
				assert.Contains(t, out.String(), err.Error(), "error output hasn't error description")
			}
		}
	})
}
