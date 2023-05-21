package main

import (
	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDisciplineRepository_GetDiscipline(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		expectedName := "Гроші та лихварство"

		redisClient, redisMock := redismock.NewClientMock()
		redisMock.ExpectHGet("2023:discipline:123", "name").SetVal(expectedName)

		repository := DisciplineRepository{redisClient}

		discipline := repository.GetDiscipline(2023, 123)

		assert.Equal(t, expectedName, discipline.Name)
		assert.Equal(t, uint(123), discipline.Id)
	})
}
