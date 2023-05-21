package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/kneu-messenger-pigeon/events"
)

type DisciplineRepositoryInterface interface {
	GetDiscipline(year int, disciplineId uint) events.Discipline
}

type DisciplineRepository struct {
	redis redis.UniversalClient
}

func (repository *DisciplineRepository) GetDiscipline(year int, disciplineId uint) events.Discipline {
	return events.Discipline{
		Id: disciplineId,
		Name: repository.redis.HGet(
			context.Background(), fmt.Sprintf("%d:discipline:%d", year, disciplineId), "name",
		).Val(),
	}
}
