package main

import (
	"github.com/go-redis/redismock/v9"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriteDiscipline(t *testing.T) {
	t.Run("write discipline", func(t *testing.T) {
		event := events.DisciplineEvent{
			Year: 2045,
			Discipline: events.Discipline{
				Id:   200,
				Name: "Фінанси (модуль 1 Гроші та кредит, модуль 2 Фінанси)",
			},
		}

		redis, redisMock := redismock.NewClientMock()

		redisMock.ExpectHGet("2045:discipline:200", "origName").RedisNil()
		redisMock.ExpectHSet("2045:discipline:200", "name", "Фінанси", "origName", event.Name).SetVal(2)

		disciplineWriter := DisciplineWriter{}
		disciplineWriter.setRedis(redis)
		err := disciplineWriter.write(&event)

		assert.IsType(t, disciplineWriter.getExpectedEventType(), &event)
		assert.Equal(t, disciplineWriter.getExpectedMessageKey(), events.DisciplineEventName)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("discipline already exists", func(t *testing.T) {
		event := events.DisciplineEvent{
			Year: 2045,
			Discipline: events.Discipline{
				Id:   200,
				Name: "Фінанси (модуль 1 Гроші та кредит, модуль 2 Фінанси)",
			},
		}

		redis, redisMock := redismock.NewClientMock()

		redisMock.ExpectHGet("2045:discipline:200", "origName").SetVal(event.Name)

		disciplineWriter := DisciplineWriter{}
		disciplineWriter.setRedis(redis)
		err := disciplineWriter.write(&event)

		assert.IsType(t, disciplineWriter.getExpectedEventType(), &event)
		assert.Equal(t, disciplineWriter.getExpectedMessageKey(), events.DisciplineEventName)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())

	})
}

func TestClearDisciplineName(t *testing.T) {
	expectMap := map[string]string{
		"Фінанси (модуль 1 Гроші та кредит, модуль 2 Фінанси)":                         "Фінанси",
		"Тренінг-курс `Управління командами`":                                          "Управління командами",
		"Тренінг-курс(Створення власного ІТ-бізнесу)":                                  "Створення власного ІТ-бізнесу",
		"Тренінг-курс `Start-up`, 4 сем., Марк.":                                       "Start-up",
		"Тренінг-курс «Інформаційні технології»":                                       "Інформаційні технології",
		"Тренінг-курс «Smart-технології в бізнесі»":                                    "Smart-технології в бізнесі",
		"Іноземна мова (залік)":                                                        "Іноземна мова",
		"Іноземна мова 2":                                                              "Іноземна мова",
		"Основи філософських знань":                                                    "Основи філософських знань",
		"Проектний менеджмент (англомовна)":                                            "Проектний менеджмент",
		"Фізичне виховання (1 сем)":                                                    "Фізичне виховання",
		"Дискретна математика#":                                                        "Дискретна математика",
		"$Вища математика":                                                             "Вища математика",
		"Фінансова математика, 4 сем., Інф. Інст.":                                     "Фінансова математика",
		"Комп`ютерна математика":                                                       "Компʼютерна математика",
		"$Вища математика для економістів":                                             "Вища математика для економістів",
		"Географія туризму (Туристичне країнознавство)":                                "Географія туризму",
		"#Мікроекономіка":                                                              "Мікроекономіка",
		"#Регіональна економіка":                                                       "Регіональна економіка",
		"Українська мова як іноземна//":                                                "Українська мова як іноземна",
		"Операційні системи#":                                                          "Операційні системи",
		"Бухоблік з використанням 1С: Бухгалтерія":                                     "Бухоблік з використанням 1С: Бухгалтерія",
		"Капітал підприємства: формування та використання":                             "Капітал підприємства: формування та використання",
		"1 С: Бухгалтерія (Україна), 6 сем., Фін.":                                     "1С: Бухгалтерія",
		"Фінанси підприємсив: базовий курс":                                            "Фінанси підприємсив: базовий курс",
		"Демократія: від теорії до практики, 3 сем., Юр. Інст.":                        "Демократія: від теорії до практики",
		"Права людини: система та механізм забезпечення, 3 сем., Юр. Інст.":            "Права людини: система та механізм забезпечення",
		"Комплексні системи захисту інформації: проектування, впровадження, супровід":  "Комплексні системи захисту інформації: проектування, впровадження, супровід",
		"Економікс: мікро та макроекономічний аналіз":                                  "Економікс: мікро та макроекономічний аналіз",
		"Тренінг: Інформаційний аналіз ~фінансових процесів 1":                         "Тренінг: Інформаційний аналіз фінансових процесів",
		"Курсова робота з фінансів підприємств: базовий курс (викл. укр./англ. мовою)": "Курсова робота з фінансів підприємств: базовий курс",
		"Митне** право~, 5 сем., Юрінст":                                               "Митне право",
		"Етика у фінансах та інвестиціях (укр. мова),4 сем., Фін.":                     "Етика у фінансах та інвестиціях",
		"Геоінформаційні  системи, 5 сем., Інфінст":                                    "Геоінформаційні системи",
		"Інтернет-комунікації, 6 сем., Марк.":                                          "Інтернет-комунікації",
		"Друга іноз мова (нім мова), 5 сем.":                                           "Друга іноземна мова",
		"Тренінг-курс `Інформаційні технології в управлінні персоналом`, 7 сем., УП":   "Інформаційні технології в управлінні персоналом",
		"Фінанси підприємств, 5 сем., Екон. Упр.":                                      "Фінанси підприємств",
		"Фінанси_ \\+підприємств_":                                                     "Фінанси підприємств",
	}

	for name, expectedClearName := range expectMap {
		assert.Equal(t, expectedClearName, clearDisciplineName(name))
	}
}
