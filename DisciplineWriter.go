package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/kneu-messenger-pigeon/events"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"regexp"
	"strings"
)

type DisciplineWriter struct {
	redis redis.UniversalClient
}

func (writer *DisciplineWriter) setRedis(redis redis.UniversalClient) {
	writer.redis = redis
}

func (writer *DisciplineWriter) getExpectedMessageKey() string {
	return events.DisciplineEventName
}

func (writer *DisciplineWriter) getExpectedEventType() any {
	return &events.DisciplineEvent{}
}

func (writer *DisciplineWriter) write(e interface{}) error {
	event := e.(*events.DisciplineEvent)

	key := fmt.Sprintf("%d:discipline:%d", event.Year, event.Id)
	if writer.redis.HGet(context.Background(), key, "origName").Val() != event.Name {
		return writer.redis.HSet(
			context.Background(), key,
			"name", clearDisciplineName(event.Name),
			"origName", event.Name,
		).Err()
	}

	return nil
}

var regexps = [6]*regexp.Regexp{
	/**
	 * Remove starting with: "Тренінг-курс(Створення власного ІТ-бізнесу)", "Тренінг-курс `Управління командами`"
	 * https://regex101.com/r/j8BjSd/1
	 */
	regexp.MustCompile(`(?i)^\s*Тренінг-курс\s*\(?`),

	/**
	 * Remove Faculty shortnames: Маркетинг в агробізнесі, 5 сем., Фт маркет.
	 * https://regex101.com/r/9RHG5U/1
	 */
	regexp.MustCompile(`,\s*Фт\.?\s+\p{L}{3,10}\s*($|,)`),

	/**
	 * Remove Faculty abbreviation: Контролінг, 6 сем., ФЕУ; Бюджетування на підприємстві, 6 сем., ФЕУ
	 * https://regex101.com/r/eLjT6G/1
	 */
	regexp.MustCompile(`,\s*[А-ЯIЇ]{2,4}\s*($|,)`),

	/**
	 * remove ending with: ", Юр. Інст."; ", Фін.", ", Марк.", ", Інф. Інст."
	 * https://regex101.com/r/Q1Uq1f/1
	 */
	regexp.MustCompile(`(?i),(\s*\p{L}{2,5}\.){1,2}\s*$`),

	/**
	 * Remove ending with: ", 3 сем.", ", 4 сем.", ", 5 сем., Юрінст", ", 5 сем., Інфінст"
	 * https://regex101.com/r/kWUKoH/2
	 */
	regexp.MustCompile(`(,\s*)?[1-9-]{1,3}\s+сем\.?\s*(,\s*\p{L}{2,7})?$`),

	/**
	 * Remove parentheses: " (Туристичне країнознавство)", " (залік)", " (англомовна)"
	 */
	regexp.MustCompile(`\s*\([^\)]*\)`),
}

var removeDuplicateSpaces = regexp.MustCompile(`\s+`)

/**
 * Normalize apostrophe: "Компʼютерна математика"
 * https://regex101.com/r/xtA6HI/1
 */
var replaceApostrophe = regexp.MustCompile("(\\p{L})[`’'ʼ](\\p{L})")

var ukrainianToUpper = cases.Upper(language.Ukrainian)

var replacers = [8]*strings.Replacer{
	strings.NewReplacer("`", ""),
	strings.NewReplacer("\\", ""),
	strings.NewReplacer("1 С:", "1С:"),
	strings.NewReplacer("іноз мова", "іноземна мова"),
	strings.NewReplacer("_", " "),
	strings.NewReplacer("+", " "),
	strings.NewReplacer("~", " "),
	strings.NewReplacer("*", " "),
}

func clearDisciplineName(name string) string {
	name = replaceApostrophe.ReplaceAllString(name, "$1\\ʼ$2")

	for _, replacer := range replacers {
		name = replacer.Replace(name)
	}

	for i := 0; i < len(regexps); i++ {
		name = regexps[i].ReplaceAllString(name, "")
	}

	name = strings.TrimSpace(name)
	name = strings.TrimRight(name, "_-`.123  #&$/»)")
	name = strings.TrimLeft(name, "_-`.  #&$«(")
	name = removeDuplicateSpaces.ReplaceAllString(name, " ")

	if len(name) != 0 {
		name = ukrainianToUpper.String(name[:1]) + name[1:]
	}

	return name
}
