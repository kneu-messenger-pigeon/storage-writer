package main

import "fmt"

func getDisciplineKey(year int, semester uint8, disciplineId uint) string {
	return fmt.Sprintf("%d:%d:lessons:%d", year, semester, disciplineId)
}

func getLessonKey(lessonId uint) string {
	return fmt.Sprintf("%d", lessonId)
}
