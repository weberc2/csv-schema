package main

import (
	"log"
)

func main() {
	schema, err := ParseSchema(".")
	if err != nil {
		log.Fatal("Failed to parse schema:", err)
	}

	if err := Validate(FileSystemRepo{"."}, schema); err != nil {
		log.Fatal("Validation error:", err)
	}
}
