package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

func main() {
	data, err := ioutil.ReadFile("./schema.json")
	if err != nil {
		log.Fatal(err)
	}

	var schema []TableSpec
	if err := json.Unmarshal(data, &schema); err != nil {
		log.Fatal(err)
	}

	if err := Validate(FileSystemRepo{"."}, schema); err != nil {
		log.Fatal("Validation error:", err)
	}
}
