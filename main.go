package main

import "log"

func main() {
	if err := Validate(
		FileSystemRepo{RootDirectory: "."},
		[]Table{
			Table{
				Name: "items.csv",
				Columns: []Column{
					Column{
						Name:       "id",
						Type:       DataTypeInt{},
						PrimaryKey: true,
					},
					Column{
						Name:    "name",
						Type:    DataTypeString{},
						NotNull: true,
					},
				},
			},
		},
	); err != nil {
		log.Fatal(err)
	}
}
