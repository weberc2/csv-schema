package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var metaschema = []Table{
	Table{
		Name: "schema.csv",
		Columns: []Column{
			Column{
				Name:    "table",
				NotNull: true,
				Type:    DataTypeString{},
			},
			Column{
				Name:    "column",
				NotNull: true,
				Type:    DataTypeString{},
			},
			Column{
				Name:    "not_null",
				NotNull: true,
				Type:    DataTypeBool{},
			},
			Column{
				Name:    "unique",
				NotNull: true,
				Type:    DataTypeBool{},
			},
			Column{
				Name:    "primary_key",
				NotNull: true,
				Type:    DataTypeBool{},
			},
			Column{
				Name:    "type",
				NotNull: true,
				Type:    DataTypeString{},
			},
			Column{
				Name:    "references_table",
				Type:    DataTypeString{},
				NotNull: false,
			},
			Column{
				Name:    "references_column",
				Type:    DataTypeString{},
				NotNull: false,
			},
		},
	},
}

func parseColumnType(typeString string) (DataType, error) {
	switch typeString {
	case "int":
		return DataTypeInt{}, nil
	case "bool":
		return DataTypeBool{}, nil
	case "string":
		return DataTypeString{}, nil
	default:
		if strings.HasPrefix(typeString, "date(") &&
			strings.HasSuffix(typeString, ")") {
			return DataTypeDate{
				Format: typeString[len("date(") : len(typeString)-len(")")],
			}, nil
		}
		return nil, fmt.Errorf("Couldn't match type: '%s'", typeString)
	}
}

func mustParseBool(b string) bool {
	switch b {
	case "true":
		return true
	case "false":
		return false
	default:
		panic("Failed to parse bool: '" + b + "'")
	}
}

func ParseSchema(directory string) ([]Table, error) {
	if err := Validate(FileSystemRepo{directory}, metaschema); err != nil {
		return nil, err
	}

	file, err := os.Open(filepath.Join(directory, "schema.csv"))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	// Because of validation, we know there is an initial column and that all
	// of the columns in the schema exist; however, we don't know their
	// positions, so we have to search for them.
	headers, rows := records[0], records[1:]
	var columnTable int
	var columnColumn int
	var columnNotNull int
	var columnUnique int
	var columnPrimaryKey int
	var columnReferencesTable int
	var columnReferencesColumn int
	var columnType int
	for i, header := range headers {
		switch header {
		case "table":
			columnTable = i
		case "column":
			columnColumn = i
		case "not_null":
			columnNotNull = i
		case "unique":
			columnUnique = i
		case "primary_key":
			columnPrimaryKey = i
		case "type":
			columnType = i
		case "references_table":
			columnReferencesTable = i
		case "references_column":
			columnReferencesColumn = i
		}
	}

	tables := map[string]Table{}
	for i, row := range rows {
		table, found := tables[row[columnTable]]
		if !found {
			table.Name = row[columnTable]
		}
		columnType, err := parseColumnType(row[columnType])
		if err != nil {
			return nil, fmt.Errorf(
				"Error parsing column type on line %d: %v",
				i+2, // 1 for the zero-indexing and 1 for the header line
				err,
			)
		}
		var columnRef *ColumnRef
		if row[columnReferencesTable] != "" &&
			row[columnReferencesColumn] != "" {
			columnRef = &ColumnRef{
				Table:  row[columnReferencesTable],
				Column: row[columnReferencesColumn],
			}
		}

		table.Columns = append(table.Columns, Column{
			Name:       row[columnColumn],
			NotNull:    mustParseBool(row[columnNotNull]),
			Unique:     mustParseBool(row[columnUnique]),
			PrimaryKey: mustParseBool(row[columnPrimaryKey]),
			Type:       columnType,
			References: columnRef,
		})
		tables[table.Name] = table
	}

	schema := make([]Table, 0, len(tables))
	for _, table := range tables {
		schema = append(schema, table)
	}
	return schema, nil
}
