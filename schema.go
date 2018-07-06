package main

import (
	"fmt"
	"strconv"
	"time"
)

type DataType interface {
	ValidateDataType(value string) error
}

type DataTypeBool struct{}

func (dtb DataTypeBool) ValidateDataType(value string) error {
	switch value {
	case "true":
		return nil
	case "false":
		return nil
	default:
		return fmt.Errorf("Illegal value for type 'bool': '%s'", value)
	}
}

type DataTypeInt struct{}

func (dti DataTypeInt) ValidateDataType(value string) error {
	if _, err := strconv.Atoi(value); err != nil {
		return fmt.Errorf("Illegal value for type 'int': '%s'", value)
	}
	return nil
}

type DataTypeString struct{}

func (dti DataTypeString) ValidateDataType(value string) error { return nil }

type DataTypeDate struct{ Format string }

func (dtd DataTypeDate) ValidateDataType(value string) error {
	if _, err := time.Parse(dtd.Format, value); err != nil {
		return fmt.Errorf(
			"Illegal value for type 'date(\"%s\")': '%s'",
			dtd.Format,
			value,
		)
	}
	return nil
}

type ColumnRef struct {
	Table  string
	Column string
}

type Column struct {
	Name       string
	Type       DataType
	Unique     bool
	NotNull    bool
	PrimaryKey bool
	References *ColumnRef // optional
}

type Table struct {
	Name    string
	Columns []Column
}
