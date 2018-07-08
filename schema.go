package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

type DataType string

const (
	DataTypeInt    DataType = "int"
	DataTypeString DataType = "string"
	DataTypeBool   DataType = "bool"
)

func ValidateDataType(dt DataType, value string) error {
	switch dt {
	case DataTypeInt:
		if _, err := strconv.Atoi(value); err != nil {
			return fmt.Errorf("Illegal value for type 'int': '%s'", value)
		}
		return nil
	case DataTypeBool:
		switch value {
		case "true", "false":
			return nil
		default:
			return fmt.Errorf("Illegal value for type 'bool': '%s'", value)
		}
	case DataTypeString:
		return nil
	default:
		panic(fmt.Sprintf("Invalid data type: '%s'", dt))
	}
}

type ColumnName string

type Column struct {
	Name ColumnName
	Tail *Column
}

func (c Column) MarshalJSON() ([]byte, error) {
	var toSlice func(c Column) []ColumnName
	toSlice = func(c Column) []ColumnName {
		if c.Tail == nil {
			return []ColumnName{c.Name}
		}
		return append([]ColumnName{c.Name}, toSlice(*c.Tail)...)
	}

	return json.Marshal(toSlice(c))
}

func (c *Column) UnmarshalJSON(data []byte) error {
	var columnNames []ColumnName
	if err := json.Unmarshal(data, &columnNames); err != nil {
		return err
	}
	if len(columnNames) < 1 {
		return fmt.Errorf("Composite columns must be at least one column long")
	}
	var fromSlice func(ColumnName, []ColumnName) Column
	fromSlice = func(head ColumnName, tail []ColumnName) Column {
		if len(tail) < 1 {
			return Column{Name: head}
		}
		t := fromSlice(tail[0], tail[1:])
		return Column{Name: head, Tail: &t}
	}
	*c = fromSlice(columnNames[0], columnNames[1:])
	return nil
}

func (c Column) Len() int {
	if c.Tail == nil {
		return 1
	}
	return 1 + c.Tail.Len()
}

func (c Column) String() string {
	if c.Tail == nil {
		return "'" + string(c.Name) + "'"
	}

	var helper func(*Column, *bytes.Buffer)
	helper = func(column *Column, buf *bytes.Buffer) {
		buf.WriteByte('\'')
		buf.WriteString(string(column.Name))
		buf.WriteByte('\'')
		if column.Tail == nil {
			return
		}
		buf.WriteString(", ")
		helper(column.Tail, buf)
	}

	buf := bytes.NewBuffer(make([]byte, 1024))
	buf.WriteByte('(')
	helper(&c, buf)
	buf.WriteByte(')')
	return buf.String()
}

func (c Column) Equal(other Column) bool {
	if c.Tail == nil {
		return c.Name == other.Name
	}

	return other.Tail != nil && c.Name == other.Name && c.Tail.Equal(*other.Tail)
}

type TableName string

type ColumnSpec struct {
	Name    ColumnName `json:"name"`
	Type    DataType   `json:"type"`
	NotNull bool       `json:"not_null"`
}

type ForeignKeyMapping struct {
	LocalColumn   Column    `json:"local_column"`
	ForeignTable  TableName `json:"foreign_table"`
	ForeignColumn Column    `json:"foreign_column"`
}

type TableSpec struct {
	Name          TableName           `json:"name"`
	PrimaryKey    *Column             `json:"primary_key"`
	UniqueColumns []Column            `json:"unique_columns"`
	ForeignKeys   []ForeignKeyMapping `json:"foreign_keys"`
	Columns       []ColumnSpec        `json:"columns"`
}
