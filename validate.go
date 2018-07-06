package main

import (
	"fmt"
	"strings"
)

func notNullCheck(value string) error {
	if value == "" {
		return fmt.Errorf("Found null value in not-null column")
	}
	return nil
}

func makeUniqueCheck() func(string) error {
	seen := make(map[string]struct{})
	return func(value string) error {
		if _, found := seen[value]; found {
			return fmt.Errorf("Found duplicate value in unique column")
		}
		seen[value] = struct{}{}
		return nil
	}
}

func Validate(repo Repo, tables []Table) error {
	// Schema-only checks
	// ==================
	// [x] Table names are unique in a schema
	// [x] Column names are unique in a table
	// [x] Table names are valid (not-empty)
	// [x] Column names are valid (not-empty)
	// [x] Only one primary key column per table
	// [x] Foreign key constraints reference primary key columns

	// Schema+headers checks
	// =====================
	// [x] All columns exist in file

	// Schema+data checks
	// ==================
	// [x] Each row of the data has the correct number of columns
	// [x] Data for each column is correctly-typed
	// [x] Primary key column data is unique and null-free
	// [x] Unique column data is unique
	// [x] Not-null column data is null-free
	// [ ] Foreign key values exist in the foreign column

	// Make sure table names are unique and valid
	tableNames := map[string]struct{}{}
	for _, table := range tables {
		if table.Name == "" {
			return fmt.Errorf("Invalid table name: ''")
		}

		if _, found := tableNames[table.Name]; found {
			return fmt.Errorf("Table name exists: '%s'", table.Name)
		}
		tableNames[table.Name] = struct{}{}
	}

	// Make sure column names are unique within a table and each table has at
	// most one primary key column
	for _, table := range tables {
		columnNames := map[string]struct{}{}
		primaryKeyColumns := []string{}
		for _, column := range table.Columns {
			if column.Name == "" {
				return fmt.Errorf("Invalid column name: '%s'.''", table.Name)
			}
			if column.PrimaryKey {
				primaryKeyColumns = append(primaryKeyColumns, column.Name)
			}
			if column.References != nil {
				for _, foreignTable := range tables {
					if foreignTable.Name == column.References.Table {
						for _, foreignColumn := range foreignTable.Columns {
							if foreignColumn.Name == column.References.Column {
								goto DONE
							}
						}
						return fmt.Errorf(
							"Column '%s'.'%s' references '%s'.'%s'; "+
								"found table, but column is missing from "+
								"the table's schema",
							table.Name,
							column.Name,
							column.References.Table,
							column.References.Column,
						)
					}
				}
				return fmt.Errorf(
					"Column '%s'.'%s' references table '%s' but it is "+
						"missing from the schema",
					table.Name,
					column.Name,
					column.References.Table,
				)
			DONE:
			}

			if _, found := columnNames[column.Name]; found {
				return fmt.Errorf(
					"Column name exists: '%s'.'%s'",
					table.Name,
					column.Name,
				)
			}
			columnNames[column.Name] = struct{}{}
		}

		if len(primaryKeyColumns) > 1 {
			for i, name := range primaryKeyColumns {
				primaryKeyColumns[i] = "'" + name + "'"
			}
			return fmt.Errorf(
				"Multiple primary key columns in table '%s': %s",
				table.Name,
				strings.Join(primaryKeyColumns, ", "),
			)
		}
	}

	for _, table := range tables {
		if err := repo.WithTable(table.Name, func(rows Rows) error {
			// Make sure referenced columns exist in repo
			if len(table.Columns) != len(rows.Headers) {
				return fmt.Errorf(
					"Column number mismatch; wanted %d columns, found %d",
					len(table.Columns),
					len(rows.Headers),
				)
			}

			type _column struct {
				name        string
				valueChecks []func(string) error
			}

			// Check that all column names have corresponding headers.
			columnsByHeader := make([]_column, len(table.Columns))
		OUTER:
			for _, column := range table.Columns {
				for headerNumber, header := range rows.Headers {
					if column.Name == header {
						c := _column{
							name: column.Name,
							valueChecks: []func(string) error{
								column.Type.ValidateDataType,
							},
						}
						if column.PrimaryKey {
							c.valueChecks = append(
								c.valueChecks,
								makeUniqueCheck(),
								notNullCheck,
							)
						} else {
							if column.Unique {
								c.valueChecks = append(
									c.valueChecks,
									makeUniqueCheck(),
								)
							}
							if column.NotNull {
								c.valueChecks = append(
									c.valueChecks,
									notNullCheck,
								)
							}
						}
						columnsByHeader[headerNumber] = c
						continue OUTER
					}
				}
				// If we get here, then we didn't find a corresponding header
				// for the schema column; return an error
				return fmt.Errorf(
					"Column not found: '%s'.'%s'",
					table.Name,
					column.Name,
				)
			}

			// For each row in the data...
			for lineNumber := 1; rows.Next(); lineNumber++ {
				// ...make sure the row has the correct number of columns
				if len(rows.CurrentRow) != len(table.Columns) {
					return fmt.Errorf(
						"Column count mismatch at line %d: wanted %d "+
							"columns, found %d",
						lineNumber,
						len(table.Columns),
						len(rows.CurrentRow),
					)
				}

				// ...validate the row's values
				for headerNumber, value := range rows.CurrentRow {
					c := columnsByHeader[headerNumber]
					for _, check := range c.valueChecks {
						if err := check(value); err != nil {
							return fmt.Errorf(
								"'%s'.'%s' line %d: %v",
								table.Name,
								c.name,
								lineNumber,
								err,
							)
						}
					}
				}
			}

			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}
