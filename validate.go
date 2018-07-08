package main

import (
	"fmt"
	"strings"
)

type tableChecker struct {
	TableSpec
	pkeyCols []int
}

type schemaChecker map[TableName]tableChecker

func checkSchemaConsistency(schema []TableSpec) (schemaChecker, error) {
	// Schema checks
	// [x] Table names are unique
	// [x] Column names are unique
	// [x] Primary key columns exist
	// [x] Unique columns exist
	// [x] Foreign key local and remote columns exist
	// [x] Foreign key remote columns are primary keys
	// [x] Foreign key local and remote columns have matching types

	tableCheckers := map[TableName]tableChecker{}
	for _, table := range schema {
		// The table name is unique
		if _, found := tableCheckers[table.Name]; found {
			return nil, fmt.Errorf("Table exists: '%s'", table.Name)
		}
		tableCheckers[table.Name] = tableChecker{TableSpec: table}

		// Column names are unique
		columns := map[ColumnName]ColumnSpec{}
		for _, column := range table.Columns {
			if _, found := columns[column.Name]; found {
				return nil, fmt.Errorf(
					"Column exists: '%s'.'%s'",
					table.Name,
					column.Name,
				)
			}
			columns[column.Name] = column
		}

		// Primary key columns exist
		pkeyCols := make([]int, 0, 10)
	OUTER:
		for pkey := table.PrimaryKey; pkey != nil; pkey = pkey.Tail {
			for i, column := range table.Columns {
				if column.Name == pkey.Name {
					pkeyCols = append(pkeyCols, i)
					continue OUTER
				}
			}
			return nil, fmt.Errorf(
				"Primary key column not found: '%s'.'%s'",
				table.Name,
				pkey.Name,
			)
		}
		tableChecker := tableCheckers[table.Name]
		tableChecker.pkeyCols = pkeyCols
		tableCheckers[table.Name] = tableChecker

		// Unique columns exist
		for _, column := range table.UniqueColumns {
			for cs := &column; cs != nil; cs = cs.Tail {
				if _, found := columns[cs.Name]; !found {
					return nil, fmt.Errorf(
						"Column not found for unique constraint: '%s'.'%s'",
						table.Name,
						cs.Name,
					)
				}
			}
		}

		// Validate foreign keys
		for _, mapping := range table.ForeignKeys {
			if err := checkForeignKey(
				table,
				mapping,
				tableCheckers,
			); err != nil {
				return nil, err
			}
		}
	}

	return tableCheckers, nil
}

type Set map[string]Set

func (s Set) Exists(ss []string) bool {
	if len(ss) < 1 {
		return false
	}
	next, found := s[ss[0]]
	return found && next.Exists(ss[1:])
}

func (s Set) Put(ss []string) {
	if len(ss) < 1 {
		return
	}
	next, found := s[ss[0]]
	if !found {
		next = Set{}
		s[ss[0]] = next
	}
	next.Put(ss[1:])
}

func Validate(repo Repo, schema []TableSpec) error {
	tableCheckers, err := checkSchemaConsistency(schema)
	if err != nil {
		return err
	}

	// Data checks
	// [x] Columns exist in data
	// [x] Columns are ordered according to the schema
	// [x] Each row has the proper number of cells
	// [x] Primary key columns are unique
	// [ ] Primary key columns are null-free (does this hold for every column
	//	   in a composite key?)
	// [x] Column values are properly typed
	// [x] Not-null columns are null-free
	// [ ] Unique columns are unique
	// [ ] Foreign key values exist in remote columns

	for _, table := range schema {
		if err := repo.WithTable(string(table.Name), func(rows Rows) error {
			if len(table.Columns) != len(rows.Headers) {
				return fmt.Errorf(
					"Mismatched number of schema columns vs data columns "+
						"in table '%s': %d schema columns vs %d data columns",
					table.Name,
					len(table.Columns),
					len(rows.Headers),
				)
			}
			for i, column := range table.Columns {
				if rows.Headers[i] != string(column.Name) {
					return fmt.Errorf(
						"Column %d in table '%s' should be '%s', but got '%s'",
						i,
						table.Name,
						column.Name,
						rows.Headers[i],
					)
				}
			}

			var rowChecks []func(row []string) error
			rowChecks = append(
				rowChecks,
				func(row []string) error {
					if len(row) != len(table.Columns) {
						return fmt.Errorf(
							"Wrong number of cells; wanted %d, got %d",
							len(table.Columns),
							len(row),
						)
					}
					return nil
				},
				func(row []string) error {
					for i, column := range table.Columns {
						if err := ValidateDataType(
							column.Type,
							row[i],
						); err != nil {
							return fmt.Errorf(
								"Type error in column %d:",
								i,
								err,
							)
						}
					}
					return nil
				},
			)

			notNullColumns := make([]struct {
				name  ColumnName
				colID int
			}, 0, 10)
			for i, column := range table.Columns {
				if column.NotNull {
					notNullColumns = append(notNullColumns, struct {
						name  ColumnName
						colID int
					}{column.Name, i})
				}
			}
			if len(notNullColumns) > 0 {
				rowChecks = append(rowChecks, func(row []string) error {
					for _, col := range notNullColumns {
						if row[col.colID] == "" {
							return fmt.Errorf(
								"Null value found in not-null column '%s'",
								col.name,
							)
						}
					}
					return nil
				})
			}

			if table.PrimaryKey != nil {
				seen := Set{}
				pkeyCols := tableCheckers[table.Name].pkeyCols
				buf := make([]string, len(pkeyCols))
				rowChecks = append(rowChecks, func(row []string) error {
					for i, colID := range pkeyCols {
						buf[i] = row[colID]
					}
					if seen.Exists(buf) {
						return fmt.Errorf(
							"Duplicate value found for primary key column: "+
								"(%s)",
							strings.Join(buf, ", "),
						)
					}
					seen.Put(buf)
					return nil
				})
			}

			for i := 2; rows.Next(); i++ {
				for _, check := range rowChecks {
					if err := check(rows.CurrentRow); err != nil {
						return fmt.Errorf(
							"Error in table '%s' row %d: %v",
							table.Name,
							i,
							err,
						)
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

func findSpecs(column Column, table TableSpec) ([]ColumnSpec, error) {
	specs := make([]ColumnSpec, 0, column.Len())
OUTER:
	for c := &column; c != nil; c = c.Tail {
		for _, spec := range table.Columns {
			if c.Name == spec.Name {
				specs = append(specs, spec)
				continue OUTER
			}
		}
		return nil, fmt.Errorf(
			"Column '%s' not found on table '%s'",
			c.Name,
			table.Name,
		)
	}
	return specs, nil
}

func checkForeignKey(
	table TableSpec,
	fkm ForeignKeyMapping,
	tableCheckers map[TableName]tableChecker,
) error {
	if fkm.LocalColumn.Len() != fkm.ForeignColumn.Len() {
		return fmt.Errorf(
			"Mismatched foreign key tuple size: %s vs %s",
			fkm.LocalColumn,
			fkm.ForeignColumn,
		)
	}

	foreignTable, found := tableCheckers[fkm.ForeignTable]
	if !found {
		return fmt.Errorf(
			"Table not found for foreign key: '%s'",
			fkm.ForeignTable,
		)
	}

	// Make sure all foreign key columns exist, are primary keys, and have the
	// right types.
	if foreignTable.PrimaryKey == nil {
		return fmt.Errorf(
			"Foreign key column in table '%s' references a column in table "+
				"'%s', but '%s' has no primary key. Foreign keys must map to "+
				"primary key columns. Foreign key column must be the primary "+
				"key of the foreign table",
			table.Name,
			foreignTable.Name,
		)
	}

	// Make sure the foreign column is the primary key of the foreign table
	if !fkm.ForeignColumn.Equal(*foreignTable.PrimaryKey) {
		return fmt.Errorf(
			"Foreign key isn't the primary key on the foreign table: "+
				"(foreign key column: %s) "+
				"(foreign table's primary key column: %s)",
			fkm.ForeignColumn,
			foreignTable.PrimaryKey,
		)
	}

	// Find the local column specs for the local column
	localSpecs, err := findSpecs(fkm.LocalColumn, table)
	if err != nil {
		return fmt.Errorf(
			"%s but it is referenced in the table's foreign keys",
			err.Error(),
		)
	}

	// Grab the foreign column types
	foreignSpecs, err := findSpecs(fkm.ForeignColumn, foreignTable.TableSpec)
	if err != nil {
		return fmt.Errorf(
			"%s but it is referenced in a foreign key in table '%s'",
			err.Error(),
			table.Name,
		)
	}

	// Make sure the foreign table types match with the local table types
	if len(foreignSpecs) != len(localSpecs) {
		return fmt.Errorf(
			"Foreign key column count mismatch; column '%s'.%s references "+
				"'%s'.%s; local column had %d columns but foreign column has "+
				"%d columns",
			table.Name,
			fkm.LocalColumn,
			foreignTable.Name,
			fkm.ForeignColumn,
			len(localSpecs),
			len(foreignSpecs),
		)
	}
	for i, foreignSpec := range foreignSpecs {
		if foreignSpec.Type != localSpecs[i].Type {
			localTypeStrings := make([]string, len(localSpecs))
			foreignTypeStrings := make([]string, len(foreignSpecs))
			for i, spec := range localSpecs {
				localTypeStrings[i] = string(spec.Type)
				foreignTypeStrings[i] = string(foreignSpecs[i].Type)
			}
			return fmt.Errorf(
				"Foreign key column type mismatch; column '%s'.%s with type"+
					"%s references '%s'.%s with type %s",
				table.Name,
				fkm.LocalColumn,
				"("+strings.Join(localTypeStrings, ", ")+")",
				foreignTable.Name,
				fkm.ForeignColumn,
				"("+strings.Join(foreignTypeStrings, ", ")+")",
			)
		}
	}

	return nil
}
