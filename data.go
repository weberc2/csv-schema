package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type Rows struct {
	Source     *csv.Reader
	Headers    []string
	CurrentRow []string
	Err        error
}

func (r *Rows) Next() bool {
	if r.Err != nil {
		return false
	}
	r.CurrentRow, r.Err = r.Source.Read()
	return r.Err == nil
}

func OpenTable(table *csv.Reader) Rows {
	rows := Rows{Source: table}
	rows.Headers, rows.Err = rows.Source.Read()
	return rows
}

type Repo interface {
	WithTable(table string, f func(r Rows) error) error
}

type FileSystemRepo struct {
	RootDirectory string
}

func (fsr FileSystemRepo) WithTable(table string, f func(r Rows) error) error {
	if strings.Contains(table, "..") {
		return fmt.Errorf("Illegal character in table identifier: '..'")
	}

	file, err := os.Open(filepath.Join(fsr.RootDirectory, table))
	if err != nil {
		return err
	}
	defer file.Close()

	rows := OpenTable(csv.NewReader(file))
	if rows.Err != nil {
		return rows.Err
	}

	if err := f(rows); err != nil {
		return err
	}

	if rows.Err != nil && rows.Err != io.EOF {
		return rows.Err
	}

	return nil
}
