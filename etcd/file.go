package main

import (
	"fmt"
	"io"
	"math"
	"os"
)

type renameFile struct {
	*os.File
	filename string
}

func (f *renameFile) Close() error {
	f.File.Sync()

	if err := f.File.Close(); err != nil {
		return err
	}
	return os.Rename(f.File.Name(), f.filename)
}

func create(filename string) (io.WriteCloser, error) {
	tmpFilename := filename + ".tmp"

	f, err := os.Create(tmpFilename)
	if err != nil {
		return nil, err
	}

	rf := &renameFile{
		File:     f,
		filename: filename,
	}
	return rf, nil
}
