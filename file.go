package main

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
)

type renameFile struct {
	*os.File
	filename string
}

func (f *renameFile) Close() error {
	if err := f.File.Close(); err != nil {
		return err
	}
	return os.Rename(f.File.Name(), f.filename)
}

func create(filename string) (io.WriteCloser, error) {
	tmpFilename := fmt.Sprintf("%s.%d", filename, rand.Int63n(math.MaxInt64))

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
