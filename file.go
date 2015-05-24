package main

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
)

type renamedFile struct {
	file     *os.File
	filename string
}

func (f *renamedFile) Write(b []byte) (int, error) {
	return f.file.Write(b)
}

func (f *renamedFile) Close() error {
	if err := f.file.Close(); err != nil {
		return err
	}
	return os.Rename(f.file.Name(), f.filename)
}

func create(filename string) (io.WriteCloser, error) {
	tmpFilename := fmt.Sprintf("%s.%d", filename, rand.Int63n(math.MaxInt64))

	f, err := os.Create(tmpFilename)
	if err != nil {
		return nil, err
	}

	rf := &renamedFile{
		file:     f,
		filename: filename,
	}
	return rf, nil
}
