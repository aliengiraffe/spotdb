package helpers

import (
	"io"
	"log"
)

// This should be called from init() functions in test files
func SilenceLogOutput() {
	log.SetOutput(io.Discard)
}

// Reader defines the interface for mocking readers
type Reader interface {
	io.Reader
	io.Seeker
	io.Closer
	ReadAt(p []byte, off int64) (n int, err error)
}

// Writer defines the interface for mocking writers
type Writer interface {
	io.Writer
	io.Closer
}

// MockReader implements the Reader interface for testing
type MockReader struct {
	ReadFunc   func(p []byte) (n int, err error)
	CloseFunc  func() error
	SeekFunc   func(offset int64, whence int) (int64, error)
	ReadAtFunc func(p []byte, off int64) (n int, err error)
}

func (m *MockReader) Read(p []byte) (n int, err error) {
	if m.ReadFunc != nil {
		return m.ReadFunc(p)
	}
	return 0, nil
}

func (m *MockReader) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

func (m *MockReader) Seek(offset int64, whence int) (int64, error) {
	if m.SeekFunc != nil {
		return m.SeekFunc(offset, whence)
	}
	return 0, nil
}

func (m *MockReader) ReadAt(p []byte, off int64) (n int, err error) {
	if m.ReadAtFunc != nil {
		return m.ReadAtFunc(p, off)
	}
	return 0, nil
}

// MockWriter implements the Writer interface for testing
type MockWriter struct {
	WriteFunc func(p []byte) (n int, err error)
	CloseFunc func() error
}

func (m *MockWriter) Write(p []byte) (n int, err error) {
	if m.WriteFunc != nil {
		return m.WriteFunc(p)
	}
	return len(p), nil
}

func (m *MockWriter) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}
