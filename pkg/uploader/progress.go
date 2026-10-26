package uploader

import (
	"io"
	"time"
)

// ProgressReader wraps an io.Reader and reports progress periodically.
type ProgressReader struct {
	Reader     io.Reader
	BytesRead  int64
	LastUpdate time.Time
	UpdateFreq time.Duration
	Reporter   func(bytesRead int64)
}

func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.Reader.Read(p)
	if n > 0 {
		pr.BytesRead += int64(n)
		pr.report(false)
	}
	if err == io.EOF {
		pr.report(true) // Force update on completion
	}
	return n, err
}

func (pr *ProgressReader) report(force bool) {
	if pr.Reporter != nil && (force || time.Since(pr.LastUpdate) >= pr.UpdateFreq) {
		pr.LastUpdate = time.Now()
		pr.Reporter(pr.BytesRead)
	}
}
