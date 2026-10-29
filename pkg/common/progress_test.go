package common

import (
	"bytes"
	"testing"
	"time"
)

func TestProgressReader(t *testing.T) {
	data := []byte("1234567890")
	reader := bytes.NewReader(data)

	reports := 0
	var lastReported int64

	pr := &ProgressReader{
		Reader:     reader,
		UpdateFreq: 10 * time.Millisecond,
		Reporter: func(bytesRead int64) {
			reports++
			lastReported = bytesRead
		},
	}

	buf := make([]byte, 5)
	_, _ = pr.Read(buf) // Reads 5 bytes

	time.Sleep(15 * time.Millisecond) // Wait for throttle

	_, _ = pr.Read(buf) // Reads 5 bytes
	_, _ = pr.Read(buf) // EOF

	if reports == 0 {
		t.Fatalf("Expected reporter to be called")
	}
	if lastReported != 10 {
		t.Fatalf("Expected 10 bytes reported, got %d", lastReported)
	}
}
