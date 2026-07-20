package metrics

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLoadCTIndex_DoesNotDeadlockWhenFileMissing(t *testing.T) {
	metrics := LogMetrics{metrics: make(CTMetrics), index: make(CTCertIndex), remoteSize: make(CTRemoteSize)}
	ctIndexPath := filepath.Join(t.TempDir(), "ct_index.json")

	done := make(chan struct{})
	go func() {
		metrics.LoadCTIndex(ctIndexPath)
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("LoadCTIndex appears to deadlock when index file is missing")
	}
}

func TestGetLag_UnknownRemoteSizeReturnsZero(t *testing.T) {
	m := LogMetrics{metrics: make(CTMetrics), index: make(CTCertIndex), remoteSize: make(CTRemoteSize)}

	if lag := m.GetLag("example.com/log"); lag != 0 {
		t.Fatalf("expected lag 0 for unobserved remote size, got %v", lag)
	}
}

func TestGetLag_ComputesFloatDifferenceWithoutUnderflow(t *testing.T) {
	m := LogMetrics{metrics: make(CTMetrics), index: make(CTCertIndex), remoteSize: make(CTRemoteSize)}

	url := "example.com/log"
	m.SetRemoteSize(url, 100)
	m.index[url] = 150

	lag := m.GetLag(url)
	if lag != -50 {
		t.Fatalf("expected lag -50, got %v (possible uint64 underflow)", lag)
	}

	m.index[url] = 40
	if lag := m.GetLag(url); lag != 60 {
		t.Fatalf("expected lag 60, got %v", lag)
	}
}

func TestSetRemoteSize_ThenGetRemoteSize(t *testing.T) {
	m := LogMetrics{metrics: make(CTMetrics), index: make(CTCertIndex), remoteSize: make(CTRemoteSize)}

	url := "example.com/log"

	if _, known := m.GetRemoteSize(url); known {
		t.Fatalf("expected remote size to be unknown before it is set")
	}

	m.SetRemoteSize(url, 42)

	size, known := m.GetRemoteSize(url)
	if !known {
		t.Fatalf("expected remote size to be known after SetRemoteSize")
	}

	if size != 42 {
		t.Fatalf("expected remote size 42, got %d", size)
	}
}
