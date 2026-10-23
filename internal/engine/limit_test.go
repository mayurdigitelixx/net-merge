package engine

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"net-merge/internal/nics"
)

func TestBucketPacesAfterTheBurst(t *testing.T) {
	b := newBucket(100_000) // 0.1 MB/s
	start := time.Now()
	b.take(limitBurst) // a quiet bucket starts full, so the first chunk is free
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("the initial burst waited %s", d)
	}
	start = time.Now()
	b.take(limitChunk)
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Fatalf("16 KB at 0.1 MB/s took %s, want about 164 ms", d)
	}
}

func TestThrottleWithoutLimitIsFree(t *testing.T) {
	sl := &Slot{}
	start := time.Now()
	for i := 0; i < 100; i++ {
		sl.throttle(true, 64<<10)
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("throttling an unlimited adapter took %s", d)
	}
}

func TestThrottleKeepsDirectionsSeparate(t *testing.T) {
	sl := &Slot{}
	sl.setLimit(0.05) // 50 KB/s
	sl.throttle(false, limitBurst)
	start := time.Now()
	sl.throttle(true, limitChunk) // the download bucket is untouched
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("download waited %s behind upload traffic", d)
	}
}

func TestSmallDatagramsAreNotPaced(t *testing.T) {
	sl := &Slot{}
	sl.setLimit(0.05)
	sl.throttle(false, limitBurst)
	start := time.Now()
	sl.throttlePacket(false, 40) // a DNS query
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("a 40-byte datagram waited %s behind a bulk flow", d)
	}
}

func TestPumpHonoursLimit(t *testing.T) {
	sl := &Slot{}
	sl.setLimit(0.1) // 100 KB/s
	srcR, srcW := net.Pipe()
	dstR, dstW := net.Pipe()
	defer srcR.Close()
	defer srcW.Close()
	defer dstR.Close()
	defer dstW.Close()

	var counted atomic.Int64
	go pump(dstW, srcR, &counted, sl, false)
	go func() {
		srcW.Write(make([]byte, 2*limitChunk))
		srcW.Close()
	}()

	start := time.Now()
	got := make([]byte, 2*limitChunk)
	if _, err := io.ReadFull(dstR, got); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Fatalf("32 KB at 0.1 MB/s took %s, want about 164 ms", d)
	}
	if n := counted.Load(); n != int64(len(got)) {
		t.Fatalf("counted %d bytes, want %d", n, len(got))
	}
}

func TestLimitsFollowAdapters(t *testing.T) {
	s := NewScheduler([]nics.Nic{nic("A", "10.0.0.1", 1)})
	s.SetLimit("A", 5)
	s.SetLimit("B", 10) // not seen yet: saved for when it appears

	limitOf := func(name string) float64 {
		for _, st := range s.Snapshot() {
			if st.Name == name {
				return st.LimitMBps
			}
		}
		t.Fatalf("adapter %q not in the snapshot", name)
		return 0
	}
	if got := limitOf("A"); got != 5 {
		t.Fatalf("A has limit %v, want 5", got)
	}

	s.Update([]nics.Nic{nic("A", "10.0.0.1", 1), nic("B", "10.0.0.2", 1)})
	if got := limitOf("B"); got != 10 {
		t.Fatalf("B came back with limit %v, want 10", got)
	}
	if limits := s.Limits(); len(limits) != 2 || limits["A"] != 5 || limits["B"] != 10 {
		t.Fatalf("Limits() = %v, want A=5 B=10", limits)
	}

	s.SetLimits(map[string]float64{"C": 20})
	if got := limitOf("A"); got != 0 {
		t.Fatalf("A still has limit %v after SetLimits replaced the map", got)
	}
	if limits := s.Limits(); len(limits) != 1 || limits["C"] != 20 {
		t.Fatalf("Limits() = %v, want only C=20", limits)
	}
	s.SetLimit("C", 0)
	if limits := s.Limits(); len(limits) != 0 {
		t.Fatalf("Limits() = %v after clearing C, want empty", limits)
	}
}
