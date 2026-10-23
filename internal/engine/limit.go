package engine

import (
	"io"
	"math"
	"sync"
	"time"
)

// A speed limit is two token buckets per adapter, one per direction, shared by every flow and both
// address families: the cap is for the adapter as a whole, not per connection or per address.
const (
	// limitChunk is how many bytes a single wait covers. Smaller means smoother pacing but more
	// timer wake-ups; 16 KB is about 1.6 ms even at 10 MB/s.
	limitChunk = 16 << 10
	// limitBurst is how much may pass at once after an idle moment, and how many bytes a fresh
	// bucket starts with so the first request on a quiet link does not stall.
	limitBurst = 16 << 10
	// udpFree lets small datagrams through untouched: DNS, mDNS and calls stay responsive even
	// while a bulk QUIC download is being paced on the same adapter.
	udpFree = 512
)

// limitPair is the installed cap: nil means unlimited. It is swapped as a whole so the data path
// reads it without a lock, and changing a limit never blocks a flow.
type limitPair struct {
	in, out  *bucket
	mbPerSec float64
}

type bucket struct {
	mu     sync.Mutex
	rate   float64 // bytes/s
	burst  float64
	tokens float64
	last   time.Time
}

func newBucket(bytesPerSec float64) *bucket {
	return &bucket{rate: bytesPerSec, burst: limitBurst, tokens: limitBurst, last: time.Now()}
}

// take reserves n bytes and sleeps until they may pass. The reservation happens under the lock,
// so concurrent flows queue fairly instead of all waking at once; the sleep itself is outside it.
func (b *bucket) take(n int) {
	b.mu.Lock()
	now := time.Now()
	b.tokens = math.Min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	b.last = now
	var wait time.Duration
	if need := float64(n); need > b.tokens {
		seconds := (need - b.tokens) / b.rate
		b.tokens = 0
		b.last = now.Add(time.Duration(seconds * float64(time.Second)))
		wait = time.Duration(seconds * float64(time.Second))
	} else {
		b.tokens -= need
	}
	b.mu.Unlock()
	if wait > 0 {
		time.Sleep(wait)
	}
}

// setLimit installs the adapter's cap in MByte/s, or clears it with 0.
func (sl *Slot) setLimit(mbPerSec float64) {
	if mbPerSec <= 0 {
		sl.limit.Store(nil)
		return
	}
	rate := mbPerSec * 1e6 // MByte/s to bytes/s
	sl.limit.Store(&limitPair{in: newBucket(rate), out: newBucket(rate), mbPerSec: mbPerSec})
}

func (sl *Slot) limitMBps() float64 {
	if p := sl.limit.Load(); p != nil {
		return p.mbPerSec
	}
	return 0
}

// throttle paces n bytes in one direction (down = from the internet) on this adapter.
func (sl *Slot) throttle(down bool, n int) {
	p := sl.limit.Load()
	if p == nil || n <= 0 {
		return
	}
	b := p.out
	if down {
		b = p.in
	}
	for n > 0 {
		k := min(n, limitChunk)
		b.take(k)
		n -= k
	}
}

// throttlePacket is throttle for datagrams: small ones skip the bucket so latency-sensitive
// traffic (DNS, calls) is not stuck behind a bulk download.
func (sl *Slot) throttlePacket(down bool, n int) {
	if n <= udpFree {
		return
	}
	sl.throttle(down, n)
}

// writePaced waits for the adapter's tokens and writes p in chunks while doing so, so a large
// buffer is delivered smoothly instead of in one burst at the end of the wait.
func (sl *Slot) writePaced(dst io.Writer, p []byte, down bool) (int, error) {
	if sl.limit.Load() == nil {
		return dst.Write(p)
	}
	written := 0
	for len(p) > 0 {
		k := min(len(p), limitChunk)
		sl.throttle(down, k)
		n, err := dst.Write(p[:k])
		written += n
		if err != nil {
			return written, err
		}
		if n < k {
			return written, io.ErrShortWrite
		}
		p = p[k:]
	}
	return written, nil
}
