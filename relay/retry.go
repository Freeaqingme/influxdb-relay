package relay

import (
	"log"
	"sync"
	"time"
)

const (
	retryInitial    = 500 * time.Millisecond
	retryMultiplier = 2
)

type Operation func() error

// Buffers and retries operations, if the buffer is full operations are dropped.
// Only tries one operation at a time, the next operation is not attempted
// until success or timeout of the previous operation.
// There is no delay between attempts of different operations.
type retryBuffer struct {
	*sync.RWMutex
	contents []retryBufferItem

	size    int // current size in bytes
	maxSize int // max size in bytes

	initialInterval time.Duration
	multiplier      time.Duration
	maxInterval     time.Duration

	p poster
}

type retryBufferItem struct {
	date     time.Time
	contents []byte
	query    string
	auth     string
}

func newRetryBuffer(size, batch int, max time.Duration, p poster) *retryBuffer {
	r := &retryBuffer{
		RWMutex:         &sync.RWMutex{},
		contents:        nil,
		initialInterval: retryInitial,
		multiplier:      retryMultiplier,
		maxInterval:     max,
		maxSize:         size,
		p:               p,
	}
	return r
}

func (r *retryBuffer) getStats() map[string]string {
	return make(map[string]string, 0) // TODO
	//stats := make(map[string]string)
	//stats["buffering"] = strconv.FormatInt(int64(r.buffering), 10)
	//for k, v := range r.list.getStats() {
	//	stats[k] = v
	//}
	//return stats
}

func (r *retryBuffer) isBuffering() bool {
	r.RLock()
	if r.contents != nil {
		r.RUnlock()
		return true
	}

	r.RUnlock()
	return false
}

func (r *retryBuffer) post(buf []byte, query string, auth string, isQuery bool) (*responseData, error) {
	resp, err := r.p.post(buf, query, auth, isQuery)
	if err == nil || isQuery {
		return resp, err
	}
	r.buffer(buf, query, auth)
	return resp, err
}

func (r *retryBuffer) buffer(buf []byte, query, auth string) {
	buffering := r.isBuffering()
	if !buffering {
		r.contents = make([]retryBufferItem, 0)
	}

	r.Lock()
	if r.size+len(buf) > r.maxSize {
		log.Printf("Dropped batch of size %d because the max buffer would be exceeded. Batch is %d bytes", r.size)
		r.Unlock()
		return
	}
	r.size = r.size + len(buf)
	r.contents = append(r.contents, retryBufferItem{time.Now(), buf, query, auth})
	if !buffering {
		go r.run()
	}
	r.Unlock()
}

func (r *retryBuffer) run() {
	interval := r.initialInterval
	i := 0
	for {
		r.RLock()
		if i >= len(r.contents) {
			r.RUnlock()
			r.Lock()
			r.contents = nil
			log.Printf("RetryBuffer %d was flushed successfully", &r.p)
			r.Unlock()
			return
		}

		bufItem := r.contents[i]
		r.RUnlock()
		_, err := r.p.post(bufItem.contents, bufItem.query, bufItem.auth, false)
		if err == nil {
			i = i + 1
			continue
		}

		if interval != r.maxInterval {
			interval *= r.multiplier
			if interval > r.maxInterval {
				interval = r.maxInterval
			}
		}

		r.RLock()
		log.Printf("Buffer %d now contains %d batches of total %d bytes (max: %d). "+
			"Already handled %d batches. Retrying in %s",
			&r.p, len(r.contents), r.size, r.maxSize, i, interval,
		)
		r.RUnlock()
		time.Sleep(interval)
	}
}
