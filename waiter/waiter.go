package waiter

import (
    "time"
    "github.com/openrelayxyz/cardinal-streams/delivery"
    "github.com/openrelayxyz/cardinal-types"
    "sync"
)

type Waiter struct {
    hashes map[types.Hash]chan struct{}
    numbers map[int64]chan struct{}
    lock sync.RWMutex
    sub types.Subscription
    ch chan *delivery.Waiter
}

func NewWaiter(omp *delivery.OrderedMessageProcessor) *Waiter {
    ch := make(chan *delivery.Waiter, 1024)
    w := &Waiter{
        hashes: make(map[types.Hash]chan struct{}),
        numbers: make(map[int64]chan struct{}),
        sub: omp.Subscribe(ch),
        ch: ch,
    }
    go func() {
        for pw := range ch {
            w.lock.Lock()
            w.hashes[pw.Hash] = pw.WaitCh
            if _, ok := w.numbers[pw.Number]; !ok {
                w.numbers[pw.Number] = make(chan struct{})
            }
            go func(nch, hch chan struct{}, pw *delivery.Waiter) {
                select {
                case <-hch:
                    // The hash channel delivered, close the number channel
                    close(nch)
                    go func() {
                        // Leave the closed channels in the maps for the next 60 seconds
                        time.Sleep(60 * time.Second)
                        w.lock.Lock()
                        delete(w.numbers, pw.Number)
                        delete(w.hashes, pw.Hash)
                        w.lock.Unlock()
                    }()
                case <-nch:
                    // The number channel delivered, the hash channel may never deliver
                    go func() {
                        // Leave the closed channels in the maps for the next 60 seconds
                        time.Sleep(60 * time.Second)
                        w.lock.Lock()
                        delete(w.hashes, pw.Hash)
                        w.lock.Unlock()
                    }()
                }
            }(w.numbers[pw.Number], pw.WaitCh, pw)
            w.lock.Unlock()
        }
    }()
    return w
}

type WaitResult int

const (
    Success WaitResult = iota
    Timeout
    NotFound
)

// WaitForHash takes a hash and a duration, and returns a WaitResult of
// Success, Timeout, or NotFound. Callers should confirm that the block is not known to
// their application before calling, but the waiter keeps a brief history of known blocks
// to avoid race conditions. Calling applications should call pb.Done() on pending batches
// for the waiter to process blocks. Note that depending on how blocks get processed by the
// OrderedMessageProcessor, blocks that get reorged out may never be processed.
func (w *Waiter) WaitForHash(h types.Hash, timeout time.Duration) WaitResult {
    w.lock.RLock()
    ch, ok := w.hashes[h]
    w.lock.RUnlock()
    if !ok {
        return NotFound
    }
    select {
    case <-ch:
        return Success
    case <-time.After(timeout):
        return Timeout
    }
}

// WaitForNumber takes a hash and a duration, and returns a WaitResult of
// Success, Timeout, or NotFound. Callers should confirm that the block is not known to
// their application before calling, but the waiter keeps a brief history of known blocks
// to avoid race conditions. Calling applications should call pb.Done() on pending batches
// for the waiter to process blocks. Note that depending on how blocks get processed by the
// OrderedMessageProcessor, blocks that get reorged out may never be processed.
func (w *Waiter) WaitForNumber(n int64, timeout time.Duration) WaitResult {
    w.lock.RLock()
    ch, ok := w.numbers[n]
    w.lock.RUnlock()
    if !ok {
        return NotFound
    }
    select {
    case <-ch:
        return Success
    case <-time.After(timeout):
        return Timeout
    }
}

func (w *Waiter) Stop() {
    w.sub.Unsubscribe()
    close(w.ch)
}