package waiter

import (
    "time"
    "github.com/openrelayxyz/cardinal-streams/delivery"
    "github.com/openrelayxyz/cardinal-types"
    "github.com/openrelayxyz/cardinal-types/metrics"
    "sync"
    "runtime"
)


var (
    timeoutMetric = metrics.NewMinorCounter("/streams/waiter/timeout")
    successMetric = metrics.NewMinorCounter("/streams/waiter/success")
    unavailableMetric = metrics.NewMinorCounter("/streams/waiter/unavailable")
)


type Waiter interface {
    // WaitForHash takes a hash and a duration, and returns a WaitResult of
    // Success, Timeout, or NotFound. Callers should confirm that the block is not known to
    // their application before calling, but the waiter keeps a brief history of known blocks
    // to avoid race conditions. Calling applications should call cu.Done() on chain updates
    // for the waiter to process blocks. Note that blocks that get reorged out may never be 
    // processed even though their hashes have been seen.
    WaitForHashResult(types.Hash, time.Duration) WaitResult

    // WaitForHash behaves like WaitForHashResult without returning a result. Callers should confirm that the
    // block is not known to their application, call WaitForHash, then check their application again.
    WaitForHash(types.Hash, time.Duration)
    // WaitForNumber takes a block number and a duration, and returns a WaitResult of
    // Success, Timeout, or NotFound. Callers should confirm that the block is not known to
    // their application before calling, but the waiter keeps a brief history of known blocks
    // to avoid race conditions. Calling applications should call pb.Done() on pending batches
    // for the waiter to process blocks. Note that depending on how blocks get processed by the
    // OrderedMessageProcessor, blocks that get reorged out may never be processed.
    WaitForNumberResult(int64, time.Duration) WaitResult
    // WaitForNumber behaves like WaitForNumberResult without returning a result. Callers should confirm that the
    // block is not known to their application, call WaitForHash, then check their application again.
    WaitForNumber(int64, time.Duration)
    Stop()
}

type ompWaiter struct {
    hashes map[types.Hash]chan struct{}
    numbers map[int64]chan struct{}
    lock sync.RWMutex
    sub types.Subscription
    ch chan *delivery.Waiter
}

func NewOmpWaiter(omp *delivery.OrderedMessageProcessor) Waiter {
    ch := make(chan *delivery.Waiter, 1024)
    w := &ompWaiter{
        hashes: make(map[types.Hash]chan struct{}),
        numbers: make(map[int64]chan struct{}),
        sub: omp.Subscribe(ch),
        ch: ch,
    }
    go func() {
        var nchLock sync.Mutex
        for pw := range ch {
            w.lock.Lock()
            w.hashes[pw.Hash] = pw.WaitCh
            if _, ok := w.numbers[pw.Number]; !ok {
                w.numbers[pw.Number] = make(chan struct{})
            }
            go func(nch, hch chan struct{}, pw *delivery.Waiter) {
                select {
                case <-hch:
                    // The hash channel delivered, close the number channel (safely)
                    nchLock.Lock()
                    select {
                    case <-nch:
                        // Another goroutine already closed the channel
                    default:
                        close(nch)
                    }
                    nchLock.Unlock()
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

func (w *ompWaiter) WaitForHash(h types.Hash, timeout time.Duration) {
    w.WaitForHashResult(h, timeout)
}


func (w *ompWaiter) WaitForHashResult(h types.Hash, timeout time.Duration) WaitResult {
    runtime.Gosched() // Give other processes a chance to populate w.hashes
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


func (w *ompWaiter) WaitForNumber(n int64, timeout time.Duration) {
    w.WaitForNumberResult(n, timeout)
}

func (w *ompWaiter) WaitForNumberResult(n int64, timeout time.Duration) WaitResult {
    runtime.Gosched() // Give other processes a chance to populate w.numbers
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

func (w *ompWaiter) Stop() {
    w.sub.Unsubscribe()
    close(w.ch)
}