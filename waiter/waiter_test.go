package waiter

import (
    "time"
    "testing"
    "regexp"
    "math/big"
    "fmt"
    "github.com/openrelayxyz/cardinal-streams/delivery"
    "github.com/openrelayxyz/cardinal-types"
    "runtime"
    "sync"
)


func testPendingBatch(nonce int64, parent *delivery.PendingBatch) *delivery.PendingBatch {
    return &delivery.PendingBatch{
        Number: parent.Number + 1,
        Hash: types.BytesToHash([]byte(fmt.Sprintf("%v%v", nonce, parent.Number+1))),
        Weight: new(big.Int).Add(new(big.Int).SetInt64(1), parent.Weight),
        ParentHash: parent.Hash,
    }
}


func TestWaiterSuccess(t *testing.T) {
    zpb := &delivery.PendingBatch{
        Number: 1,
        Hash: types.Hash{255},
        ParentHash: types.Hash{},
        Weight: new(big.Int),
    }
    omp, err := delivery.NewOrderedMessageProcessor(zpb.Number, zpb.Hash, zpb.Weight, 128, []*regexp.Regexp{regexp.MustCompile(".")}, nil)
    if err != nil {
        t.Fatalf(err.Error())
    }
    ch := make(chan *delivery.ChainUpdate)
    sub := omp.Subscribe(ch)
    go func() {
        for cu := range ch {
            cu.Done()
        }        
    }()
    defer sub.Unsubscribe()
    defer close(ch)
    defer omp.Close()
    w := NewOmpWaiter(omp)
    apb := testPendingBatch(0, zpb)
    bpb := testPendingBatch(0, apb)
    omp.ProcessCompleteBatch(bpb)
    runtime.Gosched()
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        if result := w.WaitForHash(bpb.Hash, time.Second); result != Success {
            t.Errorf("Expected success for hash, got %v", result)
        }
        wg.Done()
    }()
    wg.Add(1)
    go func() {
        if result := w.WaitForNumber(bpb.Number, time.Second); result != Success {
            t.Errorf("Expected success for number, got %v", result)
        }
        wg.Done()
    }()
    omp.ProcessCompleteBatch(apb)
    wg.Wait()
}
func TestWaiterReorg(t *testing.T) {
    zpb := &delivery.PendingBatch{
        Number: 1,
        Hash: types.Hash{255},
        ParentHash: types.Hash{},
        Weight: new(big.Int),
    }
    omp, err := delivery.NewOrderedMessageProcessor(zpb.Number, zpb.Hash, zpb.Weight, 128, []*regexp.Regexp{regexp.MustCompile(".")}, nil)
    if err != nil {
        t.Fatalf(err.Error())
    }
    ch := make(chan *delivery.ChainUpdate)
    sub := omp.Subscribe(ch)
    go func() {
        for cu := range ch {
            cu.Done()
        }        
    }()
    defer sub.Unsubscribe()
    defer close(ch)
    defer omp.Close()
    w := NewOmpWaiter(omp)
    apb := testPendingBatch(0, zpb)
    bpb := testPendingBatch(0, apb)
    omp.ProcessCompleteBatch(bpb)
    runtime.Gosched()
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        if result := w.WaitForHash(bpb.Hash, 250 * time.Millisecond); result != Timeout {
            t.Errorf("Expected timeout for hash, got %v", result)
        }
        wg.Done()
    }()
    wg.Add(1)
    go func() {
        if result := w.WaitForNumber(bpb.Number, 250 * time.Millisecond); result != Timeout {
            t.Errorf("Expected timeout for number, got %v", result)
        }
        wg.Done()
    }()
    wg.Wait()
}
func TestWaiterTimeout(t *testing.T) {
    zpb := &delivery.PendingBatch{
        Number: 1,
        Hash: types.Hash{255},
        ParentHash: types.Hash{},
        Weight: new(big.Int),
    }
    omp, err := delivery.NewOrderedMessageProcessor(zpb.Number, zpb.Hash, zpb.Weight, 128, []*regexp.Regexp{regexp.MustCompile(".")}, nil)
    if err != nil {
        t.Fatalf(err.Error())
    }
    ch := make(chan *delivery.ChainUpdate)
    sub := omp.Subscribe(ch)
    go func() {
        for cu := range ch {
            cu.Done()
        }        
    }()
    defer sub.Unsubscribe()
    defer close(ch)
    defer omp.Close()
    w := NewOmpWaiter(omp)
    apb := testPendingBatch(0, zpb)
    bpb := testPendingBatch(0, apb)
    cpb := testPendingBatch(1, apb)
    dpb := testPendingBatch(0, cpb)
    omp.ProcessCompleteBatch(bpb)
    omp.ProcessCompleteBatch(cpb)
    omp.ProcessCompleteBatch(dpb)
    runtime.Gosched()
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        if result := w.WaitForHash(bpb.Hash, 250 * time.Millisecond); result != Timeout {
            t.Errorf("Expected timeout for hash, got %v", result)
        }
        wg.Done()
    }()
    wg.Add(1)
    go func() {
        if result := w.WaitForNumber(bpb.Number, 250 * time.Millisecond); result != Success {
            t.Errorf("Expected success for number, got %v", result)
        }
        wg.Done()
    }()
    omp.ProcessCompleteBatch(apb)
    wg.Wait()
}