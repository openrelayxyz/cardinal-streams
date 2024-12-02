package delivery

import (
  "math/big"
  "path"
  "strings"
  "github.com/hashicorp/golang-lru"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-types/metrics"
  gmetrics "github.com/rcrowley/go-metrics"
  "github.com/hamba/avro"
  log "github.com/inconshreveable/log15"
  "regexp"
  "fmt"
  "time"
)

var (
  deltaTimer gmetrics.Timer
  deltaTimerMinor = metrics.NewMinorTimer("/streams/minor/delta")
  pbGauge = metrics.NewMinorGauge("/streams/con/pending")
  messagesGauge = metrics.NewMinorGauge("/streams/con/messages")
  reconstructFailureCounter = metrics.NewMajorCounter("/streams/con/failures")
)

type PendingBatch struct {
  Number int64
  Weight *big.Int
  ParentHash types.Hash
  Hash types.Hash
  Values map[string][]byte
  Deletes map[string]struct{}
  resumption map[string]int64
  resumptionDefaults map[string]int64
  prefixes map[string]int
  batches map[types.Hash][]bool
  pendingBatches map[types.Hash]struct{}
  pendingMessages map[string]ResumptionMessage // Key -> Message
  waitCh chan struct{}
  time time.Time
  failedReconstructPanic bool
}

func (pb *PendingBatch) Resumption() string {
  result := make([]string, 0, len(pb.resumption))
  for k, v := range pb.resumption {
    result = append(result, fmt.Sprintf("%v=%v", k, v))
  }
  for k, v := range pb.resumptionDefaults {
    if _, ok := pb.resumption[k]; !ok {
      result = append(result, fmt.Sprintf("%v=%v", k, v))
    }
  }
  return strings.Join(result, ";")
}

func (pb *PendingBatch) updateResumption(source string, offset int64) {
  if o, ok := pb.resumption[source]; !ok || o > offset { pb.resumption[source] = offset }
}

func (pb *PendingBatch) Ready() bool {
  if len(pb.pendingBatches) > 0 { return false }
  for _, v := range pb.prefixes {
    if v > 0 { return false }
  }
  for _, batch := range pb.batches {
    for _, b := range batch {
      if !b { return false }
    }
  }
  // If we're not waiting ony batches, prefix messages, or batch messages, we're Ready
  return true
}

// Done should be called on a pending batch after it has been applied.
func (pb *PendingBatch) Done() {
  waitch := pb.waitCh
  if waitch != nil {
    pb.waitCh = nil
    close(waitch)
  }
  deltaTimerMinor.UpdateSince(pb.time)
  if deltaTimer != nil {
    deltaTimer.UpdateSince(pb.time)
  }
  log.Debug("Batch applied", "hash", pb.Hash, "number", pb.Number, "delta", time.Since(pb.time))
}

func (pb *PendingBatch) whyNotReady() string {
  if len(pb.pendingBatches) > 0 { return "Pending batches" }
  for prefix, v := range pb.prefixes {
    if v > 0 { return fmt.Sprintf("Pending prefix '%v'", prefix) }
  }
  for batchid, batch := range pb.batches {
    for i, b := range batch {
      if !b { fmt.Sprintf("Pending batch '%#x'[%v]", batchid, i) }
    }
  }
  return "ready"
}

func (pb *PendingBatch) ApplyMessage(m ResumptionMessage) bool {
  pb.updateResumption(m.Source(), m.Offset())
  switch MessageType(m.Key()[0]) {
  case SubBatchMsgType:
    // SubBatchMsgType.$hash.$BatchId.$Index
    batchid := types.BytesToHash(m.Key()[33:65])
    var idx int
    if err := avro.Unmarshal(intSchema, m.Key()[65:], &idx); err != nil {
      log.Error("Failed to unmarshal index at end of key", "key", m.Key())
      return true
    }
    if _, ok := pb.batches[batchid]; !ok {
      pb.pendingMessages[string(m.Key())] = m
      return false
    }
    if pb.batches[batchid][idx] { return false } // Already have it, no updates
    update := &SubBatchRecord{}
    if err := avro.Unmarshal(updateSchema, m.Value(), &update); err != nil {
      log.Error("Failed to unmarshal update record", "key", m.Key(), "value", m.Value())
      return false
    }
    for _, k := range update.Delete { pb.Deletes[k] = struct{}{} }
    for k, v := range update.Updates { pb.Values[k] = v }
    pb.batches[batchid][idx] = true
    return true
  case BatchMsgType:
    path := string(m.Key()[33:])
    if v, ok := pb.Values[path]; ok && len(v) > 0 { return false } // Already set
    if pb.DecPrefixCounter(path) {
      // If this is false, we're not tracking this prefix and should ignore the message
      pb.Values[path] = m.Value()
      return true
    }
    pb.pendingMessages[string(m.Key())] = m
    return false
  case BatchDeleteMsgType:
    path := string(m.Key()[33:])
    if _, ok := pb.Deletes[path]; ok { return false } // Already set
    if pb.DecPrefixCounter(path) {
      pb.Deletes[path] = struct{}{}
      return true
    }
    pb.pendingMessages[string(m.Key())] = m
    return false
  default:
    log.Warn("Unknown message type", "id", m.Key()[0], "key", m.Key(), "val", m.Value())
  }
  return true // If we got here, any failure to process is going to repeat later, so may as well remove it
}

func (pb *PendingBatch) DecPrefixCounter(p string) bool {
  if v, ok := pb.prefixes[p]; ok && v > 0 {
    // One of the prefixes is an exact match for p
    pb.prefixes[p]--
    return true
  }
  for d := p; d != "."; d = path.Dir(d) {
    k := strings.TrimPrefix(d, "/") + "/"
    if v, ok := pb.prefixes[k]; ok && v > 0 {
      pb.prefixes[k]--
      return true
    } else if ok && v == 0 {
      log.Error("Got a prefix match with none expected to remain. This message will likely fail to reconstruct.", "path", p, "prefix", k)
      reconstructFailureCounter.Inc(1)
      if pb.failedReconstructPanic {
        panic(fmt.Sprintf("reconstruct failed on block %v", pb.Hash))
      }
      continue // If we got lucky, messsages arived in an order that will allow us to reconstruct if we search higher
    }
  }
  return false
}

type prefixBatch struct {
  re *regexp.Regexp
  batch *PendingBatch
}

// MessageProcessor aggregates messages and emits completed blocks. Messages
// will be discarded if their blocks are older than lastEmittedNum -
// reorgThreshold, but otherwise the will be emitted in the order they are
// completed, even if they are older than the initial block or there are gaps
// between the last emitted block and this one. Subsequent layers will be used
// to ensure blocks are emitted in something resembling a sensible order and
// that reorgs are handled.
type MessageProcessor struct {
  lastEmittedNum  int64
  reorgThreshold  int64
  pendingBatches  map[types.Hash]*PendingBatch
  pendingMessages map[types.Hash]map[string]ResumptionMessage
  trackedPrefixes []*regexp.Regexp
  oldBlocks       map[types.Hash]struct{}
  completed       *lru.Cache
  feed            types.Feed
  reorgFeed       types.Feed
  waitFeed        types.Feed
  resumption      map[string]int64
  evictCh         chan int64
  failedReconstructPanic bool
}

// NewMessageProcessor instantiates a MessageProcessor. lastEmittedNum should
// indicate the last block number processed by this system for resumption.
// Blocks older than lastEmittedNum - reorgThreshold will be discarded.
// trackedPrefixes is a list of regular expressions indicating which messages
// are of interest to this consumer - use '.*' to get all messages.
func NewMessageProcessor(cfg *ConsumerConfig) *MessageProcessor {
  cache, err := lru.New(int(2 * cfg.ReorgThreshold))
  if err != nil { panic(err.Error()) }
  return &MessageProcessor{
    lastEmittedNum: cfg.LastEmittedNum,
    reorgThreshold: cfg.ReorgThreshold,
    trackedPrefixes: cfg.TrackedPrefixes,
    completed: cache,
    pendingBatches:  make(map[types.Hash]*PendingBatch),
    pendingMessages: make(map[types.Hash]map[string]ResumptionMessage),
    oldBlocks:       make(map[types.Hash]struct{}),
    resumption:      make(map[string]int64),
    evictCh:         make(chan int64, 100),
    failedReconstructPanic: cfg.FailedReconstructPanic,
  }
}

// Subscribe to pending batches.
func (mp *MessageProcessor) Subscribe(ch chan<- *PendingBatch) types.Subscription {
  return mp.feed.Subscribe(ch)
}

type Waiter struct {
  Hash   types.Hash
  Parent types.Hash
  Number int64
  WaitCh chan struct{}
}

// Subscribe to the hashes of pending batches
func (mp *MessageProcessor) SubscribeWaiters(ch chan<- *Waiter) types.Subscription {
  return mp.waitFeed.Subscribe(ch)
}

// Subscribe to reorg notifications
func (mp *MessageProcessor) SubscribeReorgs(ch chan <- map[int64]types.Hash) types.Subscription {
  return mp.reorgFeed.Subscribe(ch)
}

func (mp *MessageProcessor) evictOlderThan(n int64) {
  select {
  case mp.evictCh <- n:
  default:
  }
}

func (mp *MessageProcessor) ProcessCompleteBatch(pb *PendingBatch) {
	if pb.waitCh == nil {
		pb.waitCh = make(chan struct{})
	}
	mp.waitFeed.Send(&Waiter{
		Hash: pb.Hash,
		Parent: pb.ParentHash,
		Number: pb.Number,
		WaitCh: pb.waitCh,
	})
	mp.feed.Send(pb)
	mp.lastEmittedNum = pb.Number
	mp.completed.Add(pb.Hash, struct{}{})
	delete(mp.pendingBatches, pb.Hash)
}

// ProcessMessage takes in messages and assembles completed blocks of messages.
func (mp *MessageProcessor) ProcessMessage(m ResumptionMessage) error {
  EVICTLOOP:
  for {
    select {
    case n := <-mp.evictCh:
      for h, pb := range mp.pendingBatches {
        if pb.Number < n {
          delete(mp.pendingBatches, h)
          mp.oldBlocks[h] = struct{}{}
        }
      }
    default:
      break EVICTLOOP
    }
  }
  mp.updateResumption(m.Source(), m.Offset())
  hash := types.BytesToHash(m.Key()[1:33])
  if MessageType(m.Key()[0]) == ReorgType {
    var err error
    mp.completed, err = lru.New(int(2 * mp.reorgThreshold))
    if err != nil {
      return err
    }
    mp.oldBlocks = make(map[types.Hash]struct{})
    var num int
    if err := avro.Unmarshal(intSchema, m.Value(), &num); err != nil { return err }
    mp.lastEmittedNum = int64(num)
    mp.reorgFeed.Send(map[int64]types.Hash{mp.lastEmittedNum: hash})
    return nil
  }
  if mp.completed.Contains(hash) { return nil }   // We've recently emitted this block
  if _, ok := mp.oldBlocks[hash]; ok { return nil } // This block is older than the reorg threshold, skip anything related to it.

  switch MessageType(m.Key()[0]) {
  case BatchType:
    b := &Batch{}
    if err := avro.Unmarshal(batchSchema, m.Value(), b); err != nil { return err }
    b.Hash = hash
    if _, ok := mp.pendingBatches[hash]; ok { return nil } // We already have this block. No need to check further.
    // if b.Number < (mp.lastEmittedNum - mp.reorgThreshold) {
    //   log.Info("Discarding messages for old block", "num", b.Number, "hash", hash, "latest", mp.lastEmittedNum, "source", m.Source())
    //   mp.oldBlocks[hash] = struct{}{}
    //   delete(mp.pendingMessages, hash)
    //   return nil
    // }
    waitCh := make(chan struct{})
    mp.waitFeed.Send(&Waiter{
      Hash: b.Hash,
      Parent: b.ParentHash,
      Number: b.Number,
      WaitCh: waitCh,
    })
    mp.pendingBatches[hash] = &PendingBatch{
      Number: b.Number,
      Weight: new(big.Int).SetBytes(b.Weight),
      ParentHash: b.ParentHash,
      Hash: hash,
      Values: make(map[string][]byte),
      Deletes: make(map[string]struct{}),
      prefixes: make(map[string]int),
      batches: make(map[types.Hash][]bool),
      pendingBatches: make(map[types.Hash]struct{}),
      pendingMessages: make(map[string]ResumptionMessage),
      resumption: make(map[string]int64),
      resumptionDefaults: mp.resumptionCopy(),
      time: m.Time(),
      waitCh: waitCh,
      failedReconstructPanic: mp.failedReconstructPanic,
    }
    mp.pendingBatches[hash].updateResumption(m.Source(), m.Offset())
    for prefix, update := range b.Updates {
      // log.Info("Batch update:", "prefix", prefix, "count", update.Count, "sb", update.Subbatch)
      match := false
      for _, re := range mp.trackedPrefixes {
        if re.MatchString(prefix) {
          match = true
          break
        }
      }
      if !match { continue } // Regardless of message type, if we're ignoring the prefix, skip this update
      if len(update.Value) > 0 { mp.pendingBatches[hash].Values[prefix] = update.Value }
      if update.Delete { mp.pendingBatches[hash].Deletes[prefix] = struct{}{} }
      if update.Count != 0 {
        mp.pendingBatches[hash].prefixes[prefix] = update.Count
      }
      if update.Subbatch != (types.Hash{}) {
        mp.pendingBatches[hash].pendingBatches[update.Subbatch] = struct{}{}
      }
    }
    if _, ok := mp.pendingMessages[hash]; ok {
      // We have some messages that arrived before the batch header. Process them now.
      for _, msg := range mp.pendingMessages[hash] {
        if err := mp.ProcessMessage(msg); err != nil { return err }
      }
      delete (mp.pendingMessages, hash)
    }
    pbGauge.Update(int64(len(mp.pendingBatches)))
    messagesGauge.Update(int64(len(mp.pendingMessages)))
    log.Debug("New pending batch", "number", b.Number, "pending", len(mp.pendingBatches), "messages", len(mp.pendingMessages), "source", m.Source())
  case SubBatchHeaderType:
    if _, ok := mp.pendingBatches[hash] ; !ok {
      mp.queueMessage(hash, m)
      return nil
    }
    batchid := types.BytesToHash(m.Key()[33:])
    if _, ok := mp.pendingBatches[hash].pendingBatches[batchid]; !ok { return nil } // We've already gotten this batch
    delete(mp.pendingBatches[hash].pendingBatches, batchid)
    var counter int
    if err := avro.Unmarshal(intSchema, m.Value(), &counter); err != nil { return err }
    mp.pendingBatches[hash].batches[batchid] = make([]bool, counter)
    mp.pendingBatches[hash].updateResumption(m.Source(), m.Offset())
    for k, msg := range mp.pendingBatches[hash].pendingMessages {
      if mp.pendingBatches[hash].ApplyMessage(msg) {
        delete(mp.pendingBatches[hash].pendingMessages, k)
      }
    }
  default:
    if _, ok := mp.pendingBatches[hash] ; !ok {
      mp.queueMessage(hash, m)
      return nil
    }
    mp.pendingBatches[hash].ApplyMessage(m)
  }
  if b, ok := mp.pendingBatches[hash] ; ok && b.Ready() {
    mp.feed.Send(b)
    mp.lastEmittedNum = b.Number
    mp.completed.Add(hash, struct{}{})
    delete(mp.pendingBatches, hash)
  }
  return nil
}


func (mp *MessageProcessor) queueMessage(hash types.Hash, m ResumptionMessage) {
  if _, ok := mp.pendingMessages[hash]; !ok { mp.pendingMessages[hash] = make(map[string]ResumptionMessage) }
  mp.pendingMessages[hash][string(m.Key())] = m
}

func (mp *MessageProcessor) WhyNotReady(hash types.Hash) string {
  if pb, ok := mp.pendingBatches[hash]; ok {
    return pb.whyNotReady()
  } else {
    return "not found"
  }
}

func (mp *MessageProcessor) updateResumption(source string, offset int64) {
  if o, ok := mp.resumption[source]; !ok || o < offset { mp.resumption[source] = offset }
}

func (mp *MessageProcessor) resumptionCopy() map[string]int64 {
  m := make(map[string]int64)
  for k, v := range mp.resumption {
    m[k] = v
  }
  return m
}

// Ready should be called when the transport is synced up. This sets up metrics
// to start recording deltas once caught up, so the catchup process doesn't
// taint the metrics
func Ready() {
  if deltaTimer != nil {
    deltaTimer = metrics.NewMajorTimer("/streams/delta")
  }
}
