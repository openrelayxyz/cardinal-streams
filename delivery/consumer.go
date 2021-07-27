package delivery

import (
  "math/big"
  "path"
  "strings"
  "github.com/hashicorp/golang-lru"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/hamba/avro"
  log "github.com/inconshreveable/log15"
  "regexp"
  "fmt"
)

type PendingBatch struct {
  Number int64
  Weight *big.Int
  ParentHash types.Hash
  Hash types.Hash
  Values map[string][]byte
  Deletes map[string]struct{}
  Prefixes map[string]int
  Batches map[types.Hash][]bool
  PendingBatches map[types.Hash]struct{}
  PendingMessages map[string]Message // Key -> Message
}

func (pb *PendingBatch) Ready() bool {
  if len(pb.PendingBatches) > 0 { return false }
  for _, v := range pb.Prefixes {
    if v > 0 { return false }
  }
  for _, batch := range pb.Batches {
    for _, b := range batch {
      if !b { return false }
    }
  }
  // If we're not waiting ony batches, prefix messages, or batch messages, we're Ready
  return true
}

func (pb *PendingBatch) whyNotReady() string {
  if len(pb.PendingBatches) > 0 { return "Pending batches" }
  for prefix, v := range pb.Prefixes {
    if v > 0 { return fmt.Sprintf("Pending prefix '%v'", prefix) }
  }
  for batchid, batch := range pb.Batches {
    for i, b := range batch {
      if !b { fmt.Sprintf("Pending batch '%#x'[%v]", batchid, i) }
    }
  }
  return "ready"
}

func (pb *PendingBatch) ApplyMessage(m Message) bool {
  switch MessageType(m.Key()[0]) {
  case SubBatchMsgType:
    // SubBatchMsgType.$hash.$BatchId.$Index
    batchid := types.BytesToHash(m.Key()[33:65])
    var idx int
    if err := avro.Unmarshal(intSchema, m.Key()[65:], &idx); err != nil {
      log.Error("Failed to unmarshal index at end of key", "key", m.Key())
      return true
    }
    if _, ok := pb.Batches[batchid]; !ok {
      pb.PendingMessages[string(m.Key())] = m
      return false
    }
    if pb.Batches[batchid][idx] { return false } // Already have it, no updates
    update := &SubBatchRecord{}
    if err := avro.Unmarshal(updateSchema, m.Value(), &update); err != nil {
      log.Error("Failed to unmarshal update record", "key", m.Key(), "value", m.Value())
      return false
    }
    for _, k := range update.Delete { pb.Deletes[k] = struct{}{} }
    for k, v := range update.Updates { pb.Values[k] = v }
    pb.Batches[batchid][idx] = true
    return true
  case BatchMsgType:
    path := string(m.Key()[33:])
    if v, ok := pb.Values[path]; ok && len(v) > 0 { return false } // Already set
    if pb.DecPrefixCounter(path) {
      pb.Values[path] = m.Value()
      return true
    }
    pb.PendingMessages[string(m.Key())] = m
    return false
  case BatchDeleteMsgType:
    path := string(m.Key()[33:])
    if _, ok := pb.Deletes[path]; ok { return false } // Already set
    if pb.DecPrefixCounter(path) {
      pb.Deletes[path] = struct{}{}
      return true
    }
    pb.PendingMessages[string(m.Key())] = m
    return false
  default:
    log.Warn("Unknown message type", "id", m.Key()[0], "key", m.Key(), "val", m.Value())
  }
  return true // If we got here, any failure to process is going to repeat later, so may as well remove it
}

func (pb *PendingBatch) DecPrefixCounter(p string) bool {
  for d := p; d != ""; d = path.Dir(d) {
    k := strings.TrimPrefix(d, "/") + "/"
    if v, ok := pb.Prefixes[k]; ok && v > 0 {
      pb.Prefixes[k]--
      return true
    } else if ok && v == 0 {
      log.Error("Got a prefix match with none expected to remain. This message will likely fail to reconstruct.", "path", p, "prefix", k)
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
  pendingMessages map[types.Hash]map[string]Message
  trackedPrefixes []*regexp.Regexp
  oldBlocks       map[types.Hash]struct{}
  completed       *lru.Cache
  feed            types.Feed
  reorgFeed       types.Feed
}

// NewMessageProcessor instantiates a MessageProcessor. lastEmittedNum should
// indicate the last block number processed by this system for resumption.
// Blocks older than lastEmittedNum - reorgThreshold will be discarded.
// trackedPrefixes is a list of regular expressions indicating which messages
// are of interest to this consumer - use '.*' to get all messages.
func NewMessageProcessor(lastEmittedNum int64, reorgThreshold int64, trackedPrefixes []*regexp.Regexp) *MessageProcessor {
  cache, err := lru.New(int(2 * reorgThreshold))
  if err != nil { panic(err.Error()) }
  return &MessageProcessor{
    lastEmittedNum: lastEmittedNum,
    reorgThreshold: reorgThreshold,
    trackedPrefixes: trackedPrefixes,
    completed: cache,
    pendingBatches:  make(map[types.Hash]*PendingBatch),
    pendingMessages: make(map[types.Hash]map[string]Message),
    oldBlocks:       make(map[types.Hash]struct{}),
  }
}

// Subscribe to pending batches.
func (mp *MessageProcessor) Subscribe(ch chan<- *PendingBatch) types.Subscription {
  return mp.feed.Subscribe(ch)
}

// Subscribe to reorg notifications
func (mp *MessageProcessor) SubscribeReorgs(ch chan <- map[int64]types.Hash) types.Subscription {
  return mp.reorgFeed.Subscribe(ch)
}

// ProcessMessage takes in messages and assembles completed blocks of messages.
func (mp *MessageProcessor) ProcessMessage(m Message) error {
  hash := types.BytesToHash(m.Key()[1:33])
  if MessageType(m.Key()[0]) == ReorgType {
    var err error
    mp.completed, err = lru.New(int(2 * mp.reorgThreshold))
    return err
    mp.oldBlocks = make(map[types.Hash]struct{})
    if err := avro.Unmarshal(intSchema, m.Value(), &mp.lastEmittedNum); err != nil { return err }
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
    if b.Number < (mp.lastEmittedNum - mp.reorgThreshold) {
      log.Info("Discarding messages for old block", "num", b.Number, "hash", hash, "latest", mp.lastEmittedNum)
      mp.oldBlocks[hash] = struct{}{}
      delete(mp.pendingMessages, hash)
      return nil
    }
    mp.pendingBatches[hash] = &PendingBatch{
      Number: b.Number,
      Weight: new(big.Int).SetBytes(b.Weight),
      ParentHash: b.ParentHash,
      Hash: hash,
      Values: make(map[string][]byte),
      Deletes: make(map[string]struct{}),
      Prefixes: make(map[string]int),
      Batches: make(map[types.Hash][]bool),
      PendingBatches: make(map[types.Hash]struct{}),
      PendingMessages: make(map[string]Message),
    }
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
        mp.pendingBatches[hash].Prefixes[prefix] = update.Count
      }
      if update.Subbatch != (types.Hash{}) {
        mp.pendingBatches[hash].PendingBatches[update.Subbatch] = struct{}{}
      }
    }
    if _, ok := mp.pendingMessages[hash]; ok {
      // We have some messages that arrived before the batch header. Process them now.
      for _, msg := range mp.pendingMessages[hash] {
        if err := mp.ProcessMessage(msg); err != nil { return err }
      }
      delete (mp.pendingMessages, hash)
    }
  case SubBatchHeaderType:
    if _, ok := mp.pendingBatches[hash] ; !ok {
      mp.queueMessage(hash, m)
      return nil
    }
    // TODO: Search pb.PendingMessages for records matching this batch
    batchid := types.BytesToHash(m.Key()[33:])
    if _, ok := mp.pendingBatches[hash].PendingBatches[batchid]; !ok { return nil } // We've already gotten this batch
    delete(mp.pendingBatches[hash].PendingBatches, batchid)
    var counter int
    if err := avro.Unmarshal(intSchema, m.Value(), &counter); err != nil { return err }
    mp.pendingBatches[hash].Batches[batchid] = make([]bool, counter)
    for k, msg := range mp.pendingBatches[hash].PendingMessages {
      if mp.pendingBatches[hash].ApplyMessage(msg) {
        delete(mp.pendingBatches[hash].PendingMessages, k)
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


func (mp *MessageProcessor) queueMessage(hash types.Hash, m Message) {
  if _, ok := mp.pendingMessages[hash]; !ok { mp.pendingMessages[hash] = make(map[string]Message) }
  mp.pendingMessages[hash][string(m.Key())] = m
}
