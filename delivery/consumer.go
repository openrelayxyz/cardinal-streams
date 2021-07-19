package delivery

import (
  "path"
  "strings"
  "github.com/hashicorp/golang-lru"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/hamba/avro"
  log "github.com/inconshreveable/log15"
  "regexp"
)

type MessageType byte

const (
  BatchType MessageType = iota  // BatchType.$Hash
  SubBatchHeaderType            // SubBatchHeaderType.$Hash.$BatchId
  SubBatchMsgType               // SubBatchMsgType.$hash.$BatchId.$Index
  BatchMsgType                  // BatchMsgType.$hash./path/
  BatchDeleteMsgType            // BatchDeleteMsgType.$hash./path/
)


type Message interface {
  Key() []byte
  Value() []byte
}

type PendingBatch struct {
  Number int64
  Weight []byte
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
    } else {
      pb.PendingMessages[string(m.Key())] = m
      return false
    }
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

type MessageProcessor struct {
  lastEmittedHash types.Hash
  lastEmittedNum  int64
  reorgThreshold  int64
  pendingBatches  map[types.Hash]*PendingBatch
  pendingMessages map[types.Hash]map[string]Message
  trackedPrefixes []*regexp.Regexp
  oldBlocks       map[types.Hash]struct{}
  completed       *lru.Cache
  feed            types.Feed
}

func (mp *MessageProcessor) ProcessMessage(m Message) error {
  hash := types.BytesToHash(m.Key()[1:33])
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
      Weight: b.Weight,
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
      match := false
      for _, re := range mp.trackedPrefixes {
        if re.MatchString(prefix) {
          match = true
          break
        }
      }
      if !match { continue } // Regardless of message type, if we're ignoring the prefix, skip this update
      if len(update.Value) > 0 { mp.pendingBatches[hash].Values[prefix] = update.Value }
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
    defer delete(mp.pendingBatches[hash].PendingBatches, batchid)
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

  if b := mp.pendingBatches[hash] ; b.Ready() {
    mp.feed.Send(b)
  }
  return nil
}


func (mp *MessageProcessor) queueMessage(hash types.Hash, m Message) {
  if _, ok := mp.pendingMessages[hash]; !ok { mp.pendingMessages[hash] = make(map[string]Message) }
  mp.pendingMessages[hash][string(m.Key())] = m
}


// TODO: In the producer, enforce that no prefix can be the prefix to another prefix.
