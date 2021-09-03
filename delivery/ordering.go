package delivery

import (
  "fmt"
  "regexp"
  "math/big"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/hashicorp/golang-lru"
  log "github.com/inconshreveable/log15"
  "time"
)

type OrderedMessageProcessor struct {
  mp *MessageProcessor
  lastHash       types.Hash
  lastWeight     *big.Int
  reorgThreshold int64
  pending        map[types.Hash]*PendingBatch
  queued         map[types.Hash]map[types.Hash]struct{}
  finished       *lru.Cache
  quit           chan struct{}
  updateFeed     types.Feed
  reorgFeed      types.Feed
}


func NewOrderedMessageProcessor(lastNumber int64, lastHash types.Hash, lastWeight *big.Int, reorgThreshold int64, trackedPrefixes []*regexp.Regexp) (*OrderedMessageProcessor, error) {
  omp := &OrderedMessageProcessor{
    mp: NewMessageProcessor(lastNumber, reorgThreshold, trackedPrefixes),
    lastHash: lastHash,
    lastWeight: lastWeight,
    reorgThreshold: reorgThreshold,
    pending: make(map[types.Hash]*PendingBatch),
    queued: make(map[types.Hash]map[types.Hash]struct{}),
    quit: make(chan struct{}),
  }
  var err error
  omp.finished, err = lru.NewWithEvict(int(reorgThreshold * 2), func(k, v interface{}) {
    hash := k.(types.Hash)
    omp.evict(hash)
  })
  omp.finished.Add(lastHash, struct{}{})
  ch := make(chan *PendingBatch, 100)
  reorgCh := make(chan map[int64]types.Hash, 100)
  sub := omp.mp.Subscribe(ch)
  reorgSub := omp.mp.SubscribeReorgs(reorgCh)
  go func() {
    for {
      select {
      case pb := <-ch:
        omp.HandlePendingBatch(pb)
      case reorg := <-reorgCh:
        for num, hash := range reorg {
          omp.HandleReorg(num, hash)
          break // There should never be more than 1 kv pair in this map
        }
      case err := <-sub.Err():
        log.Error("Subscription error", "err", err.Error())
      case err := <-reorgSub.Err():
        log.Error("Reorg subscription error", "err", err.Error())
      case <-time.After(250 * time.Millisecond):
        // If we've gone 250ms without any messages, see if there's a quit
        // message. This ensures we don't quit until we're done processing,
        // without busy waiting on the quit channel.
        select {
        case <-omp.quit:
          sub.Unsubscribe()
          reorgSub.Unsubscribe()
          return
        default:
        }
      }
    }
  }()
  return omp, err
}

func (omp *OrderedMessageProcessor) evict(hash types.Hash) {
  // TODO: Clean up from MessageProcessor as well
  if pb, ok := omp.pending[hash]; ok {
    delete(omp.pending, pb.Hash)
    omp.evict(pb.ParentHash) // If the parent is still being tracked we can go ahead and get rid of it
    if children, ok := omp.queued[pb.Hash]; ok {
      for childHash := range children { omp.evict(childHash) } // Any children of this block that haven't been emitted never will be.
    }
  }
}

type ChainUpdate struct {
  added []*PendingBatch
  removed []*PendingBatch
}

func (omp *OrderedMessageProcessor) Subscribe(ch chan<- *ChainUpdate) types.Subscription {
  return omp.updateFeed.Subscribe(ch)
}

func (omp *OrderedMessageProcessor) SubscribeReorg(ch chan<- *ChainUpdate) types.Subscription {
  return omp.updateFeed.Subscribe(ch)
}

func (omp *OrderedMessageProcessor) HandlePendingBatch(pb *PendingBatch) {
  omp.pending[pb.Hash] = pb
  if omp.finished.Contains(pb.Hash) {
    // We've already handled this block. Repeats should be rare, given how the
    // MessageProcessor works, but they could happen in edge cases, and we
    // should ignore them.
    return
  } else if pb.ParentHash == omp.lastHash {
    // Emit now
    omp.prepareEmit([]*PendingBatch{pb}, []*PendingBatch{})
    return
  } else if omp.finished.Contains(pb.ParentHash) {
    if pb.Weight.Cmp(omp.lastWeight) > 0 {
      // Evaluate Reorg
      removed, added, err := omp.prepareReorg(pb, omp.pending[omp.lastHash])
      if err != nil {
        log.Error("Error finding common ancestor", "new", pb.Hash, "old", omp.lastHash, "error", err)
        return
      }
      omp.prepareEmit(added, removed)
      return
    }
    omp.finished.Add(pb.Hash, struct{}{})
    if child, ok := omp.getPendingChild(pb.Hash); ok {
      // log.Debug("Weight does not justify reorg, but child is available", "child", child)
      // This block's child is already pending, process it instead
      omp.HandlePendingBatch(omp.pending[child])
      return
    }
  } else {
    if pb.Weight.Cmp(omp.lastWeight) < 0 {
      // If the weight is less than the latest data, we can consider it
      // finished so that we can reorg back to it and it will eventuall get
      // evicted
      omp.finished.Add(pb.Hash, struct{}{})
    }
    // Queue for potential later use
    if _, ok := omp.queued[pb.ParentHash]; !ok { omp.queued[pb.ParentHash] = make(map[types.Hash]struct{}) }
    omp.queued[pb.ParentHash][pb.Hash] = struct{}{}
  }
}

func (omp *OrderedMessageProcessor) HandleReorg(num int64, hash types.Hash) {
  omp.lastHash = hash
  // omp.lastNumber = num
  omp.reorgFeed.Send(map[int64]types.Hash{num: hash})
}

func (omp *OrderedMessageProcessor) prepareEmit(new, old []*PendingBatch) {
  if len(new) > 0 {
    newest := new[len(new) - 1]
    omp.lastHash = newest.Hash
    omp.lastWeight = newest.Weight
    omp.finishSiblings(newest)
    omp.finished.Add(newest.Hash, struct{}{})
  }
  for hash, ok := omp.getPendingChild(omp.lastHash); ok; hash, ok = omp.getPendingChild(omp.lastHash) {
    pb := omp.pending[hash]
    if pb == nil { panic(fmt.Sprintf("Could not find a pending batch for %#x", hash)) }
    new = append(new, pb)
    omp.finishSiblings(pb)
    omp.lastHash = pb.Hash
    omp.lastWeight = pb.Weight
    omp.finished.Add(pb.Hash, struct{}{})
  }
  omp.updateFeed.Send(&ChainUpdate{added: new, removed: old})
}

func (omp *OrderedMessageProcessor) finishSiblings(pb *PendingBatch) {
  pendingSiblings, ok := omp.queued[pb.ParentHash]
  if !ok { return }
  for hash := range pendingSiblings {
    if hash != pb.Hash { omp.finishPendingChildren(hash) }
  }
}

func (omp *OrderedMessageProcessor) finishPendingChildren(hash types.Hash) {
  pendingChildren, ok := omp.queued[hash]
  omp.finished.Add(hash, struct{}{})
  if !ok { return }
  for child := range pendingChildren { omp.finishPendingChildren(child) }
}

// getPendingChild returns the hash of the child that parents the heaviest
// chain of descendants.
func (omp *OrderedMessageProcessor) getPendingChild(hash types.Hash) (types.Hash, bool) {
  children, ok := omp.queued[hash]
  if !ok {
    return types.Hash{}, ok
  }
  max := new(big.Int)
  var maxChild types.Hash
  for child := range children {
    td := omp.getPendingChainDifficulty(child)
    if max.Cmp(td) < 0 {
      max = td
      maxChild = child
    }
  }
  return maxChild, true
}

// getPendingChainDifficulty returns the weight of the heaviest chain
// descending from a given node.
func (omp *OrderedMessageProcessor) getPendingChainDifficulty(hash types.Hash) (*big.Int) {
  log.Debug("Getting pending chain difficulty", "block", hash, "pending", omp.queued[hash])
  children, ok := omp.queued[hash]
  if !ok { return omp.pending[hash].Weight }
  max := new(big.Int)
  for child := range children {
    weight := omp.getPendingChainDifficulty(child)
    if max.Cmp(weight) < 0 {
      max = weight
    }
  }
  return max
}

func (omp *OrderedMessageProcessor) prepareReorg(newHead, oldHead *PendingBatch) ([]*PendingBatch, []*PendingBatch, error) {
  reverted := []*PendingBatch{}
  newBlocks := []*PendingBatch{newHead}
  if oldHead == nil {
    return reverted, newBlocks, nil
  }
  for {
    for newHead.Number > oldHead.Number + 1 {
      parentHash := newHead.ParentHash
      newHead, _ = omp.pending[parentHash]
      if newHead == nil {
        return reverted, newBlocks, fmt.Errorf("Block %#x missing from history", parentHash)
      }
      newBlocks = append([]*PendingBatch{newHead}, newBlocks...)
    }
    if(oldHead.Hash == newHead.ParentHash)  {
      return reverted, newBlocks, nil
    }
    reverted = append([]*PendingBatch{oldHead}, reverted...)
    oldHead, _ = omp.pending[oldHead.ParentHash]
    if oldHead == nil {
      return reverted, newBlocks, fmt.Errorf("Reached genesis without finding common ancestor")
    }
  }
}

func (omp *OrderedMessageProcessor) ProcessMessage(m ResumptionMessage) error {
  return omp.mp.ProcessMessage(m)
}

func (omp *OrderedMessageProcessor) Close() {
  select {
  case omp.quit <- struct{}{}:
  case <-time.After(time.Second):
    log.Warn("Closing message processor not complete after 1 second")
  }
}
