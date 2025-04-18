package transports

import (
  "math/big"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-streams/v2/waiter"
  "time"
  // "github.com/openrelayxyz/cardinal-streams/delivery"
)

type ProducerCounter interface {
  ProducerCount(time.Duration) uint
}

// Producer can be used to send block metadata over a messaging transport.
type Producer interface {
  ProducerCounter
  LatestBlockFromFeed() (int64, error)
  // AddBlock will send information about a block over the transport layer.
  AddBlock(number int64, hash, parentHash types.Hash, weight *big.Int, updates map[string][]byte, deletes map[string]struct{}, batches map[string]types.Hash) error
  // SendBatch will send information about batches over the transport layer.
  // Batches should correspond to batches indicated in a previous AddBlock call
  SendBatch(batchid types.Hash, delete []string, update map[string][]byte) error
  // Reorg will send information about large chain reorgs over the transport
  // layer. The "done" function returned by the Reorg() method should be called
  // after all blocks and batches for a given reorg have been sent to the
  // producer.
  Reorg(number int64, hash types.Hash) (func(), error)
  // SetHealth allows producers to mark that they are in an unhealthy state and not currently producing
  SetHealth(bool)

  // Producers track which blocks they have seen to avoid replaying them. If applications
  // intend to replay blocks, they should call this function first.
  PurgeReplayCache()
}

// Consumer can be used to receive messages over a transport layer.
type Consumer interface {
  ProducerCounter
  // Start sets up communication with the broker and begins processing
  // messages. If you want to ensure receipt of 100% of messages, you should
  // call Start() only after setting up subscriptions with Subscribe()
  Start() error
  // Subscribe enables subscribing to either oredred chain updates or unordered
  // pending batches. Calling Subscribe on a chan *ChainUpdate will return a
  // subscription for ordered chain updates. Calling subscribe on a
  // *PendingBatch will return a subscription for unordered pending batches.
  Subscribe(ch interface{}) types.Subscription
  // SubscribeReorg subscribes to information about large chain reorgs.
  SubscribeReorg(ch chan<- map[int64]types.Hash) types.Subscription
  //Waiter returns a waiter.Waiter that can be used to wait for blocks by hash or number
  Waiter() waiter.Waiter
  // Close shuts down the transport layer, which in turn will cause
  // subscriptions to stop producing messages.
  Close()
  Ready() <-chan struct{}
  WhyNotReady(types.Hash) string
}
