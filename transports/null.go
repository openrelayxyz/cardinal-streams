package transports

import (
  "time"
  "github.com/openrelayxyz/cardinal-types"
  )

type nullConsumer struct{}

func NewNullConsumer() Consumer { return &nullConsumer{} }

func (*nullConsumer) Start() error {
  return nil
}
func (*nullConsumer) Subscribe(ch interface{}) types.Subscription {
  return nullSubscription{}
}
func (*nullConsumer) SubscribeReorg(ch chan<- map[int64]types.Hash) types.Subscription {
  return nullSubscription{}
}
func (*nullConsumer) Close() {}

func (*nullConsumer) Ready() <-chan struct{} {
  ch := make(chan struct{}, 1)
  ch <- struct{}{}
  return ch
}
func (*nullConsumer) WhyNotReady(hash types.Hash) string {
  return "null"
}
func (*nullConsumer) ProducerCount(time.Duration) uint {
  return 0
}


type nullSubscription struct{}

func (nullSubscription) Unsubscribe() {}
func (nullSubscription) Err() <-chan error {
  return make(chan error)
}
