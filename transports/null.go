package transports

import (
  "time"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-streams/waiter"
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

type nullWaiter struct{}

func (nullWaiter) WaitForHash(types.Hash, time.Duration) waiter.WaitResult {
  return waiter.NotFound
}
func (nullWaiter) WaitForNumber(int64, time.Duration) waiter.WaitResult {
  return waiter.NotFound
}
func (nullWaiter) Stop() {}

func (*nullConsumer) Waiter() waiter.Waiter {
  return nullWaiter{}
}

type nullSubscription struct{}

func (nullSubscription) Unsubscribe() {}
func (nullSubscription) Err() <-chan error {
  return make(chan error)
}
