package main

import (
  "math/big"
  "regexp"
  "flag"
  "fmt"
  "os"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-streams/transports"
  "github.com/openrelayxyz/cardinal-streams/delivery"
  log "github.com/inconshreveable/log15"
  "time"
)

func main() {
  brokerURL := flag.String("cardinal.broker.url", "", "URL of the Cardinal Broker")
  defaultTopic := flag.String("cardinal.default.topic", "", "Default topic for Cardinal broker")
  blockTopic := flag.String("cardinal.block.topic", "", "Topic for Cardinal block data")
  logTopic := flag.String("cardinal.logs.topic", "", "Topic for Cardinal log data")
  txTopic := flag.String("cardinal.tx.topic", "", "Topic for Cardinal transaction data")
  receiptTopic := flag.String("cardinal.receipt.topic", "", "Topic for Cardinal receipt data")
  codeTopic := flag.String("cardinal.code.topic", "", "Topic for Cardinal contract code")
  stateTopic := flag.String("cardinal.state.topic", "", "Topic for Cardinal state data")
  re := flag.String("regex", ".*", "Regular expression to search for")
  flag.CommandLine.Parse(os.Args[1:])
  log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.Root().GetHandler()))
  if *brokerURL == "" {
    log.Crit("--cardinal.broker.url flag is required")
    os.Exit(1)
  }
  if *defaultTopic == "" {
    log.Crit("--cardinal.default.topic flag is required")
    os.Exit(1)
  }
  regex, err := regexp.Compile(*re)
  if err != nil {
    log.Crit("Invalid value for --regexp")
    os.Exit(1)
  }
  if *blockTopic == "" { *blockTopic = fmt.Sprintf("%v-block", *defaultTopic) }
  if *logTopic == "" { *logTopic = fmt.Sprintf("%v-logs", *defaultTopic) }
  if *txTopic == "" { *txTopic = fmt.Sprintf("%v-tx", *defaultTopic) }
  if *receiptTopic == "" { *receiptTopic = fmt.Sprintf("%v-receipt", *defaultTopic) }
  if *codeTopic == "" { *codeTopic = fmt.Sprintf("%v-code", *defaultTopic) }
  if *stateTopic == "" { *stateTopic = fmt.Sprintf("%v-state", *defaultTopic) }
  log.Info("About to set up consumer")
  trackedPrefixes := []*regexp.Regexp{
    regexp.MustCompile("c/[0-9a-z]+/a/"),
    regexp.MustCompile("c/[0-9a-z]+/s"),
  }
  consumer, err := transports.ResolveConsumer(*brokerURL, *defaultTopic, []string{*defaultTopic, *codeTopic, *stateTopic}, []byte{}, -1, 0, types.Hash{}, big.NewInt(0), 128, trackedPrefixes, nil)
  if err != nil {
    log.Crit("Error resolving consumer", "err", err.Error())
    os.Exit(1)
  }
  log.Info("Consumer set up")
  ch := make(chan *delivery.ChainUpdate)
  consumer.Subscribe(ch)
  log.Info("Channel subscribed")
  consumer.Start()
  log.Info("Consumer started")

  go func(ch chan *delivery.ChainUpdate, regex *regexp.Regexp) {
    log.Info("Consumer goroutine initiated")
    for update := range ch {
      for _, block := range update.Removed() {
        for k, v := range block.Values {
          if regex.MatchString(k) {
            log.Info("Match in removed block", "blockno", block.Number, "hash", block.Hash, "key", k, "value", fmt.Sprintf("%#x", v))
          }
        }
        for k := range block.Deletes {
          if regex.MatchString(k) {
            log.Info("Deleted match in removed block", "blockno", block.Number, "hash", block.Hash, "key", k)
          }
        }
      }
      for _, block := range update.Added() {
        for k, v := range block.Values {
          if regex.MatchString(k) {
            log.Info("Match in added block", "blockno", block.Number, "hash", block.Hash, "key", k, "value", fmt.Sprintf("%#x", v))
          }
        }
        for k := range block.Deletes {
          if regex.MatchString(k) {
            log.Info("Deleted match in added block", "blockno", block.Number, "hash", block.Hash, "key", k)
          }
        }
      }
    }
  }(ch, regex)


  <-consumer.Ready()
  consumer.Close()
  time.Sleep(5)
}
