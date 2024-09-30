package main

import (
  "math/big"
  "encoding/json"
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
  flag.CommandLine.Parse(os.Args[1:])
  log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.Root().GetHandler()))
  if *brokerURL == "" {
    log.Crit("--cardinal.broker.url flag is required")
    os.Exit(1)
  }
  log.Info("About to set up consumer")
  trackedPrefixes := []*regexp.Regexp{
    regexp.MustCompile(".*"),
  }
  args := flag.CommandLine.Args()
  if len(args) == 0 {
    log.Crit("requires at least 1 argument")
    os.Exit(1)
  }
  consumer, err := transports.ResolveConsumer(*brokerURL, args[0], args, []byte{}, -1, 0, types.Hash{}, big.NewInt(0), 128, trackedPrefixes, nil)
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

  go func(ch chan *delivery.ChainUpdate) {
    log.Info("Consumer goroutine initiated")
    for update := range ch {
      result := make(map[string]interface{})
      result["added"] = update.Added()
      result["removed"] = update.Removed()
      data, _ := json.Marshal(result)
      fmt.Println(string(data))
    }
  }(ch)


  <-consumer.Ready()
  consumer.Close()
  time.Sleep(5)
}
