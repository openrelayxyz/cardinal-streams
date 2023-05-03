package main

import (
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-streams/transports"
	ctypes "github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-evm/rlp"
	"github.com/openrelayxyz/cardinal-types/metrics"
    "github.com/openrelayxyz/cardinal-evm/schema"
    rtransports "github.com/openrelayxyz/cardinal-rpc/transports"
    etypes "github.com/openrelayxyz/cardinal-evm/types"
    "time"
    "strconv"
    "math/big"
    "regexp"
    "strings"
    "os"
    log "github.com/inconshreveable/log15"
)

var (
    blockAgeTimer = metrics.NewMinorTimer("/evm/age")
)

func BlockTime(pb *delivery.PendingBatch, chainid int64) *time.Time {
	if data, ok := pb.Values[string(schema.BlockHeader(chainid, pb.Hash.Bytes()))]; ok {
		header := etypes.Header{}
		if err := rlp.DecodeBytes(data, &header); err != nil { return nil }
		t := time.Unix(int64(header.Time), 0)
		return &t
	}
	return nil
}

func main() {
    log.Info("starting", "args", os.Args)
    brokerURL := os.Args[1]
    chainid, err := strconv.Atoi(os.Args[2])
    if err != nil { panic(err) }
    trackedPrefixes := []*regexp.Regexp{
		regexp.MustCompile("c/[0-9a-z]+/b/[0-9a-z]+/h"),
    }
    parts := strings.Split(brokerURL, ";")
	topics := strings.Split(parts[1], ",")

    consumer, err := transports.NewKafkaConsumer(parts[0], topics[0], topics, []byte{}, 0, 0, ctypes.Hash{}, new(big.Int), 128, trackedPrefixes, nil)
    if err != nil { panic(err) }
    ch := make(chan *delivery.ChainUpdate)
    consumer.Subscribe(ch)

    go func() {
        for cu := range ch {
            for _, pb := range cu.Added() {
                if bt := BlockTime(pb, int64(chainid)); bt != nil {
                    blockAgeTimer.UpdateSince(*bt)
                    log.Info("New block", "number", pb.Number, "age", time.Since(*bt))
                }
            }
        }
    }()
    consumer.Start()

    tm := rtransports.NewTransportManager(8)
    tm.AddHTTPServer(6968)
    tm.Register("debug", &metrics.MetricsAPI{})
    tm.Run(9998)
}
