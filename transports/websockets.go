package transports

import (
	"encoding/json"
	"fmt"
	"math/big"
	"github.com/gorilla/websocket"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-rpc"
	"github.com/openrelayxyz/cardinal-rpc/transports"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	log "github.com/inconshreveable/log15"
	"context"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type StreamsResumption interface {
	// BlocksFrom produces PendingBatches. This stream does not deal with
	// subbatches, so the PendingBatches must include all values. BlocksFrom
	// should watch for context.Done() and stop producing blocks if the context
	// finishes before
  BlocksFrom(ctx context.Context, block uint64, hash types.Hash) (chan *delivery.PendingBatch, error)
  GetBlock(ctx context.Context, block uint64) (*delivery.PendingBatch)
}

type websocketProducer struct {
	feed types.Feed
	resumer StreamsResumption
	expectedBatches map[types.Hash]types.Hash
}

type resultMessage struct {
	Type string `json:"type"`
	Batch *transportBatch `json:"batch,omitempty"`
	SubBatch *transportSubbatch `json:"batch,omitempty"`
}

type transportBatch struct {
	Number hexutil.Uint64 `json:"number"`
	Weight *hexutil.Big `json:"weight"`
	Hash types.Hash `json:"hash"`
	ParentHash types.Hash `json:"parent"`
	Values map[string]hexutil.Bytes `json:"values"`
	Deletes []string `json:"deletes"`
	Batches map[string]types.Hash `json:"batches"`
}

type transportSubbatch struct {
	Hash types.Hash `json:"hash"`
	BatchId types.Hash `json:"batchid"`
	Values map[string]hexutil.Bytes `json:"values"`
	Deletes []string `json:"deletes"`
}

type reorgData struct {
	Hash types.Hash `json:"hash"`
	Number hexutil.Uint64 `json:"number"`
}

func NewWebsocketProducer(wsurl string, resumer StreamsResumption) (Producer, error) {
	if resumer == nil {
		return nil, fmt.Errorf("websockets producer requires a non-nil resumer")
	}
	parsedUrl, err := url.Parse(wsurl)
	if err != nil {
		return nil, err
	}
	switch parsedUrl.Scheme {
	case "wss", "ws":
	default:
		return nil, fmt.Errorf("unknown protocol")
	}
	p := &websocketProducer{
		resumer: resumer,
		expectedBatches: make(map[types.Hash]types.Hash),
	}
	port, err := strconv.Atoi(parsedUrl.Port())
	if err != nil { return nil, err }
	return p, p.Serve(int64(port))
}

func (p *websocketProducer) Serve(port int64) error {
	tm := transports.NewTransportManager(32)
	tm.Register("cardinal", p.Service())
	return tm.Run(0)
}

func (p *websocketProducer) Service() interface{} {
	return websocketStreamsService{
		feed: p.feed,
	}
}

func (p *websocketProducer) LatestBlockFromFeed() (int64, error) {
	return 0, fmt.Errorf("not implemented")
}
func (p *websocketProducer) AddBlock(number int64, hash, parentHash types.Hash, weight *big.Int, updates map[string][]byte, deletes map[string]struct{}, batches map[string]types.Hash) error {
	updateData := make(map[string]hexutil.Bytes)
	deleteData := make([]string, 0, len(deletes))
	for k, v := range updates {
		updateData[k] = hexutil.Bytes(v)
	}
	for k := range deletes {
		deleteData = append(deleteData, k)
	}
	p.feed.Send(&resultMessage{
		Type: "batch",
		Batch: &transportBatch{
			Number: hexutil.Uint64(uint64(number)),
			Weight: (*hexutil.Big)(weight),
			Hash: hash,
			ParentHash: parentHash,
			Batches: batches,
			Values: updateData,
			Deletes: deleteData,
		},
	})
	for _, v := range batches {
		p.expectedBatches[v] = hash
	}
	return nil
}
func (p *websocketProducer) SendBatch(batchid types.Hash, deletes []string, updates map[string][]byte) error {
	hash, ok := p.expectedBatches[batchid]
	if !ok { return delivery.ErrUnknownBatch}
	updateData := make(map[string]hexutil.Bytes)
	for k, v := range updates {
		updateData[k] = hexutil.Bytes(v)
	}
	p.feed.Send(&resultMessage{
		Type: "subbatch",
		SubBatch: &transportSubbatch{
			Hash: hash,
			BatchId: batchid,
			Values: updateData,
			Deletes: deletes,
		},
	})
	delete(p.expectedBatches, batchid)
	return nil
}
func (p *websocketProducer) Reorg(number int64, hash types.Hash) (func(), error) {
	p.feed.Send(&resultMessage{
		Type: "reorg",
		Batch: &transportBatch{
			Hash: hash,
			Number: hexutil.Uint64(number),
		},
	})
	return func() {}, nil
}

type websocketStreamsService struct {
	feed types.Feed
	resumer StreamsResumption
}

func (s *websocketStreamsService) StreamsBlock(ctx context.Context, number hexutil.Uint64) (*resultMessage, error) {
	block := s.resumer.GetBlock(ctx, uint64(number))
	if block != nil { return nil, fmt.Errorf("block not found") }
	values := make(map[string]hexutil.Bytes)
	deletes := make([]string, 0, len(block.Deletes))
	for k, v := range block.Values {
		values[k] = hexutil.Bytes(v)
	}
	for k, _ := range block.Deletes {
		deletes = append(deletes, k)
	}
	return &resultMessage{
		Type: "batch",
		Batch: &transportBatch{
			Number: hexutil.Uint64(uint64(block.Number)),
			Weight: (*hexutil.Big)(block.Weight),
			Hash: block.Hash,
			ParentHash: block.ParentHash,
			Batches: make(map[string]types.Hash),
			Values: values,
			Deletes: deletes,
		},
	}, nil
}

func (s *websocketStreamsService) Streams(ctx context.Context, number hexutil.Uint64, hash types.Hash) (<-chan *resultMessage, error) {
	initBlocks, err := s.resumer.BlocksFrom(ctx, uint64(number), hash)
	if err != nil {
		return nil, err
	}
	subch := make(chan *resultMessage, 1000)
	sub := s.feed.Subscribe(subch)
	go func() {
		for block := range initBlocks {
			values := make(map[string]hexutil.Bytes)
			deletes := make([]string, 0, len(block.Deletes))
			for k, v := range block.Values {
				values[k] = hexutil.Bytes(v)
			}
			for k, _ := range block.Deletes {
				deletes = append(deletes, k)
			}
			s.feed.Send(&resultMessage{
				Type: "batch",
				Batch: &transportBatch{
					Number: hexutil.Uint64(uint64(block.Number)),
					Weight: (*hexutil.Big)(block.Weight),
					Hash: block.Hash,
					ParentHash: block.ParentHash,
					Batches: make(map[string]types.Hash),
					Values: values,
					Deletes: deletes,
				},
			})
		}
		s.feed.Send(&resultMessage{
			Type: "ready",
		})
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				sub.Unsubscribe()
				close(subch)
				return
			case <-sub.Err():
				sub.Unsubscribe()
				close(subch)
				return
			}
		}
	}()
	return subch, nil
}


type websocketConsumer struct {
	url string
	omp *delivery.OrderedMessageProcessor
	chainUpdates types.Feed
	pendingBatches types.Feed
	reorgs types.Feed
	conn *websocket.Conn
	ready chan struct{}
	quit bool
	lastNum hexutil.Uint64
	lastHash types.Hash
}

func newWebsocketConsumer(omp *delivery.OrderedMessageProcessor, url string, lastNum int64, lastHash types.Hash) (Consumer, error) {
	return &websocketConsumer{url: strings.TrimPrefix(url, "cardinal://"), omp: omp, ready: make(chan struct{}), lastNum: hexutil.Uint64(lastNum), lastHash: lastHash}, nil
}

func (c *websocketConsumer) Start() error {
	var err error
	connected := make(chan struct{})
	isReady := false
	go func() {
		for !c.quit {
			c.conn, _, err = websocket.DefaultDialer.Dial(c.url, nil)
			if err != nil {
				// If we can't connect, don't slam the server with connection attempts
				log.Warn("Dial failed", "url", c.url, "error", err)
				time.Sleep(time.Second)
				continue
			}
			batches := make(map[types.Hash]*delivery.PendingBatch)
			subbatches := make(map[types.Hash]*transportSubbatch)
			pendingSubbatches := make(map[types.Hash]map[types.Hash]struct{})
			call, err := rpc.NewCall("cardinal_subscribe", "streams", c.lastNum, c.lastHash)
			if err != nil {
				log.Warn("Subscribe failed", "err", err)
				time.Sleep(time.Second)
				continue
			}
			callData, _ := json.Marshal(call)
			c.conn.WriteMessage(websocket.TextMessage, callData)
			var subid interface{}
			for subid == nil {
				_, message, err := c.conn.ReadMessage()
				if err != nil {
					log.Warn("Read error. Resetting Connection.", "err", err)
					time.Sleep(time.Second)
					continue
				}
				var response rpc.Response
				if err := json.Unmarshal(message, &response); err != nil {
					log.Warn("Server responded with invalid json", "err", err, "response", string(message))
					time.Sleep(time.Second)
					continue
				}
				if string(response.ID) == string(call.ID) {
					subid = response.Result
				} else {
					log.Warn("Unexpected response", "wanted", call.ID, "got", response.ID)
				}
			}
			var notification rpc.SubscriptionResponseRaw
			for {
				_, message, err := c.conn.ReadMessage()
				if err == nil {
					log.Debug("Read error. Resetting connection.", "err", err)
					break
				}
				if len(message) == 0 {
					// Probably a ping message
					continue
				}
				json.Unmarshal(message, &notification)
				if notification.Method != "cardinal_subscribe" {
					log.Warn("Unexpected message on channel", "message", string(message))
					continue
				}
				if notification.Params.ID != subid {
					log.Warn("Unexpected subscription id", "wanted", subid, "got", notification.Params)
					continue
				}
				var item resultMessage
				json.Unmarshal(notification.Params.Result, &item)
				switch item.Type {
				case "ready":
					if !isReady {
						isReady = true
						close(c.ready)
					}
				case "reorg":
					c.omp.HandleReorg(int64(item.Batch.Number), item.Batch.Hash)
				case "batch":
					pb := &delivery.PendingBatch{
						Number: int64(item.Batch.Number),
						Weight: item.Batch.Weight.ToInt(),
						Hash: item.Batch.Hash,
						ParentHash: item.Batch.ParentHash,
						Values: make(map[string][]byte),
						Deletes: make(map[string]struct{}),
					}
					for k, v := range item.Batch.Values {
						pb.Values[k] = []byte(v)
					}
					for _, k := range item.Batch.Deletes {
						pb.Deletes[k] = struct{}{}
					}
					pendingSubbatches[pb.Hash] = make(map[types.Hash]struct{})
					for _, batchid := range item.Batch.Batches {
						if sb, ok := subbatches[batchid]; ok {
							for k, v := range sb.Values {
								pb.Values[k] = v
							}
							for _, k := range sb.Deletes {
								pb.Deletes[k] = struct{}{}
							}
							delete(subbatches, batchid)
						} else {
							batches[pb.Hash] = pb
							pendingSubbatches[pb.Hash][batchid] = struct{}{}
						}
					}
					if len(pendingSubbatches[pb.Hash]) == 0 {
						delete(pendingSubbatches, pb.Hash)
						c.omp.ProcessCompleteBatch(pb)
						c.lastNum = hexutil.Uint64(pb.Number)
						c.lastHash = pb.Hash
					}
				case "subbatch":
					pb, ok := batches[item.SubBatch.Hash]
					if !ok {
						subbatches[item.SubBatch.BatchId] = item.SubBatch
						continue
					}
					for k, v := range item.SubBatch.Values {
						pb.Values[k] = v
					}
					for _, k := range item.SubBatch.Deletes {
						pb.Deletes[k] = struct{}{}
					}
					delete(subbatches, item.SubBatch.BatchId)
					delete(pendingSubbatches[item.SubBatch.Hash], item.SubBatch.BatchId)
					if len(pendingSubbatches[item.SubBatch.Hash]) == 0 {
						delete(pendingSubbatches, item.SubBatch.Hash)
						c.omp.ProcessCompleteBatch(pb)
						c.lastNum = hexutil.Uint64(pb.Number)
						c.lastHash = pb.Hash
					}
				}
			}
		}
	}()
	<-connected
	return nil
}
func (c *websocketConsumer) Subscribe(ch interface{}) types.Subscription {
	return c.omp.Subscribe(ch)
}
func (c *websocketConsumer) SubscribeReorg(ch chan<- map[int64]types.Hash) types.Subscription {
	return c.omp.SubscribeReorg(ch)
}
func (c *websocketConsumer) Close() {
	c.quit = true
	c.conn.Close()
}
func (c *websocketConsumer) Ready() <-chan struct{} {
	return c.ready
}
func (c *websocketConsumer) WhyNotReady(types.Hash) string {
	return "unknown"
}
