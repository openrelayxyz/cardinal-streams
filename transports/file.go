package transports

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"io"
	"math/big"
	"os"
	"path"
	"compress/gzip"
	"strings"
	"time"
	"errors"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	log "github.com/inconshreveable/log15"
)

type fileProducer struct {
	encoder *json.Encoder
	file io.WriteCloser
	compressedFile io.WriteCloser
	expectedBatches map[types.Hash]types.Hash
}

func NewFileProducer(filepath string) (Producer, error) {
	fpath := strings.TrimPrefix(filepath, "file://")
	fileinfo, err := os.Lstat(fpath)
	if err != nil {
		return nil, err
	}
	if !fileinfo.Mode().IsDir() {
		return nil, errors.New("File producer must target an existing directory")
	}
	fname := fmt.Sprintf("%v.gz", time.Now().Unix())
	f, err := os.OpenFile(path.Join(fpath, fname), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	log.Info("Opening cardinal file stream", "path", path.Join(fpath, fname))
	gzw, _ := gzip.NewWriterLevel(f, gzip.BestCompression)
	return &fileProducer{
		encoder: json.NewEncoder(gzw),
		compressedFile: gzw,
		file: f,
		expectedBatches: make(map[types.Hash]types.Hash),
	}, nil
}

func (fp *fileProducer) LatestBlockFromFeed() (int64, error) {
	return 0, errors.New("not implemented")
}
// AddBlock will send information about a block over the transport layer.
func (fp *fileProducer) AddBlock(number int64, hash, parentHash types.Hash, weight *big.Int, updates map[string][]byte, deletes map[string]struct{}, batches map[string]types.Hash) error {
	updateData := make(map[string]hexutil.Bytes)
	deleteData := make([]string, 0, len(deletes))
	for k, v := range updates {
		updateData[k] = hexutil.Bytes(v)
	}
	for k := range deletes {
		deleteData = append(deleteData, k)
	}
	if err := fp.encoder.Encode(&resultMessage{
		Type: "batch",
		Batch: &TransportBatch{
			Number: hexutil.Uint64(uint64(number)),
			Weight: (*hexutil.Big)(weight),
			Hash: hash,
			ParentHash: parentHash,
			Batches: batches,
			Values: updateData,
			Deletes: deleteData,
		},
	}); err != nil { return err }
	for _, v := range batches {
		fp.expectedBatches[v] = hash
	}
	return nil
}
// SendBatch will send information about batches over the transport layer.
// Batches should correspond to batches indicated in a previous AddBlock call
func (fp *fileProducer) SendBatch(batchid types.Hash, deletes []string, updates map[string][]byte) error {
	hash, ok := fp.expectedBatches[batchid]
	if !ok { return delivery.ErrUnknownBatch}
	updateData := make(map[string]hexutil.Bytes)
	for k, v := range updates {
		updateData[k] = hexutil.Bytes(v)
	}
	err := fp.encoder.Encode(&resultMessage{
		Type: "subbatch",
		SubBatch: &transportSubbatch{
			Hash: hash,
			BatchId: batchid,
			Values: updateData,
			Deletes: deletes,
		},
	})
	delete(fp.expectedBatches, batchid)
	return err
}
// Reorg will send information about large chain reorgs over the transport
// layer. The "done" function returned by the Reorg() method should be called
// after all blocks and batches for a given reorg have been sent to the
// producer.
func (fp *fileProducer) Reorg(number int64, hash types.Hash) (func(), error) {

	err := fp.encoder.Encode(&resultMessage{
		Type: "reorg",
		Batch: &TransportBatch{
			Hash: hash,
			Number: hexutil.Uint64(number),
		},
	})
	return func() {}, err
}

func (*fileProducer) ProducerCount(time.Duration) uint { return 0 }

func (*fileProducer) PurgeReplayCache() {}

func (*fileProducer) SetHealth(bool) {}



func (fp *fileProducer) Close() {
	fp.compressedFile.Close()
	fp.file.Close()
}


type fileConsumer struct {
	path string
	omp *delivery.OrderedMessageProcessor
	chainUpdates types.Feed
	pendingBatches types.Feed
	reorgs types.Feed
	ready chan struct{}
	quit bool
	lastNum hexutil.Uint64
	lastHash types.Hash
}

func newFileConsumer(omp *delivery.OrderedMessageProcessor, url string, lastNum int64, lastHash types.Hash) (Consumer, error) {
	return &fileConsumer{path: strings.TrimPrefix(url, "cardinal://"), omp: omp, ready: make(chan struct{}), lastNum: hexutil.Uint64(lastNum), lastHash: lastHash}, nil
}

func (fc *fileConsumer) Start() error {
	fpath := strings.TrimPrefix(fc.path, "file://")
	fileinfo, err := os.Lstat(fpath)
	if err != nil {
		return err
	}
	if !fileinfo.Mode().IsDir() {
		return errors.New("File consumer must target an existing directory")
	}
	go func () {
		batches := make(map[types.Hash]*delivery.PendingBatch)
		subbatches := make(map[types.Hash]*transportSubbatch)
		pendingSubbatches := make(map[types.Hash]map[types.Hash]struct{})
		err := filepath.Walk(fc.path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
	
			if info.IsDir() {
				return nil
			}
	
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
	
			gzipReader, err := gzip.NewReader(file)
			if err != nil {
				return err
			}
			defer gzipReader.Close()
	
			// Read and process each line
			decoder := json.NewDecoder(gzipReader)
			for {
				var item resultMessage
				if err := decoder.Decode(&item); err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				switch item.Type {
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
						fc.omp.ProcessCompleteBatch(pb)
						fc.lastNum = hexutil.Uint64(pb.Number)
						fc.lastHash = pb.Hash
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
						delete(batches, item.SubBatch.Hash)
						fc.omp.ProcessCompleteBatch(pb)
						fc.lastNum = hexutil.Uint64(pb.Number)
						fc.lastHash = pb.Hash
					}
				}
			}
	
			return nil
		})
		if err != nil {
			log.Error("Error walking file", "err", err)
		}

	}()
	return err
}
func (fc *fileConsumer) Subscribe(ch interface{}) types.Subscription {
	return fc.omp.Subscribe(ch)
}
func (fc *fileConsumer) SubscribeReorg(ch chan<- map[int64]types.Hash) types.Subscription {
	return fc.omp.SubscribeReorg(ch)
}
func (fc *fileConsumer) Close() {
	fc.quit = true
}
func (fc *fileConsumer) Ready() <-chan struct{} {
	return fc.ready
}
func (fc *fileConsumer) WhyNotReady(types.Hash) string {
	return "unknown"
}
func (fc *fileConsumer) ProducerCount(time.Duration) uint {
	return 0
}
