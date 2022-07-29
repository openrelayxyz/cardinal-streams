package transports

import (
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"path"
	"compress/gzip"
	"time"
	"errors"
	"github.com/openrelayxyz/cardinal-streams/delivery"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
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
	f, err := os.OpenFile(path.Join(fpath, fmt.Sprintf("%v.gz", time.Now().Unix())), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
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

func (fp *fileProducer) Close() {
	fp.compressedFile.Close()
	fp.file.Close()
}
