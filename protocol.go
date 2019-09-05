// Copyright (c) 2019 Perlin
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package wavelet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/djherbis/buffer"
	"github.com/perlin-network/wavelet/log"
	"github.com/perlin-network/wavelet/sys"
	"github.com/pkg/errors"
	"golang.org/x/crypto/blake2b"
)

type Protocol struct {
	ledger *Ledger
}

func (p *Protocol) Gossip(stream Wavelet_GossipServer) error {
	for {
		batch, err := stream.Recv()

		if err != nil {
			return err
		}

		for _, buf := range batch.Transactions {
			tx, err := UnmarshalTransaction(bytes.NewReader(buf))

			if err != nil {
				logger := log.TX("gossip")
				logger.Err(err).Msg("Failed to unmarshal transaction")
				continue
			}

			if err := p.ledger.AddTransaction(tx); err != nil && errors.Cause(err) != ErrMissingParents {
				fmt.Printf("error adding incoming tx to graph [%v]: %+v\n", err, tx)
			}
		}
	}
}

func (p *Protocol) Query(ctx context.Context, req *QueryRequest) (*QueryResponse, error) {
	res := &QueryResponse{}

	round, err := p.ledger.rounds.GetByIndex(req.RoundIndex)
	if err == nil {
		res.Round = round.Marshal()
		return res, nil
	}

	preferred := p.ledger.finalizer.Preferred()
	if preferred != nil {
		res.Round = preferred.(*Round).Marshal()
		return res, nil
	}

	return res, nil
}

func (p *Protocol) Sync(stream Wavelet_SyncServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	res := &SyncResponse{}
	header := &SyncInfo{LatestRound: p.ledger.rounds.Latest().Marshal()}

	diffBuffer := buffer.NewMulti(buffer.New(sys.SyncMaxChunkMemory), buffer.NewPartition(p.ledger.filePool))

	start := time.Now()

	if err := p.ledger.accounts.Snapshot().DumpDiff(req.GetRoundId(), diffBuffer); err != nil {
		return err
	}

	elapsed := time.Now().Sub(start)
	fmt.Println("[sender] dumping diff took", elapsed)

	start = time.Now()

	// Chunk dumped diff
	var chunkBuf [sys.SyncChunkSize]byte
	var diffSize int
	for {
		n, err := diffBuffer.Read(chunkBuf[:])
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, chunkBuf[:n])

			checksum := blake2b.Sum256(chunk)
			p.ledger.chunks.Put(checksum, chunk)
			header.Checksums = append(header.Checksums, checksum[:])
			diffSize += len(chunk)
		}

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}

	elapsed = time.Now().Sub(start)
	fmt.Println("[sender] chunks:", len(header.Checksums))
	fmt.Println("[sender] chunking diff took", elapsed)
	fmt.Printf("[sender] diff dump is %.2f MB\n", float64(diffSize)/(1024.0*1024.0))

	res.Data = &SyncResponse_Header{Header: header}

	if err := stream.Send(res); err != nil {
		return err
	}

	res.Data = &SyncResponse_Chunk{}

	for {
		req, err := stream.Recv()

		if err != nil {
			return err
		}

		var checksum [blake2b.Size256]byte
		copy(checksum[:], req.GetChecksum())

		chunk, err := p.ledger.chunks.Get(checksum)
		if err != nil {
			return err
		}

		logger := log.Sync("provide_chunk")
		logger.Info().
			Hex("requested_hash", req.GetChecksum()).
			Msg("Responded to sync chunk request.")

		res.Data.(*SyncResponse_Chunk).Chunk = chunk

		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (p *Protocol) CheckOutOfSync(ctx context.Context, req *OutOfSyncRequest) (*OutOfSyncResponse, error) {
	return &OutOfSyncResponse{
		OutOfSync: p.ledger.rounds.Latest().Index >= sys.SyncIfRoundsDifferBy+req.RoundIndex,
	}, nil
}

func (p *Protocol) DownloadMissingTx(ctx context.Context, req *DownloadMissingTxRequest) (*DownloadTxResponse, error) {
	res := &DownloadTxResponse{Transactions: make([][]byte, 0, len(req.Ids))}

	for _, buf := range req.Ids {
		var id TransactionID
		copy(id[:], buf)

		if tx := p.ledger.Graph().FindTransaction(id); tx != nil {
			res.Transactions = append(res.Transactions, tx.Marshal())
		}
	}

	return res, nil
}

func (p *Protocol) DownloadTx(ctx context.Context, req *DownloadTxRequest) (*DownloadTxResponse, error) {
	lowLimit := req.Depth - sys.MaxDepthDiff
	highLimit := req.Depth + sys.MaxDownloadDepthDiff

	receivedIDs := make(map[TransactionID]struct{}, len(req.SkipIds))
	for _, buf := range req.SkipIds {
		var id TransactionID
		copy(id[:], buf)

		receivedIDs[id] = struct{}{}
	}

	var txs [][]byte
	hostTXs := p.ledger.Graph().GetTransactionsByDepth(&lowLimit, &highLimit)
	for _, tx := range hostTXs {
		if _, ok := receivedIDs[tx.ID]; !ok {
			txs = append(txs, tx.Marshal())
		}
	}

	return &DownloadTxResponse{Transactions: txs}, nil
}
