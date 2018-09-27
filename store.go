// Copyright © 2018 J. Strobus White.
// This file is part of the blocktop blockchain development kit.
//
// Blocktop is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Blocktop is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with blocktop. If not, see <http://www.gnu.org/licenses/>.

package storeipfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strconv"

	cid "gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	cbor "gx/ipfs/QmPrv66vmh2P7vLJMpYx6DWLTNKvVB4Jdkyxs6V3QvWKvf/go-ipld-cbor"

	spec "github.com/blocktop/go-spec"
	"github.com/ipfs/go-ipfs/core"
	coreiface "github.com/ipfs/go-ipfs/core/coreapi/interface"
	"github.com/ipfs/go-ipfs/core/coreapi/interface/options"
)

type store struct {
	opened     bool
	Hash       string
	root       *node
	api        coreiface.CoreAPI
	ipfs       *core.IpfsNode
	MerkleTree *merkleTreeStruct
	batch      *batch
	rootFile   string
}

// ensure that store fulfills the interface specification
var _ spec.Store = (*store)(nil)

type node struct {
	data         []byte
	links        map[string]*link
	cnode        *cbor.Node
	path         coreiface.ResolvedPath
	changedLinks map[string]bool
	changedData  bool
	fromIPFS     bool
}

type link struct {
	key        string
	targetNode *node
	targetCid  cid.Cid
}

type blockHeader struct {
	blockID       string
	parentBlockID string
	blockNumber   uint64
}

const dagBatchSize = 700

func (s *store) OpenBlock(blockNumber uint64) error {
	if ok, _ := s.IsOpen(); ok {
		return errors.New("store is already open")
	}

	s.opened = true

	merkleRoot, err := s.MerkleTree.StartBatch()
	if err != nil {
		return err
	}

	s.batch = &batch{
		merkleRoot:  merkleRoot,
		blockNumber: blockNumber}

	return nil
}

func (s *store) IsOpen() (bool, uint64) {
	if !s.opened {
		return false, ^uint64(0)
	}
	return true, s.batch.blockNumber
}

func (s *store) Close() {
	s.ipfs.Close()
	Store = nil
}

func (s *store) GetRoot() string {
	return s.root.cnode.String()
}

func (s *store) SubmitBlock(ctx context.Context, block spec.Block) (string, error) {
	if ok, _ := s.IsOpen(); !ok {
		return "", errors.New("store is not currently open")
	}
	if block.GetBlockNumber() != s.batch.blockNumber {
		return "", errors.New("store was open for a different block number")
	}

	txns := block.GetTransactions()
	txnodes := make(map[string]*link, len(txns))
	for i, t := range txns {

		parties := t.Parties()
		prtynodes := make(map[string]*link, len(parties))
		for role, acct := range parties {
			anode, err := makeNodeFromAccount(acct)
			if err != nil {
				return "", err
			}
			prtynodes[role] = &link{key: role, targetNode: anode}
			err = s.MerkleTree.PutLink(ctx, makeAccountKey(acct), &link{key: "acct", targetNode: anode})
			if err != nil {
				return "", err
			}
		}

		tnode, err := makeNodeFromTransaction(t, prtynodes)
		if err != nil {
			return "", err
		}
		k := strconv.FormatInt(int64(i), 10)
		txnodes[k] = &link{key: "txn" + k, targetNode: tnode}

		err = s.MerkleTree.PutLink(ctx, makeTransactionKey(t), &link{key: "txn", targetNode: tnode})
		if err != nil {
			return "", err
		}
		for role, acct := range parties {
			err = s.MerkleTree.PutLink(ctx, makeAccountTransactionKey(acct, role), &link{key: t.GetID(), targetNode: tnode})
			if err != nil {
				return "", err
			}
		}
	}

	bnode, err := makeNodeFromBlock(block, txnodes)
	if err != nil {
		return "", err
	}
	err = s.MerkleTree.PutLink(ctx, makeBlockKey(block), &link{key: "blk", targetNode: bnode})
	if err != nil {
		return "", err
	}
	for _, t := range txns {
		err = s.MerkleTree.PutLink(ctx, makeTransactionBlockKey(t), &link{key: "blk", targetNode: bnode})
		if err != nil {
			return "", err
		}
	}

	bh := &blockHeader{
		blockID:       block.GetID(),
		parentBlockID: block.GetParentID(),
		blockNumber:   block.GetBlockNumber()}

	data, err := blockHeaderToBytes(bh)
	if err != nil {
		return "", err
	}

	links := map[string]*link{
		"parent": &link{key: "parent", targetNode: s.root},
		"block":  &link{key: "block", targetNode: bnode},
		"merkle": &link{key: "merkle", targetNode: s.batch.merkleRoot}}

	bhnode, err := makeNodeFromObj(data, links)
	if err != nil {
		return "", err
	}

	bhnode.changedData = true
	bhnode.changedLinks["block"] = true
	bhnode.changedLinks["merkle"] = true

	s.batch.blockHeader = bhnode

	return bhnode.cnode.String(), nil
}

func (s *store) Commit(ctx context.Context) error {
	if ok, _ := s.IsOpen(); !ok {
		return errors.New("store is not currently open")
	}
	if s.batch.blockHeader == nil {
		return errors.New("no block has been submitted")
	}

	err := s.batch.commit(ctx, s.api, s.batch.blockHeader)
	if err != nil {
		return err
	}

	if pin {
		err = s.pinNodes(ctx, s.batch.nodes)
		if err != nil {
			return err
		}
	}

	err = s.MerkleTree.CommitBatch()
	if err != nil {
		return err
	}
	s.root = s.batch.blockHeader
	s.Hash = s.batch.blockHeader.cnode.String()

	err = s.writeRootFile(ctx)
	if err != nil {
		return err
	}

	s.reset()
	return nil
}

func (s *store) pinNodes(ctx context.Context, nodes []*node) error {
	for _, n := range nodes {
		if n == nil {
			continue
		}
		err := s.api.Pin().Add(ctx, n.path, options.Pin.Recursive(false))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *store) Revert() error {
	if ok, _ := s.IsOpen(); !ok {
		return errors.New("store is not currently open")
	}

	err := s.MerkleTree.RevertBatch()
	if err != nil {
		return err
	}

	s.reset()
	return nil
}

func (s *store) reset() {
	s.batch = nil
	s.opened = false
}

func getObj(ctx context.Context, api coreiface.CoreAPI, path string) (*node, error) {
	cpath, err := coreiface.ParsePath(path)
	if err != nil {
		return nil, err
	}

	ipldNode, err := api.Dag().Get(ctx, cpath)
	if err != nil {
		return nil, err
	}

	n, err := makeNodeFromCBOR(ipldNode.(*cbor.Node))
	if err != nil {
		return nil, err
	}
	n.fromIPFS = true
	return n, nil
}

func putObj(ctx context.Context, api coreiface.CoreAPI, n *node) error {
	path, err := api.Dag().Put(ctx, bytes.NewReader(n.cnode.RawData()), options.Dag.InputEnc("raw"))
	if err != nil {
		return err
	}

	if pin {
		err = api.Pin().Add(ctx, path, options.Pin.Recursive(false))
	}
	return err
}

func blockHeaderToBytes(bh *blockHeader) ([]byte, error) {
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.BigEndian, *bh)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func blockHeaderFromBytes(b []byte) (*blockHeader, error) {
	bh := &blockHeader{}
	buf := bytes.NewBuffer(b)
	err := binary.Read(buf, binary.BigEndian, bh)
	if err != nil {
		return nil, err
	}
	return bh, nil
}
