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

	cid "gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	cbor "gx/ipfs/QmSywXfm2v4Qkp4DcFqo8eehj49dJK3bdUnaLVxrdFLMQn/go-ipld-cbor"

	spec "github.com/blocktop/go-spec"
	"github.com/ipfs/go-ipfs/core"
	coreiface "github.com/ipfs/go-ipfs/core/coreapi/interface"
	"github.com/ipfs/go-ipfs/core/coreapi/interface/options"
)

type store struct {
	Root       string
	root       *node
	api        coreiface.CoreAPI
	ipfs       *core.IpfsNode
	merkleTree *merkleTreeStruct
	storeBlock *storeBlock
	rootFile   string
	blockRoots map[string]*node // [blockID]rootNode
}

// ensure that store fulfills the interface specification
var _ spec.Store = (*store)(nil) 

type blockHeader struct {
	blockID       string
	parentBlockID string
	blockNumber   uint64
}

const dagBatchSize = 700

func (s *store) OpenBlock(blockNumber uint64) (spec.StoreBlock, error) {
	if s.storeBlock != nil {
		return nil, errors.New("a block is already open")
	}

	sb, err := newstoreBlock(s.root, blockNumber)
	if err != nil {
		return nil, err
	}
	s.storeBlock = sb

	return sb, nil
}

func (s *store) GetBlock(ctx context.Context, blockHash string) (spec.StoreBlock, error) {
	rootNode := s.blockRoots[blockHash]
	if rootNode == nil {
		return nil, nil
	}
	bh, err := blockHeaderFromBytes(rootNode.data) 
	if err != nil {
		return nil, err
	}
	parentLink := rootNode.links["parent"]
	if parentLink.targetNode == nil {
		pn, err := getObj(ctx, Store.api, coreiface.IpldPath(parentLink.targetCid).String())
		if err != nil {
			return nil, err
		}
		parentLink.targetNode = pn
	}
	merkleLink := rootNode.links["merkle"]
	if merkleLink.targetNode == nil {
		mn, err := getObj(ctx, Store.api, coreiface.IpldPath(merkleLink.targetCid).String())
		if err != nil {
			return nil, err
		}
		merkleLink.targetNode = mn
	}
	sb := &storeBlock{}
	sb.blockHeader = rootNode
	sb.blockNumber = bh.blockNumber
	sb.merkleRoot = merkleLink.targetNode
	sb.parent = parentLink.targetNode
	sb.readonly = true

	return sb, nil
}

func (s *store) StoreBlock() spec.StoreBlock {
	return s.storeBlock
} 

func (s *store) Close() {
	s.ipfs.Close()
	Store = nil
}

func (s *store) GetRoot() string {
	return s.root.cnode.String()
}

func (s *store) Hash(data []byte, specLinks spec.Links) (string, error) {
	links, err := makeLinks(specLinks)
	if err != nil {
		return "", err
	}
	n, err := makeNodeFromObj(data, links)
	if err != nil {
		return "", err
	}
	return n.cnode.String(), nil
}

func (s *store) Get(ctx context.Context, hash string, obj spec.Marshalled) error {
	c, err := cid.Parse(hash)
	if err != nil {
		return err
	}

	path := coreiface.IpldPath(c)

	n, err := getObj(ctx, s.api, path.String())
	if err != nil {
		return err
	}

	obj.Unmarshal(n.data, makeSpecLinks(n.links))

	return nil
}

func (s *store) Put(ctx context.Context, obj spec.Marshalled) error {
	data, specLinks, err := obj.Marshal()
	if err != nil {
		return err
	}
	links, err := makeLinks(specLinks)
	if err != nil {
		return err
	}
	n, err := makeNodeFromObj(data, links)
	if err != nil {
		return err
	}
	return putObj(ctx, s.api, n)
}

func (s *store) TreeGet(ctx context.Context, key string, obj spec.Marshalled) error {
	if s.storeBlock != nil {

	}
	n, err := s.merkleTree.getNode(ctx, key, "", false) 
	if err != nil {
		return err
	}

	obj.Unmarshal(n.data, makeSpecLinks(n.links))

	return nil
}

func (s *store) reset() {
	s.storeBlock = nil
}

func (s *store) setRoot(ctx context.Context, root *node) error {
	s.root = root
	s.Root = root.cnode.String()

	err := s.writeRootFile(ctx)
	if err != nil {
		return err
	}
	return nil
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
