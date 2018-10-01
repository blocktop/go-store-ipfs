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
	"errors"
	"strings"
	"sync"

	cid "gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	mh "gx/ipfs/QmPnFwZ2JXKnXgMw8CdBPxn7FWh6LLdjUjxV1fKHuJnkr8/go-multihash"
	cbor "gx/ipfs/QmPrv66vmh2P7vLJMpYx6DWLTNKvVB4Jdkyxs6V3QvWKvf/go-ipld-cbor"

	spec "github.com/blocktop/go-spec"
	"github.com/ipfs/go-ipfs/core"
	coreapi "github.com/ipfs/go-ipfs/core/coreapi"
	coreiface "github.com/ipfs/go-ipfs/core/coreapi/interface"
)

type merkleTreeStruct struct {
	locked bool
	api    coreiface.CoreAPI
	root   *node
	batch  *merkleTreeBatch
}

type merkleTreeBatch struct {
	sync.Mutex
	api  coreiface.CoreAPI
	root *node
}

const val = "val"

func initMerkle(ctx context.Context, ipfs *core.IpfsNode, merkleRoot string) (*merkleTreeStruct, error) {
	merkleTree := &merkleTreeStruct{}

	merkleTree.api = coreapi.NewCoreAPI(ipfs)

	err := merkleTree.initRoot(ctx, merkleRoot)
	if err != nil {
		return nil, err
	}

	return merkleTree, nil
}

func (m *merkleTreeStruct) initRoot(ctx context.Context, merkleRoot string) error {
	var n *node
	var err error
	if merkleRoot == "" {
		n, err = makeNodeFromObj([]byte("tree"), nil)
		if err != nil {
			return err
		}
		err = putObj(ctx, m.api, n)
	} else {
		c, err := cid.Parse(merkleRoot)
		if err != nil {
			return err
		}
		n, err = getObj(ctx, m.api, coreiface.IpldPath(c).String())
	}

	if err != nil {
		return err
	}

	m.root = n
	return nil
}

func (m *merkleTreeStruct) StartBatch() (*node, error) {
	if m.locked {
		return nil, errors.New("the merkle tree is already in batch")
	}

	m.locked = true

	cnode, err := cbor.Decode(m.root.cnode.RawData(), mh.SHA2_256, -1)
	if err != nil {
		return nil, err
	}
	batchRoot, err := makeNodeFromCBOR(cnode)
	if err != nil {
		return nil, err
	}

	m.batch = &merkleTreeBatch{
		api:  m.api,
		root: batchRoot}

	return batchRoot, nil
}

func (m *merkleTreeStruct) CommitBatch() error {
	if !m.locked {
		return errors.New("the merkle tree is not in batch")
	}

	m.root = m.batch.root
	m.batch = nil
	m.locked = false
	return nil
}

func (m *merkleTreeStruct) RevertBatch() error {
	if !m.locked {
		return errors.New("the merkle tree is not in batch")
	}

	m.batch = nil
	m.locked = false
	return nil
}

func (m *merkleTreeStruct) getRoot() string {
	if m.locked {
		return m.batch.root.path.Cid().String()
	}
	return m.root.path.Cid().String()
}

func (m *merkleTreeStruct) getValue(ctx context.Context, key string, inBatch bool) ([]byte, error) {
	n, err := m.getNode(ctx, key, "", inBatch)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, nil
	}
	return n.data, nil
}

func (m *merkleTreeStruct) getLinks(ctx context.Context, key string, inBatch bool) (map[string]*link, error) {
	n, err := m.getNode(ctx, key, "", inBatch)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, nil
	}
	return n.links, nil
}

func (m *merkleTreeStruct) getLink(ctx context.Context, key string, linkName string, inBatch bool) (*node, error) {
	return m.getNode(ctx, key, linkName, inBatch)
}

func (m *merkleTreeStruct) getNode(ctx context.Context, key string, linkName string, inBatch bool) (*node, error) {
	if inBatch {
		return m.getNodeFromBatch(ctx, key, linkName)
	}

	keyPath := m.root.path.String() + "/" + strings.Join(strings.Split(key, ""), "/")
	if linkName != "" {
		keyPath += "/" + linkName
	}
	n, err := getObj(ctx, m.api, keyPath)
	if err == cbor.ErrNoSuchLink {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return n, nil
}

func (m *merkleTreeStruct) getNodeFromBatch(ctx context.Context, key string, linkName string) (*node, error) {
	if !m.locked {
		return nil, errors.New("the tree is not currently in batch")
	}
	root := m.batch.root
	n, err := m.getKey(ctx, root, key)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, nil
	}
	if linkName == "" {
		return n, nil
	}
	if n.links == nil || n.links[linkName] == nil {
		return nil, nil
	}
	lnk := n.links[linkName]
	if lnk.targetNode != nil {
		return lnk.targetNode, nil
	}
	if lnk.targetCid == cid.Undef {
		return nil, nil
	}
	path := coreiface.IpldPath(lnk.targetCid).String()
	return getObj(ctx, m.api, path)
}

func (m *merkleTreeStruct) getKey(ctx context.Context, n *node, key string) (*node, error) {
	if len(key) == 0 {
		return n, nil
	}
	k := key[:1]
	krest := key[1:]
	if n.links == nil {
		n.links = make(map[string]*link)
	}
	lnk := n.links[k]
	if lnk == nil || lnk.targetNode == nil {
		path := n.path.String() + "/" + k
		nk, err := getObj(ctx, m.api, path)
		if err != nil {
			return nil, err
		}

		if lnk == nil {
			lnk = &link{key: k}
			n.links[k] = lnk
		}
		lnk.targetNode = nk
	}

	return m.getKey(ctx, lnk.targetNode, krest)
}

func (m *merkleTreeStruct) putValue(ctx context.Context, key string, value []byte) error {
	return m.put(ctx, key, value, false)
}
func (m *merkleTreeStruct) putLink(ctx context.Context, key string, ln *link) error {
	return m.put(ctx, key, ln, true)
}

func (m *merkleTreeStruct) putNode(ctx context.Context, key string, n *node) error {
	err := m.put(ctx, key, n.data, false)
	if err != nil {
		return err
	}
	if n.links == nil {
		return nil
	}
	for _, lnk := range n.links {
		err = m.put(ctx, key, lnk, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *merkleTreeStruct) put(ctx context.Context, key string, value interface{}, valueIsLink bool) error {
	if !m.locked {
		return errors.New("the tree is not currently in batch")
	}

	var err error
	var root *node

	m.batch.Lock()
	defer m.batch.Unlock()

	root = m.batch.root
	root, err = m.batch.putKey(ctx, root, key, value, valueIsLink)
	if err != nil {
		return err
	}
	m.batch.root = root

	return nil
}

func (b *merkleTreeBatch) putKey(ctx context.Context, n *node, key string, value interface{}, valueIsLink bool) (*node, error) {
	var change bool

	if len(key) == 0 {
		if valueIsLink {
			ln := value.(*link)
			if n.links == nil {
				n.links = make(map[string]*link)
			}
			if n.links[ln.key] == nil || n.links[ln.key].targetNode.cnode.String() != ln.targetNode.cnode.String() {
				n.links[ln.key] = ln
				n.changedLinks[ln.key] = true
				change = true
			}
		} else {
			v := value.([]byte)

			n.changedData = bytes.Compare(v, n.data) != 0
			// required becuase bytes.Compare treats nil and empty slice as equal
			if (v == nil && n.data != nil) ||
				(v != nil && n.data == nil) {
				n.changedData = true
				change = true
			}
			n.data = v
		}
		if change {
			return recomputeNode(n)
		}
		return n, nil
	}

	k := key[:1]
	krest := key[1:]

	if n.links == nil {
		n.links = make(map[string]*link)
	}

	if n.links[k] == nil {
		n.links[k] = &link{key: k}
	}
	lnk := n.links[k]

	// search IPFS for target
	if lnk.targetNode == nil && n.fromIPFS {
		path := n.path.String() + "/" + k
		nk, err := getObj(ctx, b.api, path)
		if err != nil && err != cbor.ErrNoSuchLink {
			return nil, err
		}
		if err == nil {
			lnk.targetNode = nk
		}
	}

	// If the target is still not found then make a new node,
	// otherwise put the value at this target (continue recursion).
	if lnk.targetNode == nil {
		nk, err := b.makeChild(ctx, krest, value, valueIsLink)
		if err != nil {
			return nil, err
		}
		lnk.targetNode = nk
		change = true
	} else {
		cidInitial := lnk.targetNode.cnode.String()
		nk, err := b.putKey(ctx, lnk.targetNode, krest, value, valueIsLink)
		if err != nil {
			return nil, err
		}
		cidFinal := nk.cnode.String()
		change = cidFinal != cidInitial
	}

	if change {
		n.changedLinks[k] = true
		return recomputeNode(n)
	}
	return n, nil
}

func (b *merkleTreeBatch) makeChild(ctx context.Context, key string, value interface{}, valueIsLink bool) (*node, error) {
	var err error
	if len(key) == 0 {
		if valueIsLink {
			ln := value.(*link)
			links := map[string]*link{ln.key: ln}
			n, err := makeNodeFromObj(nil, links)
			if err != nil {
				return nil, err
			}
			n.changedLinks[ln.key] = true
			return n, nil
		}

		data, ok := value.([]byte)
		if !ok {
			return nil, errors.New("value must be a []byte")
		}
		n, err := makeNodeFromObj(data, nil)
		if err != nil {
			return nil, err
		}
		n.changedData = true
		return n, nil
	}

	k := key[:1]
	krest := key[1:]
	nk, err := b.makeChild(ctx, krest, value, valueIsLink)
	if err != nil {
		return nil, err
	}
	lnk := &link{key: k, targetNode: nk}
	links := map[string]*link{k: lnk}
	n, err := makeNodeFromObj(nil, links)
	if err != nil {
		return nil, err
	}
	n.changedLinks[k] = true

	return n, nil
}

func makeBlockKey(block spec.Block) string {
	return "blk" + block.Hash()
}

func makeTransactionKey(txn spec.Transaction) string {
	return "txn" + txn.Hash()
}

func makeTransactionBlockKey(txn spec.Transaction) string {
	return "txnblk" + txn.Hash()
}

func makeAccountKey(acct spec.Account) string {
	return "act" + acct.Address()
}

func makeAccountTransactionKey(acct spec.Account, role string) string {
	return "acttxn" + role + acct.Address()
}
