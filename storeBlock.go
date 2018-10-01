package storeipfs

import (
	"context"
	"errors"
	"strconv"

	spec "github.com/blocktop/go-spec"
	"github.com/ipfs/go-ipfs/core/coreapi/interface/options"
)

var _ spec.StoreBlock = (*storeBlock)(nil)

type storeBlock struct {
	parent      *node
	blockNumber uint64
	merkleRoot  *node
	batch       *batch
	blockHeader *node
	opened      bool
	readonly	  bool
}

func newstoreBlock(parent *node, blockNumber uint64) (*storeBlock, error) {
	merkleRoot, err := Store.merkleTree.StartBatch()
	if err != nil {
		return nil, err
	}

	s := &storeBlock{
		parent:      parent,
		blockNumber: blockNumber,
		merkleRoot:  merkleRoot}

	s.batch = &batch{}

	return s, nil
}

func (s *storeBlock) IsOpen() (bool, uint64) {
	if !s.opened {
		return false, ^uint64(0)
	}
	return true, s.blockNumber

}

func (s *storeBlock) Submit(ctx context.Context, block spec.Block) (string, error) {
	if ok, _ := s.IsOpen(); !ok {
		return "", errors.New("store is not currently open")
	}
	if block.BlockNumber() != s.blockNumber {
		return "", errors.New("store was open for a different block number")
	}

	txns := block.Transactions()
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
			err = Store.merkleTree.putLink(ctx, makeAccountKey(acct), &link{key: "acct", targetNode: anode})
			if err != nil {
				return "", err
			}
		}

		tnode, err := makeNodeFromTransaction(t)
		if err != nil {
			return "", err
		}
		k := strconv.FormatInt(int64(i), 10)
		txnodes[k] = &link{key: "txn" + k, targetNode: tnode}

		err = Store.merkleTree.putLink(ctx, makeTransactionKey(t), &link{key: "txn", targetNode: tnode})
		if err != nil {
			return "", err
		}
		for role, acct := range parties {
			err = Store.merkleTree.putLink(ctx, makeAccountTransactionKey(acct, role), &link{key: t.Hash(), targetNode: tnode})
			if err != nil {
				return "", err
			}
		}
	}

	bnode, err := makeNodeFromBlock(block)
	if err != nil {
		return "", err
	}
	err = Store.merkleTree.putLink(ctx, makeBlockKey(block), &link{key: "blk", targetNode: bnode})
	if err != nil {
		return "", err
	}
	for _, t := range txns {
		err = Store.merkleTree.putLink(ctx, makeTransactionBlockKey(t), &link{key: "blk", targetNode: bnode})
		if err != nil {
			return "", err
		}
	}

	bh := &blockHeader{
		blockID:       block.Hash(),
		parentBlockID: block.ParentHash(),
		blockNumber:   block.BlockNumber()}

	data, err := blockHeaderToBytes(bh)
	if err != nil {
		return "", err
	}

	links := map[string]*link{
		"parent": &link{key: "parent", targetNode: s.parent},
		"block":  &link{key: "block", targetNode: bnode},
		"merkle": &link{key: "merkle", targetNode: s.merkleRoot}}

	bhnode, err := makeNodeFromObj(data, links)
	if err != nil {
		return "", err
	}

	bhnode.changedData = true
	bhnode.changedLinks["block"] = true
	bhnode.changedLinks["merkle"] = true

	s.blockHeader = bhnode

	rootHash := bhnode.cnode.String()
	Store.blockRoots[block.Hash()] = bhnode

	return rootHash, nil
}

func (s *storeBlock) Commit(ctx context.Context) error {
	if ok, _ := s.IsOpen(); !ok {
		return errors.New("store is not currently open")
	}
	if s.blockHeader == nil {
		return errors.New("no block has been submitted")
	}

	err := s.batch.commit(ctx, Store.api, s.blockHeader)
	if err != nil {
		return err
	}

	if pin {
		err = s.pinNodes(ctx, s.batch.nodes)
		if err != nil {
			return err
		}
	}

	err = Store.merkleTree.CommitBatch()
	if err != nil {
		return err
	}

	err = Store.setRoot(ctx, s.blockHeader)
	if err != nil {
		return err
	}

	Store.reset()
	s.opened = false
	return nil
}

func (s *storeBlock) pinNodes(ctx context.Context, nodes []*node) error {
	for _, n := range nodes {
		if n == nil {
			continue
		}
		err := Store.api.Pin().Add(ctx, n.path, options.Pin.Recursive(false))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *storeBlock) Revert() error {
	if ok, _ := s.IsOpen(); !ok {
		return errors.New("store is not currently open")
	}

	err := Store.merkleTree.RevertBatch()
	if err != nil {
		return err
	}

	Store.reset()
	s.opened = false
	return nil
}

func (s *storeBlock) GetRoot() string {
	if s.blockHeader == nil {
		return ""
	}
	return s.blockHeader.cnode.String()
}

func (s *storeBlock) TreeGet(ctx context.Context, key string, obj spec.Marshalled) error {

	n, err := Store.merkleTree.getNode(ctx, key, "", true)
	if err != nil {
		return err
	}

	obj.Unmarshal(n.data, makeSpecLinks(n.links))

	return nil
}

func (s *storeBlock) TreePut(ctx context.Context, key string, obj spec.Marshalled) error {
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
	return Store.merkleTree.putNode(ctx, key, n)
}
