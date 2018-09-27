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
	"fmt"

	coreiface "github.com/ipfs/go-ipfs/core/coreapi/interface"
	"github.com/ipfs/go-ipfs/core/coreapi/interface/options"
)

type batch struct {
	dagBatch    coreiface.DagBatch
	blockHeader *node
	merkleRoot  *node
	blockNumber uint64
	nodes       []*node
	nodeIndex   map[string]int
}

func (b *batch) commit(ctx context.Context, api coreiface.CoreAPI, root *node) error {
	nodes := make([]*node, 1)
	b.nodeIndex = make(map[string]int)
	nodes[0] = (*node)(nil) // so that zeroth index is unavailabe

	nodes, err := collectChangedNodes(root, nodes, b.nodeIndex)
	if err != nil {
		return err
	}
	b.nodes = nodes

	// Commit from the leaves up to the root.
	// The root is in nodes[1]. nodes[0] is nil.
	starti := len(b.nodes) - 1
	for starti > 0 {
		b.dagBatch = api.Dag().Batch(ctx)
		for i := starti; i > starti-dagBatchSize; i-- {
			if i == 0 {
				break
			}
			byts := bytes.NewReader(b.nodes[i].cnode.RawData())
			b.dagBatch.Put(ctx, byts, options.Dag.InputEnc("raw"))
		}

		err := b.dagBatch.Commit(ctx)
		if err != nil {
			return err
		}

		starti -= dagBatchSize
	}

	return nil
}

func collectChangedNodes(n *node, nodes []*node, nodeIndex map[string]int) ([]*node, error) {
	var added bool
	for k := range n.changedLinks {
		if !added {
			nodeIndex[n.cnode.String()] = len(nodes)
			nodes = append(nodes, n)
			added = true
		}
		tn := n.links[k].targetNode
		if tn == nil {
			return nil, fmt.Errorf("no data for changed link %s", k)
		}
		cidS := tn.cnode.String()
		if nodeIndex[cidS] == 0 {
			var err error
			nodes, err = collectChangedNodes(tn, nodes, nodeIndex)
			if err != nil {
				return nil, err
			}
		}
	}

	if n.changedData && !added {
		nodeIndex[n.cnode.String()] = len(nodes)
		nodes = append(nodes, n)
	}
	return nodes, nil
}
