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
	"encoding/base64"
	"encoding/json"
	"fmt"
	cid "gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	mh "gx/ipfs/QmPnFwZ2JXKnXgMw8CdBPxn7FWh6LLdjUjxV1fKHuJnkr8/go-multihash"
	cbor "gx/ipfs/QmPrv66vmh2P7vLJMpYx6DWLTNKvVB4Jdkyxs6V3QvWKvf/go-ipld-cbor"

	spec "github.com/blocktop/go-spec"
	"github.com/gogo/protobuf/proto"
	coreiface "github.com/ipfs/go-ipfs/core/coreapi/interface"
)

func makeNodeFromObj(data []byte, links map[string]*link) (*node, error) {
	obj := map[string]interface{}{
		val: data}

	if links != nil {
		for k, ln := range links {
			if k == val {
				return nil, fmt.Errorf("link key may to be '%s'", val)
			}
			if ln.targetNode == nil {
				obj[k] = ln.targetCid
			} else {
				obj[k] = ln.targetNode.cnode.Cid()
			}
		}
	}

	cnode, err := cbor.WrapObject(obj, mh.SHA2_256, -1)
	if err != nil {
		return nil, err
	}

	n := &node{
		cnode:        cnode,
		data:         data,
		links:        links,
		path:         coreiface.IpldPath(cnode.Cid()),
		changedData:  false,
		changedLinks: make(map[string]bool)}

	return n, nil
}

func makeNodeFromCBOR(cnode *cbor.Node) (*node, error) {
	// convert ipld node to map[string]interface{}
	jb, err := cnode.MarshalJSON()
	if err != nil {
		return nil, err
	}
	obj := map[string]interface{}{}
	err = json.Unmarshal(jb, &obj)
	if err != nil {
		return nil, err
	}

	n := &node{
		cnode:        cnode,
		links:        make(map[string]*link),
		path:         coreiface.IpldPath(cnode.Cid()),
		changedData:  false,
		changedLinks: make(map[string]bool)}

	for k, v := range obj {
		if k == val {
			byts, err := base64.StdEncoding.DecodeString(v.(string))
			if err != nil {
				return nil, err
			}
			n.data = byts
		} else {
			v, ok := v.(map[string]interface{})
			if ok {
				c, err := cid.Parse(v["/"])
				if err != nil {
					return nil, err
				}
				n.links[k] = &link{key: k, targetCid: c}
			}
		}
	}

	return n, nil
}

func recomputeNode(n *node) (*node, error) {
	n2, err := makeNodeFromObj(n.data, n.links)
	if err != nil {
		return nil, err
	}

	n.cnode = n2.cnode
	n.path = n2.path

	return n, nil
}

func makeNodeFromBlock(block spec.Block, txns map[string]*link) (*node, error) {
	msg := block.Marshal()
	return makeNodeFromProtoMessage(msg, txns)
}

func makeNodeFromTransaction(txn spec.Transaction, parties map[string]*link) (*node, error) {
	msg := txn.Marshal()
	return makeNodeFromProtoMessage(msg, parties)
}

func makeNodeFromAccount(acct spec.Account) (*node, error) {
	msg := acct.Marshal()
	return makeNodeFromProtoMessage(msg, nil)
}

func makeNodeFromProtoMessage(msg proto.Message, links map[string]*link) (*node, error) {
	byts, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	n, err := makeNodeFromObj(byts, links)
	if err != nil {
		return nil, err
	}
	n.changedData = true
	if links != nil {
		for k := range links {
			n.changedLinks[k] = true
		}
	}

	return n, nil
}
