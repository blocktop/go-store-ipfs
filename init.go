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
	"context"
	"io/ioutil"
	"os"
	"path"

	"github.com/ipfs/go-ipfs/core/coreapi"
	coreiface "github.com/ipfs/go-ipfs/core/coreapi/interface"
	"github.com/spf13/viper"
)

var Store *store
var pin bool

func InitStore(ctx context.Context) error {
	pin = viper.GetBool("store.ipfs.pin")

	ipfs, err := initIPFS(ctx)
	if err != nil {
		return err
	}

	api := coreapi.NewCoreAPI(ipfs)

	var merkleRoot string
	Store = &store{ipfs: ipfs, api: api}
	Store.rootFile = path.Join(viper.GetString("store.datadir"), "root")
	root, err := Store.getPreviousRoot(ctx)
	if err != nil {
		return err
	}
	if root == nil {
		root, err = Store.makeNilRoot(ctx)
		if err != nil {
			return err
		}
		Store.root = root
	} else {
		Store.root = root
		if root.links["merkle"] != nil {
			merkleRoot = root.links["merkle"].targetCid.String()
		}
	}

	merkle, err := initMerkle(ctx, ipfs, merkleRoot)
	if err != nil {
		return err
	}
	Store.MerkleTree = merkle
	Store.root.links["merkle"] = &link{key: "merkle", targetNode: merkle.root}
	Store.root.changedLinks["merkle"] = true
	root, err = recomputeNode(Store.root)
	if err != nil {
		return err
	}
	Store.root = root

	err = putObj(ctx, api, Store.root)
	if err != nil {
		return err
	}

	return Store.writeRootFile(ctx)
}

func (s *store) getPreviousRoot(ctx context.Context) (*node, error) {
	if _, err := os.Stat(s.rootFile); os.IsNotExist(err) {
		return nil, nil
	}
	rb, err := ioutil.ReadFile(s.rootFile)
	if err != nil {
		return nil, err
	}
	path := string(rb)
	return getObj(ctx, s.api, path)
}

func (s *store) writeRootFile(ctx context.Context) error {
	path := coreiface.IpldPath(s.root.cnode.Cid())
	pathb := []byte(path.String())
	return ioutil.WriteFile(s.rootFile, pathb, os.FileMode(0744))
}

func (s *store) makeNilRoot(ctx context.Context) (*node, error) {
	return makeNodeFromObj([]byte("root"), make(map[string]*link))
}
