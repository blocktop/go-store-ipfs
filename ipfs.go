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
	"fmt"
	"os"

	config "gx/ipfs/QmSoYrBMibm2T3LupaLuez7LPGnyrJwdRxvTfPUyCp691u/go-ipfs-config"

	"github.com/ipfs/go-ipfs/core"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
	"github.com/spf13/viper"
)

func initIPFS(ctx context.Context) (*core.IpfsNode, error) {
	dataDir := viper.GetString("store.datadir")
	if _, err := fsrepo.ConfigAt(dataDir); err != nil {
		err = initRepo(dataDir)
		if err != nil {
			return nil, err
		}
	}

	repo, err := fsrepo.Open(dataDir)
	if err != nil {
		return nil, err
	}

	repoCfg, err := repo.Config()
	if err != nil {
		return nil, err
	}

	// swap in bootstrap list from config, if any
	bsList := viper.GetStringSlice("store.ipfs.bootstraplist")
	if bsList != nil && len(bsList) > 0 {
		peers := make([]config.BootstrapPeer, len(bsList))
		for i, p := range bsList {

			remotePeer, err := config.ParseBootstrapPeer(p)
			if err != nil {
				return nil, err
			}

			peers[i] = remotePeer
		}

		repoCfg.SetBootstrapPeers(peers)
	}

	swarmHosts := viper.GetStringSlice("store.ipfs.swarmhosts")
	swarmPort := viper.GetInt("store.ipfs.swarmport")
	addrs := make([]string, len(swarmHosts))
	for i, h := range swarmHosts {
		addrs[i] = fmt.Sprintf("%s/%d", h, swarmPort)
	}
	repoCfg.Addresses.Swarm = addrs
	repoCfg.Swarm.DisableNatPortMap = viper.GetBool("store.ipfs.disablenat")

	repo.SetConfig(repoCfg)

	cfg := core.BuildCfg{
		Online:    true,
		Permanent: true,
		Repo:      repo}

	ipfsNode, err := core.NewNode(ctx, &cfg)
	if err != nil {
		return nil, err
	}

	return ipfsNode, nil
}

func initRepo(dataDir string) error {
	conf, err := config.Init(os.Stdout, 2048)
	if err != nil {
		return nil
	}

	err = fsrepo.Init(dataDir, conf)
	if err != nil {
		return err
	}

	return nil
}
