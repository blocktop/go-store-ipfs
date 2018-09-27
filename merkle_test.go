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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/spf13/viper"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	nilStoreRoot  = "/ipld/zdpuAmUr33yuaUdvJKUQUAvsy8a8fGfeCoucuFuhUybEdbP6M"
	nilMerkleRoot = "/ipld/zdpuAsSjSbrPSXPoW5nBw15MbQby59kULnfzBCDeArYgUZg7p"
)

var _ = Describe("Merkle", func() {

	var ctx context.Context
	BeforeSuite(func() {
		ctx = context.Background()
		removeDataDir()
		initialize(ctx)
	})

	AfterSuite(func() {
		if Store != nil {
			Store.Close()
		}
	})

	Describe("basic functions", func() {

		It("initializes", func() {
			rb, err := ioutil.ReadFile(getRootFile())
			failIfErr(err)
			path := string(rb)
			Expect(path).To(Equal(nilStoreRoot))
			Expect(Store.root.path.String()).To(Equal(nilStoreRoot))

			Expect(Store.MerkleTree.root.path.String()).To(Equal(nilMerkleRoot))
		})

		It("initializes from existing root", func() {
			Store.Close()
			err := InitStore(ctx)
			failIfErr(err)

			Expect(Store.root.path.String()).To(Equal(nilStoreRoot))
			Expect(Store.MerkleTree.root.path.String()).To(Equal(nilMerkleRoot))
		})

	})

	Describe("merkle", func() {
		It("puts and gets", func() {
			openStore(ctx)

			err := Store.MerkleTree.PutValue(ctx, "testputkey", []byte("testputvalue"))
			failIfErr(err)

			c, t := commitMerkle(ctx)
			GinkgoWriter.Write([]byte(fmt.Sprintf("Commit took %f ms for %d nodes", t, c)))

			value, err := Store.MerkleTree.GetValue(ctx, "testputkey")
			failIfErr(err)

			Expect(string(value)).To(Equal("testputvalue"))
		})

		It("puts and gets overlapping keys", func() {
			openStore(ctx)

			err := Store.MerkleTree.PutValue(ctx, "testputkey", []byte("testputvalue"))
			failIfErr(err)

			err = Store.MerkleTree.PutValue(ctx, "testput2key", []byte("testput2value"))
			failIfErr(err)

			err = Store.MerkleTree.PutValue(ctx, "testput22key", []byte("testput22value"))
			failIfErr(err)

			c, t := commitMerkle(ctx)
			GinkgoWriter.Write([]byte(fmt.Sprintf("Commit took %f ms for %d nodes", t, c)))

			value, err := Store.MerkleTree.GetValue(ctx, "testput22key")
			failIfErr(err)
			Expect(string(value)).To(Equal("testput22value"))

			value, err = Store.MerkleTree.GetValue(ctx, "testputkey")
			failIfErr(err)
			Expect(string(value)).To(Equal("testputvalue"))
		})

		It("puts and gets links", func() {
			openStore(ctx)

			n, err := makeNodeFromObj([]byte("foo"), nil)
			failIfErr(err)
			failIfErr(putObj(ctx, Store.api, n))

			ln := &link{key: "fookey", targetNode: n}

			failIfErr(Store.MerkleTree.PutLink(ctx, "testputlink", ln))

			c, t := commitMerkle(ctx)
			GinkgoWriter.Write([]byte(fmt.Sprintf("Commit took %f ms for %d nodes", t, c)))

			n2, err := Store.MerkleTree.GetLink(ctx, "testputlink", "fookey")
			failIfErr(err)

			Expect(string(n2.data)).To(Equal("foo"))
			
		})

		It("commit time", func() {
			openStore(ctx)

			r := rand.Reader
			v := make([]byte, 32)
			count := 200
			for count > 0 {
				r.Read(v)
				keyb := sha256.Sum256(v)
				key := hex.EncodeToString(keyb[:])
				err := Store.MerkleTree.PutValue(ctx, key, v)
				failIfErr(err)
				count--
			}

			GinkgoWriter.Write([]byte("Committing...\n"))
			c, t := commitMerkle(ctx)
			GinkgoWriter.Write([]byte(fmt.Sprintf("Commit took %f ms for %d nodes", t, c)))
		})

	})
})

func BenchmarkPut(b *testing.B) {
	ctx := context.Background()
	if Store == nil {
		initialize(ctx)
		openStore(ctx)
	}
	r := rand.Reader
	value := make([]byte, 32)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Read(value)
		key := fmt.Sprintf("item%dval%dval%dval%d", i, value[0], value[1], value[2])
		Store.MerkleTree.PutValue(ctx, key, value)
	}
}

func commitMerkle(ctx context.Context) (int, float32) {
	start := time.Now().UnixNano()

	err := Store.batch.commit(ctx, Store.api, Store.batch.merkleRoot)
	failIfErr(err)

	nodeCount := len(Store.batch.nodes) - 1

	if pin {
		err = Store.pinNodes(ctx, Store.batch.nodes)
		failIfErr(err)
	}

	Store.MerkleTree.CommitBatch()
	Store.reset()

	t := float32(time.Now().UnixNano()-start) / float32(time.Millisecond)
	return nodeCount, t
}

func initialize(ctx context.Context) {
	initViper()
	makeDataDir()
	removeRoot()
	InitStore(ctx)
}

func openStore(ctx context.Context) {
	if Store == nil {
		initialize(ctx)
	}
	Store.reset()
	Store.OpenBlock(1)
	Store.batch.dagBatch = Store.api.Dag().Batch(ctx)
}

func initViper() {
	viper.SetDefault("store.datadir", getDataDir())
	viper.SetDefault("store.ipfs.pin", false)
	viper.SetDefault("store.ipfs.swarmhosts", []string{"/ip4/127.0.0.1/tcp"})
	viper.SetDefault("store.ipfs.swarmport", 4001)
	viper.SetDefault("store.ipfs.disablenat", true)
	viper.SetDefault("store.debug", true)
}

func getDataDir() string {
	dir := os.ExpandEnv("$GOPATH/src/github.com/blocktop/go-store-ipfs/test/data")
	return dir
}

func getRootFile() string {
	return path.Join(getDataDir(), "root")
}

func makeDataDir() {
	failIfErr(os.MkdirAll(getDataDir(), os.FileMode(0755)))
}

func removeDataDir() {
	failIfErr(os.RemoveAll(getDataDir()))
}

func removeRoot() {
	err := os.Remove(getRootFile())
	if !os.IsNotExist(err) {
		failIfErr(err)
	}
}

func failIfErr(err error) {
	if err != nil {
		Fail(err.Error())
	}
}
