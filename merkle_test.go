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

			Expect(Store.merkleTree.root.path.String()).To(Equal(nilMerkleRoot))
		})

		It("initializes from existing root", func() {
			Store.Close()
			err := InitStore(ctx)
			failIfErr(err)

			Expect(Store.root.path.String()).To(Equal(nilStoreRoot))
			Expect(Store.merkleTree.root.path.String()).To(Equal(nilMerkleRoot))
		})

	})

	Describe("merkle", func() {
		It("puts and gets", func() {
			storeb := openStore(ctx)

			err := Store.merkleTree.putValue(ctx, "testputkey", []byte("testputvalue"))
			failIfErr(err)

			c, t := commitMerkle(ctx, storeb)
			GinkgoWriter.Write([]byte(fmt.Sprintf("Commit took %f ms for %d nodes", t, c)))

			value, err := Store.merkleTree.getValue(ctx, "testputkey", false)
			failIfErr(err)

			Expect(string(value)).To(Equal("testputvalue"))
		})

		It("puts and gets overlapping keys", func() {
			storeb := openStore(ctx)

			err := Store.merkleTree.putValue(ctx, "testputkey", []byte("testputvalue"))
			failIfErr(err)

			err = Store.merkleTree.putValue(ctx, "testput2key", []byte("testput2value"))
			failIfErr(err)

			err = Store.merkleTree.putValue(ctx, "testput22key", []byte("testput22value"))
			failIfErr(err)

			// get outside of batch returns nil
			value, err := Store.merkleTree.getValue(ctx, "testput2key", false)
			failIfErr(err)
			Expect(value).To(BeNil())

			value, err = Store.merkleTree.getValue(ctx, "testput22key", true)
			failIfErr(err)
			Expect(string(value)).To(Equal("testput22value"))

			c, t := commitMerkle(ctx, storeb)
			GinkgoWriter.Write([]byte(fmt.Sprintf("Commit took %f ms for %d nodes", t, c)))

			value, err = Store.merkleTree.getValue(ctx, "testputkey", false)
			failIfErr(err)
			Expect(string(value)).To(Equal("testputvalue"))

			// get within batch when no batch is happening reeturns error
			value, err = Store.merkleTree.getValue(ctx, "testputkey", true)
			Expect(err).ToNot(BeNil())
		})

		It("puts and gets links", func() {
			storeb := openStore(ctx)

			n, err := makeNodeFromObj([]byte("foo"), nil)
			failIfErr(err)
			failIfErr(putObj(ctx, Store.api, n))

			ln := &link{key: "fookey", targetNode: n}

			failIfErr(Store.merkleTree.putLink(ctx, "testputlink", ln))

			c, t := commitMerkle(ctx, storeb)
			GinkgoWriter.Write([]byte(fmt.Sprintf("Commit took %f ms for %d nodes", t, c)))

			n2, err := Store.merkleTree.getLink(ctx, "testputlink", "fookey", false)
			failIfErr(err)

			Expect(string(n2.data)).To(Equal("foo"))
			
		})

		It("commit time", func() {
			storeb := openStore(ctx)

			r := rand.Reader
			v := make([]byte, 32)
			count := 200
			for count > 0 {
				r.Read(v)
				keyb := sha256.Sum256(v)
				key := hex.EncodeToString(keyb[:])
				err := Store.merkleTree.putValue(ctx, key, v)
				failIfErr(err)
				count--
			}

			GinkgoWriter.Write([]byte("Committing...\n"))
			c, t := commitMerkle(ctx, storeb)
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
		Store.merkleTree.putValue(ctx, key, value)
	}
}

func commitMerkle(ctx context.Context, storeb *storeBlock) (int, float32) {
	start := time.Now().UnixNano()

	err := storeb.batch.commit(ctx, Store.api, storeb.merkleRoot)
	failIfErr(err)

	nodeCount := len(storeb.batch.nodes) - 1

	if pin {
		err = storeb.pinNodes(ctx, storeb.batch.nodes)
		failIfErr(err)
	}

	Store.merkleTree.CommitBatch()
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

func openStore(ctx context.Context) *storeBlock {
	if Store == nil {
		initialize(ctx)
	}
	Store.reset()
	sb, _ := Store.OpenBlock(1)
	storeb := sb.(*storeBlock)
	storeb.batch.dagBatch = Store.api.Dag().Batch(ctx)
	return storeb
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
