package leveldb_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/table"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Regression test: the block cache is keyed by (file number, offset), so a new
// table that reused a removed table's number was served the removed table's
// blocks. Reads missed live keys, and compaction persisted the stale block:
// live keys were lost and deleted keys came back.

const (
	reuseNKeys = 8000
	reuseNB    = 1500
)

func reuseKey(set string, i int) []byte { return []byte(fmt.Sprintf("k%s%05d", set, i)) }

// Keyset A and B share the same key length/value size so block layouts align.
func reuseKeyAt(set string, i int) []byte {
	// same prefix for both sets => B keys sort into the same table range as A keys
	return []byte(fmt.Sprintf("key%05d%s", i, set))
}

func reuseVal(set string, i int) []byte {
	return bytes.Repeat([]byte(set), 32)
}

type reuseArm struct {
	name  string
	bloom bool
	opts  func(o *opt.Options)
}

// sstKeys scans every .ldb in dir without a block cache; returns key -> kinds seen.
func sstScan(t *testing.T, dir string) (live map[string][]string, tomb map[string][]string) {
	live, tomb = map[string][]string{}, map[string][]string{}
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".ldb") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		st, _ := f.Stat()
		o := &opt.Options{Filter: filter.NewBloomFilter(10)}
		r, err := table.NewReader(f, st.Size(), storage.FileDesc{Type: storage.TypeTable}, nil, nil, o)
		if err != nil {
			t.Fatalf("NewReader %s: %v", p, err)
		}
		it := r.NewIterator(nil, nil)
		for it.Next() {
			k := it.Key()
			if len(k) < 8 {
				continue
			}
			uk := string(k[:len(k)-8])
			kind := k[len(k)-8] // low byte of seq<<8|type
			if kind == 1 {
				live[uk] = append(live[uk], e.Name())
			} else {
				tomb[uk] = append(tomb[uk], e.Name())
			}
		}
		it.Release()
		r.Release()
		f.Close()
	}
	return
}

func journalHas(dir string, needle []byte) bool {
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".log") {
			b, _ := os.ReadFile(filepath.Join(dir, e.Name()))
			if bytes.Contains(b, needle) {
				return true
			}
		}
	}
	return false
}

// reuseTxWrite commits a batch through an explicit goleveldb Transaction (no journal rotation,
// tables are written straight to L0), as a large batch does.
func reuseTxWrite(t *testing.T, db *leveldb.DB, b *leveldb.Batch) {
	tr, err := db.OpenTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Write(b, nil); err != nil {
		t.Fatal(err)
	}
	if err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
}

// bounded range covering every key: an unbounded range makes CompactRange rotate the journal
// (allocating a file number) even when the memdb is empty, which defeats number rollback.
var reuseRange = util.Range{Start: []byte("a"), Limit: []byte("z")}

func dirNames(dir string) string {
	ents, _ := os.ReadDir(dir)
	var n []string
	for _, e := range ents {
		n = append(n, e.Name())
	}
	return strings.Join(n, " ")
}

func logLines(dir string) []string {
	b, _ := os.ReadFile(filepath.Join(dir, "LOG"))
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if strings.Contains(l, "table@remove removed") || strings.Contains(l, "table@build created") ||
			strings.Contains(l, "memdb@flush created") || strings.Contains(l, "table@compaction") ||
			strings.Contains(l, "L0@") || strings.Contains(l, "version@stat") && false {
			out = append(out, l)
		}
	}
	return out
}

func runReuseArm(t *testing.T, arm reuseArm) (reusedN int64, missLeg, lossLeg bool) {
	dir := t.TempDir()
	o := &opt.Options{WriteBuffer: 64 * 1024, NoSync: true}
	if arm.bloom {
		o.Filter = filter.NewBloomFilter(10)
	}
	if arm.opts != nil {
		arm.opts(o)
	}
	db, err := leveldb.OpenFile(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			db.Close()
		}
	}()

	// 1. keyset A -> table, all in block cache via Get.
	bA := new(leveldb.Batch)
	for i := 0; i < reuseNKeys; i++ {
		bA.Put(reuseKeyAt("A", i), reuseVal("A", i))
	}
	reuseTxWrite(t, db, bA)
	if err := db.CompactRange(reuseRange); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < reuseNKeys; i++ {
		if _, err := db.Get(reuseKeyAt("A", i), nil); err != nil {
			t.Fatalf("A get %d: %v", i, err)
		}
		if _, err := db.Has(reuseKeyAt("A", i), nil); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("stage1 files: %s", dirNames(dir))
	// 2. delete all A through a batch bigger than WriteBuffer (Transaction path in the field),
	// then compact: tombstones drop at the bottom, producing no output; tables get removed.
	b := new(leveldb.Batch)
	for i := 0; i < reuseNKeys; i++ {
		b.Delete(reuseKeyAt("A", i))
	}
	reuseTxWrite(t, db, b)
	t.Logf("stage2 files: %s", dirNames(dir))
	// tombstone lookups read (and cache) blocks of the new delete tables.
	for i := 0; i < reuseNKeys; i++ {
		if _, err := db.Get(reuseKeyAt("A", i), nil); err != errors.ErrNotFound {
			t.Fatalf("A deleted get %d: %v", i, err)
		}
	}
	if err := db.CompactRange(reuseRange); err != nil {
		t.Fatal(err)
	}
	// 3. keyset B, same key/value sizes, written with plain Puts; > WriteBuffer so the memdb
	// rotates and flushes to a new L0 table on its own (no table compaction yet).
	for i := 0; i < reuseNB; i++ {
		if err := db.Put(reuseKeyAt("B", i*5), reuseVal("B", i), nil); err != nil {
			t.Fatal(err)
		}
	}
	for w := 0; w < 100 && !strings.Contains(strings.Join(logLines(dir), "\n"), "memdb@flush created"); w++ {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	t.Logf("ARM=%s LOG after B flush:\n  %s", arm.name, strings.Join(logLines(dir), "\n  "))

	// miss leg: Get before any further compaction.
	var missKeys [][]byte
	for i := 0; i < reuseNB; i++ {
		k := reuseKeyAt("B", i*5)
		if _, err := db.Get(k, nil); err == errors.ErrNotFound {
			missKeys = append(missKeys, k)
		}
	}
	live, _ := sstScan(t, dir)
	rawPresentOfMissed := 0
	for _, k := range missKeys {
		if len(live[string(k)]) > 0 {
			rawPresentOfMissed++
		}
	}
	missLeg = len(missKeys) > 0 && rawPresentOfMissed == len(missKeys)
	t.Logf("ARM=%s missed=%d/%d rawSSTPresentOfMissed=%d", arm.name, len(missKeys), reuseNB, rawPresentOfMissed)
	if len(missKeys) > 0 {
		t.Logf("ARM=%s missed key range: first=%s last=%s", arm.name, missKeys[0], missKeys[len(missKeys)-1])
	}

	// 4. loss leg: table compaction covering the new table.
	if err := db.CompactRange(reuseRange); err != nil {
		t.Fatal(err)
	}
	lines := logLines(dir)
	t.Logf("ARM=%s LOG after compaction:\n  %s", arm.name, strings.Join(lines, "\n  "))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	// reuse proof: a file number created more than once in LOG.
	created := map[int64]int{}
	for _, l := range lines {
		if i := strings.Index(l, "created L"); i >= 0 {
			var lv int
			var n int64
			if _, err := fmt.Sscanf(l[i+len("created L"):], "%d@%d", &lv, &n); err == nil {
				created[n]++
			}
		}
	}
	var reusedNums []int64
	for n, c := range created {
		if c > 1 {
			reusedNums = append(reusedNums, n)
		}
	}
	sort.Slice(reusedNums, func(i, j int) bool { return reusedNums[i] < reusedNums[j] })
	reusedN = int64(len(reusedNums))

	live, tomb := sstScan(t, dir)
	lost := 0
	for _, k := range missKeys {
		if len(live[string(k)]) == 0 && len(tomb[string(k)]) == 0 && !journalHas(dir, k) {
			lost++
		}
	}
	// A keys that came back from stale blocks (deleted keys resurrected).
	resurrected := 0
	for i := 0; i < reuseNKeys; i++ {
		if len(live[string(reuseKeyAt("A", i))]) > 0 {
			resurrected++
		}
	}
	lossLeg = lost > 0
	checkDB, checkErr := leveldb.OpenFile(dir, &opt.Options{ReadOnly: true, DisableBlockCache: true})
	if checkErr != nil {
		t.Fatal(checkErr)
	}
	badB, visibleA, missingB, wrongB, rawMissingB := 0, 0, 0, 0, 0
	for i := 0; i < reuseNB; i++ {
		v, e := checkDB.Get(reuseKeyAt("B", i*5), nil)
		if e != nil || !bytes.Equal(v, reuseVal("B", i)) {
			badB++
		}
		if e == errors.ErrNotFound {
			missingB++
		} else if e != nil {
			t.Fatal(e)
		} else if !bytes.Equal(v, reuseVal("B", i)) {
			wrongB++
		}
		if len(live[string(reuseKeyAt("B", i*5))]) == 0 {
			rawMissingB++
		}
	}
	for i := 0; i < reuseNKeys; i++ {
		_, e := checkDB.Get(reuseKeyAt("A", i), nil)
		if e == nil {
			visibleA++
		} else if e != errors.ErrNotFound {
			t.Fatal(e)
		}
	}
	if e := checkDB.Close(); e != nil {
		t.Fatal(e)
	}
	t.Logf("REOPEN arm=%s badB=%d visibleA=%d missingB=%d wrongB=%d rawMissingB=%d", arm.name, badB, visibleA, missingB, wrongB, rawMissingB)
	if badB != 0 || visibleA != 0 || len(missKeys) != 0 {
		t.Errorf("stale block cache after table number reuse: badB=%d visibleA=%d firstGetMiss=%d", badB, visibleA, len(missKeys))
	}
	t.Logf("ARM=%s reusedNums=%v missLeg=%v lossLeg=%v (missed=%d rawPresentOfMissed=%d lostNoTombstone=%d resurrectedAKeysInSST=%d)",
		arm.name, reusedNums, missLeg, lossLeg, len(missKeys), rawPresentOfMissed, lost, resurrected)
	return
}

func TestBlockCacheReuseAfterTableRemove(t *testing.T) {
	arms := []reuseArm{
		{name: "prod", bloom: true},
		{name: "evictRemoved", bloom: true, opts: func(o *opt.Options) { o.BlockCacheEvictRemoved = true }},
		{name: "noBlockCache", bloom: true, opts: func(o *opt.Options) { o.DisableBlockCache = true }},
		{name: "noBloom", bloom: false},
	}
	for _, a := range arms {
		a := a
		t.Run(a.name, func(t *testing.T) { runReuseArm(t, a) })
	}
}
