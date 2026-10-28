package leveldb

import (
	"bytes"
	stderrors "errors"
	"io"
	"io/ioutil"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// Generate batch records through the real writer, then split only their journal
// framing. This avoids timing-dependent memtable rotation and hand-coded batch
// sequence numbers. All storage, including error cases, is in memory.
func readOnlyJournalFixture(t *testing.T, journals int) (storage.Storage, []storage.FileDesc) {
	t.Helper()
	stor := storage.NewMemStorage()
	db, err := Open(stor, &opt.Options{WriteBuffer: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	first := new(Batch)
	first.Put([]byte("keep"), []byte("first"))
	first.Put([]byte("updated"), []byte("before"))
	first.Put([]byte("deleted"), []byte("before"))
	second := new(Batch)
	second.Put([]byte("updated"), []byte("after"))
	second.Delete([]byte("deleted"))
	second.Put([]byte("new"), []byte("second"))
	for _, batch := range []*Batch{first, second} {
		if err := db.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	fds, err := stor.List(storage.TypeJournal)
	if err != nil || len(fds) != 1 {
		t.Fatalf("producer journals=%v err=%v; want exactly one", fds, err)
	}
	raw := readJournalFixtureFile(t, stor, fds[0])
	reader := journal.NewReader(bytes.NewReader(raw), nil, true, true)
	var records [][]byte
	for {
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := ioutil.ReadAll(record)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, data)
	}
	if len(records) != 2 {
		t.Fatalf("producer records=%d; want two", len(records))
	}
	firstFD := fds[0]
	fds = nil
	for i := 0; i < journals; i++ {
		fd := storage.FileDesc{Type: storage.TypeJournal, Num: firstFD.Num + int64(i)}
		var buf bytes.Buffer
		writer := journal.NewWriter(&buf)
		for j, data := range records {
			// The first and last journals contain the two real batches. Any
			// intermediate journals are empty but must not stop recovery.
			if (j == 0 && i != 0) || (j == 1 && i != journals-1) {
				continue
			}
			record, err := writer.Next()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := record.Write(data); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		writeJournalFixtureFile(t, stor, fd, buf.Bytes())
		fds = append(fds, fd)
	}
	return stor, fds
}

func readJournalFixtureFile(t *testing.T, stor storage.Storage, fd storage.FileDesc) []byte {
	t.Helper()
	r, err := stor.Open(fd)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	data, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeJournalFixtureFile(t *testing.T, stor storage.Storage, fd storage.FileDesc, data []byte) {
	t.Helper()
	w, err := stor.Create(fd)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
}

type journalReadAudit struct {
	storage.Storage
	mu       sync.Mutex
	readers  map[*journalAuditReader]storage.FileDesc
	failOpen storage.FileDesc
	failRead storage.FileDesc
	failure  error
}

type journalAuditReader struct {
	storage.Reader
	owner   *journalReadAudit
	failure error
}

func (s *journalReadAudit) Open(fd storage.FileDesc) (storage.Reader, error) {
	if fd == s.failOpen {
		return nil, s.failure
	}
	r, err := s.Storage.Open(fd)
	if err != nil {
		return nil, err
	}
	reader := &journalAuditReader{Reader: r, owner: s}
	if fd == s.failRead {
		reader.failure = s.failure
	}
	s.mu.Lock()
	s.readers[reader] = fd
	s.mu.Unlock()
	return reader, nil
}

func (r *journalAuditReader) Read(p []byte) (int, error) {
	if r.failure != nil {
		return 0, r.failure
	}
	return r.Reader.Read(p)
}

func (r *journalAuditReader) Close() error {
	err := r.Reader.Close()
	r.owner.mu.Lock()
	delete(r.owner.readers, r)
	r.owner.mu.Unlock()
	return err
}

func TestReadOnlyJournalRecovery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		journals int
		corrupt  int // one-based journal position; zero disables injection
		failOpen int
		failRead int
	}{
		{name: "single", journals: 1},
		{name: "multiple", journals: 2},
		{name: "empty-between", journals: 3},
		{name: "many-empty-between", journals: 32},
		{name: "first-checksum", journals: 2, corrupt: 1},
		{name: "second-checksum", journals: 2, corrupt: 2},
		{name: "second-open-error", journals: 2, failOpen: 2},
		{name: "first-read-error", journals: 2, failRead: 1},
		{name: "second-read-error", journals: 2, failRead: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stor, fds := readOnlyJournalFixture(t, tc.journals)
			defer stor.Close()
			if tc.corrupt > 0 {
				fd := fds[tc.corrupt-1]
				data := readJournalFixtureFile(t, stor, fd)
				data[len(data)-1] ^= 1
				writeJournalFixtureFile(t, stor, fd, data)
			}
			failure := stderrors.New("injected journal I/O failure")
			audit := &journalReadAudit{Storage: stor, readers: make(map[*journalAuditReader]storage.FileDesc), failure: failure}
			if tc.failOpen > 0 {
				audit.failOpen = fds[tc.failOpen-1]
			}
			if tc.failRead > 0 {
				audit.failRead = fds[tc.failRead-1]
			}
			defer func() {
				// Close leaked test readers after reporting them, so the repro
				// itself does not retain resources on the unfixed dependency.
				audit.mu.Lock()
				var leaked []storage.FileDesc
				for r, fd := range audit.readers {
					leaked = append(leaked, fd)
					_ = r.Reader.Close()
				}
				audit.mu.Unlock()
				if len(leaked) > 0 {
					t.Errorf("unclosed readers: %v", leaked)
				}
			}()
			opts := &opt.Options{ReadOnly: true}
			if tc.corrupt > 0 {
				// Existing non-strict recovery may skip corrupt records. Pin
				// error propagation in strict mode without changing that policy.
				opts.Strict = opt.DefaultStrict | opt.StrictJournal
			}
			db, err := Open(audit, opts)
			if db != nil {
				defer db.Close()
			}
			switch {
			case tc.corrupt > 0:
				if !errors.IsCorrupted(err) {
					t.Fatalf("got %v; want corruption error, not EOF or success", err)
				}
			case tc.failOpen > 0 || tc.failRead > 0:
				if !stderrors.Is(err, failure) {
					t.Fatalf("got %v; want injected error", err)
				}
			default:
				if err != nil {
					t.Fatal(err)
				}
				got := make(map[string]string)
				iter := db.NewIterator(nil, nil)
				for iter.Next() {
					got[string(iter.Key())] = string(iter.Value())
				}
				iterErr := iter.Error()
				iter.Release()
				want := map[string]string{"keep": "first", "updated": "after", "new": "second"}
				if iterErr != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("got %v err=%v; want %v", got, iterErr, want)
				}
			}
			// A successful read-only open must not rotate/remove journals.
			after, err := stor.List(storage.TypeJournal)
			sort.Slice(after, func(i, j int) bool { return after[i].Num < after[j].Num })
			if err != nil || !reflect.DeepEqual(after, fds) {
				t.Fatalf("journal set changed: %v err=%v; want %v", after, err, fds)
			}
		})
	}
}
