package leveldb

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

type frozenRemoveStorage struct {
	storage.Storage
	entered chan storage.FileDesc
	release chan struct{}
	closed  chan struct{}
	once    sync.Once
	fail    bool
	mu      sync.Mutex
	logs    []string
	removed []storage.FileDesc
}

func (s *frozenRemoveStorage) Remove(fd storage.FileDesc) error {
	if fd.Type == storage.TypeJournal {
		blocked := false
		s.once.Do(func() { blocked = true; s.entered <- fd; <-s.release })
		s.mu.Lock()
		s.removed = append(s.removed, fd)
		s.mu.Unlock()
		if blocked && s.fail {
			return errors.New("injected journal remove error")
		}
	}
	return s.Storage.Remove(fd)
}
func (s *frozenRemoveStorage) Log(v string) { s.mu.Lock(); s.logs = append(s.logs, v); s.mu.Unlock() }
func (s *frozenRemoveStorage) Close() error { close(s.closed); return s.Storage.Close() }
func frozenAwait(t *testing.T, c <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout: %s", name)
	}
}
func frozenRotate(db *DB) error {
	db.writeLockC <- struct{}{}
	defer func() { <-db.writeLockC }()
	m, err := db.rotateMem(0, false)
	if m != nil {
		m.decref()
	}
	return err
}
func TestFrozenRemoveAllowsMemoryAccess(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "error"}[fail], func(t *testing.T) {
			s := &frozenRemoveStorage{Storage: storage.NewMemStorage(), entered: make(chan storage.FileDesc, 1), release: make(chan struct{}), closed: make(chan struct{}), fail: fail}
			db, err := Open(s, &opt.Options{WriteBuffer: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			released := false
			defer func() {
				if !released {
					close(s.release)
				}
				db.Close()
			}()
			if err = db.Put([]byte("old"), []byte("value"), nil); err != nil {
				t.Fatal(err)
			}
			iter := db.NewIterator(nil, nil)
			reader := db.getEffectiveMem()
			referencesReleased := false
			releaseReferences := func() {
				if !referencesReleased {
					iter.Release()
					reader.decref()
					referencesReleased = true
				}
			}
			defer releaseReferences()
			readerRefs := reader.getref()
			if err = frozenRotate(db); err != nil {
				t.Fatal(err)
			}
			go db.compTriggerWait(db.mcompCmdC)
			var fd storage.FileDesc
			select {
			case fd = <-s.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("Remove not entered")
			}
			access := make(chan struct{})
			go func() {
				m := db.getEffectiveMem()
				m.decref()
				e, f := db.getMems()
				if e != nil {
					e.decref()
				}
				if f != nil {
					f.decref()
				}
				close(access)
			}()
			frozenAwait(t, access, "getEffectiveMem/getMems while Remove blocked")
			written := make(chan struct{})
			var writeErr error
			go func() { writeErr = db.Put([]byte("new"), []byte("value2"), nil); close(written) }()
			frozenAwait(t, written, "small Write while Remove blocked")
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			db.memMu.RLock()
			if db.frozenMem != reader || db.frozenJournalFd != fd {
				t.Error("frozen state changed before Remove finished")
			}
			db.memMu.RUnlock()
			rotated := make(chan struct{})
			var rotateErr error
			rotationStarted := make(chan struct{})
			go func() {
				db.writeLockC <- struct{}{}
				close(rotationStarted)
				m, e := db.rotateMem(0, false)
				if m != nil {
					m.decref()
				}
				rotateErr = e
				<-db.writeLockC
				close(rotated)
			}()
			frozenAwait(t, rotationStarted, "rotation holds writer lock")
			select {
			case <-rotated:
				t.Fatal("rotation acknowledged before Remove finished")
			case <-time.After(50 * time.Millisecond):
			}
			close(s.release)
			released = true
			frozenAwait(t, rotated, "rotation after Remove")
			if rotateErr != nil {
				t.Fatal(rotateErr)
			}
			if err = db.compTriggerWait(db.mcompCmdC); err != nil {
				t.Fatal(err)
			}
			if !iter.First() || string(iter.Key()) != "old" || string(iter.Value()) != "value" {
				t.Fatal("reader lost frozen value")
			}
			if err := iter.Error(); err != nil {
				t.Fatal(err)
			}
			if reader.getref() != readerRefs-1 || reader.DB == nil {
				t.Fatalf("reader reference invalid: %d", reader.getref())
			}
			s.mu.Lock()
			first := s.removed[0]
			logs := strings.Join(s.logs, "\n")
			s.mu.Unlock()
			if first != fd {
				t.Fatal("wrong journal removed")
			}
			if fail && !strings.Contains(logs, "injected journal remove error") {
				t.Fatal("Remove error not logged")
			}
			releaseReferences()
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(s, nil)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range map[string]string{"old": "value", "new": "value2"} {
				got, e := db.Get([]byte(key), nil)
				if e != nil || !bytes.Equal(got, []byte(value)) {
					t.Fatalf("reopen %s: %q %v", key, got, e)
				}
			}
		})
	}
}
func TestFrozenRemoveCloseWaits(t *testing.T) {
	s := &frozenRemoveStorage{Storage: storage.NewMemStorage(), entered: make(chan storage.FileDesc, 1), release: make(chan struct{}), closed: make(chan struct{})}
	db, err := Open(s, nil)
	if err != nil {
		t.Fatal(err)
	}
	db.closer = s
	if err = db.Put([]byte("key"), []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	if err = frozenRotate(db); err != nil {
		t.Fatal(err)
	}
	go db.compTriggerWait(db.mcompCmdC)
	select {
	case <-s.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Remove not entered")
	}
	done := make(chan struct{})
	go func() { db.Close(); close(done) }()
	frozenAwait(t, db.closeC, "Close signaled workers")
	select {
	case <-s.closed:
		t.Fatal("storage closed during Remove")
	case <-done:
		t.Fatal("Close returned during Remove")
	case <-time.After(50 * time.Millisecond):
	}
	close(s.release)
	frozenAwait(t, done, "Close after Remove")
	frozenAwait(t, s.closed, "storage Close")
}
