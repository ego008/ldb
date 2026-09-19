package ldb

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func setupTestDB(t *testing.T) (*DB, func()) {
	dir, err := os.MkdirTemp("", "leveldb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}
	return db, cleanup
}

func TestHashOperations(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	bucketName := "testHash"

	// 1. 测试 HSet 与 HGetFunc
	err := db.Update(func(tx *Tx) error {
		return db.HSet(tx, bucketName, []byte("key1"), []byte("val1"))
	})
	if err != nil {
		t.Fatalf("HSet failed: %v", err)
	}

	err = db.View(func(tx *Tx) error {
		return db.HGetFunc(tx, bucketName, []byte("key1"), func(val []byte) error {
			if !bytes.Equal(val, []byte("val1")) {
				t.Errorf("Expected val1, got %s", val)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("HGetFunc failed: %v", err)
	}

	// 2. 测试 HMSet 与 HMGetFunc
	err = db.Update(func(tx *Tx) error {
		return db.HMSet(tx, bucketName, []byte("key2"), []byte("val2"), []byte("key3"), []byte("val3"))
	})
	if err != nil {
		t.Fatalf("HMSet failed: %v", err)
	}

	err = db.View(func(tx *Tx) error {
		keys := [][]byte{[]byte("key2"), []byte("key3")}
		return db.HMGetFunc(tx, bucketName, keys, func(key, val []byte) error {
			if bytes.Equal(key, []byte("key2")) && !bytes.Equal(val, []byte("val2")) {
				t.Errorf("key2 val mismatch")
			}
			return nil
		})
	})

	// 3. 测试 HScanFunc
	err = db.View(func(tx *Tx) error {
		count := 0
		err := db.HScanFunc(tx, bucketName, nil, 10, func(key, val []byte) bool {
			count++
			return true
		})
		if err != nil {
			return err
		}
		if count != 3 {
			t.Errorf("Expected 3 items from HScanFunc, got %d", count)
		}
		return nil
	})

	// 4. 测试 HIncr
	err = db.Update(func(tx *Tx) error {
		newVal, err := db.HIncr(tx, bucketName, []byte("counter"), 5)
		if err != nil || newVal != 5 {
			t.Errorf("HIncr expected 5, got %d (err: %v)", newVal, err)
		}
		return nil
	})

	// 5. 测试 HDel
	err = db.Update(func(tx *Tx) error {
		if err := db.HDel(tx, bucketName, []byte("key1")); err != nil {
			return err
		}
		return nil
	})

	err = db.View(func(tx *Tx) error {
		return db.HGetFunc(tx, bucketName, []byte("key1"), func(val []byte) error { return nil })
	})
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestZSetOperations(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	bucketName := "testZSet"

	// 1. 测试 ZSet 与 ZGetFunc
	err := db.Update(func(tx *Tx) error {
		return db.ZSet(tx, bucketName, []byte("member1"), 100)
	})
	if err != nil {
		t.Fatalf("ZSet failed: %v", err)
	}

	err = db.View(func(tx *Tx) error {
		return db.ZGetFunc(tx, bucketName, []byte("member1"), func(score uint64) error {
			if score != 100 {
				t.Errorf("Expected score 100, got %d", score)
			}
			return nil
		})
	})

	// 2. 测试 ZMSet
	err = db.Update(func(tx *Tx) error {
		return db.ZMSet(tx, bucketName, []byte("member2"), I2b(200), []byte("member3"), I2b(300))
	})
	if err != nil {
		t.Fatalf("ZMSet failed: %v", err)
	}

	// 3. 测试 ZScanFunc (按分数范围查询)
	err = db.View(func(tx *Tx) error {
		count := 0
		err := db.ZScanFunc(tx, bucketName, nil, 100, 250, 10, func(key []byte, score uint64) bool {
			count++
			return true
		})
		if err != nil {
			return err
		}
		if count != 2 {
			t.Errorf("Expected 2 items in range 100-250, got %d", count)
		}
		return nil
	})

	// 4. 测试 ZRScanFunc (逆向扫描)
	err = db.View(func(tx *Tx) error {
		var lastScore uint64 = math.MaxUint64
		err := db.ZRScanFunc(tx, bucketName, nil, 0, 0, 10, func(key []byte, score uint64) bool {
			if score > lastScore {
				t.Errorf("ZRScan returned non-descending order: %d followed by %d", lastScore, score)
			}
			lastScore = score
			return true
		})
		return err
	})

	// 5. 测试 ZIncr
	err = db.Update(func(tx *Tx) error {
		newScore, err := db.ZIncr(tx, bucketName, []byte("member1"), 50)
		if err != nil || newScore != 150 {
			t.Errorf("ZIncr expected 150, got %d", newScore)
		}
		return nil
	})

	// 6. 测试 ZDel
	err = db.Update(func(tx *Tx) error {
		return db.ZDel(tx, bucketName, []byte("member2"))
	})
	err = db.View(func(tx *Tx) error {
		return db.ZGetFunc(tx, bucketName, []byte("member2"), func(score uint64) error { return nil })
	})
	if err != ErrKeyNotFound {
		t.Errorf("Expected member2 to be deleted, got err: %v", err)
	}
}
