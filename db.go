package ldb

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	scoreMin         uint64 = 0
	scoreMax                = ^uint64(0)
	uint64EncodedLen        = 8
)

var (
	bucketHash      = []byte("h")
	bucketZetMember = []byte("zm")
	bucketZetScore  = []byte("zc")

	ErrNilDatabase     = errors.New("database is nil")
	ErrBufferTooShort  = errors.New("buffer too short")
	ErrNameTooLong     = errors.New("name length exceeds 255 bytes")
	ErrKeyTooLong      = errors.New("key length exceeds 255 bytes")
	ErrInvalidBuf      = errors.New("invalid buffer length")
	ErrKeyValuePairLen = errors.New("kvs len must be an even number")
	ErrKeyNotFound     = errors.New("key not found")
	ErrReadOnlyTx      = errors.New("read-only transaction")

	keyBufPool = sync.Pool{
		New: func() interface{} {
			return new(make([]byte, 520))
		},
	}
)

type DB struct {
	db *leveldb.DB
	mu sync.RWMutex
}

type Tx struct {
	db *leveldb.DB
	tr *leveldb.Transaction
	w  bool
}

// -------------------
// 核心事务方法
// -------------------

func (tx *Tx) Get(key []byte) ([]byte, error) {
	if tx.w {
		return tx.tr.Get(key, nil)
	}
	return tx.db.Get(key, nil)
}

func (tx *Tx) Set(key, val []byte) error {
	if !tx.w {
		return ErrReadOnlyTx
	}
	return tx.tr.Put(key, val, nil)
}

func (tx *Tx) Delete(key []byte) error {
	if !tx.w {
		return ErrReadOnlyTx
	}
	return tx.tr.Delete(key, nil)
}

func (tx *Tx) NewIter(slice *util.Range) iterator.Iterator {
	if tx.w {
		return tx.tr.NewIterator(slice, nil)
	}
	return tx.db.NewIterator(slice, nil)
}

func Open(path string) (*DB, error) {
	if path == "" {
		return nil, errors.New("empty database path")
	}
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}

func (d *DB) View(fn func(*Tx) error) error {
	if d == nil || d.db == nil || fn == nil {
		return ErrNilDatabase
	}
	return fn(&Tx{db: d.db, w: false})
}

func (d *DB) Update(fn func(*Tx) error) error {
	if d == nil || d.db == nil || fn == nil {
		return ErrNilDatabase
	}
	// OpenTransaction 支持读自己未提交的写，且提供全局事务排他锁
	tr, err := d.db.OpenTransaction()
	if err != nil {
		return err
	}
	tx := &Tx{db: d.db, tr: tr, w: true}
	if err := fn(tx); err != nil {
		tr.Discard()
		return err
	}
	return tr.Commit()
}

func (d *DB) Close() error {
	if d == nil {
		return ErrNilDatabase
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db == nil {
		return nil
	}
	err := d.db.Close()
	d.db = nil
	return err
}

// -------------------
// Hash 功能函数
// -------------------

func (d *DB) HSet(tx *Tx, name string, key, val []byte) error {
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, err := encodeKey(bucketHash, name, key, bufPtr)
	if err != nil {
		return err
	}
	return tx.Set(reallyKey, val)
}

func (d *DB) HMSet(tx *Tx, name string, kvs ...[]byte) error {
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return ErrKeyValuePairLen
	}
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	for i := 0; i < len(kvs)-1; i += 2 {
		reallyKey, err := encodeKey(bucketHash, name, kvs[i], bufPtr)
		if err != nil {
			return err
		}
		if err = tx.Set(reallyKey, kvs[i+1]); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) HIncr(tx *Tx, name string, key []byte, step int64) (uint64, error) {
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, err := encodeKey(bucketHash, name, key, bufPtr)
	if err != nil {
		return 0, err
	}

	var current uint64
	v, err := tx.Get(reallyKey)
	if err != nil && !errors.Is(err, leveldb.ErrNotFound) {
		return 0, err
	}
	if err == nil {
		if len(v) == uint64EncodedLen {
			current = B2i(v)
		} else if len(v) > 0 {
			if parsedUint, err2 := parseUintBytes(v); err2 == nil {
				current = parsedUint
			} else if parsedInt, err3 := parseIntBytes(v); err3 == nil {
				current = uint64(parsedInt)
			} else {
				return 0, fmt.Errorf("hash value is not a valid integer")
			}
		}
	}

	newVal := uint64(int64(current) + step)
	if err := tx.Set(reallyKey, I2b(newVal)); err != nil {
		return 0, err
	}
	return newVal, nil
}

func (d *DB) HGetFunc(tx *Tx, name string, key []byte, fn func(val []byte) error) error {
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, err := encodeKey(bucketHash, name, key, bufPtr)
	if err != nil {
		return err
	}

	v, err := tx.Get(reallyKey)
	if errors.Is(err, leveldb.ErrNotFound) {
		return ErrKeyNotFound
	} else if err != nil {
		return err
	}
	return fn(v)
}

func (d *DB) HMGetFunc(tx *Tx, name string, keys [][]byte, fn func(key, val []byte) error) error {
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	for _, key := range keys {
		reallyKey, err := encodeKey(bucketHash, name, key, bufPtr)
		if err != nil {
			continue
		}
		v, err := tx.Get(reallyKey)
		if errors.Is(err, leveldb.ErrNotFound) {
			v = nil
			err = nil
		} else if err != nil {
			return err
		}
		if err = fn(key, v); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) HScanFunc(tx *Tx, name string, keyStart []byte, limit int, fn func(key, val []byte) bool) error {
	if limit <= 0 {
		return nil
	}
	// 双缓冲池 + LIFO 释放机制
	bufPtr1 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr1)
	bufPtr2 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr2)

	startKey, err := encodeKey(bucketHash, name, keyStart, bufPtr1)
	if err != nil {
		return err
	}
	prefixLen := len(bucketHash) + 1 + len(name)
	prefix := startKey[:prefixLen]

	upper := keyUpperBoundToBuf(prefix, bufPtr2)
	slice := &util.Range{Start: startKey, Limit: upper}

	iter := tx.NewIter(slice)
	defer iter.Release() // 严格在 bufPut 之前释放迭代器

	n := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		k := iter.Key()
		if len(keyStart) > 0 && bytes.Compare(k, startKey) <= 0 {
			continue
		}
		if !fn(k[prefixLen:], iter.Value()) {
			break
		}
		n++
		if n == limit {
			break
		}
	}
	return iter.Error()
}

func (d *DB) HRScanFunc(tx *Tx, name string, keyStart []byte, limit int, fn func(key, val []byte) bool) error {
	if limit <= 0 {
		return nil
	}
	bufPtr1 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr1)
	bufPtr2 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr2)

	var seekKey, prefix []byte
	prefixLen := len(bucketHash) + 1 + len(name)
	isStartEmpty := len(keyStart) == 0

	if isStartEmpty {
		prefix, _ = encodeKey(bucketHash, name, nil, bufPtr1)
	} else {
		seekKey, _ = encodeKey(bucketHash, name, keyStart, bufPtr1)
		prefix = seekKey[:prefixLen]
	}

	upper := keyUpperBoundToBuf(prefix, bufPtr2)
	slice := &util.Range{Start: prefix, Limit: upper}

	iter := tx.NewIter(slice)
	defer iter.Release()

	n := 0
	var valid bool
	if isStartEmpty {
		valid = iter.Last()
	} else {
		if iter.Seek(seekKey) {
			valid = iter.Prev()
		} else {
			valid = iter.Last()
		}
	}

	for ; valid; valid = iter.Prev() {
		if !fn(iter.Key()[prefixLen:], iter.Value()) {
			break
		}
		n++
		if n == limit {
			break
		}
	}
	return iter.Error()
}

func (d *DB) HDel(tx *Tx, name string, key []byte) error {
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	reallyKey, err := encodeKey(bucketHash, name, key, bufPtr)
	if err != nil {
		return err
	}
	return tx.Delete(reallyKey)
}

func (d *DB) HMDel(tx *Tx, name string, keys [][]byte) error {
	for _, key := range keys {
		if err := d.HDel(tx, name, key); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) HDelBucket(tx *Tx, name string) error {
	prefix, err := encodeKey(bucketHash, name, nil, nil)
	if err != nil {
		return err
	}
	slice := &util.Range{Start: prefix, Limit: keyUpperBound(prefix)}
	iter := tx.NewIter(slice)
	defer iter.Release()

	for valid := iter.First(); valid; valid = iter.Next() {
		if err := tx.Delete(iter.Key()); err != nil {
			return err
		}
	}
	return iter.Error()
}

// -------------------
// ZSet 功能函数
// -------------------

func (d *DB) ZSet(tx *Tx, name string, key []byte, score uint64) error {
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, err := encodeKey(bucketZetMember, name, key, bufPtr)
	if err != nil {
		return err
	}

	scoreByte := I2b(score)
	oldScoreByte, err := tx.Get(reallyKey)
	if err == nil {
		if bytes.Equal(oldScoreByte, scoreByte) {
			return nil
		}
		var oldScore uint64
		if len(oldScoreByte) == uint64EncodedLen {
			oldScore = B2i(oldScoreByte)
		} else {
			oldScore, _ = parseUintBytes(oldScoreByte)
		}
		oldScoreKey, _ := encodeZsetScoreKey(bucketZetScore, name, key, oldScore, nil)
		_ = tx.Delete(oldScoreKey)
	} else if !errors.Is(err, leveldb.ErrNotFound) {
		return err
	}

	if err = tx.Set(reallyKey, scoreByte); err != nil {
		return err
	}

	newScoreKey, err := encodeZsetScoreKey(bucketZetScore, name, key, score, bufPtr)
	if err != nil {
		return err
	}
	return tx.Set(newScoreKey, []byte{})
}

func (d *DB) ZSetF(tx *Tx, name string, key []byte, score float64) error {
	return d.ZSet(tx, name, key, Float64ToSortableUint64(score))
}

func (d *DB) ZMSet(tx *Tx, name string, kvs ...[]byte) error {
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return ErrKeyValuePairLen
	}
	for i := 0; i < len(kvs)-1; i += 2 {
		scoreBuf := kvs[i+1]
		var score uint64
		if len(scoreBuf) == uint64EncodedLen {
			score = B2i(scoreBuf)
		} else {
			parsedUint, err := parseUintBytes(scoreBuf)
			if err != nil {
				return err
			}
			score = parsedUint
		}
		if err := d.ZSet(tx, name, kvs[i], score); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) ZIncr(tx *Tx, name string, key []byte, step int64) (uint64, error) {
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	reallyKey, err := encodeKey(bucketZetMember, name, key, bufPtr)
	if err != nil {
		return 0, err
	}

	var current uint64
	v, err := tx.Get(reallyKey)
	if err == nil {
		if len(v) == uint64EncodedLen {
			current = B2i(v)
		} else if parsedUint, err := parseUintBytes(v); err == nil {
			current = parsedUint
		}
	}

	newScore := uint64(int64(current) + step)
	if err := d.ZSet(tx, name, key, newScore); err != nil {
		return 0, err
	}
	return newScore, nil
}

func (d *DB) ZGetFunc(tx *Tx, name string, key []byte, fn func(score uint64) error) error {
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, err := encodeKey(bucketZetMember, name, key, bufPtr)
	if err != nil {
		return err
	}

	v, err := tx.Get(reallyKey)
	if errors.Is(err, leveldb.ErrNotFound) {
		return ErrKeyNotFound
	} else if err != nil {
		return err
	}

	var score uint64
	if len(v) == uint64EncodedLen {
		score = B2i(v)
	} else {
		score, _ = parseUintBytes(v)
	}
	return fn(score)
}

func (d *DB) ZMGetFunc(tx *Tx, name string, keys [][]byte, fn func(key []byte, score uint64, exists bool) error) error {
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	for _, key := range keys {
		reallyKey, _ := encodeKey(bucketZetMember, name, key, bufPtr)
		v, err := tx.Get(reallyKey)
		if errors.Is(err, leveldb.ErrNotFound) {
			if err := fn(key, 0, false); err != nil {
				return err
			}
			continue
		} else if err != nil {
			return err
		}

		var score uint64
		if len(v) == uint64EncodedLen {
			score = B2i(v)
		} else {
			score, _ = parseUintBytes(v)
		}

		if err := fn(key, score, true); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) ZGetFuncF(tx *Tx, name string, key []byte, fn func(score float64) error) error {
	return d.ZGetFunc(tx, name, key, func(score uint64) error {
		return fn(SortableUint64ToFloat64(score))
	})
}

func (d *DB) ZScanFunc(tx *Tx, name string, keyStart []byte, scoreStart, scoreEnd uint64, limit int, fn func(key []byte, score uint64) bool) error {
	if limit <= 0 {
		return nil
	}
	if scoreEnd == 0 || scoreEnd < scoreStart {
		scoreEnd = scoreMax
	}
	bufPtr1 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr1)
	bufPtr2 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr2)

	reallyKeyStart, _ := encodeZsetScoreKey(bucketZetScore, name, keyStart, scoreStart, bufPtr1)
	prefixLen := len(bucketZetScore) + 1 + len(name)
	prefix := reallyKeyStart[:prefixLen]

	upper := keyUpperBoundToBuf(prefix, bufPtr2)
	slice := &util.Range{Start: reallyKeyStart, Limit: upper}

	iter := tx.NewIter(slice)
	defer iter.Release()

	n := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		k := iter.Key()
		_, key, score, err := DecodeZsetScoreKey(bucketZetScore, k)
		if err != nil {
			continue
		}
		if score > scoreEnd {
			break
		}
		if len(keyStart) > 0 && bytes.Compare(k, reallyKeyStart) <= 0 {
			continue
		}
		if !fn(key, score) {
			break
		}
		n++
		if n == limit {
			break
		}
	}
	return iter.Error()
}

func (d *DB) ZScanFuncF(tx *Tx, name string, keyStart []byte, scoreStart, scoreEnd float64, limit int, fn func(key []byte, score float64) bool) error {
	if scoreStart == 0 && scoreEnd == 0 && len(keyStart) == 0 {
		return d.ZScanFunc(tx, name, keyStart, 0, 0, limit, func(key []byte, score uint64) bool {
			return fn(key, SortableUint64ToFloat64(score))
		})
	}
	sStart := Float64ToSortableUint64(scoreStart)
	sEnd := Float64ToSortableUint64(scoreEnd)
	if scoreEnd < scoreStart {
		sEnd = scoreMax
	}
	return d.ZScanFunc(tx, name, keyStart, sStart, sEnd, limit, func(key []byte, score uint64) bool {
		return fn(key, SortableUint64ToFloat64(score))
	})
}

func (d *DB) ZRScanFunc(tx *Tx, name string, keyStart []byte, scoreStart, scoreEnd uint64, limit int, fn func(key []byte, score uint64) bool) error {
	if limit <= 0 {
		return nil
	}
	isStartEmpty := scoreStart == 0 && len(keyStart) == 0
	if isStartEmpty {
		scoreStart = scoreMax
	}
	if scoreEnd > scoreStart {
		scoreEnd = scoreMin
	}

	bufPtr1 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr1)
	bufPtr2 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr2)

	var seekKey, prefix []byte
	prefixLen := len(bucketZetScore) + 1 + len(name)

	if isStartEmpty {
		prefix, _ = encodeKey(bucketZetScore, name, nil, bufPtr1)
	} else {
		seekKey, _ = encodeZsetScoreKey(bucketZetScore, name, keyStart, scoreStart, bufPtr1)
		prefix = seekKey[:prefixLen]
	}

	upper := keyUpperBoundToBuf(prefix, bufPtr2)
	slice := &util.Range{Start: prefix, Limit: upper}

	iter := tx.NewIter(slice)
	defer iter.Release()

	var valid bool
	if isStartEmpty {
		valid = iter.Last()
	} else {
		if iter.Seek(seekKey) {
			valid = iter.Prev()
		} else {
			valid = iter.Last()
		}
	}

	n := 0
	for ; valid; valid = iter.Prev() {
		_, key, score, err := DecodeZsetScoreKey(bucketZetScore, iter.Key())
		if err != nil {
			continue
		}
		if score < scoreEnd {
			break
		}
		if !fn(key, score) {
			break
		}
		n++
		if n == limit {
			break
		}
	}
	return iter.Error()
}

func (d *DB) ZRScanFuncF(tx *Tx, name string, keyStart []byte, scoreStart, scoreEnd float64, limit int, fn func(key []byte, score float64) bool) error {
	if scoreStart == 0 && scoreEnd == 0 && len(keyStart) == 0 {
		return d.ZRScanFunc(tx, name, keyStart, 0, 0, limit, func(key []byte, score uint64) bool {
			return fn(key, SortableUint64ToFloat64(score))
		})
	}
	sStart := Float64ToSortableUint64(scoreStart)
	sEnd := Float64ToSortableUint64(scoreEnd)
	if scoreEnd > scoreStart {
		sEnd = scoreMin
	}
	return d.ZRScanFunc(tx, name, keyStart, sStart, sEnd, limit, func(key []byte, score uint64) bool {
		return fn(key, SortableUint64ToFloat64(score))
	})
}

func (d *DB) ZDel(tx *Tx, name string, key []byte) error {
	reallyKey, _ := encodeKey(bucketZetMember, name, key, nil)
	oldScoreByte, err := tx.Get(reallyKey)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}

	var oldScore uint64
	if len(oldScoreByte) == uint64EncodedLen {
		oldScore = B2i(oldScoreByte)
	} else {
		oldScore, _ = parseUintBytes(oldScoreByte)
	}

	if err := tx.Delete(reallyKey); err != nil {
		return err
	}
	reallyScoreKey, _ := encodeZsetScoreKey(bucketZetScore, name, key, oldScore, nil)
	return tx.Delete(reallyScoreKey)
}

func (d *DB) ZMDel(tx *Tx, name string, keys [][]byte) error {
	for _, key := range keys {
		if err := d.ZDel(tx, name, key); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) ZDelBucket(tx *Tx, name string) error {
	prefixMember, _ := encodeKey(bucketZetMember, name, nil, nil)
	iterM := tx.NewIter(&util.Range{Start: prefixMember, Limit: keyUpperBound(prefixMember)})
	for valid := iterM.First(); valid; valid = iterM.Next() {
		_ = tx.Delete(iterM.Key())
	}
	iterM.Release()

	prefixScore, _ := encodeKey(bucketZetScore, name, nil, nil)
	iterS := tx.NewIter(&util.Range{Start: prefixScore, Limit: keyUpperBound(prefixScore)})
	for valid := iterS.First(); valid; valid = iterS.Next() {
		_ = tx.Delete(iterS.Key())
	}
	iterS.Release()
	return nil
}

// -----------------------
// 统计与压缩
// -----------------------

func (d *DB) Compact() error {
	if d == nil || d.db == nil {
		return ErrNilDatabase
	}
	return d.db.CompactRange(util.Range{Start: nil, Limit: nil})
}

// CompactZip (goleveldb) 获取数据快照，全量写入新 DB 实例完成极致压缩，随后打包为 ZIP
func (d *DB) CompactZip(dstPath, zipPath string) error {
	if d == nil || d.db == nil {
		return errors.New("nil database")
	}

	// 1. 触发主库全量 Compaction（可选，因为后面会全量重建，但为了遵循你的接口逻辑保留）
	err := d.db.CompactRange(util.Range{Start: nil, Limit: nil})
	if err != nil {
		return err
	}

	isTemp := false
	if dstPath == "" {
		dstPath = filepath.Join(os.TempDir(), "leveldb_compact_tmp")
		isTemp = true
	}
	_ = os.RemoveAll(dstPath)

	// 2. 获取主库的一致性读快照
	snapshot, err := d.db.GetSnapshot()
	if err != nil {
		return err
	}
	defer snapshot.Release()

	// 创建用于承载压缩数据的新 DB
	dstDB, err := leveldb.OpenFile(dstPath, nil)
	if err != nil {
		return err
	}

	// 3. 全量迭代并写入新 DB (模拟 bbolt 的重新填充)
	iter := snapshot.NewIterator(nil, nil)
	batch := new(leveldb.Batch)

	for iter.Next() {
		batch.Put(iter.Key(), iter.Value())
		if batch.Len() >= 10000 { // 批量提交，避免内存爆满
			if err := dstDB.Write(batch, nil); err != nil {
				iter.Release()
				dstDB.Close()
				return err
			}
			batch.Reset()
		}
	}
	iter.Release()

	if err := iter.Error(); err != nil {
		dstDB.Close()
		return err
	}
	if batch.Len() > 0 {
		if err := dstDB.Write(batch, nil); err != nil {
			dstDB.Close()
			return err
		}
	}
	// 必须关闭新 DB，确保文件被刷入磁盘且锁被释放
	dstDB.Close()

	defer func() {
		if isTemp {
			_ = os.RemoveAll(dstPath)
		}
	}()

	// 4. 将新的紧凑型 DB 目录打包为 zip 文件
	if err := zipDirectory(dstPath, zipPath); err != nil {
		_ = os.Remove(zipPath)
		return err
	}

	return nil
}

// -----------------------
// 辅助与编码解码函数 (同上保持不变)
// -----------------------

func Float64ToSortableUint64(f float64) uint64 {
	u := math.Float64bits(f)
	if u&(1<<63) != 0 {
		return ^u
	}
	return u | (1 << 63)
}

func SortableUint64ToFloat64(u uint64) float64 {
	if u&(1<<63) != 0 {
		u = u &^ (1 << 63)
	} else {
		u = ^u
	}
	return math.Float64frombits(u)
}

func encodeKey(bucket []byte, name string, key []byte, bufPtr *[]byte) ([]byte, error) {
	if len(name) > 255 {
		return nil, ErrNameTooLong
	}
	if len(key) > 255 {
		return nil, ErrKeyTooLong
	}
	reqLen := len(bucket) + 1 + len(name) + len(key)

	var buf []byte
	if bufPtr == nil {
		buf = make([]byte, reqLen)
	} else {
		if cap(*bufPtr) < reqLen {
			*bufPtr = make([]byte, reqLen)
		}
		buf = (*bufPtr)[:reqLen]
	}

	copy(buf, bucket)
	idx := len(bucket)
	buf[idx] = byte(len(name))
	idx++
	copy(buf[idx:], name)
	idx += len(name)
	copy(buf[idx:], key)
	return buf, nil
}

func encodeZsetScoreKey(bucket []byte, name string, key []byte, score uint64, bufPtr *[]byte) ([]byte, error) {
	if len(name) > 255 {
		return nil, ErrNameTooLong
	}
	if len(key) > 255 {
		return nil, ErrKeyTooLong
	}
	reqLen := len(bucket) + 1 + len(name) + uint64EncodedLen + len(key)

	var buf []byte
	if bufPtr == nil {
		buf = make([]byte, reqLen)
	} else {
		if cap(*bufPtr) < reqLen {
			*bufPtr = make([]byte, reqLen)
		}
		buf = (*bufPtr)[:reqLen]
	}

	copy(buf, bucket)
	idx := len(bucket)
	buf[idx] = byte(len(name))
	idx++
	copy(buf[idx:], name)
	idx += len(name)
	binary.BigEndian.PutUint64(buf[idx:idx+uint64EncodedLen], score)
	idx += uint64EncodedLen
	copy(buf[idx:], key)
	return buf, nil
}

func DecodeZsetScoreKey(bucket []byte, buf []byte) (name string, key []byte, score uint64, err error) {
	minLen := len(bucket) + 1 + uint64EncodedLen
	if len(buf) < minLen {
		return "", nil, 0, ErrInvalidBuf
	}
	idx := len(bucket)
	nameLen := int(buf[idx])
	idx++
	if len(buf) < idx+nameLen+uint64EncodedLen {
		return "", nil, 0, ErrInvalidBuf
	}
	name = string(buf[idx : idx+nameLen])
	idx += nameLen
	score = binary.BigEndian.Uint64(buf[idx : idx+uint64EncodedLen])
	idx += uint64EncodedLen
	key = make([]byte, len(buf)-idx)
	copy(key, buf[idx:])
	return name, key, score, nil
}

func keyUpperBound(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return end
		}
	}
	return nil
}

func I2b(v uint64) []byte {
	b := make([]byte, uint64EncodedLen)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func B2i(v []byte) uint64 {
	if len(v) < uint64EncodedLen {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

func parseUintBytes(b []byte) (uint64, error) {
	if len(b) == 0 {
		return 0, errors.New("empty bytes")
	}
	var n uint64
	for _, ch := range b {
		if ch < '0' || ch > '9' {
			return 0, errors.New("invalid char")
		}
		n = n*10 + uint64(ch-'0')
	}
	return n, nil
}

func parseIntBytes(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, errors.New("empty bytes")
	}
	neg := false
	if b[0] == '-' {
		neg = true
		b = b[1:]
	}
	if len(b) == 0 {
		return 0, errors.New("empty bytes")
	}
	var n int64
	for _, ch := range b {
		if ch < '0' || ch > '9' {
			return 0, errors.New("invalid char")
		}
		n = n*10 + int64(ch-'0')
	}
	if neg {
		return -n, nil
	}
	return n, nil
}

// 优化后：零分配求上界
func keyUpperBoundToBuf(b []byte, bufPtr *[]byte) []byte {
	if len(b) == 0 {
		return nil
	}

	reqLen := len(b)
	var end []byte

	if bufPtr == nil {
		end = make([]byte, reqLen)
	} else {
		if cap(*bufPtr) < reqLen {
			*bufPtr = make([]byte, reqLen)
		}
		end = (*bufPtr)[:reqLen]
	}

	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			return end[:i+1]
		}
	}
	return nil
}

// zipDirectory 辅助函数：将一个包含多文件的目录结构打包为 .zip
func zipDirectory(sourceDir, zipPath string) (err error) {
	outFile, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := outFile.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	zipWriter := zip.NewWriter(outFile)
	defer func() {
		if cerr := zipWriter.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	return filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 获取相对于根目录的相对路径
		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		if relPath == "." {
			return nil // 跳过根目录本身
		}

		// ZIP 规范要求使用正斜杠 (/)
		relPath = filepath.ToSlash(relPath)
		if info.IsDir() {
			relPath += "/"
		}

		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		header.Name = relPath
		header.Method = zip.Deflate

		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}

		// 如果是目录，写入 Header 后即可返回
		if info.IsDir() {
			return nil
		}

		// 如果是文件，拷贝文件内容
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = io.Copy(writer, file)
		return err
	})
}
