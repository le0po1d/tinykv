package standalone_storage

import (
	"bytes"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

type StandAloneStorageReader struct {
	txn  *badger.Txn
}

func (sr StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	iter := engine_util.NewCFIterator(cf, sr.txn)
	defer iter.Close()
	for iter.Seek([]byte("")); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Equal(item.Key(), key) {
			val, err := item.Value()
			if err != nil {
				return nil, err
			}else{
				return val, err
			}
		}
	}
	return nil, nil
}

func (sr StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, sr.txn)
	return iter
}

func (sr StandAloneStorageReader) Close() {
	sr.txn.Discard()
}


func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	badgerDb, err := badger.Open(opts)
	if err != nil {
		log.Fatal("initialize stand alone storage failed %v", err)
	}
	saStorage := &StandAloneStorage{
		db: badgerDb,
	}
	return saStorage
}

func (s StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	//false代表read-write true代表read-only
	txn := s.db.NewTransaction(false)
	sr := StandAloneStorageReader{
		txn: txn,
	}
	return sr, nil
}

func (s StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	db := s.db
	db.Update(func(txn *badger.Txn) error {
		for _, modify := range batch{
			key := []byte(modify.Cf() + "_" + string(modify.Key()))
			switch modify.Data.(type) {
			case storage.Put:
				err := txn.Set(key, modify.Value())
				if err != nil {
					return err
				}
				break
			case storage.Delete:
				err := txn.Delete(key)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	return nil
}
