package storage

import (
	"aliansys/inMemDBExperiment/internal/compute"
	"errors"
)

type (
	Storage interface {
		Get(key string) (string, bool)
		Set(key, val string)
		Del(key string)
	}

	WAL interface {
		Set(key, val string) chan error
		Del(key string) chan error
	}

	db struct {
		storage Storage
		wal     WAL
	}
)

func New(storage Storage, wal WAL) *db {
	return &db{
		storage: storage,
		wal:     wal,
	}
}

func (db *db) Process(q compute.Query) (string, error) {
	switch q.Command {
	case compute.CommandGet:
		return db.get(q.Key)
	case compute.CommandSet:
		return db.set(q.Key, q.Arg)
	case compute.CommandDel:
		return db.del(q.Key)
	}

	return "", nil
}

func (db *db) get(key string) (string, error) {
	v, ok := db.storage.Get(key)
	if !ok {
		return "", errors.New("key not found")
	}

	return v, nil
}

func (db *db) set(key, val string) (string, error) {
	if db.wal != nil {
		wait := db.wal.Set(key, val)
		err := <-wait
		if err != nil {
			return "", err
		}
	}
	db.storage.Set(key, val)
	return "", nil
}

func (db *db) del(key string) (string, error) {
	if db.wal != nil {
		wait := db.wal.Del(key)
		err := <-wait
		if err != nil {
			return "", err
		}
	}
	db.storage.Del(key)
	return "", nil
}
