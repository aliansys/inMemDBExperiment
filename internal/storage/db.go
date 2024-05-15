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

	db struct {
		storage Storage
	}
)

func New(storage Storage) *db {
	return &db{
		storage: storage,
	}
}

func (db *db) Process(q compute.Query) (string, error) {
	switch q.Command {
	case compute.CommandGet:
		v, ok := db.storage.Get(q.Key)
		if !ok {
			return "", errors.New("key not found")
		}

		return v, nil
	case compute.CommandSet:
		db.storage.Set(q.Key, q.Arg)
	case compute.CommandDel:
		db.storage.Del(q.Key)
	}

	return "", nil
}
