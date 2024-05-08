package memory

import (
	"go.uber.org/zap"
	"testing"
)

func TestStorage_Get(t *testing.T) {
	t.Run("get non-existing key", func(t *testing.T) {
		s := New(zap.NewNop())

		_, ok := s.Get("k")
		if ok {
			t.Error("key should not exist")
		}
	})

	t.Run("get existing key", func(t *testing.T) {
		s := New(zap.NewNop())
		s.Set("k", "v")

		_, ok := s.Get("k")
		if !ok {
			t.Error("key must exist")
		}
	})
}

func TestStorage_Set(t *testing.T) {
	t.Run("set non-existing key", func(t *testing.T) {
		s := New(zap.NewNop())
		s.Set("k", "v")

		_, ok := s.Get("k")
		if !ok {
			t.Error("key should exist")
		}
	})

	t.Run("set existing key", func(t *testing.T) {
		s := New(zap.NewNop())
		s.Set("k", "v")
		s.Set("k", "v2")

		v, ok := s.Get("k")
		if !ok || v != "v2" {
			t.Error("key must be v2")
		}
	})
}

func TestStorage_Del(t *testing.T) {
	t.Run("del non-existing key", func(t *testing.T) {
		s := New(zap.NewNop())
		s.Del("k")

		_, ok := s.Get("k")
		if ok {
			t.Error("key should not exist")
		}
	})

	t.Run("del existing key", func(t *testing.T) {
		s := New(zap.NewNop())
		s.Set("k", "v")
		s.Del("k")

		_, ok := s.Get("k")
		if ok {
			t.Error("key should not exist")
		}
	})
}
