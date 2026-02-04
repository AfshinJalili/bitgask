package keydir

import (
	iradix "github.com/hashicorp/go-immutable-radix/v2"
)

type Radix struct {
	tree *iradix.Tree[interface{}]
}

func NewRadix() *Radix {
	return &Radix{tree: iradix.New[interface{}]()}
}

func (r *Radix) Get(key []byte) (interface{}, bool) {
	return r.tree.Get(key)
}

func (r *Radix) Set(key []byte, value interface{}) {
	k := append([]byte(nil), key...)
	tree, _, _ := r.tree.Insert(k, value)
	r.tree = tree
}

func (r *Radix) Delete(key []byte) {
	tree, _, _ := r.tree.Delete(key)
	r.tree = tree
}

func (r *Radix) Len() int {
	return r.tree.Len()
}

func (r *Radix) Range(fn Iterator) {
	r.tree.Root().Walk(func(key []byte, value interface{}) bool {
		return !fn(key, value)
	})
}

func (r *Radix) Prefix(prefix []byte, fn Iterator) {
	r.tree.Root().WalkPrefix(prefix, func(key []byte, value interface{}) bool {
		return !fn(key, value)
	})
}

func (r *Radix) Snapshot() Snapshot {
	return &radixSnapshot{tree: r.tree}
}

type radixSnapshot struct {
	tree *iradix.Tree[interface{}]
}

func (s *radixSnapshot) Get(key []byte) (interface{}, bool) {
	return s.tree.Get(key)
}

func (s *radixSnapshot) Len() int {
	return s.tree.Len()
}

func (s *radixSnapshot) Range(fn Iterator) {
	s.tree.Root().Walk(func(key []byte, value interface{}) bool {
		return !fn(key, value)
	})
}

func (s *radixSnapshot) Prefix(prefix []byte, fn Iterator) {
	s.tree.Root().WalkPrefix(prefix, func(key []byte, value interface{}) bool {
		return !fn(key, value)
	})
}
