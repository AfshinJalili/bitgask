package keydir

import art "github.com/plar/go-adaptive-radix-tree"

type ART struct {
	tree art.Tree
}

func NewART() *ART {
	return &ART{tree: art.New()}
}

func (a *ART) Get(key []byte) (interface{}, bool) {
	return a.tree.Search(key)
}

func (a *ART) Set(key []byte, value interface{}) {
	k := append([]byte(nil), key...)
	a.tree.Insert(k, value)
}

func (a *ART) Delete(key []byte) {
	a.tree.Delete(key)
}

func (a *ART) Len() int {
	return a.tree.Size()
}

func (a *ART) Range(fn Iterator) {
	a.tree.ForEach(func(node art.Node) bool {
		return fn(node.Key(), node.Value())
	})
}

func (a *ART) Prefix(prefix []byte, fn Iterator) {
	a.tree.ForEachPrefix(prefix, func(node art.Node) bool {
		return fn(node.Key(), node.Value())
	})
}
