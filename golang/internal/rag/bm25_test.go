package rag

import (
	"testing"
)

func TestBM25Retrieval(t *testing.T) {
	bm := NewRetriever()

	cases := []struct {
		query    string
		wantTop1 string // 期望Top1命中的知识点ID
	}{
		{"一元二次方程怎么解", "quadratic_eq"},
		{"勾股定理", "pythagorean"},
		{"抛物线顶点", "quadratic_func"},
		{"分数 概率", "probability"},
	}
	for _, c := range cases {
		hits := bm.Search(c.query, 1)
		if len(hits) == 0 {
			t.Errorf("query %q: no results", c.query)
			continue
		}
		if hits[0].Doc.ID != c.wantTop1 {
			t.Errorf("query %q: top1=%s want %s (score=%.4f)", c.query, hits[0].Doc.ID, c.wantTop1, hits[0].Score)
		}
	}
}

func TestBM25TopK(t *testing.T) {
	bm := NewRetriever()
	hits := bm.Search("方程", 3)
	if len(hits) > 3 {
		t.Fatalf("topK=3 but got %d hits", len(hits))
	}
	if len(hits) == 0 {
		t.Fatal("expected hits for 方程")
	}
	// 分数应降序
	for i := 1; i < len(hits); i++ {
		if hits[i].Score > hits[i-1].Score {
			t.Fatalf("scores not sorted: %v > %v", hits[i].Score, hits[i-1].Score)
		}
	}
}

func TestTokenize(t *testing.T) {
	tokens := tokenize("一元二次方程 ax² + bx + c = 0")
	if len(tokens) == 0 {
		t.Fatal("tokenize returned empty")
	}
	// 标点/空白不应产生token
	for _, tk := range tokens {
		if tk == "+" || tk == "=" || tk == " " {
			t.Fatalf("invalid token: %q", tk)
		}
	}
}
