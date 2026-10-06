package rag

import (
	"fmt"
	"strings"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// BM25 检索器基准测试
//
// 动机：Search 的实现里，termFreq 会对**每个 query token** 重新 tokenize
// 整篇文档（bm25.go:123-131），没有倒排索引。Tutor Agent 每次生成回复都会
// 调用一次 Search（agents.go:178/238），因此这是每请求的固定 CPU 成本。
//
// 运行：
//   go test ./internal/rag/ -bench . -benchmem
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkSearch 在真实内置教材库（8 篇）上测检索成本。
func BenchmarkSearch(b *testing.B) {
	bm := NewRetriever()
	queries := []string{
		"一元二次方程怎么解",
		"勾股定理",
		"抛物线顶点坐标",
		"二次函数 y = x² + 2x + 1 的顶点在哪里",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Search(queries[i%len(queries)], 3)
	}
}

// BenchmarkSearchByCorpusSize 教材库越大，Search 的劣化程度。
//
// Search 的复杂度约为 O(|query tokens| × Σ|doc tokens|)，因为对每个
// query token 都要遍历全部文档并逐个重新切词——随语料规模线性甚至更差。
// 这个基准回答的问题是：「教材库从 8 篇扩到 1000 篇后，检索还够快吗？」
func BenchmarkSearchByCorpusSize(b *testing.B) {
	base := NewRetriever()

	for _, n := range []int{8, 100, 1000} {
		b.Run(fmt.Sprintf("corpus=%d", n), func(b *testing.B) {
			docs := make([]Document, 0, n)
			// 用真实教材文档循环填充，保持文档长度与词分布接近真实
			for i := 0; i < n; i++ {
				src := base.corpus[i%len(base.corpus)]
				docs = append(docs, Document{
					ID:      fmt.Sprintf("%s-%d", src.ID, i),
					Title:   src.Title,
					Content: src.Content,
				})
			}
			bm := NewBM25(docs)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bm.Search("一元二次方程怎么解", 3)
			}
		})
	}
}

// BenchmarkTokenize 单独测切词成本，用于确认 Search 的开销主要来自
// 「对每个 query token 重复切词」这一实现选择。
func BenchmarkTokenize(b *testing.B) {
	text := strings.Repeat("一元二次方程的求根公式与判别式", 20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tokenize(text)
	}
}
