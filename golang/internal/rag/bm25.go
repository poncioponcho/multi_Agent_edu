// Package rag 实现检索增强生成（RAG）的检索侧：
// 纯标准库实现的 BM25 检索器 + 教材文档集。
//
// 设计要点（面试向）：
//   - 不依赖任何第三方库，BM25 公式手写实现，体现对信息检索原理的理解
//   - 中文切词采用 unigram + bigram 字符混合（无需分词器依赖，
//     对中文短文本检索效果稳定，是工程上的常见折中）
//   - Retriever 与生成侧解耦：Tutor Agent 与 MCP Server 共用同一检索器
package rag

import (
	"math"
	"sort"
	"strings"
)

// Document 教材文档（一篇文档对应一个知识点的讲解片段）
type Document struct {
	ID      string `json:"id"`      // 知识点ID，与知识图谱对齐
	Title   string `json:"title"`   // 章节标题
	Content string `json:"content"` // 正文
}

// ScoredDoc 检索结果
type ScoredDoc struct {
	Doc    Document `json:"doc"`
	Score  float64  `json:"score"`
	Tokens []string `json:"-"`
}

// BM25 检索器 -- Okapi BM25 实现
type BM25 struct {
	corpus     []Document
	docLens    []int
	avgDocLen  float64
	docFreq    map[string]int // token -> 包含该token的文档数
	totalDocs  int
	k1, b      float64
}

// NewBM25 构建BM25索引
func NewBM25(docs []Document) *BM25 {
	bm := &BM25{
		corpus:    docs,
		docLens:   make([]int, len(docs)),
		docFreq:   make(map[string]int),
		totalDocs: len(docs),
		k1:        1.5,
		b:         0.75,
	}
	if len(docs) == 0 {
		return bm
	}

	var totalLen int
	for i, d := range docs {
		tokens := tokenize(d.Content)
		bm.docLens[i] = len(tokens)
		totalLen += len(tokens)

		seen := make(map[string]bool)
		for _, t := range tokens {
			if !seen[t] {
				seen[t] = true
				bm.docFreq[t]++
			}
		}
	}
	bm.avgDocLen = float64(totalLen) / float64(len(docs))
	return bm
}

// Search 检索与query最相关的topK篇文档
func (bm *BM25) Search(query string, topK int) []ScoredDoc {
	if bm.totalDocs == 0 {
		return nil
	}
	qTokens := tokenize(query)
	if len(qTokens) == 0 {
		return nil
	}

	scores := make([]float64, bm.totalDocs)
	for _, qt := range qTokens {
		df := bm.docFreq[qt]
		if df == 0 {
			continue
		}
		// IDF: 逆文档频率（平滑版）
		idf := math.Log(1 + (float64(bm.totalDocs)-float64(df)+0.5)/(float64(df)+0.5))
		for i, d := range bm.corpus {
			tf := bm.termFreq(d.Content, qt)
			if tf == 0 {
				continue
			}
			// 文档长度归一化
			tfF := float64(tf)
			denom := tfF + bm.k1*(1-bm.b+bm.b*float64(bm.docLens[i])/bm.avgDocLen)
			scores[i] += idf * (tfF * (bm.k1 + 1)) / denom
		}
	}

	idx := make([]int, bm.totalDocs)
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool { return scores[idx[a]] > scores[idx[b]] })

	out := make([]ScoredDoc, 0, topK)
	for _, i := range idx {
		if scores[i] <= 0 {
			break
		}
		out = append(out, ScoredDoc{Doc: bm.corpus[i], Score: scores[i]})
		if len(out) >= topK {
			break
		}
	}
	return out
}

// termFreq 统计token在文档中的出现次数
func (bm *BM25) termFreq(content, token string) int {
	count := 0
	for _, t := range tokenize(content) {
		if t == token {
			count++
		}
	}
	return count
}

// tokenize 中文切词：rune级别的 unigram + bigram 混合
func tokenize(text string) []string {
	text = strings.ToLower(text)
	runes := []rune(text)
	tokens := make([]string, 0, len(runes)*2)
	for i := 0; i < len(runes); i++ {
		if isTokenRune(runes[i]) {
			tokens = append(tokens, string(runes[i]))
		}
		if i+1 < len(runes) && isTokenRune(runes[i]) && isTokenRune(runes[i+1]) {
			tokens = append(tokens, string(runes[i:i+2]))
		}
	}
	return tokens
}

// isTokenRune 参与索引的字符：中文/字母/数字，过滤标点与空白
func isTokenRune(r rune) bool {
	if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
		return true
	}
	if r >= 0x4E00 && r <= 0x9FFF { // CJK 统一汉字
		return true
	}
	return false
}
