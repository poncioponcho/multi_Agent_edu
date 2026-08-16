package model

// KnowledgeNode 知识图谱节点
type KnowledgeNode struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	Difficulty    float64  `json:"difficulty"`
	Prerequisites []string `json:"prerequisites"`
}

// KnowledgeGraph 知识图谱 -- 有向无环图(DAG)，箭头表示前置依赖
// 数据来源：docs/knowledge-points.md（初中数学示例体系）
var KnowledgeGraph = []KnowledgeNode{
	{ID: "arithmetic", Name: "四则运算", Difficulty: 0.1},
	{ID: "fractions", Name: "分数运算", Difficulty: 0.2, Prerequisites: []string{"arithmetic"}},
	{ID: "negative_numbers", Name: "负数", Difficulty: 0.15, Prerequisites: []string{"arithmetic"}},
	{ID: "sets", Name: "集合", Difficulty: 0.25, Prerequisites: []string{"arithmetic"}},
	{ID: "logic", Name: "简易逻辑", Difficulty: 0.3, Prerequisites: []string{"sets"}},
	{ID: "algebraic_expr", Name: "代数式", Difficulty: 0.3, Prerequisites: []string{"arithmetic", "negative_numbers"}},
	{ID: "factoring", Name: "因式分解", Difficulty: 0.4, Prerequisites: []string{"algebraic_expr"}},
	{ID: "sequence", Name: "数列", Difficulty: 0.5, Prerequisites: []string{"algebraic_expr"}},
	{ID: "linear_eq_1", Name: "一元一次方程", Difficulty: 0.35, Prerequisites: []string{"algebraic_expr"}},
	{ID: "linear_eq_2", Name: "二元一次方程组", Difficulty: 0.45, Prerequisites: []string{"linear_eq_1"}},
	{ID: "quadratic_eq", Name: "一元二次方程", Difficulty: 0.55, Prerequisites: []string{"factoring", "linear_eq_1"}},
	{ID: "inequality", Name: "不等式", Difficulty: 0.4, Prerequisites: []string{"linear_eq_1"}},
	{ID: "coordinate", Name: "平面直角坐标系", Difficulty: 0.3, Prerequisites: []string{"negative_numbers"}},
	{ID: "linear_func", Name: "一次函数", Difficulty: 0.45, Prerequisites: []string{"linear_eq_1", "coordinate"}},
	{ID: "quadratic_func", Name: "二次函数", Difficulty: 0.6, Prerequisites: []string{"quadratic_eq"}},
	{ID: "pythagorean", Name: "勾股定理", Difficulty: 0.35, Prerequisites: []string{"arithmetic"}},
	{ID: "similar_triangle", Name: "相似三角形", Difficulty: 0.5, Prerequisites: []string{"pythagorean", "fractions"}},
	{ID: "trig_basic", Name: "三角函数基础", Difficulty: 0.55, Prerequisites: []string{"pythagorean", "fractions"}},
	{ID: "probability", Name: "概率初步", Difficulty: 0.4, Prerequisites: []string{"fractions"}},
	{ID: "statistics", Name: "数据统计", Difficulty: 0.35, Prerequisites: []string{"arithmetic", "fractions"}},
}

var knowledgeIndex = func() map[string]KnowledgeNode {
	m := make(map[string]KnowledgeNode, len(KnowledgeGraph))
	for _, n := range KnowledgeGraph {
		m[n.ID] = n
	}
	return m
}()

// GetKnowledge 按ID查询知识点
func GetKnowledge(id string) (KnowledgeNode, bool) {
	n, ok := knowledgeIndex[id]
	return n, ok
}

// GetPrerequisites 获取某个知识点的前置知识点（名称展开）
func GetPrerequisites(id string) []KnowledgeNode {
	n, ok := knowledgeIndex[id]
	if !ok {
		return nil
	}
	var out []KnowledgeNode
	for _, pid := range n.Prerequisites {
		if p, ok := knowledgeIndex[pid]; ok {
			out = append(out, p)
		}
	}
	return out
}

// RecommendNext 基于掌握度推荐可学节点：
// 仅推荐「前置知识已达标（mastery >= 0.6）」且「自身未掌握」的知识点
func RecommendNext(mastery map[string]float64) []KnowledgeNode {
	var out []KnowledgeNode
	for _, n := range KnowledgeGraph {
		if mastery[n.ID] >= 0.6 {
			continue
		}
		ready := true
		for _, pid := range n.Prerequisites {
			if mastery[pid] < 0.6 {
				ready = false
				break
			}
		}
		if ready {
			out = append(out, n)
		}
	}
	return out
}
