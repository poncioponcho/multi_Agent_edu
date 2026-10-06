package model

import (
	"sync"
	"sync/atomic"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// 并发正确性测试：LearnerModel.UpdateMastery 的读-改-写（RMW）原子性
//
// 背景：UpdateMastery 内部先调用 GetState 拿到 *KnowledgeState 指针（该调用
// 内部持锁），但随后的读-改-写（读 Mastery → 贝叶斯更新 → 写回 Mastery /
// Attempts++ / CorrectCount++）全部在锁外执行。
//
// 后果：同一 learner + 同一 knowledge_id 的两个并发提交会读到同一个旧
// Mastery 并互相覆盖，即经典的 lost update（静默丢数据，不报错）。
//
// 运行方式（两个都要跑，作用不同）：
//   go test ./internal/model/ -run Concurrent            # 断言丢更新
//   go test ./internal/model/ -race -run Concurrent      # 额外拿到竞态报告
// ─────────────────────────────────────────────────────────────────────────────

// TestUpdateMasteryConcurrentLostUpdate 断言：N 个 goroutine 各提交 M 次，
// 最终 Attempts 必须等于 N*M —— 每一次提交都必须被计入。
func TestUpdateMasteryConcurrentLostUpdate(t *testing.T) {
	const (
		goroutines   = 8
		perGoroutine = 200
		wantAttempts = goroutines * perGoroutine
		wantCorrect  = wantAttempts / 2 // 每轮 i%2==0 判对，恰好一半
	)

	m := NewLearnerModel("learner-race")
	var wg sync.WaitGroup
	start := make(chan struct{}) // 对齐起跑线，最大化线程交错

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < perGoroutine; i++ {
				m.UpdateMastery("kp-1", i%2 == 0)
			}
		}()
	}
	close(start)
	wg.Wait()

	state := m.GetState("kp-1")

	if state.Attempts != wantAttempts {
		t.Errorf("lost update: Attempts=%d, want %d —— 丢失了 %d 次提交（%.1f%%）",
			state.Attempts, wantAttempts, wantAttempts-state.Attempts,
			100*float64(wantAttempts-state.Attempts)/float64(wantAttempts))
	}
	if state.CorrectCount != wantCorrect {
		t.Errorf("lost update: CorrectCount=%d, want %d", state.CorrectCount, wantCorrect)
	}
}

// TestUpdateMasteryConcurrentDistinctKnowledgePoints 是对照组：每个 goroutine
// 操作各自独立的知识点，不存在跨 goroutine 的共享状态，因此不应丢更新。
//
// 这个用例的作用是证明上一条的失败确实来自「同一 key 的 RMW 竞争」，
// 而不是测试写法本身有问题。
func TestUpdateMasteryConcurrentDistinctKnowledgePoints(t *testing.T) {
	const (
		goroutines   = 8
		perGoroutine = 200
	)

	m := NewLearnerModel("learner-isolated")
	var wg sync.WaitGroup
	start := make(chan struct{})

	for g := 0; g < goroutines; g++ {
		kp := "kp-" + string(rune('A'+g)) // 每个 goroutine 一个独立知识点
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < perGoroutine; i++ {
				m.UpdateMastery(kp, i%2 == 0)
			}
		}()
	}
	close(start)
	wg.Wait()

	for g := 0; g < goroutines; g++ {
		kp := "kp-" + string(rune('A'+g))
		if got := m.GetState(kp).Attempts; got != perGoroutine {
			t.Errorf("知识点 %s: Attempts=%d, want %d", kp, got, perGoroutine)
		}
	}
}

// TestUpdateMasteryMasteryMonotonicUnderConcurrency 观测另一个症状：并发提交
// 全部答对时，mastery 应当单调上升并逼近 1.0。
//
// 丢更新会同时压低 mastery 的收敛速度——这条用来量化「丢更新对业务指标的
// 影响」，而不只是计数对不上。
func TestUpdateMasteryMasteryMonotonicUnderConcurrency(t *testing.T) {
	const (
		goroutines   = 8
		perGoroutine = 200
	)

	concurrent := NewLearnerModel("learner-concurrent")
	sequential := NewLearnerModel("learner-sequential")

	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < perGoroutine; i++ {
				concurrent.UpdateMastery("kp-1", true) // 全部答对
			}
		}()
	}
	close(start)
	wg.Wait()

	// 同样次数的串行提交作为基准
	for i := 0; i < goroutines*perGoroutine; i++ {
		sequential.UpdateMastery("kp-1", true)
	}

	cMastery := concurrent.GetState("kp-1").Mastery
	sMastery := sequential.GetState("kp-1").Mastery

	t.Logf("全部答对 %d 次后：并发 mastery=%.4f, 串行 mastery=%.4f, 差距=%.4f",
		goroutines*perGoroutine, cMastery, sMastery, sMastery-cMastery)

	if cMastery < sMastery {
		t.Errorf("并发 mastery(%.4f) 低于串行 mastery(%.4f)：丢更新拖慢了掌握度收敛",
			cMastery, sMastery)
	}
}

// TestGetStateConcurrentReadWrite 覆盖另一个读侧竞态：
// UpdateMastery 在锁外写 state.Mastery，而 API 层（handler.go 的
// /next-topics）在锁外读 state.Mastery —— 读写未同步，-race 会直接报错。
func TestGetStateConcurrentReadWrite(t *testing.T) {
	m := NewLearnerModel("learner-readwrite")
	var stop atomic.Bool

	var wg sync.WaitGroup
	wg.Add(2)

	// 写者
	go func() {
		defer wg.Done()
		for !stop.Load() {
			m.UpdateMastery("kp-1", true)
		}
	}()

	// 读者：模拟 handler.go 里 m.GetState(id).Mastery 的读法
	go func() {
		defer wg.Done()
		for i := 0; i < 5000; i++ {
			_ = m.GetState("kp-1").Mastery
		}
		stop.Store(true)
	}()

	wg.Wait()
}
