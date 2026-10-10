package eventbus

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// Store 事件持久化存储：append-only JSONL（每行一个 Event 的 JSON）。
//
// 设计取舍（面试高频，别只说"我加了持久化"）：
//
//  1. **纯标准库**，保住项目"零第三方依赖"的定位——不需要为了"不丢"就上 Kafka。
//  2. **只追加、不原地修改**：写入是 O(1) 的顺序写，且崩溃时最坏情况只是最后一行
//     写了一半，回放时会跳过它，不会损坏整个文件。
//  3. **同步写但不 fsync**：进程被 kill（含 SIGKILL）后数据仍在 OS page cache 中，
//     由内核负责落盘；只有**机器断电**才可能丢最近的写入。这是"不丢"与
//     "不拖慢 Publish"之间的折中——每次 fsync 会让单次发布慢一个数量级。
//  4. **不做轮转**：日志只追加、不切分，生产环境需配合 logrotate 或按大小切分。
//     当前定位是"让事件活过进程重启"，不是长期归档。
//
// 注意：持久化解决的是"历史不丢"，**不等于"消息一定送到"**。
// 送达语义还需要 ack + 消费位点 + 幂等，见手册 §Q3.5。
type Store struct {
	path string
	f    *os.File
	mu   sync.Mutex
}

// OpenStore 打开（或创建）事件日志文件，以追加模式。
func OpenStore(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create event log dir: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open event log: %w", err)
	}
	return &Store{path: path, f: f}, nil
}

// Append 追加一条事件到日志。并发安全。
//
// 写入失败会原样返回错误，由调用方决定是记日志继续还是中断——
// 当前 EventBus 的选择是记日志继续（持久化失败不应让整个教学流程停摆）。
func (s *Store) Append(e Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	data = append(data, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return fmt.Errorf("store closed")
	}
	_, err = s.f.Write(data)
	return err
}

// LoadAll 按写入顺序回放全部事件。
//
// 容错策略：解析失败的行**跳过而不是报错终止**。原因是进程被 kill 时
// 最后一行很可能只写了一半，这种情况下整个服务不应该起不来。
func (s *Store) LoadAll() ([]Event, error) {
	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var events []Event
	skipped := 0

	sc := bufio.NewScanner(f)
	// 单行上限 4 MB：Event.Data 里可能带较长的教学文本（LLM 回复 + 教材引用）
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			skipped++
			continue
		}
		events = append(events, e)
	}
	if err := sc.Err(); err != nil {
		return events, fmt.Errorf("scan event log: %w", err)
	}
	if skipped > 0 {
		log.Printf("[EventBus] event log %s: skipped %d malformed line(s)", s.path, skipped)
	}
	return events, nil
}

// Close 关闭日志文件。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

// Path 返回日志文件路径。
func (s *Store) Path() string { return s.path }
