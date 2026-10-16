package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const DefaultFileWatcherPollInterval = time.Second

// StreamTailer watches an external harness stream and emits normalized blocks.
type StreamTailer interface {
	// Tail starts watching a file/directory for new JSONL lines.
	// It sends normalized CogBlocks on the output channel.
	// It respects context cancellation for graceful shutdown.
	Tail(ctx context.Context, path string, out chan<- CogBlock) error
	// Name returns the adapter name (e.g., "claude-code").
	Name() string
}

// TailerStats captures manager-side ingestion state for a single tailer.
type TailerStats struct {
	EventsIngested uint64
	Errors         uint64
	LastEventTime  time.Time
}

type tailerRegistration struct {
	tailer StreamTailer
	path   string
}

// TailerManager runs multiple stream tailers and tracks per-tailer stats.
type TailerManager struct {
	mu            sync.RWMutex
	registrations []tailerRegistration
	stats         map[string]TailerStats
	out           chan<- CogBlock
}

// NewTailerManager creates a manager that forwards normalized blocks to out.
func NewTailerManager(out chan<- CogBlock) *TailerManager {
	return &TailerManager{
		stats: make(map[string]TailerStats),
		out:   out,
	}
}

// Register adds a tailer and source path to the manager.
func (m *TailerManager) Register(tailer StreamTailer, path string) error {
	if tailer == nil {
		return errors.New("stream tailer: nil tailer")
	}
	if path == "" {
		return fmt.Errorf("stream tailer %q: empty path", tailer.Name())
	}
	if tailer.Name() == "" {
		return errors.New("stream tailer: empty name")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.stats[tailer.Name()]; exists {
		return fmt.Errorf("stream tailer %q already registered", tailer.Name())
	}

	m.registrations = append(m.registrations, tailerRegistration{tailer: tailer, path: path})
	m.stats[tailer.Name()] = TailerStats{}
	return nil
}

// Stats returns a snapshot of current per-tailer metrics.
func (m *TailerManager) Stats() map[string]TailerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := make(map[string]TailerStats, len(m.stats))
	for name, stats := range m.stats {
		snapshot[name] = stats
	}
	return snapshot
}

// Run starts all registered tailers and blocks until they stop.
func (m *TailerManager) Run(ctx context.Context) error {
	if m.out == nil {
		return errors.New("tailer manager: nil output channel")
	}

	m.mu.RLock()
	registrations := append([]tailerRegistration(nil), m.registrations...)
	m.mu.RUnlock()

	if len(registrations) == 0 {
		return nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, len(registrations))
	var workers sync.WaitGroup

	for _, registration := range registrations {
		registration := registration
		localOut := make(chan CogBlock, 16)

		workers.Add(1)
		go func() {
			defer workers.Done()
			for block := range localOut {
				m.recordEvent(registration.tailer.Name(), block.Timestamp)
				select {
				case m.out <- block:
				case <-runCtx.Done():
				}
			}
		}()

		workers.Add(1)
		go func() {
			defer workers.Done()
			defer close(localOut)

			err := registration.tailer.Tail(runCtx, registration.path, localOut)
			if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}

			m.recordError(registration.tailer.Name())
			errCh <- fmt.Errorf("%s: %w", registration.tailer.Name(), err)
			cancel()
		}()
	}

	workers.Wait()
	close(errCh)

	var runErr error
	for err := range errCh {
		runErr = errors.Join(runErr, err)
	}
	return runErr
}

func (m *TailerManager) recordEvent(name string, blockTime time.Time) {
	if blockTime.IsZero() {
		blockTime = time.Now().UTC()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	stats := m.stats[name]
	stats.EventsIngested++
	stats.LastEventTime = blockTime
	m.stats[name] = stats
}

func (m *TailerManager) recordError(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats := m.stats[name]
	stats.Errors++
	m.stats[name] = stats
}

// FileWatcher polls a file for newly appended newline-delimited content.
type FileWatcher struct {
	PollInterval time.Duration
}

// NewFileWatcher creates a polling file watcher.
func NewFileWatcher(pollInterval time.Duration) *FileWatcher {
	return &FileWatcher{PollInterval: pollInterval}
}

// Watch monitors path for appended lines and invokes onLine for each complete line.
func (w *FileWatcher) Watch(ctx context.Context, path string, onLine func([]byte) error) error {
	if onLine == nil {
		return errors.New("file watcher: nil line callback")
	}

	state := fileWatcherState{path: path}
	defer state.close()

	ticker := time.NewTicker(w.pollInterval())
	defer ticker.Stop()

	for {
		if err := state.poll(onLine); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (w *FileWatcher) pollInterval() time.Duration {
	if w == nil || w.PollInterval <= 0 {
		return DefaultFileWatcherPollInterval
	}
	return w.PollInterval
}

type fileWatcherState struct {
	path       string
	file       *os.File
	fileInfo   os.FileInfo
	offset     int64
	partialBuf []byte
}

func (s *fileWatcherState) poll(onLine func([]byte) error) error {
	currentInfo, statErr := os.Stat(s.path)
	if statErr != nil && !os.IsNotExist(statErr) {
		return fmt.Errorf("stat watched file %q: %w", s.path, statErr)
	}

	if s.file == nil {
		if currentInfo == nil {
			return nil
		}
		if err := s.openCurrent(); err != nil {
			return err
		}
	}

	if currentInfo != nil && s.fileInfo != nil && !os.SameFile(s.fileInfo, currentInfo) {
		if err := s.readAvailable(onLine); err != nil {
			return err
		}
		s.close()
		if err := s.openCurrent(); err != nil {
			return err
		}
	}

	if s.file == nil {
		return nil
	}

	return s.readAvailable(onLine)
}

func (s *fileWatcherState) openCurrent() error {
	file, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open watched file %q: %w", s.path, err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("stat opened file %q: %w", s.path, err)
	}

	s.file = file
	s.fileInfo = fileInfo
	s.offset = 0
	s.partialBuf = nil
	return nil
}

func (s *fileWatcherState) readAvailable(onLine func([]byte) error) error {
	fileInfo, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("stat active file %q: %w", s.path, err)
	}

	if fileInfo.Size() < s.offset {
		if _, err := s.file.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("rewind truncated file %q: %w", s.path, err)
		}
		s.offset = 0
		s.partialBuf = nil
	}

	if fileInfo.Size() == s.offset {
		s.fileInfo = fileInfo
		return nil
	}

	if _, err := s.file.Seek(s.offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek watched file %q: %w", s.path, err)
	}

	data, err := io.ReadAll(s.file)
	if err != nil {
		return fmt.Errorf("read watched file %q: %w", s.path, err)
	}
	if len(data) == 0 {
		s.fileInfo = fileInfo
		return nil
	}

	s.offset += int64(len(data))
	combined := append(append([]byte(nil), s.partialBuf...), data...)
	start := 0
	for index, value := range combined {
		if value != '\n' {
			continue
		}

		line := combined[start:index]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if err := onLine(append([]byte(nil), line...)); err != nil {
			return err
		}
		start = index + 1
	}

	if start < len(combined) {
		s.partialBuf = append([]byte(nil), combined[start:]...)
	} else {
		s.partialBuf = nil
	}
	s.fileInfo = fileInfo
	return nil
}

func (s *fileWatcherState) close() {
	if s.file != nil {
		_ = s.file.Close()
	}
	s.file = nil
	s.fileInfo = nil
	s.offset = 0
	s.partialBuf = nil
}
