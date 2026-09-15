// bep_model.go — AgentSyncModel: handles Index exchange, Request/Response for
// file transfer, and conflict detection. Bridges BEP protocol messages to the
// BEPProvider's ReceiveAgentCRD/RemoveAgentCRD for local writes.
// Extracted from root package main into internal/engine as Phase 2 S1.

package engine

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	bep "github.com/myrgic/cogos/pkg/substrate/bep"
)

const agentSyncFolderID = "cogos-agent-defs"

// ─── AgentSyncModel ─────────────────────────────────────────────────────────────

// AgentSyncModel manages the sync state for agent CRD files between peers.
// It owns the local index, processes incoming Index/IndexUpdate messages,
// and dispatches Request/Response messages to transfer files.
type AgentSyncModel struct {
	mu sync.Mutex

	engine   *BEPEngine
	folderID string
	shortID  uint64
	watchDir string
	stateDir string

	localIndex map[string]*bep.IndexEntry                  // filename → entry
	peerIndex  map[bep.DeviceID]map[string]*bep.IndexEntry // per-peer index

	nextRequestID atomic.Int32
	pendingReqs   map[int32]*pendingRequest
	pendingMu     sync.Mutex

	emitEvent func(bep.SyncEvent) // event emission callback
}

type pendingRequest struct {
	filename string
	peerID   bep.DeviceID
	entry    *bep.IndexEntry
}

// NewAgentSyncModel creates a sync model for the given engine.
func NewAgentSyncModel(engine *BEPEngine, watchDir, stateDir string, shortID uint64) *AgentSyncModel {
	return &AgentSyncModel{
		engine:      engine,
		folderID:    agentSyncFolderID,
		shortID:     shortID,
		watchDir:    watchDir,
		stateDir:    stateDir,
		localIndex:  make(map[string]*bep.IndexEntry),
		peerIndex:   make(map[bep.DeviceID]map[string]*bep.IndexEntry),
		pendingReqs: make(map[int32]*pendingRequest),
	}
}

// SetEventEmitter sets the callback for sync event emission.
func (m *AgentSyncModel) SetEventEmitter(fn func(bep.SyncEvent)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.emitEvent = fn
}

// ─── Index lifecycle ────────────────────────────────────────────────────────────

// LoadAndScanIndex loads the persisted index, scans disk for changes, and
// returns the current local index as BEPFileInfo list for initial Index message.
func (m *AgentSyncModel) LoadAndScanIndex() []*bep.FileInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Load previously persisted index.
	prev, err := bep.LoadPersistedIndex(m.stateDir)
	if err != nil {
		log.Printf("[bep-model] failed to load persisted index: %v", err)
		prev = make(map[string]*bep.IndexEntry)
	}

	// Scan disk and detect changes since last run.
	m.localIndex = bep.ScanLocalIndex(m.watchDir, m.shortID, prev)

	// Persist updated index.
	deviceIDStr := ""
	if m.engine != nil {
		deviceIDStr = bep.FormatDeviceID(m.engine.deviceID)
	}
	if err := bep.PersistIndex(m.stateDir, deviceIDStr, m.localIndex); err != nil {
		log.Printf("[bep-model] failed to persist index: %v", err)
	}

	// Convert to BEP format.
	var files []*bep.FileInfo
	for _, entry := range m.localIndex {
		files = append(files, entry.ToBEPFileInfo(m.shortID))
	}
	return files
}

// ─── Handle incoming Index ──────────────────────────────────────────────────────

// HandleIndex processes a full Index message from a peer.
func (m *AgentSyncModel) HandleIndex(peerID bep.DeviceID, files []*bep.FileInfo) {
	m.mu.Lock()

	peerIdx := make(map[string]*bep.IndexEntry, len(files))
	for _, fi := range files {
		peerIdx[fi.Name] = bep.IndexEntryFromBEP(fi)
	}
	m.peerIndex[peerID] = peerIdx

	diff := bep.DiffIndex(m.localIndex, peerIdx)
	m.mu.Unlock()

	if m.emitEvent != nil {
		m.emitEvent(bep.SyncEvent{
			Type: bep.SyncEventIndexComplete,
			Summary: map[string]any{
				"peer":       bep.FormatDeviceID(peerID)[:7],
				"files":      len(files),
				"to_request": len(diff.ToRequest),
				"conflicts":  len(diff.Conflicts),
			},
		})
	}

	for _, name := range diff.Conflicts {
		log.Printf("[bep-model] conflict detected: %s (peer %s)", name, bep.FormatDeviceID(peerID)[:7])
		if m.emitEvent != nil {
			m.emitEvent(bep.SyncEvent{
				Type:    bep.SyncEventConflict,
				Summary: map[string]any{"file": name, "peer": bep.FormatDeviceID(peerID)[:7]},
			})
		}
	}

	for _, name := range diff.ToRequest {
		entry := peerIdx[name]
		if entry != nil && entry.Deleted {
			m.applyRemoteDeletion(peerID, name, entry)
		} else {
			m.sendRequest(peerID, name, entry)
		}
	}
}

// HandleIndexUpdate processes an IndexUpdate from a peer (incremental).
func (m *AgentSyncModel) HandleIndexUpdate(peerID bep.DeviceID, files []*bep.FileInfo) {
	m.mu.Lock()

	if _, ok := m.peerIndex[peerID]; !ok {
		m.peerIndex[peerID] = make(map[string]*bep.IndexEntry)
	}
	for _, fi := range files {
		m.peerIndex[peerID][fi.Name] = bep.IndexEntryFromBEP(fi)
	}

	updatedRemote := make(map[string]*bep.IndexEntry, len(files))
	for _, fi := range files {
		updatedRemote[fi.Name] = bep.IndexEntryFromBEP(fi)
	}
	diff := bep.DiffIndex(m.localIndex, updatedRemote)
	m.mu.Unlock()

	for _, name := range diff.Conflicts {
		log.Printf("[bep-model] conflict on update: %s (peer %s)", name, bep.FormatDeviceID(peerID)[:7])
		if m.emitEvent != nil {
			m.emitEvent(bep.SyncEvent{
				Type:    bep.SyncEventConflict,
				Summary: map[string]any{"file": name, "peer": bep.FormatDeviceID(peerID)[:7]},
			})
		}
	}

	for _, name := range diff.ToRequest {
		entry := updatedRemote[name]
		if entry != nil && entry.Deleted {
			m.applyRemoteDeletion(peerID, name, entry)
		} else {
			m.sendRequest(peerID, name, entry)
		}
	}
}

// ─── Remote deletion ────────────────────────────────────────────────────────────

func (m *AgentSyncModel) applyRemoteDeletion(peerID bep.DeviceID, filename string, entry *bep.IndexEntry) {
	peerShort := bep.FormatDeviceID(peerID)[:7]

	if m.engine != nil && m.engine.provider != nil {
		if err := m.engine.provider.RemoveAgentCRD(peerShort, filename); err != nil {
			log.Printf("[bep-model] failed to remove %s on deletion sync: %v", filename, err)
		}
	}

	log.Printf("[bep-model] applied remote deletion of %s from peer %s", filename, peerShort)
	if m.emitEvent != nil {
		m.emitEvent(bep.SyncEvent{
			Type:    bep.SyncEventFileReceived,
			Summary: map[string]any{"file": filename, "peer": peerShort, "action": "delete"},
		})
	}

	m.updateLocalIndex(filename, entry)
}

// ─── Request / Response ─────────────────────────────────────────────────────────

func (m *AgentSyncModel) sendRequest(peerID bep.DeviceID, filename string, entry *bep.IndexEntry) {
	reqID := m.nextRequestID.Add(1)

	m.pendingMu.Lock()
	m.pendingReqs[reqID] = &pendingRequest{
		filename: filename,
		peerID:   peerID,
		entry:    entry,
	}
	m.pendingMu.Unlock()

	req := &bep.Request{
		ID:     reqID,
		Folder: m.folderID,
		Name:   filename,
		Offset: 0,
		Size:   int32(entry.Size),
	}

	if m.engine != nil {
		m.engine.SendToPeer(peerID, bep.MessageTypeRequest, req.Marshal())
	}
}

// HandleRequest processes an incoming file request from a peer.
func (m *AgentSyncModel) HandleRequest(req *bep.Request) *bep.Response {
	resp := &bep.Response{ID: req.ID}

	// bep.IsAgentCRDFile only checks the ".agent.yaml" suffix — it is not a
	// path guard. bep_receiver.go's ReceiveAgentCRD/RemoveAgentCRD (the
	// write/delete siblings of this read path, driven by the peer-supplied
	// filenames in Index/IndexUpdate messages) additionally reject any
	// name containing a path separator or that isn't its own base name;
	// this read path was missing that same guard (myrgic/cogos#489 round
	// 4 — a caller-supplied identifier, here a BEP peer's requested
	// filename, reaching filepath.Join unsanitized). Without it, a peer
	// requesting Name: "../../../../some/where/x.agent.yaml" would have
	// this node serve back the contents of any file ending in
	// ".agent.yaml" reachable by lexical traversal from watchDir.
	if !bep.IsAgentCRDFile(req.Name) ||
		strings.ContainsAny(req.Name, `/\`) ||
		req.Name != filepath.Base(req.Name) {
		resp.Code = bep.ErrorCodeInvalidFile
		return resp
	}

	filePath := filepath.Join(m.watchDir, req.Name)
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			resp.Code = bep.ErrorCodeNoSuchFile
		} else {
			resp.Code = bep.ErrorCodeGeneric
		}
		return resp
	}

	resp.Data = data
	resp.Code = bep.ErrorCodeNoError
	return resp
}

// HandleResponse processes a file response from a peer.
func (m *AgentSyncModel) HandleResponse(resp *bep.Response) {
	m.pendingMu.Lock()
	pending, ok := m.pendingReqs[resp.ID]
	if ok {
		delete(m.pendingReqs, resp.ID)
	}
	m.pendingMu.Unlock()

	if !ok {
		log.Printf("[bep-model] received response for unknown request %d", resp.ID)
		return
	}

	peerShort := bep.FormatDeviceID(pending.peerID)[:7]

	if resp.Code != bep.ErrorCodeNoError {
		log.Printf("[bep-model] peer %s returned error %d for %s", peerShort, resp.Code, pending.filename)
		return
	}

	// Handle deletion.
	if pending.entry != nil && pending.entry.Deleted {
		if m.engine != nil && m.engine.provider != nil {
			if err := m.engine.provider.RemoveAgentCRD(peerShort, pending.filename); err != nil {
				log.Printf("[bep-model] failed to remove %s: %v", pending.filename, err)
			}
		}
		if m.emitEvent != nil {
			m.emitEvent(bep.SyncEvent{
				Type:    bep.SyncEventFileReceived,
				Summary: map[string]any{"file": pending.filename, "peer": peerShort, "action": "delete"},
			})
		}
		m.updateLocalIndex(pending.filename, pending.entry)
		return
	}

	// Write received file via provider (atomic write + validation).
	if m.engine != nil && m.engine.provider != nil {
		if err := m.engine.provider.ReceiveAgentCRD(peerShort, pending.filename, resp.Data); err != nil {
			log.Printf("[bep-model] failed to write %s: %v", pending.filename, err)
			return
		}
	}

	log.Printf("[bep-model] received %s from peer %s (%d bytes)", pending.filename, peerShort, len(resp.Data))
	if m.emitEvent != nil {
		m.emitEvent(bep.SyncEvent{
			Type:    bep.SyncEventFileReceived,
			Summary: map[string]any{"file": pending.filename, "peer": peerShort, "size": len(resp.Data)},
		})
	}

	m.updateLocalIndex(pending.filename, pending.entry)
}

func (m *AgentSyncModel) updateLocalIndex(filename string, entry *bep.IndexEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry.Deleted {
		if existing, ok := m.localIndex[filename]; ok {
			existing.Deleted = true
			existing.Version.Merge(entry.Version)
		}
	} else {
		m.localIndex[filename] = entry
	}

	// Persist.
	deviceIDStr := ""
	if m.engine != nil {
		deviceIDStr = bep.FormatDeviceID(m.engine.deviceID)
	}
	if err := bep.PersistIndex(m.stateDir, deviceIDStr, m.localIndex); err != nil {
		log.Printf("[bep-model] failed to persist index: %v", err)
	}
}

// ─── Local change notification ──────────────────────────────────────────────────

// NotifyLocalChange handles a local file change detected by BEPProvider.
func (m *AgentSyncModel) NotifyLocalChange(filename string) {
	m.mu.Lock()

	prev := m.localIndex
	m.localIndex = bep.ScanLocalIndex(m.watchDir, m.shortID, prev)

	entry, ok := m.localIndex[filename]
	if !ok {
		if _, existed := prev[filename]; existed {
			entry = &bep.IndexEntry{
				Name:    filename,
				Deleted: true,
				Version: bep.NewVersionVector(),
			}
			if prevEntry := prev[filename]; prevEntry != nil && prevEntry.Version != nil {
				entry.Version.Merge(prevEntry.Version)
			}
			entry.Sequence = int64(entry.Version.Increment(m.shortID))
			m.localIndex[filename] = entry
		} else {
			m.mu.Unlock()
			return
		}
	}

	fi := entry.ToBEPFileInfo(m.shortID)

	deviceIDStr := ""
	if m.engine != nil {
		deviceIDStr = bep.FormatDeviceID(m.engine.deviceID)
	}
	_ = bep.PersistIndex(m.stateDir, deviceIDStr, m.localIndex)
	m.mu.Unlock()

	update := &bep.Index{
		Folder: m.folderID,
		Files:  []*bep.FileInfo{fi},
	}

	if m.engine != nil {
		m.engine.BroadcastMessage(bep.MessageTypeIndexUpdate, update.Marshal())
	}

	if m.emitEvent != nil {
		m.emitEvent(bep.SyncEvent{
			Type:    bep.SyncEventFileSent,
			Summary: map[string]any{"file": filename, "deleted": entry.Deleted},
		})
	}
}
