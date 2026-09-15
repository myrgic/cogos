// trm.go — Pure Go implementation of MambaTRM inference.
//
// The TRM (Temporal Retrieval Model) uses Mamba selective state spaces to
// process temporally ordered session events and predict which workspace
// chunks are most relevant. It maintains a "light cone" — compressed SSM
// hidden state representing the observer's trajectory through the workspace.
//
// This is a zero-dependency inference engine: no gonum, no CGO. All math
// is done with raw float32 slices and manual loops. The model is tiny
// (~2.3M params, 2,282,113 exactly for DefaultTRMConfig — see
// docs/EVALUATION.md), so this is efficient enough.
//
// Binary weight format (TRM1):
//
//	4 bytes: magic "TRM1"
//	4 bytes: uint32 number of tensors
//	Per tensor: name_len(4) + name(N) + ndim(4) + shape(4*ndim) + data(4*numel)
//	All values little-endian. Data is float32, row-major.
package engine

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
)

// ── Configuration ────────────────────────────────────────────────────────────

// TRMConfig holds the model hyperparameters. Must match the training config.
type TRMConfig struct {
	DModel     int // embedding dimension (384)
	DState     int // SSM state dimension (4)
	DConv      int // convolution kernel width (2)
	NLayers    int // number of Mamba blocks (2)
	Expand     int // expansion factor (1)
	NProbes    int // number of attention probes (4)
	DHead      int // attention head dimension (128)
	NEventType int // number of event types (4)
}

// DefaultTRMConfig returns the config matching the trained model.
func DefaultTRMConfig() TRMConfig {
	return TRMConfig{
		DModel:     384,
		DState:     4,
		DConv:      2,
		NLayers:    2,
		Expand:     1,
		NProbes:    4,
		DHead:      128,
		NEventType: 4,
	}
}

// ── Core types ───────────────────────────────────────────────────────────────

// Linear represents a dense layer: y = x @ W^T + b
type Linear struct {
	Weight [][]float32 // [out_features][in_features]
	Bias   []float32   // [out_features] or nil
	InDim  int
	OutDim int
}

// LayerNorm implements layer normalization: (x - mean) / sqrt(var + eps) * w + b
type LayerNorm struct {
	Weight []float32 // [dim]
	Bias   []float32 // [dim]
	Dim    int
	Eps    float32
}

// Conv1D implements depthwise 1D convolution with kernel size K.
// For single-step inference, we store the last (K-1) inputs as state.
type Conv1D struct {
	Weight   [][][]float32 // [channels][1][kernel_size]
	Bias     []float32     // [channels]
	Channels int
	Kernel   int
}

// MambaBlock is a single selective SSM block with pre-norm residual.
type MambaBlock struct {
	Norm    LayerNorm
	InProj  [][]float32 // [2*d_inner][d_model] — projects to (x_ssm, z)
	Conv    Conv1D
	XProj   [][]float32 // [d_state*2+1][d_inner] — projects to (B, C, delta)
	LogA    [][]float32 // [d_inner][d_state]
	D       []float32   // [d_inner] skip connection
	OutProj [][]float32 // [d_model][d_inner]
	DInner  int
	DState  int
}

// AttentionProbe lets trajectory context attend over the candidate set.
type AttentionProbe struct {
	QProj   [][]float32 // [d_head][d_model]
	KProj   [][]float32 // [d_head][d_model]
	VProj   [][]float32 // [d_head][d_model]
	OutProj [][]float32 // [d_model][d_head]
	DHead   int
}

// ScoreHead is the final scoring MLP: Linear(2*d) → GELU → Linear(1).
type ScoreHead struct {
	W1   [][]float32 // [d_model][2*d_model]
	B1   []float32   // [d_model]
	W2   [][]float32 // [1][d_model]
	B2   []float32   // [1]
	DIn  int         // 2*d_model
	DMid int         // d_model
}

// MambaTRM is the full temporal retrieval model.
type MambaTRM struct {
	Config     TRMConfig
	TypeEmbed  [][]float32      // [n_event_types][d_model]
	InputProj  Linear           // 2*d_model → d_model
	Layers     []MambaBlock     // [n_layers]
	FinalNorm  LayerNorm        // d_model
	Probes     []AttentionProbe // [n_probes]
	ProbeNorms []LayerNorm      // [n_probes]
	Head       ScoreHead
}

// MambaState is the SSM hidden state for one layer.
type MambaState struct {
	H [][]float32 // [d_inner][d_state]
}

// LightCone holds per-layer SSM states — the compressed observer trajectory.
type LightCone struct {
	States []MambaState // one per layer
}

// ── Math primitives ──────────────────────────────────────────────────────────

func silu(x float32) float32 {
	return x * sigmoid(x)
}

func sigmoid(x float32) float32 {
	return 1.0 / (1.0 + float32(math.Exp(float64(-x))))
}

func gelu(x float32) float32 {
	// Approximation: x * 0.5 * (1 + tanh(sqrt(2/pi) * (x + 0.044715*x^3)))
	c := float32(math.Sqrt(2.0 / math.Pi))
	return x * 0.5 * (1.0 + float32(math.Tanh(float64(c*(x+0.044715*x*x*x)))))
}

func softplus(x float32) float32 {
	if x > 20 {
		return x // avoid overflow
	}
	return float32(math.Log(1.0 + math.Exp(float64(x))))
}

// matVec computes y = M @ x where M is [rows][cols] and x is [cols].
func matVec(m [][]float32, x []float32) []float32 {
	rows := len(m)
	y := make([]float32, rows)
	for i := 0; i < rows; i++ {
		var s float32
		row := m[i]
		for j := range row {
			s += row[j] * x[j]
		}
		y[i] = s
	}
	return y
}

// matVecAdd computes y = M @ x + b.
func matVecAdd(m [][]float32, x []float32, b []float32) []float32 {
	y := matVec(m, x)
	if b != nil {
		for i := range y {
			y[i] += b[i]
		}
	}
	return y
}

// dotProduct computes the dot product of two vectors.
func dotProduct(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

// vecAdd computes a + b element-wise, storing result in a.
func vecAdd(a, b []float32) {
	for i := range a {
		a[i] += b[i]
	}
}

// softmax computes stable softmax over a vector.
func softmaxVec(x []float32) []float32 {
	n := len(x)
	out := make([]float32, n)
	// Find max for numerical stability
	maxVal := x[0]
	for i := 1; i < n; i++ {
		if x[i] > maxVal {
			maxVal = x[i]
		}
	}
	var sumExp float32
	for i := 0; i < n; i++ {
		out[i] = float32(math.Exp(float64(x[i] - maxVal)))
		sumExp += out[i]
	}
	invSum := 1.0 / sumExp
	for i := 0; i < n; i++ {
		out[i] *= invSum
	}
	return out
}

// ── LayerNorm ────────────────────────────────────────────────────────────────

func (ln *LayerNorm) Apply(x []float32) []float32 {
	n := ln.Dim
	out := make([]float32, n)

	// Compute mean
	var mean float32
	for i := 0; i < n; i++ {
		mean += x[i]
	}
	mean /= float32(n)

	// Compute variance
	var variance float32
	for i := 0; i < n; i++ {
		d := x[i] - mean
		variance += d * d
	}
	variance /= float32(n)

	// Normalize
	invStd := float32(1.0 / math.Sqrt(float64(variance+ln.Eps)))
	for i := 0; i < n; i++ {
		out[i] = (x[i]-mean)*invStd*ln.Weight[i] + ln.Bias[i]
	}
	return out
}

// ── Linear ───────────────────────────────────────────────────────────────────

func (l *Linear) Apply(x []float32) []float32 {
	return matVecAdd(l.Weight, x, l.Bias)
}

// ── MambaBlock.Step ──────────────────────────────────────────────────────────

// Step processes a single event through the Mamba block, updating the SSM state.
// Input: x [d_model], state: MambaState (or nil for fresh).
// Returns: output [d_model], new state.
//
// Note: unlike the forward path, step() does NOT include the residual connection.
// This matches the Python SelectiveSSM.step() method used for inference.
func (mb *MambaBlock) Step(x []float32, state *MambaState) ([]float32, *MambaState) {
	dInner := mb.DInner
	dState := mb.DState

	// Pre-norm (no residual in step mode — matches Python inference path)
	xNorm := mb.Norm.Apply(x)

	// Input projection: xNorm → (x_ssm, z) each [d_inner]
	xz := matVec(mb.InProj, xNorm) // [2*d_inner]
	xSSM := xz[:dInner]
	z := xz[dInner:]

	// SiLU activation on x_ssm (skip conv1d in single-step mode, matching Python)
	for i := range xSSM {
		xSSM[i] = silu(xSSM[i])
	}

	// Input-dependent SSM parameters
	xParams := matVec(mb.XProj, xSSM) // [d_state*2 + 1]
	B := xParams[:dState]
	C := xParams[dState : 2*dState]
	delta := softplus(xParams[2*dState])

	// Initialize state if nil
	if state == nil {
		state = &MambaState{
			H: make([][]float32, dInner),
		}
		for i := range state.H {
			state.H[i] = make([]float32, dState)
		}
	}

	// SSM update: h = exp(delta * A) * h + (delta * B) * x_ssm
	//             y = (h * C).sum(-1) + D * x_ssm
	newState := &MambaState{
		H: make([][]float32, dInner),
	}
	y := make([]float32, dInner)

	for i := 0; i < dInner; i++ {
		newH := make([]float32, dState)
		var ySum float32
		for j := 0; j < dState; j++ {
			// A is negative (log_A stores log of positive values)
			a := -float32(math.Exp(float64(mb.LogA[i][j])))
			dA := float32(math.Exp(float64(delta * a)))
			dB := delta * B[j]

			newH[j] = dA*state.H[i][j] + dB*xSSM[i]
			ySum += newH[j] * C[j]
		}
		newState.H[i] = newH
		y[i] = ySum + mb.D[i]*xSSM[i]
	}

	// Gate with SiLU(z)
	for i := range y {
		y[i] *= silu(z[i])
	}

	// Output projection
	out := matVec(mb.OutProj, y) // [d_model]

	return out, newState
}

// ── AttentionProbe ───────────────────────────────────────────────────────────

// Apply runs single-head attention: context attends over candidates.
// context: [d_model], candidates: [n_candidates][d_model]
// Returns: [d_model] attention-enriched context.
func (ap *AttentionProbe) Apply(context []float32, candidates [][]float32) []float32 {
	nCand := len(candidates)
	dHead := ap.DHead

	// Q = context @ Wq^T → [d_head]
	q := matVec(ap.QProj, context)

	// K = candidates @ Wk^T → [n_cand][d_head]
	// V = candidates @ Wv^T → [n_cand][d_head]
	k := make([][]float32, nCand)
	v := make([][]float32, nCand)
	for i := 0; i < nCand; i++ {
		k[i] = matVec(ap.KProj, candidates[i])
		v[i] = matVec(ap.VProj, candidates[i])
	}

	// Attention scores: Q @ K^T / sqrt(d_head) → [n_cand]
	scale := float32(1.0 / math.Sqrt(float64(dHead)))
	scores := make([]float32, nCand)
	for i := 0; i < nCand; i++ {
		scores[i] = dotProduct(q, k[i]) * scale
	}

	// Softmax
	attnWeights := softmaxVec(scores)

	// Weighted sum: attn @ V → [d_head]
	attnOut := make([]float32, dHead)
	for i := 0; i < nCand; i++ {
		for j := 0; j < dHead; j++ {
			attnOut[j] += attnWeights[i] * v[i][j]
		}
	}

	// Output projection: [d_head] → [d_model]
	return matVec(ap.OutProj, attnOut)
}

// ── MambaTRM methods ─────────────────────────────────────────────────────────

// Step processes a single event through the full model, updating the light cone.
// event: [d_model] embedding, eventType: 0-3 (query/retrieval/search/edit).
// Returns context vector [d_model] and updated light cone.
func (m *MambaTRM) Step(event []float32, eventType int, lc *LightCone) ([]float32, *LightCone) {
	cfg := m.Config

	// Type embedding
	typeEmb := m.TypeEmbed[eventType] // [d_model]

	// Concatenate event + type_embed → [2*d_model]
	combined := make([]float32, 2*cfg.DModel)
	copy(combined, event)
	copy(combined[cfg.DModel:], typeEmb)

	// Input projection: [2*d_model] → [d_model]
	x := m.InputProj.Apply(combined)

	// Initialize light cone if needed
	if lc == nil {
		lc = &LightCone{
			States: make([]MambaState, cfg.NLayers),
		}
	}

	newLC := &LightCone{
		States: make([]MambaState, cfg.NLayers),
	}

	// Process through Mamba blocks
	for i := 0; i < cfg.NLayers; i++ {
		var state *MambaState
		if len(lc.States) > i && len(lc.States[i].H) > 0 {
			state = &lc.States[i]
		}
		var newState *MambaState
		x, newState = m.Layers[i].Step(x, state)
		newLC.States[i] = *newState
	}

	// Final norm
	x = m.FinalNorm.Apply(x)

	return x, newLC
}

// ScoreCandidates scores a set of candidates against a trajectory context.
// context: [d_model] from Step(), candidates: [n][d_model] embeddings.
// Returns [n] scores (higher = more relevant).
func (m *MambaTRM) ScoreCandidates(context []float32, candidates [][]float32) []float32 {
	cfg := m.Config
	nCand := len(candidates)

	// Iterative attention refinement: context attends over candidates N times
	ctx := make([]float32, cfg.DModel)
	copy(ctx, context)

	for i := 0; i < cfg.NProbes; i++ {
		normed := m.ProbeNorms[i].Apply(ctx)
		attnOut := m.Probes[i].Apply(normed, candidates)
		vecAdd(ctx, attnOut)
	}

	// Score each candidate: concat(context, candidate) → MLP → scalar
	scores := make([]float32, nCand)
	for i := 0; i < nCand; i++ {
		// Concatenate: [2*d_model]
		combined := make([]float32, 2*cfg.DModel)
		copy(combined, ctx)
		copy(combined[cfg.DModel:], candidates[i])

		// First linear + GELU
		h := matVecAdd(m.Head.W1, combined, m.Head.B1)
		for j := range h {
			h[j] = gelu(h[j])
		}

		// Second linear → scalar
		out := matVecAdd(m.Head.W2, h, m.Head.B2)
		scores[i] = out[0]
	}

	return scores
}

// GetLightConeNorms returns per-layer SSM state norms and a compressed scalar.
func (m *MambaTRM) GetLightConeNorms(lc *LightCone) ([]float64, float64) {
	if lc == nil || len(lc.States) == 0 {
		return nil, 0
	}

	norms := make([]float64, len(lc.States))
	var total float64

	for i, state := range lc.States {
		var sumSq float64
		for _, row := range state.H {
			for _, val := range row {
				sumSq += float64(val) * float64(val)
			}
		}
		norms[i] = math.Sqrt(sumSq)
		total += norms[i]
	}

	compressed := total / float64(len(lc.States))
	return norms, compressed
}

// ── Weight loading ───────────────────────────────────────────────────────────

// rawTensor holds a loaded tensor with its name and shape.
type rawTensor struct {
	Name  string
	Shape []int
	Data  []float32
}

// loadTensors reads all tensors from a TRM1 binary file.
func loadTensors(path string) ([]rawTensor, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	// Read magic
	magic := make([]byte, 4)
	if _, err := io.ReadFull(f, magic); err != nil {
		return nil, fmt.Errorf("read magic: %w", err)
	}
	if string(magic) != "TRM1" {
		return nil, fmt.Errorf("bad magic: %q (want TRM1)", string(magic))
	}

	// Read tensor count
	var nTensors uint32
	if err := binary.Read(f, binary.LittleEndian, &nTensors); err != nil {
		return nil, fmt.Errorf("read tensor count: %w", err)
	}

	tensors := make([]rawTensor, nTensors)
	for i := uint32(0); i < nTensors; i++ {
		// Name
		var nameLen uint32
		if err := binary.Read(f, binary.LittleEndian, &nameLen); err != nil {
			return nil, fmt.Errorf("tensor %d: read name len: %w", i, err)
		}
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(f, nameBytes); err != nil {
			return nil, fmt.Errorf("tensor %d: read name: %w", i, err)
		}

		// Shape
		var ndim uint32
		if err := binary.Read(f, binary.LittleEndian, &ndim); err != nil {
			return nil, fmt.Errorf("tensor %d: read ndim: %w", i, err)
		}
		shape := make([]int, ndim)
		numel := 1
		for j := uint32(0); j < ndim; j++ {
			var dim uint32
			if err := binary.Read(f, binary.LittleEndian, &dim); err != nil {
				return nil, fmt.Errorf("tensor %d: read shape[%d]: %w", i, j, err)
			}
			shape[j] = int(dim)
			numel *= int(dim)
		}

		// Data
		data := make([]float32, numel)
		if err := binary.Read(f, binary.LittleEndian, data); err != nil {
			return nil, fmt.Errorf("tensor %d (%s): read data: %w", i, string(nameBytes), err)
		}

		tensors[i] = rawTensor{
			Name:  string(nameBytes),
			Shape: shape,
			Data:  data,
		}
	}

	return tensors, nil
}

// reshape2D converts a flat float32 slice into a [rows][cols] slice.
func reshape2D(data []float32, rows, cols int) [][]float32 {
	out := make([][]float32, rows)
	for i := 0; i < rows; i++ {
		out[i] = data[i*cols : (i+1)*cols]
	}
	return out
}

// reshape3D converts a flat float32 slice into a [a][b][c] slice.
func reshape3D(data []float32, a, b, c int) [][][]float32 {
	out := make([][][]float32, a)
	for i := 0; i < a; i++ {
		out[i] = make([][]float32, b)
		for j := 0; j < b; j++ {
			start := (i*b + j) * c
			out[i][j] = data[start : start+c]
		}
	}
	return out
}

// LoadTRM loads a MambaTRM from a TRM1 binary weights file.
func LoadTRM(weightsPath string) (*MambaTRM, error) {
	tensors, err := loadTensors(weightsPath)
	if err != nil {
		return nil, err
	}

	// Build name→tensor lookup
	lookup := make(map[string]*rawTensor, len(tensors))
	for i := range tensors {
		lookup[tensors[i].Name] = &tensors[i]
	}

	get := func(name string) (*rawTensor, error) {
		t, ok := lookup[name]
		if !ok {
			return nil, fmt.Errorf("missing tensor: %s", name)
		}
		return t, nil
	}

	cfg := DefaultTRMConfig()
	m := &MambaTRM{Config: cfg}

	// Type embedding: [n_event_types][d_model]
	t, err := get("type_embed.weight")
	if err != nil {
		return nil, err
	}
	m.TypeEmbed = reshape2D(t.Data, cfg.NEventType, cfg.DModel)

	// Input projection: [d_model][2*d_model]
	tw, err := get("input_proj.weight")
	if err != nil {
		return nil, err
	}
	tb, err := get("input_proj.bias")
	if err != nil {
		return nil, err
	}
	m.InputProj = Linear{
		Weight: reshape2D(tw.Data, cfg.DModel, 2*cfg.DModel),
		Bias:   tb.Data,
		InDim:  2 * cfg.DModel,
		OutDim: cfg.DModel,
	}

	// Mamba layers
	dInner := cfg.DModel * cfg.Expand
	m.Layers = make([]MambaBlock, cfg.NLayers)
	for i := 0; i < cfg.NLayers; i++ {
		prefix := fmt.Sprintf("layers.%d.", i)
		mb := MambaBlock{
			DInner: dInner,
			DState: cfg.DState,
		}

		// Norm
		nw, err := get(prefix + "norm.weight")
		if err != nil {
			return nil, err
		}
		nb, err := get(prefix + "norm.bias")
		if err != nil {
			return nil, err
		}
		mb.Norm = LayerNorm{Weight: nw.Data, Bias: nb.Data, Dim: cfg.DModel, Eps: 1e-5}

		// InProj: [2*d_inner][d_model]
		ip, err := get(prefix + "in_proj.weight")
		if err != nil {
			return nil, err
		}
		mb.InProj = reshape2D(ip.Data, 2*dInner, cfg.DModel)

		// Conv1D: [d_inner][1][kernel]
		cw, err := get(prefix + "conv1d.weight")
		if err != nil {
			return nil, err
		}
		cb, err := get(prefix + "conv1d.bias")
		if err != nil {
			return nil, err
		}
		mb.Conv = Conv1D{
			Weight:   reshape3D(cw.Data, dInner, 1, cfg.DConv),
			Bias:     cb.Data,
			Channels: dInner,
			Kernel:   cfg.DConv,
		}

		// XProj: [d_state*2+1][d_inner]
		xp, err := get(prefix + "x_proj.weight")
		if err != nil {
			return nil, err
		}
		mb.XProj = reshape2D(xp.Data, cfg.DState*2+1, dInner)

		// LogA: [d_inner][d_state]
		la, err := get(prefix + "log_A")
		if err != nil {
			return nil, err
		}
		mb.LogA = reshape2D(la.Data, dInner, cfg.DState)

		// D: [d_inner]
		dd, err := get(prefix + "D")
		if err != nil {
			return nil, err
		}
		mb.D = dd.Data

		// OutProj: [d_model][d_inner]
		op, err := get(prefix + "out_proj.weight")
		if err != nil {
			return nil, err
		}
		mb.OutProj = reshape2D(op.Data, cfg.DModel, dInner)

		m.Layers[i] = mb
	}

	// Final norm
	fnw, err := get("final_norm.weight")
	if err != nil {
		return nil, err
	}
	fnb, err := get("final_norm.bias")
	if err != nil {
		return nil, err
	}
	m.FinalNorm = LayerNorm{Weight: fnw.Data, Bias: fnb.Data, Dim: cfg.DModel, Eps: 1e-5}

	// Attention probes
	m.Probes = make([]AttentionProbe, cfg.NProbes)
	m.ProbeNorms = make([]LayerNorm, cfg.NProbes)
	for i := 0; i < cfg.NProbes; i++ {
		prefix := fmt.Sprintf("attn_probes.%d.", i)

		qw, err := get(prefix + "q_proj.weight")
		if err != nil {
			return nil, err
		}
		kw, err := get(prefix + "k_proj.weight")
		if err != nil {
			return nil, err
		}
		vw, err := get(prefix + "v_proj.weight")
		if err != nil {
			return nil, err
		}
		ow, err := get(prefix + "out_proj.weight")
		if err != nil {
			return nil, err
		}
		m.Probes[i] = AttentionProbe{
			QProj:   reshape2D(qw.Data, cfg.DHead, cfg.DModel),
			KProj:   reshape2D(kw.Data, cfg.DHead, cfg.DModel),
			VProj:   reshape2D(vw.Data, cfg.DHead, cfg.DModel),
			OutProj: reshape2D(ow.Data, cfg.DModel, cfg.DHead),
			DHead:   cfg.DHead,
		}

		// Probe norms
		pnw, err := get(fmt.Sprintf("probe_norms.%d.weight", i))
		if err != nil {
			return nil, err
		}
		pnb, err := get(fmt.Sprintf("probe_norms.%d.bias", i))
		if err != nil {
			return nil, err
		}
		m.ProbeNorms[i] = LayerNorm{Weight: pnw.Data, Bias: pnb.Data, Dim: cfg.DModel, Eps: 1e-5}
	}

	// Score head: score_head.0 (Linear 2*d→d), score_head.3 (Linear d→1)
	sh0w, err := get("score_head.0.weight")
	if err != nil {
		return nil, err
	}
	sh0b, err := get("score_head.0.bias")
	if err != nil {
		return nil, err
	}
	sh3w, err := get("score_head.3.weight")
	if err != nil {
		return nil, err
	}
	sh3b, err := get("score_head.3.bias")
	if err != nil {
		return nil, err
	}
	m.Head = ScoreHead{
		W1:   reshape2D(sh0w.Data, cfg.DModel, 2*cfg.DModel),
		B1:   sh0b.Data,
		W2:   reshape2D(sh3w.Data, 1, cfg.DModel),
		B2:   sh3b.Data,
		DIn:  2 * cfg.DModel,
		DMid: cfg.DModel,
	}

	return m, nil
}
