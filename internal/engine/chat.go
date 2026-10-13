// chat.go — interactive chat REPL
//
// Two modes:
//
//	--direct  Calls OllamaProvider directly (no daemon needed). Useful for
//	          offline testing or when the daemon isn't running.
//	(default) POSTs to the running daemon at localhost:PORT/v1/chat/completions
//	          and streams the SSE response to stdout.
//
// Usage:
//
//	cogos chat              # connect to daemon on default port 6931
//	cogos chat --port 6931  # explicit port
//	cogos --port 6931 chat  # flags must precede subcommand name
//	cogos chat --direct     # bypass daemon, talk directly to Ollama
package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// runChat is the entry point for the "chat" subcommand.
func runChat(args []string, defaultWorkspace string, defaultPort int) {
	fs := flag.NewFlagSet("chat", flag.ExitOnError)
	direct := fs.Bool("direct", false, "Call Ollama directly without the daemon")
	port := fs.Int("port", defaultPort, "Daemon port (server mode)")
	workspace := fs.String("workspace", defaultWorkspace, "Workspace root (direct mode)")
	model := fs.String("model", "", "Model name override (e.g. gemma4:e4b, gemma4:e2b)")
	_ = fs.Parse(args)

	if *port == 0 {
		*port = 6931
	}

	if *direct {
		runDirectChat(*workspace, *model)
	} else {
		runServerChat(*workspace, *port, *model)
	}
}

// ── Direct mode ───────────────────────────────────────────────────────────────

// runDirectChat creates an OllamaProvider and runs the REPL without a daemon.
func runDirectChat(workspace, model string) {
	cfg, err := LoadConfig(workspace, 0)
	if err != nil {
		// If no workspace found, use a minimal config.
		cfg = &Config{WorkspaceRoot: ".", CogDir: ".cog"}
	}

	router, err := BuildRouter(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: build router: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, "cogos chat (direct mode — Ctrl+C or Ctrl+D to exit)")

	var messages []ProviderMessage
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			fmt.Println()
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}
		if input == "/exit" || input == "/quit" {
			break
		}

		messages = append(messages, ProviderMessage{Role: "user", Content: input})

		req := &CompletionRequest{
			Messages:      messages,
			ModelOverride: model,
			Metadata: RequestMetadata{
				RequestID:    fmt.Sprintf("chat-%d", time.Now().UnixNano()),
				ProcessState: "active",
				Source:       "chat-direct",
			},
		}

		ctx := context.Background()
		provider, _, err := router.Route(ctx, req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: route: %v\n", err)
			continue
		}

		chunks, err := provider.Stream(ctx, req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: stream: %v\n", err)
			continue
		}

		var sb strings.Builder
		for sc := range chunks {
			if sc.Error != nil {
				fmt.Fprintf(os.Stderr, "\nerror: %v\n", sc.Error)
				break
			}
			fmt.Print(sc.Delta)
			sb.WriteString(sc.Delta)
			if sc.Done {
				break
			}
		}
		fmt.Println()

		if sb.Len() > 0 {
			messages = append(messages, ProviderMessage{Role: "assistant", Content: sb.String()})
		}
	}
}

// ── Server mode ───────────────────────────────────────────────────────────────

// runServerChat connects to the running daemon and streams responses.
func runServerChat(workspace string, port int, model string) {
	baseURL := resolveClientEndpoint(workspace, port)

	// Quick health check.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: daemon not reachable at %s — start it with: cogos serve or cogos start\n", baseURL)
		os.Exit(1)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "warning: daemon health check returned %d\n", resp.StatusCode)
	}

	fmt.Fprintf(os.Stderr, "cogos chat (daemon at %s — Ctrl+C or Ctrl+D to exit)\n", baseURL)

	var messages []oaiMessage
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			fmt.Println()
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}
		if input == "/exit" || input == "/quit" {
			break
		}

		messages = append(messages, oaiMessage{Role: "user", Content: mustMarshalString(input)})

		reply, err := sendChatRequest(baseURL, messages, model)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			// Pop the failed message so the user can retry.
			messages = messages[:len(messages)-1]
			continue
		}
		fmt.Println()

		if reply != "" {
			messages = append(messages, oaiMessage{Role: "assistant", Content: mustMarshalString(reply)})
		}
	}
}

// sendChatRequest sends a streaming chat request and prints chunks to stdout.
// Returns the complete assistant reply.
func sendChatRequest(baseURL string, messages []oaiMessage, model string) (string, error) {
	if model == "" {
		model = "local"
	}
	payload := map[string]any{
		"model":    model,
		"messages": messages,
		"stream":   true,
	}
	body, _ := json.Marshal(payload)

	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("server returned %d: %s", resp.StatusCode, string(data))
	}

	var sb strings.Builder
	scanner := newStreamScanner(resp.Body)

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}
		if len(chunk.Choices) > 0 {
			delta := chunk.Choices[0].Delta.Content
			fmt.Print(delta)
			sb.WriteString(delta)
		}
	}
	if err := scanner.Err(); err != nil {
		return sb.String(), fmt.Errorf("read response: %w", err)
	}
	return sb.String(), nil
}
