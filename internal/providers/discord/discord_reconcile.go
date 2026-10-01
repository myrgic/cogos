package discord

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// ─── Config types (match .cog/config/discord/server.yaml) ───────────────────

type DiscordServerConfig struct {
	Version    string           `yaml:"version"`
	Guild      GuildConfig      `yaml:"guild"`
	Reconciler ReconcilerConfig `yaml:"reconciler"`
}

type GuildConfig struct {
	ID          string           `yaml:"id"`
	Name        string           `yaml:"name"`
	Description string           `yaml:"description"`
	ManagedBy   string           `yaml:"managed_by"`
	Roles       []RoleConfig     `yaml:"roles"`
	Categories  []CategoryConfig `yaml:"categories"`
}

type RoleConfig struct {
	Name        string   `yaml:"name"`
	Color       string   `yaml:"color"`
	Permissions []string `yaml:"permissions"`
	Hoist       bool     `yaml:"hoist"`
	Mentionable bool     `yaml:"mentionable"`
	Position    int      `yaml:"position"`
	ManagedBy   string   `yaml:"managed_by"`
}

type CategoryConfig struct {
	Name                 string              `yaml:"name"`
	Position             int                 `yaml:"position"`
	ManagedBy            string              `yaml:"managed_by"`
	PermissionOverwrites []PermOverwriteConf `yaml:"permission_overwrites"`
	Channels             []ChannelConfig     `yaml:"channels"`
}

type ChannelConfig struct {
	Name                 string              `yaml:"name"`
	Type                 string              `yaml:"type"`
	Topic                string              `yaml:"topic"`
	Position             int                 `yaml:"position"`
	Slowmode             int                 `yaml:"slowmode"`
	NSFW                 bool                `yaml:"nsfw"`
	ManagedBy            string              `yaml:"managed_by"`
	PermissionOverwrites []PermOverwriteConf `yaml:"permission_overwrites"`
}

type PermOverwriteConf struct {
	TargetType string   `yaml:"target_type"` // "role" or "member"
	Target     string   `yaml:"target"`      // role name or member ID
	Allow      []string `yaml:"allow"`
	Deny       []string `yaml:"deny"`
}

type ReconcilerConfig struct {
	DryRun              bool   `yaml:"dry_run"`
	PruneUnmanaged      bool   `yaml:"prune_unmanaged"`
	RespectUserManaged  bool   `yaml:"respect_user_managed"`
	MaxAPICalls         int    `yaml:"max_api_calls"`
	LogLevel            string `yaml:"log_level"`
}

// ─── Discord API response types ─────────────────────────────────────────────

type DiscordChannel struct {
	ID                   string                 `json:"id"`
	Type                 int                    `json:"type"`
	GuildID              string                 `json:"guild_id"`
	Position             int                    `json:"position"`
	PermissionOverwrites []DiscordPermOverwrite `json:"permission_overwrites"`
	Name                 string                 `json:"name"`
	Topic                string                 `json:"topic"`
	NSFW                 bool                   `json:"nsfw"`
	RateLimitPerUser     int                    `json:"rate_limit_per_user"`
	ParentID             *string                `json:"parent_id"`
}

type DiscordPermOverwrite struct {
	ID    string `json:"id"`
	Type  int    `json:"type"` // 0=role, 1=member
	Allow string `json:"allow"`
	Deny  string `json:"deny"`
}

type DiscordRole struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Color       int    `json:"color"`
	Hoist       bool   `json:"hoist"`
	Position    int    `json:"position"`
	Permissions string `json:"permissions"`
	Managed     bool   `json:"managed"`
	Mentionable bool   `json:"mentionable"`
}

// ─── Plan types ─────────────────────────────────────────────────────────────

type PlanAction struct {
	Action       string            `json:"action"`        // create, update, delete, skip
	ResourceType string            `json:"resource_type"` // category, channel, role, permission
	Name         string            `json:"name"`
	Details      map[string]any    `json:"details"`
}

type Plan struct {
	GuildID     string       `json:"guild_id"`
	GuildName   string       `json:"guild_name"`
	GeneratedAt string       `json:"generated_at"`
	ConfigPath  string       `json:"config_path"`
	Actions     []PlanAction `json:"actions"`
	Summary     PlanSummary  `json:"summary"`
	Warnings    []string     `json:"warnings"`
}

type PlanSummary struct {
	Creates int `json:"creates"`
	Updates int `json:"updates"`
	Deletes int `json:"deletes"`
	Skipped int `json:"skipped"`
}

// ─── Discord channel type mapping ───────────────────────────────────────────

var channelTypeFromString = map[string]int{
	"text":         0,
	"voice":        2,
	"category":     4,
	"announcement": 5,
	"stage":        13,
	"forum":        15,
}

var channelTypeToString = map[int]string{
	0:  "text",
	2:  "voice",
	4:  "category",
	5:  "announcement",
	13: "stage",
	15: "forum",
}

// ─── Discord permission bitfield mapping ────────────────────────────────────

// Ordered list so YAML output is deterministic
var discordPermBits = []struct {
	Name string
	Bit  uint64
}{
	{"CREATE_INSTANT_INVITE", 1 << 0},
	{"KICK_MEMBERS", 1 << 1},
	{"BAN_MEMBERS", 1 << 2},
	{"ADMINISTRATOR", 1 << 3},
	{"MANAGE_CHANNELS", 1 << 4},
	{"MANAGE_GUILD", 1 << 5},
	{"ADD_REACTIONS", 1 << 6},
	{"VIEW_AUDIT_LOG", 1 << 7},
	{"PRIORITY_SPEAKER", 1 << 8},
	{"STREAM", 1 << 9},
	{"VIEW_CHANNEL", 1 << 10},
	{"SEND_MESSAGES", 1 << 11},
	{"SEND_TTS_MESSAGES", 1 << 12},
	{"MANAGE_MESSAGES", 1 << 13},
	{"EMBED_LINKS", 1 << 14},
	{"ATTACH_FILES", 1 << 15},
	{"READ_MESSAGE_HISTORY", 1 << 16},
	{"MENTION_EVERYONE", 1 << 17},
	{"USE_EXTERNAL_EMOJIS", 1 << 18},
	{"VIEW_GUILD_INSIGHTS", 1 << 19},
	{"CONNECT", 1 << 20},
	{"SPEAK", 1 << 21},
	{"MUTE_MEMBERS", 1 << 22},
	{"DEAFEN_MEMBERS", 1 << 23},
	{"MOVE_MEMBERS", 1 << 24},
	{"USE_VAD", 1 << 25},
	{"CHANGE_NICKNAME", 1 << 26},
	{"MANAGE_NICKNAMES", 1 << 27},
	{"MANAGE_ROLES", 1 << 28},
	{"MANAGE_WEBHOOKS", 1 << 29},
	{"MANAGE_GUILD_EXPRESSIONS", 1 << 30},
	{"USE_APPLICATION_COMMANDS", 1 << 31},
	{"REQUEST_TO_SPEAK", 1 << 32},
	{"MANAGE_EVENTS", 1 << 33},
	{"MANAGE_THREADS", 1 << 34},
	{"CREATE_PUBLIC_THREADS", 1 << 35},
	{"CREATE_PRIVATE_THREADS", 1 << 36},
	{"USE_EXTERNAL_STICKERS", 1 << 37},
	{"SEND_MESSAGES_IN_THREADS", 1 << 38},
	{"USE_EMBEDDED_ACTIVITIES", 1 << 39},
	{"MODERATE_MEMBERS", 1 << 40},
	{"VIEW_CREATOR_MONETIZATION_ANALYTICS", 1 << 41},
	{"USE_SOUNDBOARD", 1 << 42},
	{"CREATE_GUILD_EXPRESSIONS", 1 << 43},
	{"CREATE_EVENTS", 1 << 44},
	{"USE_EXTERNAL_SOUNDS", 1 << 45},
	{"SEND_VOICE_MESSAGES", 1 << 46},
	{"USE_CLYDE_AI", 1 << 47},
	{"SET_VOICE_CHANNEL_STATUS", 1 << 48},
	{"SEND_POLLS", 1 << 49},
	{"USE_EXTERNAL_APPS", 1 << 50},
	{"PIN_MESSAGES", 1 << 51},
	{"BYPASS_SLOWMODE", 1 << 52},
}

// unknownPermPrefix names a bit Discord set that discordPermBits does not
// (yet) know. Emitting it keeps snapshot -> plan lossless: dropping unknown
// bits made a fresh snapshot plan to strip live permissions from roles.
const unknownPermPrefix = "PERM_BIT_"

func permBitsToNames(bitfieldStr string) []string {
	var bitfield uint64
	fmt.Sscanf(bitfieldStr, "%d", &bitfield)
	if bitfield == 0 {
		return nil
	}
	var names []string
	var known uint64
	for _, p := range discordPermBits {
		known |= p.Bit
		if bitfield&p.Bit != 0 {
			names = append(names, p.Name)
		}
	}
	for i := 0; i < 64; i++ {
		if b := uint64(1) << i; bitfield&b != 0 && known&b == 0 {
			names = append(names, fmt.Sprintf("%s%d", unknownPermPrefix, i))
		}
	}
	return names
}

func intColorToHex(color int) string {
	if color == 0 {
		return "000000"
	}
	return fmt.Sprintf("%06x", color)
}

// ─── Discord API client ─────────────────────────────────────────────────────

const discordAPIBase = "https://discord.com/api/v10"

type discordClient struct {
	token      string
	httpClient *http.Client
	apiCalls   int
	maxCalls   int
}

func newDiscordClient(token string, maxCalls int) *discordClient {
	return &discordClient{
		token:      token,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		maxCalls:   maxCalls,
	}
}

func (c *discordClient) get(path string) ([]byte, error) {
	if c.maxCalls > 0 && c.apiCalls >= c.maxCalls {
		return nil, fmt.Errorf("API call limit reached (%d)", c.maxCalls)
	}
	c.apiCalls++

	url := discordAPIBase + path
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bot "+c.token)
	req.Header.Set("User-Agent", "CogOS-Reconciler/1.0")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode == 429 {
		return nil, fmt.Errorf("rate limited on GET %s (retry-after: %s)", path, resp.Header.Get("Retry-After"))
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("GET %s returned %d: %s", path, resp.StatusCode, string(body))
	}

	return body, nil
}

func (c *discordClient) doMutation(method, path string, payload any) ([]byte, error) {
	if c.maxCalls > 0 && c.apiCalls >= c.maxCalls {
		return nil, fmt.Errorf("API call limit reached (%d)", c.maxCalls)
	}
	c.apiCalls++

	url := discordAPIBase + path

	var bodyReader io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		bodyReader = strings.NewReader(string(data))
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bot "+c.token)
	req.Header.Set("User-Agent", "CogOS-Reconciler/1.0")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode == 429 {
		return nil, fmt.Errorf("rate limited on %s %s (retry-after: %s)", method, path, resp.Header.Get("Retry-After"))
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("%s %s returned %d: %s", method, path, resp.StatusCode, string(body))
	}

	return body, nil
}

func (c *discordClient) fetchChannels(guildID string) ([]DiscordChannel, error) {
	data, err := c.get(fmt.Sprintf("/guilds/%s/channels", guildID))
	if err != nil {
		return nil, err
	}
	var channels []DiscordChannel
	if err := json.Unmarshal(data, &channels); err != nil {
		return nil, fmt.Errorf("parsing channels: %w", err)
	}
	return channels, nil
}

func (c *discordClient) fetchRoles(guildID string) ([]DiscordRole, error) {
	data, err := c.get(fmt.Sprintf("/guilds/%s/roles", guildID))
	if err != nil {
		return nil, err
	}
	var roles []DiscordRole
	if err := json.Unmarshal(data, &roles); err != nil {
		return nil, fmt.Errorf("parsing roles: %w", err)
	}
	return roles, nil
}

// ─── Config loading ─────────────────────────────────────────────────────────

func loadDiscordServerConfig(root string) (*DiscordServerConfig, string, error) {
	// Try HCL first
	hclPath := filepath.Join(root, ".cog", "config", "discord", "server.hcl")
	if _, err := os.Stat(hclPath); err == nil {
		cfg, err := parseHCLConfig(hclPath)
		return cfg, hclPath, err
	}

	// Fall back to YAML
	configPath := filepath.Join(root, ".cog", "config", "discord", "server.yaml")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, configPath, fmt.Errorf("reading config: %w", err)
	}

	var cfg DiscordServerConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, configPath, fmt.Errorf("parsing config: %w", err)
	}

	// Defaults
	if cfg.Reconciler.MaxAPICalls == 0 {
		cfg.Reconciler.MaxAPICalls = 60
	}
	if cfg.Reconciler.LogLevel == "" {
		cfg.Reconciler.LogLevel = "info"
	}

	return &cfg, configPath, nil
}

// resolveToken finds the Discord bot token from (in order):
// 1. --token CLI flag
// 2. DISCORD_BOT_TOKEN env var
// 3. .cog/config/discord/auth.yaml (gitignored)
func resolveToken(root string, flagToken string) (string, error) {
	if flagToken != "" {
		return flagToken, nil
	}
	if t := os.Getenv("DISCORD_BOT_TOKEN"); t != "" {
		return t, nil
	}

	authPath := filepath.Join(root, ".cog", "config", "discord", "auth.yaml")
	data, err := os.ReadFile(authPath)
	if err == nil {
		var auth struct {
			Token        string   `yaml:"token"`
			TokenCommand []string `yaml:"token_command"`
		}
		if err := yaml.Unmarshal(data, &auth); err != nil {
			return "", fmt.Errorf("discord: parse %s: %w", authPath, err)
		}
		// token_command wins over a literal token: it is a pointer to the
		// secret store (credential_process style), so the token itself never
		// has to sit in the workspace.
		if len(auth.TokenCommand) > 0 {
			return runTokenCommand(root, auth.TokenCommand)
		}
		if auth.Token != "" {
			return auth.Token, nil
		}
	}

	return "", fmt.Errorf("no Discord token found. Set DISCORD_BOT_TOKEN, or token / token_command in .cog/config/discord/auth.yaml")
}

// tokenCommandTimeout bounds a token_command so a hung secret store cannot
// wedge the kernel. Var so tests can shorten it.
var tokenCommandTimeout = 30 * time.Second

// runTokenCommand executes argv (relative argv[0] resolved against the
// workspace root) and returns its trimmed stdout as the token. Stderr is
// surfaced on failure; stdout is never included in errors.
func runTokenCommand(root string, argv []string) (string, error) {
	bin := argv[0]
	if !filepath.IsAbs(bin) && strings.Contains(bin, "/") {
		bin = filepath.Join(root, bin)
	}
	ctx, cancel := context.WithTimeout(context.Background(), tokenCommandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, argv[1:]...)
	cmd.Dir = root
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("discord: token_command %q failed: %w (stderr: %s)", argv[0], err, strings.TrimSpace(stderr.String()))
	}
	tok := strings.TrimSpace(string(out))
	if tok == "" {
		return "", fmt.Errorf("discord: token_command %q produced no token", argv[0])
	}
	if strings.ContainsAny(tok, " \n	") {
		return "", fmt.Errorf("discord: token_command %q produced malformed output (whitespace inside token)", argv[0])
	}
	return tok, nil
}

// ─── Diff / plan computation ────────────────────────────────────────────────

func computePlan(cfg *DiscordServerConfig, channels []DiscordChannel, roles []DiscordRole) *Plan {
	plan := &Plan{
		GuildID:     cfg.Guild.ID,
		GuildName:   cfg.Guild.Name,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		ConfigPath:  ".cog/config/discord/server.yaml",
	}

	// Build lookup maps from live state
	// Categories keyed by lowercase name
	liveCategoriesByName := map[string]DiscordChannel{}
	// Channels keyed by "categoryID/lowercaseName" for category-aware matching
	liveChannelsByKey := map[string]DiscordChannel{}
	// Also keep a flat list of all non-category channels for skip reporting
	allLiveChannels := []DiscordChannel{}
	liveRolesByName := map[string]DiscordRole{}
	categoryIDToName := map[string]string{}

	for _, ch := range channels {
		if ch.Type == 4 { // category
			liveCategoriesByName[strings.ToLower(ch.Name)] = ch
			categoryIDToName[ch.ID] = ch.Name
		} else {
			allLiveChannels = append(allLiveChannels, ch)
		}
	}
	// Build channel keys after categories are indexed
	for _, ch := range allLiveChannels {
		parentKey := ""
		if ch.ParentID != nil {
			parentKey = *ch.ParentID
		}
		key := parentKey + "/" + strings.ToLower(ch.Name)
		liveChannelsByKey[key] = ch
	}
	for _, r := range roles {
		liveRolesByName[strings.ToLower(r.Name)] = r
	}

	// Track which live resources are matched (for delete/skip detection)
	matchedCategories := map[string]bool{}
	matchedChannelKeys := map[string]bool{}
	matchedRoles := map[string]bool{}

	// ── Diff roles ──
	for _, desired := range cfg.Guild.Roles {
		nameLower := strings.ToLower(desired.Name)
		if live, ok := liveRolesByName[nameLower]; ok {
			matchedRoles[nameLower] = true
			diffs := diffRole(desired, live)
			if len(diffs) > 0 {
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "update",
					ResourceType: "role",
					Name:         desired.Name,
					Details:      map[string]any{"changes": diffs, "id": live.ID},
				})
			}
		} else {
			plan.Actions = append(plan.Actions, PlanAction{
				Action:       "create",
				ResourceType: "role",
				Name:         desired.Name,
				Details: map[string]any{
					"color":       desired.Color,
					"hoist":       desired.Hoist,
					"mentionable": desired.Mentionable,
					"permissions": desired.Permissions,
				},
			})
		}
	}

	// ── Diff categories and channels ──
	for _, cat := range cfg.Guild.Categories {
		catNameLower := strings.ToLower(cat.Name)
		if liveCat, ok := liveCategoriesByName[catNameLower]; ok {
			matchedCategories[catNameLower] = true
			catDiffs := diffCategory(cat, liveCat)
			if len(catDiffs) > 0 {
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "update",
					ResourceType: "category",
					Name:         cat.Name,
					Details:      map[string]any{"changes": catDiffs, "id": liveCat.ID},
				})
			}

			// Diff channels within this category (match by categoryID + name)
			for _, ch := range cat.Channels {
				chKey := liveCat.ID + "/" + strings.ToLower(ch.Name)
				if liveCh, ok := liveChannelsByKey[chKey]; ok {
					matchedChannelKeys[chKey] = true
					chDiffs := diffChannel(ch, liveCh, liveCat.ID)
					if len(chDiffs) > 0 {
						plan.Actions = append(plan.Actions, PlanAction{
							Action:       "update",
							ResourceType: "channel",
							Name:         ch.Name,
							Details:      map[string]any{"changes": chDiffs, "id": liveCh.ID, "category": cat.Name},
						})
					}
				} else {
					plan.Actions = append(plan.Actions, PlanAction{
						Action:       "create",
						ResourceType: "channel",
						Name:         ch.Name,
						Details: map[string]any{
							"type":     ch.Type,
							"category": cat.Name,
							"topic":    ch.Topic,
							"position": ch.Position,
							"slowmode": ch.Slowmode,
						},
					})
				}
			}
		} else {
			// Category doesn't exist — create it and all its channels
			plan.Actions = append(plan.Actions, PlanAction{
				Action:       "create",
				ResourceType: "category",
				Name:         cat.Name,
				Details:      map[string]any{"position": cat.Position},
			})
			for _, ch := range cat.Channels {
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "create",
					ResourceType: "channel",
					Name:         ch.Name,
					Details: map[string]any{
						"type":     ch.Type,
						"category": cat.Name,
						"topic":    ch.Topic,
						"position": ch.Position,
						"slowmode": ch.Slowmode,
					},
				})
			}
		}
	}

	// ── Detect deletions / skips for unmatched live resources ──
	if cfg.Reconciler.PruneUnmanaged {
		for key, ch := range liveChannelsByKey {
			if !matchedChannelKeys[key] {
				catName := ""
				if ch.ParentID != nil {
					catName = categoryIDToName[*ch.ParentID]
				}
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "delete",
					ResourceType: "channel",
					Name:         ch.Name,
					Details:      map[string]any{"id": ch.ID, "type": channelTypeToString[ch.Type], "category": catName},
				})
			}
		}
		for name, cat := range liveCategoriesByName {
			if !matchedCategories[name] {
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "delete",
					ResourceType: "category",
					Name:         cat.Name,
					Details:      map[string]any{"id": cat.ID},
				})
			}
		}
		for name, role := range liveRolesByName {
			if !matchedRoles[name] && !role.Managed && name != "@everyone" {
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "delete",
					ResourceType: "role",
					Name:         role.Name,
					Details:      map[string]any{"id": role.ID},
				})
			}
		}
	} else {
		for key, ch := range liveChannelsByKey {
			if !matchedChannelKeys[key] {
				catName := ""
				if ch.ParentID != nil {
					catName = categoryIDToName[*ch.ParentID]
				}
				reason := "not in config"
				if catName != "" {
					reason = fmt.Sprintf("not in config (under %q)", catName)
				}
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "skip",
					ResourceType: "channel",
					Name:         ch.Name,
					Details:      map[string]any{"reason": reason},
				})
			}
		}
		for name, cat := range liveCategoriesByName {
			if !matchedCategories[name] {
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "skip",
					ResourceType: "category",
					Name:         cat.Name,
					Details:      map[string]any{"reason": "not in config"},
				})
			}
		}
		for name, role := range liveRolesByName {
			if !matchedRoles[name] {
				reason := "not in config"
				if role.Managed {
					reason = "discord-managed (integration/bot role)"
				}
				if name == "@everyone" {
					reason = "built-in @everyone role"
				}
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "skip",
					ResourceType: "role",
					Name:         role.Name,
					Details:      map[string]any{"reason": reason},
				})
			}
		}
	}

	// Compute summary
	for _, a := range plan.Actions {
		switch a.Action {
		case "create":
			plan.Summary.Creates++
		case "update":
			plan.Summary.Updates++
		case "delete":
			plan.Summary.Deletes++
		case "skip":
			plan.Summary.Skipped++
		}
	}

	return plan
}

func diffRole(desired RoleConfig, live DiscordRole) []string {
	var diffs []string

	// Color: config is hex string without #, live is int
	desiredColor := hexColorToInt(desired.Color)
	if desiredColor != live.Color {
		diffs = append(diffs, fmt.Sprintf("color: #%06x -> #%s", live.Color, desired.Color))
	}
	if desired.Hoist != live.Hoist {
		diffs = append(diffs, fmt.Sprintf("hoist: %v -> %v", live.Hoist, desired.Hoist))
	}
	if desired.Mentionable != live.Mentionable {
		diffs = append(diffs, fmt.Sprintf("mentionable: %v -> %v", live.Mentionable, desired.Mentionable))
	}

	return diffs
}

func diffCategory(desired CategoryConfig, live DiscordChannel) []string {
	var diffs []string
	if desired.Position != live.Position {
		diffs = append(diffs, fmt.Sprintf("position: %d -> %d", live.Position, desired.Position))
	}
	return diffs
}

func diffChannel(desired ChannelConfig, live DiscordChannel, expectedParentID string) []string {
	var diffs []string

	desiredType := channelTypeFromString[desired.Type]
	if desiredType != live.Type {
		diffs = append(diffs, fmt.Sprintf("type: %s -> %s", channelTypeToString[live.Type], desired.Type))
	}
	if desired.Topic != live.Topic {
		diffs = append(diffs, fmt.Sprintf("topic: %q -> %q", live.Topic, desired.Topic))
	}
	if desired.Slowmode != live.RateLimitPerUser {
		diffs = append(diffs, fmt.Sprintf("slowmode: %d -> %d", live.RateLimitPerUser, desired.Slowmode))
	}
	if desired.NSFW != live.NSFW {
		diffs = append(diffs, fmt.Sprintf("nsfw: %v -> %v", live.NSFW, desired.NSFW))
	}
	if live.ParentID != nil && *live.ParentID != expectedParentID {
		diffs = append(diffs, "parent: moved to correct category")
	}

	return diffs
}

func hexColorToInt(hex string) int {
	hex = strings.TrimPrefix(hex, "#")
	var result int
	fmt.Sscanf(hex, "%x", &result)
	return result
}

// ─── Apply logic ────────────────────────────────────────────────────────────

type ApplyResult struct {
	Phase     string `json:"phase"`
	Action    string `json:"action"`
	Name      string `json:"name"`
	Status    string `json:"status"` // succeeded, failed, skipped
	Error     string `json:"error,omitempty"`
	CreatedID string `json:"created_id,omitempty"`
}

func applyPlan(client *discordClient, plan *Plan, guildID string, roles []DiscordRole, channels []DiscordChannel) ([]ApplyResult, error) {
	var results []ApplyResult

	// Build role name→ID map for permission resolution
	roleNameToID := map[string]string{}
	for _, r := range roles {
		roleNameToID[strings.ToLower(r.Name)] = r.ID
	}

	// Build live category name→ID map for parent resolution
	liveCategoryIDs := map[string]string{}
	for _, ch := range channels {
		if ch.Type == 4 { // category
			liveCategoryIDs[strings.ToLower(ch.Name)] = ch.ID
		}
	}

	// Track newly created IDs
	createdCategoryIDs := map[string]string{} // name → ID
	createdChannelIDs := map[string]string{}   // name → ID

	// Phase A: Create/update roles
	for _, a := range plan.Actions {
		if a.ResourceType != "role" || (a.Action != "create" && a.Action != "update") {
			continue
		}

		if a.Action == "create" {
			payload := map[string]any{
				"name":        a.Name,
				"mentionable": false,
				"hoist":       false,
			}
			if color, ok := a.Details["color"]; ok {
				payload["color"] = hexColorToInt(fmt.Sprint(color))
			}
			if hoist, ok := a.Details["hoist"]; ok {
				payload["hoist"] = hoist
			}
			if ment, ok := a.Details["mentionable"]; ok {
				payload["mentionable"] = ment
			}

			body, err := client.doMutation("POST", fmt.Sprintf("/guilds/%s/roles", guildID), payload)
			if err != nil {
				results = append(results, ApplyResult{Phase: "roles", Action: "create", Name: a.Name, Status: "failed", Error: err.Error()})
				continue
			}

			var created DiscordRole
			json.Unmarshal(body, &created)
			roleNameToID[strings.ToLower(a.Name)] = created.ID
			results = append(results, ApplyResult{Phase: "roles", Action: "create", Name: a.Name, Status: "succeeded", CreatedID: created.ID})
		}
		// Note: role updates require PATCH /guilds/{id}/roles/{role_id} — skipped for v1
	}

	// Phase B: Create/update categories
	for _, a := range plan.Actions {
		if a.ResourceType != "category" {
			continue
		}

		if a.Action == "create" {
			payload := map[string]any{
				"name": a.Name,
				"type": 4, // category
			}
			if pos, ok := a.Details["position"]; ok {
				payload["position"] = pos
			}

			body, err := client.doMutation("POST", fmt.Sprintf("/guilds/%s/channels", guildID), payload)
			if err != nil {
				results = append(results, ApplyResult{Phase: "categories", Action: "create", Name: a.Name, Status: "failed", Error: err.Error()})
				continue
			}

			var created DiscordChannel
			json.Unmarshal(body, &created)
			createdCategoryIDs[strings.ToLower(a.Name)] = created.ID
			results = append(results, ApplyResult{Phase: "categories", Action: "create", Name: a.Name, Status: "succeeded", CreatedID: created.ID})
		}

		if a.Action == "update" {
			id, _ := a.Details["id"].(string)
			if id == "" {
				results = append(results, ApplyResult{Phase: "categories", Action: "update", Name: a.Name, Status: "failed", Error: "no ID"})
				continue
			}
			payload := map[string]any{}
			if changes, ok := a.Details["changes"].([]string); ok {
				for _, c := range changes {
					if strings.HasPrefix(c, "position:") {
						// Extract new position from the diff string
						if pos, ok := a.Details["position"]; ok {
							payload["position"] = pos
						}
					}
				}
			}
			if len(payload) > 0 {
				_, err := client.doMutation("PATCH", fmt.Sprintf("/channels/%s", id), payload)
				if err != nil {
					results = append(results, ApplyResult{Phase: "categories", Action: "update", Name: a.Name, Status: "failed", Error: err.Error()})
				} else {
					results = append(results, ApplyResult{Phase: "categories", Action: "update", Name: a.Name, Status: "succeeded"})
				}
			}
		}
	}

	// Phase C: Create/update channels
	for _, a := range plan.Actions {
		if a.ResourceType != "channel" {
			continue
		}

		if a.Action == "create" {
			catName := ""
			if c, ok := a.Details["category"]; ok {
				catName = fmt.Sprint(c)
			}

			chType := channelTypeFromString[fmt.Sprint(a.Details["type"])]

			payload := map[string]any{
				"name": a.Name,
				"type": chType,
			}
			if topic, ok := a.Details["topic"]; ok && topic != "" {
				payload["topic"] = topic
			}
			if sm, ok := a.Details["slowmode"]; ok {
				payload["rate_limit_per_user"] = sm
			}

			// Resolve parent category ID (newly created or pre-existing)
			if catName != "" {
				catLower := strings.ToLower(catName)
				if id, ok := createdCategoryIDs[catLower]; ok {
					payload["parent_id"] = id
				} else if id, ok := liveCategoryIDs[catLower]; ok {
					payload["parent_id"] = id
				}
			}

			body, err := client.doMutation("POST", fmt.Sprintf("/guilds/%s/channels", guildID), payload)
			if err != nil {
				results = append(results, ApplyResult{Phase: "channels", Action: "create", Name: a.Name, Status: "failed", Error: err.Error()})
				continue
			}

			var created DiscordChannel
			json.Unmarshal(body, &created)
			createdChannelIDs[strings.ToLower(a.Name)] = created.ID
			results = append(results, ApplyResult{Phase: "channels", Action: "create", Name: a.Name, Status: "succeeded", CreatedID: created.ID})
		}

		if a.Action == "update" {
			id, _ := a.Details["id"].(string)
			if id == "" {
				results = append(results, ApplyResult{Phase: "channels", Action: "update", Name: a.Name, Status: "failed", Error: "no ID"})
				continue
			}

			payload := map[string]any{}
			if changes, ok := a.Details["changes"]; ok {
				if cs, ok := changes.([]string); ok {
					for _, c := range cs {
						if strings.Contains(c, "topic:") {
							// Extract the new topic
							parts := strings.SplitN(c, "-> ", 2)
							if len(parts) == 2 {
								topic := strings.Trim(parts[1], "\"")
								payload["topic"] = topic
							}
						}
						if strings.Contains(c, "slowmode:") {
							parts := strings.SplitN(c, "-> ", 2)
							if len(parts) == 2 {
								var sm int
								fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &sm)
								payload["rate_limit_per_user"] = sm
							}
						}
					}
				}
			}

			if len(payload) > 0 {
				_, err := client.doMutation("PATCH", fmt.Sprintf("/channels/%s", id), payload)
				if err != nil {
					results = append(results, ApplyResult{Phase: "channels", Action: "update", Name: a.Name, Status: "failed", Error: err.Error()})
				} else {
					results = append(results, ApplyResult{Phase: "channels", Action: "update", Name: a.Name, Status: "succeeded"})
				}
			}
		}
	}

	// Phase D: Renames (roles, categories, channels)
	for _, a := range plan.Actions {
		if a.Action != "rename" {
			continue
		}
		id, _ := a.Details["id"].(string)
		newName, _ := a.Details["new_name"].(string)
		if id == "" || newName == "" {
			results = append(results, ApplyResult{Phase: "renames", Action: "rename", Name: a.Name, Status: "failed", Error: "missing id or new_name"})
			continue
		}

		payload := map[string]any{"name": newName}
		var err error
		if a.ResourceType == "role" {
			_, err = client.doMutation("PATCH", fmt.Sprintf("/guilds/%s/roles/%s", guildID, id), payload)
		} else {
			_, err = client.doMutation("PATCH", fmt.Sprintf("/channels/%s", id), payload)
		}
		if err != nil {
			results = append(results, ApplyResult{Phase: "renames", Action: "rename", Name: a.Name, Status: "failed", Error: err.Error()})
		} else {
			results = append(results, ApplyResult{Phase: "renames", Action: "rename", Name: a.Name, Status: "succeeded"})
		}
	}

	// Phase E: Moves (channels to different parent categories)
	for _, a := range plan.Actions {
		if a.Action != "move" {
			continue
		}
		id, _ := a.Details["id"].(string)
		newParentID, _ := a.Details["new_parent_id"].(string)
		if id == "" {
			results = append(results, ApplyResult{Phase: "moves", Action: "move", Name: a.Name, Status: "failed", Error: "missing id"})
			continue
		}

		payload := map[string]any{}
		if newParentID != "" {
			payload["parent_id"] = newParentID
		}
		_, err := client.doMutation("PATCH", fmt.Sprintf("/channels/%s", id), payload)
		if err != nil {
			results = append(results, ApplyResult{Phase: "moves", Action: "move", Name: a.Name, Status: "failed", Error: err.Error()})
		} else {
			results = append(results, ApplyResult{Phase: "moves", Action: "move", Name: a.Name, Status: "succeeded"})
		}
	}

	// Phase F: Deletes (channels first, then categories, then roles)
	for _, resType := range []string{"channel", "category", "role"} {
		for _, a := range plan.Actions {
			if a.ResourceType != resType || a.Action != "delete" {
				continue
			}
			id, _ := a.Details["id"].(string)
			if id == "" {
				results = append(results, ApplyResult{Phase: "deletes", Action: "delete", Name: a.Name, Status: "failed", Error: "no ID"})
				continue
			}

			var err error
			if resType == "role" {
				_, err = client.doMutation("DELETE", fmt.Sprintf("/guilds/%s/roles/%s", guildID, id), nil)
			} else {
				_, err = client.doMutation("DELETE", fmt.Sprintf("/channels/%s", id), nil)
			}

			if err != nil {
				results = append(results, ApplyResult{Phase: "deletes", Action: "delete", Name: a.Name, Status: "failed", Error: err.Error()})
			} else {
				results = append(results, ApplyResult{Phase: "deletes", Action: "delete", Name: a.Name, Status: "succeeded"})
			}
		}
	}

	return results, nil
}


// convertPermOverwrites turns Discord API permission overwrites into config format
func convertPermOverwrites(overwrites []DiscordPermOverwrite, roleIDToName map[string]string) []PermOverwriteConf {
	if len(overwrites) == 0 {
		return nil
	}

	var result []PermOverwriteConf
	for _, ow := range overwrites {
		allowPerms := permBitsToNames(ow.Allow)
		denyPerms := permBitsToNames(ow.Deny)

		// Skip empty overwrites
		if len(allowPerms) == 0 && len(denyPerms) == 0 {
			continue
		}

		targetType := "role"
		if ow.Type == 1 {
			targetType = "member"
		}

		target := ow.ID
		if targetType == "role" {
			if name, ok := roleIDToName[ow.ID]; ok {
				target = name
			}
		}

		result = append(result, PermOverwriteConf{
			TargetType: targetType,
			Target:     target,
			Allow:      allowPerms,
			Deny:       denyPerms,
		})
	}

	// Discord returns overwrites in no stable order; sort so repeated
	// snapshots of an unchanged guild are byte-identical.
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].TargetType != result[j].TargetType {
			return result[i].TargetType < result[j].TargetType
		}
		return result[i].Target < result[j].Target
	})
	return result
}

// ─── State file types ────────────────────────────────────────────────────────

type DiscordState struct {
	Version     int             `json:"version"`
	Lineage     string          `json:"lineage"`
	Serial      int             `json:"serial"`
	GuildID     string          `json:"guild_id"`
	GeneratedAt string          `json:"generated_at"`
	Resources   []StateResource `json:"resources"`
}

type StateResource struct {
	Address         string         `json:"address"`
	Type            string         `json:"type"`                      // role, category, channel
	Mode            string         `json:"mode"`                      // managed, unmanaged, data
	DiscordID       string         `json:"discord_id"`
	Name            string         `json:"name"`
	ParentAddress   string         `json:"parent_address,omitempty"`
	ParentID        string         `json:"parent_id,omitempty"`
	Attributes      map[string]any `json:"attributes,omitempty"`
	UnmanagedReason string         `json:"unmanaged_reason,omitempty"`
	LastRefreshed   string         `json:"last_refreshed"`
}

func statePath(root string) string {
	return filepath.Join(root, ".cog", "config", "discord", ".state.json")
}

func loadState(root string) (*DiscordState, error) {
	data, err := os.ReadFile(statePath(root))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // no state file yet
		}
		return nil, fmt.Errorf("reading state: %w", err)
	}
	var state DiscordState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("parsing state: %w", err)
	}
	return &state, nil
}

func writeState(root string, state *DiscordState) error {
	state.GeneratedAt = time.Now().UTC().Format(time.RFC3339)
	state.Serial++

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling state: %w", err)
	}

	sp := statePath(root)
	tmp := sp + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("writing tmp state: %w", err)
	}
	if err := os.Rename(tmp, sp); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("renaming state: %w", err)
	}
	return nil
}

func generateLineage() string {
	return reconcile.GenerateLineage()
}

// resourceAddress builds the hierarchical address for a resource.
func roleAddress(name string) string {
	return "role/" + name
}

func categoryAddress(name string) string {
	return "category/" + name
}

func channelAddress(catName, chName string) string {
	return "category/" + catName + "/channel/" + chName
}

// stateResourceIndex builds a lookup map from address → *StateResource.
func stateResourceIndex(state *DiscordState) map[string]*StateResource {
	idx := make(map[string]*StateResource, len(state.Resources))
	for i := range state.Resources {
		idx[state.Resources[i].Address] = &state.Resources[i]
	}
	return idx
}

// stateResourceByID builds a lookup map from discord_id → *StateResource.
func stateResourceByID(state *DiscordState) map[string]*StateResource {
	idx := make(map[string]*StateResource, len(state.Resources))
	for i := range state.Resources {
		idx[state.Resources[i].DiscordID] = &state.Resources[i]
	}
	return idx
}

// ─── Permission helpers ──────────────────────────────────────────────────────

func permNamesToBits(names []string) uint64 {
	var bits uint64
	for _, name := range names {
		if strings.HasPrefix(name, unknownPermPrefix) {
			var i int
			if _, err := fmt.Sscanf(strings.TrimPrefix(name, unknownPermPrefix), "%d", &i); err == nil && i >= 0 && i < 64 {
				bits |= uint64(1) << i
			}
			continue
		}
		for _, p := range discordPermBits {
			if p.Name == name {
				bits |= p.Bit
				break
			}
		}
	}
	return bits
}

type permOverwriteDiff struct {
	TargetID   string
	TargetType string // "role" or "member"
	TargetName string
	Action     string // "add", "update", "remove"
	AllowDiff  string // e.g. "0 -> 1024"
	DenyDiff   string
}

func diffPermOverwrites(desired []PermOverwriteConf, live []DiscordPermOverwrite, roleNameToID map[string]string) []permOverwriteDiff {
	var diffs []permOverwriteDiff

	// Build live index by target ID
	liveByTarget := map[string]DiscordPermOverwrite{}
	for _, ow := range live {
		liveByTarget[ow.ID] = ow
	}

	matchedTargets := map[string]bool{}

	for _, d := range desired {
		targetID := d.Target
		if d.TargetType == "role" {
			if id, ok := roleNameToID[strings.ToLower(d.Target)]; ok {
				targetID = id
			}
		}

		desiredAllow := permNamesToBits(d.Allow)
		desiredDeny := permNamesToBits(d.Deny)

		if liveOW, ok := liveByTarget[targetID]; ok {
			matchedTargets[targetID] = true
			var liveAllow, liveDeny uint64
			fmt.Sscanf(liveOW.Allow, "%d", &liveAllow)
			fmt.Sscanf(liveOW.Deny, "%d", &liveDeny)

			if desiredAllow != liveAllow || desiredDeny != liveDeny {
				diffs = append(diffs, permOverwriteDiff{
					TargetID:   targetID,
					TargetType: d.TargetType,
					TargetName: d.Target,
					Action:     "update",
					AllowDiff:  fmt.Sprintf("%d -> %d", liveAllow, desiredAllow),
					DenyDiff:   fmt.Sprintf("%d -> %d", liveDeny, desiredDeny),
				})
			}
		} else {
			diffs = append(diffs, permOverwriteDiff{
				TargetID:   targetID,
				TargetType: d.TargetType,
				TargetName: d.Target,
				Action:     "add",
				AllowDiff:  fmt.Sprintf("0 -> %d", desiredAllow),
				DenyDiff:   fmt.Sprintf("0 -> %d", desiredDeny),
			})
		}
	}

	// Detect removed overwrites. A live overwrite with allow=0 and deny=0 is
	// a no-op (snapshot omits it), so "removing" it is not a change.
	for _, liveOW := range live {
		if !matchedTargets[liveOW.ID] && !(isZeroPerm(liveOW.Allow) && isZeroPerm(liveOW.Deny)) {
			diffs = append(diffs, permOverwriteDiff{
				TargetID:   liveOW.ID,
				TargetType: map[int]string{0: "role", 1: "member"}[liveOW.Type],
				Action:     "remove",
			})
		}
	}

	return diffs
}

func isZeroPerm(s string) bool {
	var v uint64
	fmt.Sscanf(s, "%d", &v)
	return v == 0
}

func diffRolePermissions(desired []string, live string) string {
	desiredBits := permNamesToBits(desired)
	var liveBits uint64
	fmt.Sscanf(live, "%d", &liveBits)
	if desiredBits != liveBits {
		return fmt.Sprintf("permissions: %d -> %d", liveBits, desiredBits)
	}
	return ""
}

// ─── State-aware plan computation ────────────────────────────────────────────

func computePlanWithState(cfg *DiscordServerConfig, channels []DiscordChannel, roles []DiscordRole, state *DiscordState) *Plan {
	if state == nil {
		return computePlan(cfg, channels, roles)
	}

	plan := &Plan{
		GuildID:     cfg.Guild.ID,
		GuildName:   cfg.Guild.Name,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		ConfigPath:  ".cog/config/discord/server.yaml",
	}

	// Build live lookup maps by Discord ID
	liveChannelsByID := map[string]DiscordChannel{}
	liveCategoriesByID := map[string]DiscordChannel{}
	liveRolesByID := map[string]DiscordRole{}
	categoryIDToName := map[string]string{}

	for _, ch := range channels {
		if ch.Type == 4 {
			liveCategoriesByID[ch.ID] = ch
			categoryIDToName[ch.ID] = ch.Name
		} else {
			liveChannelsByID[ch.ID] = ch
		}
	}
	for _, r := range roles {
		liveRolesByID[r.ID] = r
	}

	// Build name-based fallback maps for resources not in state
	liveRolesByName := map[string]DiscordRole{}
	liveCategoriesByName := map[string]DiscordChannel{}
	for _, r := range roles {
		liveRolesByName[strings.ToLower(r.Name)] = r
	}
	for _, ch := range channels {
		if ch.Type == 4 {
			liveCategoriesByName[strings.ToLower(ch.Name)] = ch
		}
	}

	// Build role name → ID map for permission resolution
	roleNameToID := map[string]string{}
	for _, r := range roles {
		roleNameToID[strings.ToLower(r.Name)] = r.ID
	}

	// State indexes
	stateByAddr := stateResourceIndex(state)
	stateByID := stateResourceByID(state)

	// Track matched live resources
	matchedRoleIDs := map[string]bool{}
	matchedCatIDs := map[string]bool{}
	matchedChannelIDs := map[string]bool{}

	// Track config addresses for rename detection
	configAddresses := map[string]bool{}

	// ── Diff roles ──
	for _, desired := range cfg.Guild.Roles {
		addr := roleAddress(desired.Name)
		configAddresses[addr] = true

		var liveRole *DiscordRole

		// Try state-based ID matching first
		if sr, ok := stateByAddr[addr]; ok {
			if r, ok := liveRolesByID[sr.DiscordID]; ok {
				liveRole = &r
			}
		}
		// Fallback to name-based
		if liveRole == nil {
			if r, ok := liveRolesByName[strings.ToLower(desired.Name)]; ok {
				liveRole = &r
			}
		}

		if liveRole != nil {
			matchedRoleIDs[liveRole.ID] = true
			diffs := diffRole(desired, *liveRole)
			// Also check permissions
			if permDiff := diffRolePermissions(desired.Permissions, liveRole.Permissions); permDiff != "" {
				diffs = append(diffs, permDiff)
			}
			if len(diffs) > 0 {
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "update",
					ResourceType: "role",
					Name:         desired.Name,
					Details:      map[string]any{"changes": diffs, "id": liveRole.ID},
				})
			}
		} else {
			plan.Actions = append(plan.Actions, PlanAction{
				Action:       "create",
				ResourceType: "role",
				Name:         desired.Name,
				Details: map[string]any{
					"color":       desired.Color,
					"hoist":       desired.Hoist,
					"mentionable": desired.Mentionable,
					"permissions": desired.Permissions,
				},
			})
		}
	}

	// ── Diff categories and channels ──
	for _, cat := range cfg.Guild.Categories {
		catAddr := categoryAddress(cat.Name)
		configAddresses[catAddr] = true

		var liveCat *DiscordChannel

		if sr, ok := stateByAddr[catAddr]; ok {
			if c, ok := liveCategoriesByID[sr.DiscordID]; ok {
				liveCat = &c
			}
		}
		if liveCat == nil {
			if c, ok := liveCategoriesByName[strings.ToLower(cat.Name)]; ok {
				liveCat = &c
			}
		}

		if liveCat != nil {
			matchedCatIDs[liveCat.ID] = true

			// Check if name changed (rename detection via state)
			if sr, ok := stateByAddr[catAddr]; ok && sr.DiscordID == liveCat.ID {
				if liveCat.Name != cat.Name && !strings.EqualFold(liveCat.Name, cat.Name) {
					plan.Actions = append(plan.Actions, PlanAction{
						Action:       "rename",
						ResourceType: "category",
						Name:         cat.Name,
						Details:      map[string]any{"id": liveCat.ID, "old_name": liveCat.Name, "new_name": cat.Name},
					})
				}
			}

			catDiffs := diffCategory(cat, *liveCat)

			// Permission overwrite diffing for category
			if len(cat.PermissionOverwrites) > 0 || len(liveCat.PermissionOverwrites) > 0 {
				permDiffs := diffPermOverwrites(cat.PermissionOverwrites, liveCat.PermissionOverwrites, roleNameToID)
				if len(permDiffs) > 0 {
					var permStrs []string
					for _, pd := range permDiffs {
						permStrs = append(permStrs, fmt.Sprintf("perm %s %s: allow %s, deny %s", pd.Action, pd.TargetName, pd.AllowDiff, pd.DenyDiff))
					}
					catDiffs = append(catDiffs, permStrs...)
				}
			}

			if len(catDiffs) > 0 {
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "update",
					ResourceType: "category",
					Name:         cat.Name,
					Details:      map[string]any{"changes": catDiffs, "id": liveCat.ID},
				})
			}

			// Diff channels within this category
			for _, ch := range cat.Channels {
				chAddr := channelAddress(cat.Name, ch.Name)
				configAddresses[chAddr] = true

				var liveCh *DiscordChannel

				// State-based ID matching
				if sr, ok := stateByAddr[chAddr]; ok {
					if c, ok := liveChannelsByID[sr.DiscordID]; ok {
						liveCh = &c
					}
				}
				// Name-based fallback (within this category, or orphaned)
				if liveCh == nil {
					for _, c := range channels {
						if c.Type != 4 && strings.EqualFold(c.Name, ch.Name) {
							// Match if already in the right category, or if orphaned (no parent)
							if (c.ParentID != nil && *c.ParentID == liveCat.ID) || c.ParentID == nil {
								liveCh = &c
								break
							}
						}
					}
				}

				if liveCh != nil {
					matchedChannelIDs[liveCh.ID] = true

					// Detect move: channel exists but parent is wrong or missing
					needsMove := liveCh.ParentID == nil || *liveCh.ParentID != liveCat.ID
					if needsMove {
						oldCatName := "(none)"
						if liveCh.ParentID != nil {
							oldCatName = categoryIDToName[*liveCh.ParentID]
						}
						plan.Actions = append(plan.Actions, PlanAction{
							Action:       "move",
							ResourceType: "channel",
							Name:         ch.Name,
							Details: map[string]any{
								"id":              liveCh.ID,
								"from_category":   oldCatName,
								"to_category":     cat.Name,
								"new_parent_id":   liveCat.ID,
							},
						})
					}

					// Detect rename via state
					if sr, ok := stateByAddr[chAddr]; ok && sr.DiscordID == liveCh.ID {
						if liveCh.Name != ch.Name && !strings.EqualFold(liveCh.Name, ch.Name) {
							plan.Actions = append(plan.Actions, PlanAction{
								Action:       "rename",
								ResourceType: "channel",
								Name:         ch.Name,
								Details:      map[string]any{"id": liveCh.ID, "old_name": liveCh.Name, "new_name": ch.Name},
							})
						}
					}

					chDiffs := diffChannel(ch, *liveCh, liveCat.ID)

					// Permission overwrite diffing for channel
					if len(ch.PermissionOverwrites) > 0 || len(liveCh.PermissionOverwrites) > 0 {
						permDiffs := diffPermOverwrites(ch.PermissionOverwrites, liveCh.PermissionOverwrites, roleNameToID)
						if len(permDiffs) > 0 {
							var permStrs []string
							for _, pd := range permDiffs {
								permStrs = append(permStrs, fmt.Sprintf("perm %s %s: allow %s, deny %s", pd.Action, pd.TargetName, pd.AllowDiff, pd.DenyDiff))
							}
							chDiffs = append(chDiffs, permStrs...)
						}
					}

					if len(chDiffs) > 0 {
						plan.Actions = append(plan.Actions, PlanAction{
							Action:       "update",
							ResourceType: "channel",
							Name:         ch.Name,
							Details:      map[string]any{"changes": chDiffs, "id": liveCh.ID, "category": cat.Name},
						})
					}
				} else {
					plan.Actions = append(plan.Actions, PlanAction{
						Action:       "create",
						ResourceType: "channel",
						Name:         ch.Name,
						Details: map[string]any{
							"type":     ch.Type,
							"category": cat.Name,
							"topic":    ch.Topic,
							"position": ch.Position,
							"slowmode": ch.Slowmode,
						},
					})
				}
			}
		} else {
			// Category doesn't exist — create it and all channels
			plan.Actions = append(plan.Actions, PlanAction{
				Action:       "create",
				ResourceType: "category",
				Name:         cat.Name,
				Details:      map[string]any{"position": cat.Position},
			})
			for _, ch := range cat.Channels {
				chAddr := channelAddress(cat.Name, ch.Name)
				configAddresses[chAddr] = true
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "create",
					ResourceType: "channel",
					Name:         ch.Name,
					Details: map[string]any{
						"type":     ch.Type,
						"category": cat.Name,
						"topic":    ch.Topic,
						"position": ch.Position,
						"slowmode": ch.Slowmode,
					},
				})
			}
		}
	}

	// ── Rename detection: orphaned state entries matched to unmatched config ──
	var orphanedState []StateResource
	for _, sr := range state.Resources {
		if sr.Mode != "managed" {
			continue
		}
		if configAddresses[sr.Address] {
			continue
		}
		// Check the resource still exists on Discord
		switch sr.Type {
		case "role":
			if _, ok := liveRolesByID[sr.DiscordID]; ok {
				orphanedState = append(orphanedState, sr)
			}
		case "category":
			if _, ok := liveCategoriesByID[sr.DiscordID]; ok {
				orphanedState = append(orphanedState, sr)
			}
		case "channel":
			if _, ok := liveChannelsByID[sr.DiscordID]; ok {
				orphanedState = append(orphanedState, sr)
			}
		}
	}

	// Find unmatched config entries (not in state, not already matched)
	var unmatchedConfig []string // addresses
	for addr := range configAddresses {
		if _, ok := stateByAddr[addr]; !ok {
			// Check it's not already handled as a create
			alreadyPlanned := false
			for _, a := range plan.Actions {
				if a.Action == "create" {
					// Reconstruct address from action
					var testAddr string
					switch a.ResourceType {
					case "role":
						testAddr = roleAddress(a.Name)
					case "category":
						testAddr = categoryAddress(a.Name)
					case "channel":
						if cat, ok := a.Details["category"]; ok {
							testAddr = channelAddress(fmt.Sprint(cat), a.Name)
						}
					}
					if testAddr == addr {
						alreadyPlanned = true
						break
					}
				}
			}
			if !alreadyPlanned {
				unmatchedConfig = append(unmatchedConfig, addr)
			}
		}
	}

	// Auto-detect renames: exactly one orphan + one unmatched of same type
	if len(orphanedState) > 0 && len(unmatchedConfig) > 0 {
		// Group by type
		orphansByType := map[string][]StateResource{}
		for _, sr := range orphanedState {
			orphansByType[sr.Type] = append(orphansByType[sr.Type], sr)
		}
		unmatchedByType := map[string][]string{}
		for _, addr := range unmatchedConfig {
			parts := strings.Split(addr, "/")
			var t string
			switch parts[0] {
			case "role":
				t = "role"
			case "category":
				if len(parts) > 2 && parts[2] == "channel" {
					t = "channel"
				} else {
					t = "category"
				}
			}
			unmatchedByType[t] = append(unmatchedByType[t], addr)
		}

		for resType, orphans := range orphansByType {
			unmatched := unmatchedByType[resType]
			if len(orphans) == 1 && len(unmatched) == 1 {
				// Auto-detect rename
				sr := orphans[0]
				newAddr := unmatched[0]
				parts := strings.Split(newAddr, "/")
				var newName string
				switch resType {
				case "role":
					newName = parts[1]
				case "category":
					newName = parts[1]
				case "channel":
					if len(parts) >= 4 {
						newName = parts[3]
					}
				}

				// Check if we already emitted a rename for this ID
				alreadyRenamed := false
				for _, a := range plan.Actions {
					if a.Action == "rename" && a.Details["id"] == sr.DiscordID {
						alreadyRenamed = true
						break
					}
				}
				if !alreadyRenamed && newName != "" {
					plan.Actions = append(plan.Actions, PlanAction{
						Action:       "rename",
						ResourceType: resType,
						Name:         newName,
						Details: map[string]any{
							"id":       sr.DiscordID,
							"old_name": sr.Name,
							"new_name": newName,
							"detected": "auto (orphan+unmatched)",
						},
					})
				}
			} else if len(orphans) > 1 || len(unmatched) > 1 {
				plan.Warnings = append(plan.Warnings,
					fmt.Sprintf("Ambiguous rename: %d orphaned %s(s) in state, %d unmatched in config. Use 'cog import discord' to disambiguate.",
						len(orphans), resType, len(unmatched)))
			}
		}
	}

	// ── Detect deletions / skips for unmatched live resources ──
	if cfg.Reconciler.PruneUnmanaged {
		for _, ch := range channels {
			if ch.Type == 4 {
				if !matchedCatIDs[ch.ID] {
					plan.Actions = append(plan.Actions, PlanAction{
						Action:       "delete",
						ResourceType: "category",
						Name:         ch.Name,
						Details:      map[string]any{"id": ch.ID},
					})
				}
			} else {
				if !matchedChannelIDs[ch.ID] {
					catName := ""
					if ch.ParentID != nil {
						catName = categoryIDToName[*ch.ParentID]
					}
					plan.Actions = append(plan.Actions, PlanAction{
						Action:       "delete",
						ResourceType: "channel",
						Name:         ch.Name,
						Details:      map[string]any{"id": ch.ID, "type": channelTypeToString[ch.Type], "category": catName},
					})
				}
			}
		}
		for _, role := range roles {
			if !matchedRoleIDs[role.ID] && !role.Managed && strings.ToLower(role.Name) != "@everyone" {
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "delete",
					ResourceType: "role",
					Name:         role.Name,
					Details:      map[string]any{"id": role.ID},
				})
			}
		}
	} else {
		for _, ch := range channels {
			if ch.Type == 4 {
				if !matchedCatIDs[ch.ID] {
					plan.Actions = append(plan.Actions, PlanAction{
						Action:       "skip",
						ResourceType: "category",
						Name:         ch.Name,
						Details:      map[string]any{"reason": "not in config"},
					})
				}
			} else {
				if !matchedChannelIDs[ch.ID] {
					catName := ""
					if ch.ParentID != nil {
						catName = categoryIDToName[*ch.ParentID]
					}
					reason := "not in config"
					if catName != "" {
						reason = fmt.Sprintf("not in config (under %q)", catName)
					}
					plan.Actions = append(plan.Actions, PlanAction{
						Action:       "skip",
						ResourceType: "channel",
						Name:         ch.Name,
						Details:      map[string]any{"reason": reason},
					})
				}
			}
		}
		for _, role := range roles {
			if !matchedRoleIDs[role.ID] {
				reason := "not in config"
				if role.Managed {
					reason = "discord-managed (integration/bot role)"
				}
				if strings.ToLower(role.Name) == "@everyone" {
					reason = "built-in @everyone role"
				}
				plan.Actions = append(plan.Actions, PlanAction{
					Action:       "skip",
					ResourceType: "role",
					Name:         role.Name,
					Details:      map[string]any{"reason": reason},
				})
			}
		}
	}

	// ── Detect external deletions from state ──
	for _, sr := range state.Resources {
		if sr.Mode != "managed" {
			continue
		}
		found := false
		switch sr.Type {
		case "role":
			_, found = liveRolesByID[sr.DiscordID]
		case "category":
			_, found = liveCategoriesByID[sr.DiscordID]
		case "channel":
			_, found = liveChannelsByID[sr.DiscordID]
		}
		if !found {
			plan.Warnings = append(plan.Warnings,
				fmt.Sprintf("External deletion: %s %q (ID %s) exists in state but not on Discord",
					sr.Type, sr.Name, sr.DiscordID))
		}
	}

	// Compute summary
	for _, a := range plan.Actions {
		switch a.Action {
		case "create":
			plan.Summary.Creates++
		case "update":
			plan.Summary.Updates++
		case "delete":
			plan.Summary.Deletes++
		case "skip":
			plan.Summary.Skipped++
		case "rename":
			plan.Summary.Updates++
		case "move":
			plan.Summary.Updates++
		}
	}

	// suppress unused warning for stateByID
	_ = stateByID

	return plan
}

// ─── Build state from live server data ───────────────────────────────────────

func buildStateFromLive(guildID string, cfg *DiscordServerConfig, channels []DiscordChannel, roles []DiscordRole, existingState *DiscordState) *DiscordState {
	now := time.Now().UTC().Format(time.RFC3339)

	lineage := generateLineage()
	serial := 0
	if existingState != nil {
		lineage = existingState.Lineage
		serial = existingState.Serial
	}

	state := &DiscordState{
		Version: 1,
		Lineage: lineage,
		Serial:  serial,
		GuildID: guildID,
	}

	// Build config address sets for managed detection
	configRoles := map[string]bool{}
	configCategories := map[string]bool{}
	configChannels := map[string]bool{} // "catName/chName"
	if cfg != nil {
		for _, r := range cfg.Guild.Roles {
			configRoles[strings.ToLower(r.Name)] = true
		}
		for _, cat := range cfg.Guild.Categories {
			configCategories[strings.ToLower(cat.Name)] = true
			for _, ch := range cat.Channels {
				configChannels[strings.ToLower(cat.Name)+"/"+strings.ToLower(ch.Name)] = true
			}
		}
	}

	// Category ID → name
	categoryIDToName := map[string]string{}
	for _, ch := range channels {
		if ch.Type == 4 {
			categoryIDToName[ch.ID] = ch.Name
		}
	}

	// ── Roles ──
	for _, r := range roles {
		mode := "unmanaged"
		unmanagedReason := ""

		if r.ID == guildID {
			mode = "data"
			unmanagedReason = "built-in @everyone"
		} else if r.Managed {
			mode = "unmanaged"
			unmanagedReason = "discord-managed"
		} else if configRoles[strings.ToLower(r.Name)] {
			mode = "managed"
		} else {
			unmanagedReason = "not in config"
		}

		attrs := map[string]any{
			"color":       intColorToHex(r.Color),
			"hoist":       r.Hoist,
			"mentionable": r.Mentionable,
			"position":    r.Position,
			"permissions": r.Permissions,
		}

		state.Resources = append(state.Resources, StateResource{
			Address:         roleAddress(r.Name),
			Type:            "role",
			Mode:            mode,
			DiscordID:       r.ID,
			Name:            r.Name,
			Attributes:      attrs,
			UnmanagedReason: unmanagedReason,
			LastRefreshed:   now,
		})
	}

	// ── Categories and channels ──
	for _, ch := range channels {
		if ch.Type != 4 {
			continue
		}

		mode := "unmanaged"
		unmanagedReason := ""
		if configCategories[strings.ToLower(ch.Name)] {
			mode = "managed"
		} else {
			unmanagedReason = "not in config"
		}

		attrs := map[string]any{
			"position": ch.Position,
		}
		if len(ch.PermissionOverwrites) > 0 {
			attrs["permission_overwrites"] = len(ch.PermissionOverwrites)
		}

		state.Resources = append(state.Resources, StateResource{
			Address:         categoryAddress(ch.Name),
			Type:            "category",
			Mode:            mode,
			DiscordID:       ch.ID,
			Name:            ch.Name,
			Attributes:      attrs,
			UnmanagedReason: unmanagedReason,
			LastRefreshed:   now,
		})
	}

	for _, ch := range channels {
		if ch.Type == 4 {
			continue
		}

		catName := ""
		catID := ""
		if ch.ParentID != nil {
			catID = *ch.ParentID
			catName = categoryIDToName[catID]
		}

		mode := "unmanaged"
		unmanagedReason := ""
		if catName != "" {
			key := strings.ToLower(catName) + "/" + strings.ToLower(ch.Name)
			if configChannels[key] {
				mode = "managed"
			} else {
				unmanagedReason = "not in config"
			}
		} else {
			unmanagedReason = "uncategorized"
		}

		chType := channelTypeToString[ch.Type]
		if chType == "" {
			chType = strconv.Itoa(ch.Type)
		}

		attrs := map[string]any{
			"type":     chType,
			"position": ch.Position,
			"topic":    ch.Topic,
			"nsfw":     ch.NSFW,
			"slowmode": ch.RateLimitPerUser,
		}
		if len(ch.PermissionOverwrites) > 0 {
			attrs["permission_overwrites"] = len(ch.PermissionOverwrites)
		}

		addr := channelAddress(catName, ch.Name)
		if catName == "" {
			addr = "channel/" + ch.Name // uncategorized
		}

		state.Resources = append(state.Resources, StateResource{
			Address:         addr,
			Type:            "channel",
			Mode:            mode,
			DiscordID:       ch.ID,
			Name:            ch.Name,
			ParentAddress:   categoryAddress(catName),
			ParentID:        catID,
			Attributes:      attrs,
			UnmanagedReason: unmanagedReason,
			LastRefreshed:   now,
		})
	}

	return state
}
