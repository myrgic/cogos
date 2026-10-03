// discord_provider.go
// DiscordProvider implements the reconcile.Reconcilable interface for Discord
// server infrastructure reconciliation. This is a thin adapter layer that
// delegates to the existing functions in discord_reconcile.go.

package discord

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// DiscordLiveState bundles the live Discord data fetched from the API.
type DiscordLiveState struct {
	Channels []DiscordChannel
	Roles    []DiscordRole
}

// DiscordProvider implements reconcile.Reconcilable for Discord server management.
type DiscordProvider struct {
	// Token is the Discord bot token. Set before calling FetchLive/ApplyPlan.
	Token string

	// WorkspaceRoot is consulted by Health() as a fallback signal (presence of
	// .cog/config/discord/config.hcl) when no token is set directly. Mirrors
	// the daemon-side stub's prior token-presence check so re-homing this
	// provider does not regress proprioception health reporting. Optional —
	// SetWorkspaceRoot wires it in from the daemon boot sequence; if never
	// called, Health() falls back to just Token/DISCORD_BOT_TOKEN.
	WorkspaceRoot string
}

// Type returns "discord".
func (d *DiscordProvider) Type() string { return "discord" }

// SetToken implements reconcile.Tokenable for generic CLI dispatch.
func (d *DiscordProvider) SetToken(token string) { d.Token = token }

// SetWorkspaceRoot wires the resolved workspace root into the provider so
// Health() can check for a declared config file even before a token has been
// set. Called by the daemon package at boot (mirrors SetWorkspaceRoot on the
// other daemon-side providers).
func (d *DiscordProvider) SetWorkspaceRoot(root string) { d.WorkspaceRoot = root }

// LoadConfig loads the Discord server config (HCL-first, YAML fallback).
// Returns *DiscordServerConfig as any.
func (d *DiscordProvider) LoadConfig(root string) (any, error) {
	if d.WorkspaceRoot == "" {
		d.WorkspaceRoot = root
	}
	cfg, _, err := loadDiscordServerConfig(root)
	return cfg, err
}

// ensureToken resolves the bot token through the full chain (env, then
// auth.yaml token_command / token) when nothing was injected via SetToken.
// Without this, the generic reconcile CLI path only ever saw {TYPE}_TOKEN env
// vars and never auth.yaml, so plan/apply failed where snapshot succeeded.
func (d *DiscordProvider) ensureToken() error {
	if d.Token != "" {
		return nil
	}
	if d.WorkspaceRoot == "" {
		return fmt.Errorf("discord: bot token not set")
	}
	tok, err := resolveToken(d.WorkspaceRoot, "")
	if err != nil {
		return err
	}
	d.Token = tok
	return nil
}

// FetchLive retrieves the current Discord server state from the API.
// Config must be *DiscordServerConfig. Returns *DiscordLiveState as any.
func (d *DiscordProvider) FetchLive(ctx context.Context, config any) (any, error) {
	cfg, ok := config.(*DiscordServerConfig)
	if !ok {
		return nil, fmt.Errorf("discord: expected *DiscordServerConfig, got %T", config)
	}

	if err := d.ensureToken(); err != nil {
		return nil, err
	}

	client := newDiscordClient(d.Token, cfg.Reconciler.MaxAPICalls)

	channels, err := client.fetchChannels(cfg.Guild.ID)
	if err != nil {
		return nil, fmt.Errorf("discord: fetching channels: %w", err)
	}

	roles, err := client.fetchRoles(cfg.Guild.ID)
	if err != nil {
		return nil, fmt.Errorf("discord: fetching roles: %w", err)
	}

	return &DiscordLiveState{Channels: channels, Roles: roles}, nil
}

// ComputePlan compares declared config against live state to produce a plan.
// Config must be *DiscordServerConfig, live must be *DiscordLiveState.
// Returns *reconcile.Plan (wrapping the Discord-specific Plan).
func (d *DiscordProvider) ComputePlan(config any, live any, state *reconcile.State) (*reconcile.Plan, error) {
	cfg, ok := config.(*DiscordServerConfig)
	if !ok {
		return nil, fmt.Errorf("discord: expected *DiscordServerConfig, got %T", config)
	}
	liveState, ok := live.(*DiscordLiveState)
	if !ok {
		return nil, fmt.Errorf("discord: expected *DiscordLiveState, got %T", live)
	}

	// Convert generic state to Discord state for the existing function
	var discordState *DiscordState
	if state != nil {
		discordState = reconcileStateToDiscordState(state)
	}

	plan, err := computePlanWithState(cfg, liveState.Channels, liveState.Roles, discordState)
	if err != nil {
		return nil, fmt.Errorf("discord: %w", err)
	}

	// Convert Discord Plan to generic reconcile.Plan
	return discordPlanToReconcilePlan(plan), nil
}

// ApplyPlan executes the planned changes against Discord.
func (d *DiscordProvider) ApplyPlan(ctx context.Context, plan *reconcile.Plan) ([]reconcile.Result, error) {
	if err := d.ensureToken(); err != nil {
		return nil, err
	}

	// Convert generic plan back to Discord plan
	discordPlan := reconcilePlanToDiscordPlan(plan)

	guildID, _ := plan.Metadata["guild_id"].(string)
	if guildID == "" {
		return nil, fmt.Errorf("discord: guild_id not found in plan metadata")
	}

	maxCalls := 60
	if mc, ok := plan.Metadata["max_api_calls"].(float64); ok {
		maxCalls = int(mc)
	}

	client := newDiscordClient(d.Token, maxCalls)

	// Need roles and channels for apply (name→ID resolution)
	roles, err := client.fetchRoles(guildID)
	if err != nil {
		return nil, fmt.Errorf("discord: fetching roles for apply: %w", err)
	}
	channels, err := client.fetchChannels(guildID)
	if err != nil {
		return nil, fmt.Errorf("discord: fetching channels for apply: %w", err)
	}

	results, err := applyPlan(client, discordPlan, guildID, roles, channels)
	if err != nil {
		return nil, err
	}

	// Convert to generic results
	genericResults := make([]reconcile.Result, len(results))
	for i, r := range results {
		genericResults[i] = reconcile.Result{
			Phase:     r.Phase,
			Action:    r.Action,
			Name:      r.Name,
			Status:    reconcile.ApplyStatus(r.Status),
			Error:     r.Error,
			CreatedID: r.CreatedID,
		}
	}
	return genericResults, nil
}

// BuildState constructs state from live data for snapshot/import.
func (d *DiscordProvider) BuildState(config any, live any, existing *reconcile.State) (*reconcile.State, error) {
	cfg, ok := config.(*DiscordServerConfig)
	if !ok {
		return nil, fmt.Errorf("discord: expected *DiscordServerConfig, got %T", config)
	}
	liveState, ok := live.(*DiscordLiveState)
	if !ok {
		return nil, fmt.Errorf("discord: expected *DiscordLiveState, got %T", live)
	}

	var existingDiscord *DiscordState
	if existing != nil {
		existingDiscord = reconcileStateToDiscordState(existing)
	}

	ds := buildStateFromLive(cfg.Guild.ID, cfg, liveState.Channels, liveState.Roles, existingDiscord)
	return discordStateToReconcileState(ds), nil
}

// Health returns the current three-axis status for the Discord provider.
// A token set directly (SetToken/Tokenable) or via DISCORD_BOT_TOKEN is
// treated as configured; failing that, the presence of a declared
// .cog/config/discord/config.hcl under WorkspaceRoot is also accepted as
// "configured" — this mirrors the daemon-side stub's prior check so
// proprioception reporting does not regress.
func (d *DiscordProvider) Health() reconcile.ResourceStatus {
	token := d.Token
	if token == "" {
		token = os.Getenv("DISCORD_BOT_TOKEN")
	}
	if token == "" {
		if d.WorkspaceRoot != "" {
			hclPath := filepath.Join(d.WorkspaceRoot, ".cog", "config", "discord", "config.hcl")
			if _, err := os.Stat(hclPath); err == nil {
				return reconcile.NewResourceStatus(reconcile.SyncStatusUnknown, reconcile.HealthHealthy)
			}
		}
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthMissing,
			Operation: reconcile.OperationIdle,
			Message:   "no bot token configured",
		}
	}
	return reconcile.NewResourceStatus(reconcile.SyncStatusUnknown, reconcile.HealthHealthy)
}

// ─── Config Export (ConfigExporter interface) ───────────────────────────────

// ExportConfig implements reconcile.ConfigExporter. It reads the live Discord
// server state (roles + channels) and regenerates the declarative
// .cog/config/discord/server.yaml — the "snapshot" operation (live → spec)
// that establishes the declared-state baseline.
//
// The bot token is resolved from config the same way SetupDiscordProvider does
// (flag/env/auth.yaml) unless one is already set via SetToken.
func (d *DiscordProvider) ExportConfig(root string) error {
	// Resolve token if not already set so the CLI can call this uniformly.
	if d.Token == "" {
		token, err := resolveToken(root, "")
		if err != nil {
			return fmt.Errorf("discord: resolve token for snapshot: %w", err)
		}
		d.Token = token
	}

	// Load the existing config to learn the guild ID/name to crawl.
	cfg, _, err := loadDiscordServerConfig(root)
	if err != nil {
		return fmt.Errorf("discord: load config for snapshot: %w", err)
	}
	if cfg.Guild.ID == "" {
		return fmt.Errorf("discord: snapshot requires guild.id in %s", filepath.Join(".cog", "config", "discord", "server.yaml"))
	}

	live, err := d.FetchLive(context.Background(), cfg)
	if err != nil {
		return fmt.Errorf("discord: fetch live state for snapshot: %w", err)
	}
	liveState, ok := live.(*DiscordLiveState)
	if !ok {
		return fmt.Errorf("discord: unexpected live state type %T", live)
	}

	guildName := cfg.Guild.Name
	guildDesc := cfg.Guild.Description
	newCfg := buildDiscordConfigFromLive(liveState, cfg.Guild.ID, guildName, guildDesc)
	// Snapshot captures live → spec for the guild only. The reconciler block
	// is operator policy, not live state; carry it through unchanged rather
	// than resetting dry_run / respect_user_managed to zero values (#615).
	newCfg.Reconciler = cfg.Reconciler

	if err := writeDiscordServerYAML(root, newCfg); err != nil {
		return fmt.Errorf("discord: write snapshot: %w", err)
	}
	return nil
}

// buildDiscordConfigFromLive maps live Discord state (flat channel list + roles)
// into a declarative *DiscordServerConfig, reconstructing the category→channel
// nesting from each channel's parent_id and type (type 4 = category). Secrets
// are never part of this shape, so nothing is redacted. Ordering is
// deterministic: roles by descending position, categories/channels by ascending
// position.
func buildDiscordConfigFromLive(live *DiscordLiveState, guildID, guildName, guildDesc string) *DiscordServerConfig {
	// role ID → name for permission-overwrite target resolution.
	roleIDToName := map[string]string{}
	for _, r := range live.Roles {
		roleIDToName[r.ID] = r.Name
	}

	// ── Roles ── (skip @everyone and discord-managed roles)
	var roles []RoleConfig
	for _, r := range live.Roles {
		if r.ID == guildID || r.Managed {
			continue
		}
		roles = append(roles, RoleConfig{
			Name:        r.Name,
			Color:       intColorToHex(r.Color),
			Permissions: permBitsToNames(r.Permissions),
			Hoist:       r.Hoist,
			Mentionable: r.Mentionable,
			Position:    r.Position,
			ManagedBy:   "cog",
		})
	}
	sort.SliceStable(roles, func(i, j int) bool {
		return roles[i].Position > roles[j].Position
	})

	// ── Categories ── (channel type 4)
	catByID := map[string]*CategoryConfig{}
	var categories []*CategoryConfig
	for _, ch := range live.Channels {
		if ch.Type != 4 {
			continue
		}
		cat := &CategoryConfig{
			Name:                 ch.Name,
			Position:             ch.Position,
			ManagedBy:            "cog",
			PermissionOverwrites: convertPermOverwrites(ch.PermissionOverwrites, roleIDToName),
		}
		catByID[ch.ID] = cat
		categories = append(categories, cat)
	}

	// ── Channels ── nested under their parent category.
	for _, ch := range live.Channels {
		if ch.Type == 4 {
			continue
		}
		if ch.ParentID == nil {
			continue // uncategorized channels are not part of the managed tree
		}
		cat, ok := catByID[*ch.ParentID]
		if !ok {
			continue
		}
		chType := channelTypeToString[ch.Type]
		if chType == "" {
			chType = fmt.Sprintf("%d", ch.Type)
		}
		cat.Channels = append(cat.Channels, ChannelConfig{
			Name:                 ch.Name,
			Type:                 chType,
			Topic:                ch.Topic,
			Position:             ch.Position,
			Slowmode:             ch.RateLimitPerUser,
			NSFW:                 ch.NSFW,
			ManagedBy:            "cog",
			PermissionOverwrites: convertPermOverwrites(ch.PermissionOverwrites, roleIDToName),
			Tags:                 tagsFromLive(ch.AvailableTags),
			DefaultSort:          liveForumSort(ch),
			DefaultLayout:        liveForumLayout(ch),
		})
	}

	sort.SliceStable(categories, func(i, j int) bool {
		return categories[i].Position < categories[j].Position
	})
	for _, cat := range categories {
		sort.SliceStable(cat.Channels, func(i, j int) bool {
			return cat.Channels[i].Position < cat.Channels[j].Position
		})
	}

	catVals := make([]CategoryConfig, len(categories))
	for i, cat := range categories {
		catVals[i] = *cat
	}

	return &DiscordServerConfig{
		Version: "1.0",
		Guild: GuildConfig{
			ID:          guildID,
			Name:        guildName,
			Description: guildDesc,
			ManagedBy:   "cog",
			Roles:       roles,
			Categories:  catVals,
		},
	}
}

// writeDiscordServerYAML marshals cfg to YAML, prepends the standard header
// comment block, and writes .cog/config/discord/server.yaml (0644).
func writeDiscordServerYAML(root string, cfg *DiscordServerConfig) error {
	yamlData, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	var buf strings.Builder
	buf.WriteString("# Discord Server Configuration (Declarative)\n")
	buf.WriteString("# Generated by: cog snapshot discord\n")
	buf.WriteString(fmt.Sprintf("# Generated at: %s\n", time.Now().UTC().Format(time.RFC3339)))
	buf.WriteString("#\n")
	buf.WriteString("# This file was auto-generated from the live Discord server state.\n")
	buf.WriteString("# The reconciler diffs this against the live server and applies\n")
	buf.WriteString("# creates/updates/deletes for managed resources.\n")
	buf.WriteString("#\n")
	buf.WriteString("# Usage:\n")
	buf.WriteString("#   cogos reconcile discord --dry-run   # see what would change\n")
	buf.WriteString("#   cogos reconcile discord             # apply changes\n")
	buf.WriteString("#   cogos reconcile discord --snapshot  # re-crawl and regenerate this file\n")
	buf.WriteString("\n")
	buf.Write(yamlData)

	outPath := filepath.Join(root, ".cog", "config", "discord", "server.yaml")
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	if err := os.WriteFile(outPath, []byte(buf.String()), 0644); err != nil {
		return fmt.Errorf("write server.yaml: %w", err)
	}
	return nil
}

// --- Conversion helpers between Discord-specific and generic reconcile types ---

func discordPlanToReconcilePlan(plan *Plan) *reconcile.Plan {
	actions := make([]reconcile.Action, len(plan.Actions))
	for i, a := range plan.Actions {
		actions[i] = reconcile.Action{
			Action:       reconcile.ActionType(a.Action),
			ResourceType: a.ResourceType,
			Name:         a.Name,
			Details:      a.Details,
		}
	}
	return &reconcile.Plan{
		ResourceType: "discord",
		GeneratedAt:  plan.GeneratedAt,
		ConfigPath:   plan.ConfigPath,
		Actions:      actions,
		Summary: reconcile.Summary{
			Creates: plan.Summary.Creates,
			Updates: plan.Summary.Updates,
			Deletes: plan.Summary.Deletes,
			Skipped: plan.Summary.Skipped,
		},
		Warnings: plan.Warnings,
		Metadata: map[string]any{
			"guild_id":   plan.GuildID,
			"guild_name": plan.GuildName,
		},
	}
}

func reconcilePlanToDiscordPlan(plan *reconcile.Plan) *Plan {
	actions := make([]PlanAction, len(plan.Actions))
	for i, a := range plan.Actions {
		actions[i] = PlanAction{
			Action:       string(a.Action),
			ResourceType: a.ResourceType,
			Name:         a.Name,
			Details:      a.Details,
		}
	}
	guildID, _ := plan.Metadata["guild_id"].(string)
	guildName, _ := plan.Metadata["guild_name"].(string)
	return &Plan{
		GuildID:     guildID,
		GuildName:   guildName,
		GeneratedAt: plan.GeneratedAt,
		ConfigPath:  plan.ConfigPath,
		Actions:     actions,
		Summary: PlanSummary{
			Creates: plan.Summary.Creates,
			Updates: plan.Summary.Updates,
			Deletes: plan.Summary.Deletes,
			Skipped: plan.Summary.Skipped,
		},
		Warnings: plan.Warnings,
	}
}

func discordStateToReconcileState(ds *DiscordState) *reconcile.State {
	resources := make([]reconcile.Resource, len(ds.Resources))
	for i, r := range ds.Resources {
		resources[i] = reconcile.Resource{
			Address:         r.Address,
			Type:            r.Type,
			Mode:            reconcile.ResourceMode(r.Mode),
			ExternalID:      r.DiscordID,
			Name:            r.Name,
			ParentAddress:   r.ParentAddress,
			ParentID:        r.ParentID,
			Attributes:      r.Attributes,
			UnmanagedReason: r.UnmanagedReason,
			LastRefreshed:   r.LastRefreshed,
		}
	}
	return &reconcile.State{
		Version:      ds.Version,
		Lineage:      ds.Lineage,
		Serial:       ds.Serial,
		ResourceType: "discord",
		GeneratedAt:  ds.GeneratedAt,
		Resources:    resources,
		Metadata:     discordStateMetadata(ds),
	}
}

func reconcileStateToDiscordState(state *reconcile.State) *DiscordState {
	resources := make([]StateResource, len(state.Resources))
	for i, r := range state.Resources {
		resources[i] = StateResource{
			Address:         r.Address,
			Type:            r.Type,
			Mode:            string(r.Mode),
			DiscordID:       r.ExternalID,
			Name:            r.Name,
			ParentAddress:   r.ParentAddress,
			ParentID:        r.ParentID,
			Attributes:      r.Attributes,
			UnmanagedReason: r.UnmanagedReason,
			LastRefreshed:   r.LastRefreshed,
		}
	}
	guildID, _ := state.Metadata["guild_id"].(string)
	return &DiscordState{
		Version:     state.Version,
		Lineage:     state.Lineage,
		Serial:      state.Serial,
		GuildID:     guildID,
		GeneratedAt: state.GeneratedAt,
		Resources:   resources,
		TagIDs:      tagIDsFromMetadata(state.Metadata),
	}
}

// ResolveDiscordToken resolves the bot token from flag or environment.
// Exported for use by verb dispatchers.
func ResolveDiscordToken(root, flagToken string) (string, error) {
	return resolveToken(root, flagToken)
}

// SetupDiscordProvider creates a DiscordProvider with a resolved token.
func SetupDiscordProvider(root, flagToken string) (*DiscordProvider, error) {
	token, err := resolveToken(root, flagToken)
	if err != nil {
		return nil, err
	}
	return &DiscordProvider{Token: token}, nil
}

// DiscordProviderHealthCheck performs a live health check by pinging the Discord API.
func DiscordProviderHealthCheck(token string) reconcile.ResourceStatus {
	if token == "" {
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthMissing,
			Operation: reconcile.OperationIdle,
			Message:   "no bot token",
		}
	}

	client := newDiscordClient(token, 1)
	_, err := client.get("/users/@me")
	if err != nil {
		return reconcile.ResourceStatus{
			Sync:      reconcile.SyncStatusUnknown,
			Health:    reconcile.HealthDegraded,
			Operation: reconcile.OperationIdle,
			Message:   fmt.Sprintf("API unreachable: %v", err),
		}
	}
	return reconcile.ResourceStatus{
		Sync:      reconcile.SyncStatusUnknown,
		Health:    reconcile.HealthHealthy,
		Operation: reconcile.OperationIdle,
		Message:   fmt.Sprintf("API reachable (checked %s)", time.Now().UTC().Format(time.RFC3339)),
	}
}
