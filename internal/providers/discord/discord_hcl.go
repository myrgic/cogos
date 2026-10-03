package discord

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// ─── HCL struct types ───────────────────────────────────────────────────────

type hclConfigFile struct {
	Guild      *hclGuild      `hcl:"guild,block"`
	Reconciler *hclReconciler `hcl:"reconciler,block"`
	Roles      []hclRole      `hcl:"role,block"`
	Categories []hclCategory  `hcl:"category,block"`
	Remain     hcl.Body       `hcl:",remain"` // captures variable/locals blocks
}

type hclGuild struct {
	ID          string `hcl:"id"`
	Name        string `hcl:"name"`
	Description string `hcl:"description,optional"`
}

type hclReconciler struct {
	DryRun             bool `hcl:"dry_run,optional"`
	PruneUnmanaged     bool `hcl:"prune_unmanaged,optional"`
	RespectUserManaged bool `hcl:"respect_user_managed,optional"`
	MaxAPICalls        int  `hcl:"max_api_calls,optional"`
}

type hclRole struct {
	Name        string   `hcl:"name,label"`
	Color       string   `hcl:"color,optional"`
	Permissions []string `hcl:"permissions,optional"`
	Hoist       bool     `hcl:"hoist,optional"`
	Mentionable bool     `hcl:"mentionable,optional"`
	Position    int      `hcl:"position,optional"`
}

type hclCategory struct {
	Name        string          `hcl:"name,label"`
	Permissions []hclPermission `hcl:"permission,block"`
	Channels    []hclChannel    `hcl:"channel,block"`
	Remain      hcl.Body        `hcl:",remain"` // captures channels_from
}

type hclChannel struct {
	Name        string          `hcl:"name,label"`
	Type        string          `hcl:"type,optional"`
	Topic       string          `hcl:"topic,optional"`
	Slowmode    int             `hcl:"slowmode,optional"`
	NSFW        bool            `hcl:"nsfw,optional"`
	Permissions []hclPermission `hcl:"permission,block"`

	// Forum-only.
	Tags          []hclTag `hcl:"tag,block"`
	DefaultSort   string   `hcl:"default_sort,optional"`
	DefaultLayout string   `hcl:"default_layout,optional"`
}

type hclTag struct {
	Name      string `hcl:"name,label"`
	Emoji     string `hcl:"emoji,optional"`
	Moderated bool   `hcl:"moderated,optional"`
}

type hclPermission struct {
	Role   string   `hcl:"role,optional"`
	Member string   `hcl:"member,optional"`
	Allow  []string `hcl:"allow,optional"`
	Deny   []string `hcl:"deny,optional"`
}

// ─── HCL variable/locals extraction ─────────────────────────────────────────

// parseHCLVariables extracts variable blocks and returns a cty object for var.*
func parseHCLVariables(body *hclsyntax.Body, ctx *hcl.EvalContext) (cty.Value, error) {
	vars := map[string]cty.Value{}

	for _, block := range body.Blocks {
		if block.Type != "variable" {
			continue
		}
		if len(block.Labels) < 1 {
			continue
		}
		name := block.Labels[0]

		// Look for "default" attribute in the block body
		if attr, ok := block.Body.Attributes["default"]; ok {
			val, diags := attr.Expr.Value(ctx)
			if diags.HasErrors() {
				return cty.NilVal, fmt.Errorf("evaluating variable %q default: %s", name, diags.Error())
			}
			vars[name] = val
		} else {
			vars[name] = cty.StringVal("")
		}
	}

	if len(vars) == 0 {
		return cty.EmptyObjectVal, nil
	}
	return cty.ObjectVal(vars), nil
}

// parseHCLLocals extracts locals blocks and returns a cty object for local.*
func parseHCLLocals(body *hclsyntax.Body, ctx *hcl.EvalContext) (cty.Value, error) {
	locals := map[string]cty.Value{}

	for _, block := range body.Blocks {
		if block.Type != "locals" {
			continue
		}
		for name, attr := range block.Body.Attributes {
			val, diags := attr.Expr.Value(ctx)
			if diags.HasErrors() {
				return cty.NilVal, fmt.Errorf("evaluating local %q: %s", name, diags.Error())
			}
			locals[name] = val
		}
	}

	if len(locals) == 0 {
		return cty.EmptyObjectVal, nil
	}
	return cty.ObjectVal(locals), nil
}

// ─── HCL parsing ────────────────────────────────────────────────────────────

func parseHCLConfig(path string) (*DiscordServerConfig, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}

	file, diags := hclsyntax.ParseConfig(src, path, hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return nil, fmt.Errorf("parsing HCL: %s", diags.Error())
	}

	body := file.Body.(*hclsyntax.Body)

	// Step 1: Extract variables → build var.* context
	baseCtx := &hcl.EvalContext{
		Variables: map[string]cty.Value{},
	}
	varObj, err := parseHCLVariables(body, baseCtx)
	if err != nil {
		return nil, err
	}
	baseCtx.Variables["var"] = varObj

	// Step 2: Evaluate locals with var context available
	localObj, err := parseHCLLocals(body, baseCtx)
	if err != nil {
		return nil, err
	}
	baseCtx.Variables["local"] = localObj

	// Step 3: Decode the main config blocks using gohcl
	var config hclConfigFile
	diags = gohcl.DecodeBody(file.Body, baseCtx, &config)
	if diags.HasErrors() {
		return nil, fmt.Errorf("decoding HCL config: %s", diags.Error())
	}

	// Step 4: Expand channels_from in categories
	for i := range config.Categories {
		if err := expandChannelsFrom(&config.Categories[i], baseCtx); err != nil {
			return nil, fmt.Errorf("expanding channels_from in category %q: %w", config.Categories[i].Name, err)
		}
	}

	// Step 5: Convert to DiscordServerConfig
	return hclToDiscordConfig(&config), nil
}

// expandChannelsFrom checks for a channels_from attribute in a category's Remain body
// and appends the generated channels.
func expandChannelsFrom(cat *hclCategory, ctx *hcl.EvalContext) error {
	if cat.Remain == nil {
		return nil
	}

	// Use PartialContent to extract channels_from attribute from the Remain body
	content, _, diags := cat.Remain.PartialContent(&hcl.BodySchema{
		Attributes: []hcl.AttributeSchema{
			{Name: "channels_from"},
		},
	})
	if diags.HasErrors() {
		return nil // no channels_from, that's fine
	}

	cfAttr, ok := content.Attributes["channels_from"]
	if !ok {
		return nil
	}

	val, diags := cfAttr.Expr.Value(ctx)
	if diags.HasErrors() {
		return fmt.Errorf("evaluating channels_from: %s", diags.Error())
	}

	if !val.CanIterateElements() {
		return fmt.Errorf("channels_from must be a list, got %s", val.Type().FriendlyName())
	}

	iter := val.ElementIterator()
	for iter.Next() {
		_, elem := iter.Element()

		ch := hclChannel{}

		if elem.Type().IsObjectType() || elem.Type().IsMapType() {
			if nameVal := elem.GetAttr("name"); nameVal.IsKnown() && nameVal.Type() == cty.String {
				ch.Name = nameVal.AsString()
			}
			// Optional attributes
			if elem.Type().HasAttribute("type") {
				if v := elem.GetAttr("type"); v.IsKnown() && v.Type() == cty.String {
					ch.Type = v.AsString()
				}
			}
			if elem.Type().HasAttribute("topic") {
				if v := elem.GetAttr("topic"); v.IsKnown() && v.Type() == cty.String {
					ch.Topic = v.AsString()
				}
			}
		} else if elem.Type() == cty.String {
			ch.Name = elem.AsString()
		}

		if ch.Name != "" {
			cat.Channels = append(cat.Channels, ch)
		}
	}

	return nil
}

// ─── HCL → DiscordServerConfig conversion ──────────────────────────────────

func hclToDiscordConfig(cfg *hclConfigFile) *DiscordServerConfig {
	dc := &DiscordServerConfig{
		Version: "1.0",
	}

	// Guild
	if cfg.Guild != nil {
		dc.Guild.ID = cfg.Guild.ID
		dc.Guild.Name = cfg.Guild.Name
		dc.Guild.Description = cfg.Guild.Description
		dc.Guild.ManagedBy = "cog"
	}

	// Reconciler
	if cfg.Reconciler != nil {
		dc.Reconciler.DryRun = cfg.Reconciler.DryRun
		dc.Reconciler.PruneUnmanaged = cfg.Reconciler.PruneUnmanaged
		dc.Reconciler.RespectUserManaged = cfg.Reconciler.RespectUserManaged
		dc.Reconciler.MaxAPICalls = cfg.Reconciler.MaxAPICalls
	}
	// Defaults
	if dc.Reconciler.MaxAPICalls == 0 {
		dc.Reconciler.MaxAPICalls = 60
	}
	if dc.Reconciler.LogLevel == "" {
		dc.Reconciler.LogLevel = "info"
	}

	// Roles — position by declaration order if not explicit
	for i, r := range cfg.Roles {
		rc := RoleConfig{
			Name:        r.Name,
			Color:       r.Color,
			Permissions: r.Permissions,
			Hoist:       r.Hoist,
			Mentionable: r.Mentionable,
			Position:    r.Position,
			ManagedBy:   "cog",
		}
		if rc.Position == 0 && i > 0 {
			rc.Position = i
		}
		if rc.Permissions == nil {
			rc.Permissions = []string{}
		}
		dc.Guild.Roles = append(dc.Guild.Roles, rc)
	}

	// Categories — position by declaration order
	for catIdx, cat := range cfg.Categories {
		catPerms := convertHCLPermissions(cat.Permissions)

		cc := CategoryConfig{
			Name:                 cat.Name,
			Position:             catIdx,
			ManagedBy:            "cog",
			PermissionOverwrites: catPerms,
		}

		// Channels — position by declaration order within category
		for chIdx, ch := range cat.Channels {
			chType := ch.Type
			if chType == "" {
				chType = "text"
			}

			// Permission inheritance: if channel has no permission blocks, inherit from category
			var chPerms []PermOverwriteConf
			if len(ch.Permissions) > 0 {
				chPerms = convertHCLPermissions(ch.Permissions)
			} else {
				chPerms = copyPermissions(catPerms)
			}

			chc := ChannelConfig{
				Name:                 ch.Name,
				Type:                 chType,
				Topic:                ch.Topic,
				Position:             chIdx,
				Slowmode:             ch.Slowmode,
				NSFW:                 ch.NSFW,
				ManagedBy:            "cog",
				PermissionOverwrites: chPerms,
				DefaultSort:          ch.DefaultSort,
				DefaultLayout:        ch.DefaultLayout,
			}
			for _, t := range ch.Tags {
				chc.Tags = append(chc.Tags, TagConfig{Name: t.Name, Emoji: t.Emoji, Moderated: t.Moderated})
			}
			cc.Channels = append(cc.Channels, chc)
		}

		dc.Guild.Categories = append(dc.Guild.Categories, cc)
	}

	return dc
}

func convertHCLPermissions(perms []hclPermission) []PermOverwriteConf {
	var result []PermOverwriteConf
	for _, p := range perms {
		poc := PermOverwriteConf{
			Allow: p.Allow,
			Deny:  p.Deny,
		}
		if poc.Allow == nil {
			poc.Allow = []string{}
		}
		if poc.Deny == nil {
			poc.Deny = []string{}
		}

		if p.Role != "" {
			poc.TargetType = "role"
			poc.Target = p.Role
		} else if p.Member != "" {
			poc.TargetType = "member"
			poc.Target = p.Member
		}
		result = append(result, poc)
	}
	return result
}

func copyPermissions(perms []PermOverwriteConf) []PermOverwriteConf {
	if len(perms) == 0 {
		return []PermOverwriteConf{}
	}
	cp := make([]PermOverwriteConf, len(perms))
	for i, p := range perms {
		cp[i] = PermOverwriteConf{
			TargetType: p.TargetType,
			Target:     p.Target,
			Allow:      append([]string{}, p.Allow...),
			Deny:       append([]string{}, p.Deny...),
		}
	}
	return cp
}

// ─── HCL emission (used by tests) ──────────────────────────────────────────

func discordConfigToHCL(cfg *DiscordServerConfig) string {
	var b strings.Builder

	// Variable for guild_id
	b.WriteString(fmt.Sprintf("variable \"guild_id\" {\n  default = %q\n}\n\n", cfg.Guild.ID))

	// Detect repeated permission patterns for locals
	type permKey struct {
		target string
		allow  string
		deny   string
	}
	permCounts := map[permKey]int{}
	for _, cat := range cfg.Guild.Categories {
		for _, p := range cat.PermissionOverwrites {
			pk := permKey{p.Target, strings.Join(p.Allow, ","), strings.Join(p.Deny, ",")}
			permCounts[pk]++
		}
	}

	// Extract permission locals for patterns used 2+ times
	type localDef struct {
		name  string
		perms []string
	}
	permLocals := map[string]string{}    // "deny:@everyone:VIEW_CHANNEL" → local name
	var localDefs []localDef

	for pk, count := range permCounts {
		if count < 2 || len(pk.deny) == 0 {
			continue
		}
		// Generate a local name based on target and permissions
		safeName := strings.ReplaceAll(pk.target, "@", "")
		safeName = strings.ReplaceAll(safeName, " ", "_")
		localName := safeName + "_deny"

		key := "deny:" + pk.target + ":" + pk.deny
		if _, exists := permLocals[key]; !exists {
			permLocals[key] = localName
			localDefs = append(localDefs, localDef{name: localName, perms: strings.Split(pk.deny, ",")})
		}
	}

	// Sort localDefs for determinism
	sort.Slice(localDefs, func(i, j int) bool { return localDefs[i].name < localDefs[j].name })

	if len(localDefs) > 0 {
		b.WriteString("locals {\n")
		for _, ld := range localDefs {
			b.WriteString(fmt.Sprintf("  %s = [", ld.name))
			for i, p := range ld.perms {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(fmt.Sprintf("%q", p))
			}
			b.WriteString("]\n")
		}
		b.WriteString("}\n\n")
	}

	// Guild block
	b.WriteString("guild {\n")
	b.WriteString("  id   = var.guild_id\n")
	b.WriteString(fmt.Sprintf("  name = %q\n", cfg.Guild.Name))
	if cfg.Guild.Description != "" {
		b.WriteString(fmt.Sprintf("  description = %q\n", cfg.Guild.Description))
	}
	b.WriteString("}\n\n")

	// Reconciler block
	b.WriteString("reconciler {\n")
	b.WriteString(fmt.Sprintf("  dry_run              = %v\n", cfg.Reconciler.DryRun))
	b.WriteString(fmt.Sprintf("  prune_unmanaged      = %v\n", cfg.Reconciler.PruneUnmanaged))
	b.WriteString(fmt.Sprintf("  respect_user_managed = %v\n", cfg.Reconciler.RespectUserManaged))
	b.WriteString(fmt.Sprintf("  max_api_calls        = %d\n", cfg.Reconciler.MaxAPICalls))
	b.WriteString("}\n\n")

	// Roles
	for _, r := range cfg.Guild.Roles {
		b.WriteString(fmt.Sprintf("role %q {\n", r.Name))
		if r.Color != "" && r.Color != "000000" {
			b.WriteString(fmt.Sprintf("  color       = %q\n", r.Color))
		}
		if len(r.Permissions) > 0 {
			b.WriteString("  permissions = [\n")
			for _, p := range r.Permissions {
				b.WriteString(fmt.Sprintf("    %q,\n", p))
			}
			b.WriteString("  ]\n")
		} else {
			b.WriteString("  permissions = []\n")
		}
		if r.Hoist {
			b.WriteString("  hoist       = true\n")
		}
		if r.Mentionable {
			b.WriteString("  mentionable = true\n")
		}
		if r.Position > 0 {
			b.WriteString(fmt.Sprintf("  position    = %d\n", r.Position))
		}
		b.WriteString("}\n\n")
	}

	// Categories
	for _, cat := range cfg.Guild.Categories {
		b.WriteString(fmt.Sprintf("category %q {\n", cat.Name))

		// Write category permissions
		for _, p := range cat.PermissionOverwrites {
			writeHCLPermission(&b, p, permLocals, "  ")
		}

		// Write channels, omitting defaults and inherited permissions
		for _, ch := range cat.Channels {
			chHasExplicitPerms := !permsEqual(ch.PermissionOverwrites, cat.PermissionOverwrites)

			// Determine if we need a block body at all
			needsBody := ch.Type != "text" || ch.Topic != "" || ch.Slowmode != 0 || ch.NSFW || chHasExplicitPerms ||
				len(ch.Tags) > 0 || ch.DefaultSort != "" || ch.DefaultLayout != ""

			if !needsBody {
				b.WriteString(fmt.Sprintf("  channel %q {}\n", ch.Name))
			} else {
				b.WriteString(fmt.Sprintf("  channel %q {\n", ch.Name))
				if ch.Type != "text" {
					b.WriteString(fmt.Sprintf("    type = %q\n", ch.Type))
				}
				if ch.Topic != "" {
					b.WriteString(fmt.Sprintf("    topic = %q\n", ch.Topic))
				}
				if ch.Slowmode != 0 {
					b.WriteString(fmt.Sprintf("    slowmode = %d\n", ch.Slowmode))
				}
				if ch.NSFW {
					b.WriteString("    nsfw = true\n")
				}
				if ch.DefaultSort != "" {
					b.WriteString(fmt.Sprintf("    default_sort = %q\n", ch.DefaultSort))
				}
				if ch.DefaultLayout != "" {
					b.WriteString(fmt.Sprintf("    default_layout = %q\n", ch.DefaultLayout))
				}
				for _, t := range ch.Tags {
					b.WriteString(fmt.Sprintf("    tag %q {\n", t.Name))
					if t.Emoji != "" {
						b.WriteString(fmt.Sprintf("      emoji = %q\n", t.Emoji))
					}
					if t.Moderated {
						b.WriteString("      moderated = true\n")
					}
					b.WriteString("    }\n")
				}
				if chHasExplicitPerms {
					for _, p := range ch.PermissionOverwrites {
						writeHCLPermission(&b, p, permLocals, "    ")
					}
				}
				b.WriteString("  }\n")
			}
		}

		b.WriteString("}\n\n")
	}

	return b.String()
}

func writeHCLPermission(b *strings.Builder, p PermOverwriteConf, permLocals map[string]string, indent string) {
	b.WriteString(indent + "permission {\n")
	if p.TargetType == "role" {
		b.WriteString(fmt.Sprintf("%s  role = %q\n", indent, p.Target))
	} else {
		b.WriteString(fmt.Sprintf("%s  member = %q\n", indent, p.Target))
	}

	if len(p.Allow) > 0 {
		b.WriteString(indent + "  allow = [")
		for i, a := range p.Allow {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(fmt.Sprintf("%q", a))
		}
		b.WriteString("]\n")
	}

	if len(p.Deny) > 0 {
		// Check if there's a local for this
		key := "deny:" + p.Target + ":" + strings.Join(p.Deny, ",")
		if localName, ok := permLocals[key]; ok {
			b.WriteString(fmt.Sprintf("%s  deny  = local.%s\n", indent, localName))
		} else {
			b.WriteString(indent + "  deny  = [")
			for i, d := range p.Deny {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(fmt.Sprintf("%q", d))
			}
			b.WriteString("]\n")
		}
	}

	b.WriteString(indent + "}\n")
}

func permsEqual(a, b []PermOverwriteConf) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].TargetType != b[i].TargetType || a[i].Target != b[i].Target {
			return false
		}
		if strings.Join(a[i].Allow, ",") != strings.Join(b[i].Allow, ",") {
			return false
		}
		if strings.Join(a[i].Deny, ",") != strings.Join(b[i].Deny, ",") {
			return false
		}
	}
	return true
}
