package discord

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Forum channel (type 15) fields: tags, default sort, default layout.
//
// Config semantics: an absent `tags` key (nil slice) leaves the live tag set
// unmanaged; an explicit empty list clears it. Likewise an empty
// default_sort / default_layout is unmanaged. This keeps hand-written forum
// configs from stripping live tags and keeps snapshot -> plan at zero diffs.

const maxForumTags = 20
const maxForumTagNameLen = 20

// TagConfig is one forum tag in the desired config. Emoji is a unicode emoji,
// or "name:id" for a custom guild emoji (the id is what Discord needs).
type TagConfig struct {
	Name      string `yaml:"name" json:"name"`
	Emoji     string `yaml:"emoji,omitempty" json:"emoji,omitempty"`
	Moderated bool   `yaml:"moderated,omitempty" json:"moderated,omitempty"`
}

// DiscordForumTag is a forum tag as Discord returns it.
type DiscordForumTag struct {
	ID        string  `json:"id,omitempty"`
	Name      string  `json:"name"`
	Moderated bool    `json:"moderated"`
	EmojiID   *string `json:"emoji_id"`
	EmojiName *string `json:"emoji_name"`
}

// DiscordDefaultReaction is a forum's default_reaction_emoji.
type DiscordDefaultReaction struct {
	EmojiID   *string `json:"emoji_id"`
	EmojiName *string `json:"emoji_name"`
}

var forumSortToInt = map[string]int{"latest_activity": 0, "creation_date": 1}
var forumSortToString = map[int]string{0: "latest_activity", 1: "creation_date"}

// default_forum_layout 0 means "not set"; it exports as empty (unmanaged).
var forumLayoutToInt = map[string]int{"list": 1, "gallery": 2}
var forumLayoutToString = map[int]string{1: "list", 2: "gallery"}

// validateChannelForum rejects forum-only fields on the wrong channel type and
// out-of-range values, so a bad config fails at plan time and never reaches
// the API.
func validateChannelForum(catName string, ch ChannelConfig) error {
	where := fmt.Sprintf("category %q channel %q", catName, ch.Name)
	isForum := ch.Type == "forum"
	if ch.Tags != nil && !isForum {
		return fmt.Errorf("%s: tags are only valid on type: forum (got %q)", where, ch.Type)
	}
	if ch.DefaultSort != "" {
		if !isForum {
			return fmt.Errorf("%s: default_sort is only valid on type: forum (got %q)", where, ch.Type)
		}
		if _, ok := forumSortToInt[ch.DefaultSort]; !ok {
			return fmt.Errorf("%s: default_sort %q must be latest_activity or creation_date", where, ch.DefaultSort)
		}
	}
	if ch.DefaultLayout != "" {
		if !isForum {
			return fmt.Errorf("%s: default_layout is only valid on type: forum (got %q)", where, ch.Type)
		}
		if _, ok := forumLayoutToInt[ch.DefaultLayout]; !ok {
			return fmt.Errorf("%s: default_layout %q must be list or gallery", where, ch.DefaultLayout)
		}
	}
	if len(ch.Tags) > maxForumTags {
		return fmt.Errorf("%s: %d tags exceeds Discord's limit of %d", where, len(ch.Tags), maxForumTags)
	}
	seen := map[string]bool{}
	for _, t := range ch.Tags {
		if t.Name == "" {
			return fmt.Errorf("%s: tag with empty name", where)
		}
		if n := utf8.RuneCountInString(t.Name); n > maxForumTagNameLen {
			return fmt.Errorf("%s: tag %q is %d characters, limit is %d", where, t.Name, n, maxForumTagNameLen)
		}
		if seen[t.Name] {
			return fmt.Errorf("%s: duplicate tag %q", where, t.Name)
		}
		seen[t.Name] = true
	}
	return nil
}

// validateConfig runs per-channel validation across the whole config.
func validateConfig(cfg *DiscordServerConfig) error {
	for _, cat := range cfg.Guild.Categories {
		for _, ch := range cat.Channels {
			if err := validateChannelForum(cat.Name, ch); err != nil {
				return err
			}
		}
	}
	return nil
}

// emojiParts splits a tag emoji string into Discord's (emoji_id, emoji_name).
func emojiParts(emoji string) (id, name *string) {
	if emoji == "" {
		return nil, nil
	}
	if i := strings.LastIndex(emoji, ":"); i > 0 && i < len(emoji)-1 {
		name, id := emoji[:i], emoji[i+1:]
		return &id, &name
	}
	return nil, &emoji
}

// emojiString is the inverse of emojiParts.
func emojiString(id, name *string) string {
	switch {
	case id != nil && *id != "":
		n := ""
		if name != nil {
			n = *name
		}
		return n + ":" + *id
	case name != nil:
		return *name
	}
	return ""
}

// liveTagEmoji is the live tag's emoji in config form.
func liveTagEmoji(t DiscordForumTag) string { return emojiString(t.EmojiID, t.EmojiName) }

// tagsFromLive converts live tags to config form (order preserved).
func tagsFromLive(live []DiscordForumTag) []TagConfig {
	if len(live) == 0 {
		return nil
	}
	out := make([]TagConfig, len(live))
	for i, t := range live {
		out[i] = TagConfig{Name: t.Name, Emoji: liveTagEmoji(t), Moderated: t.Moderated}
	}
	return out
}

// diffTags compares the tag set by name and returns a single diff line such
// as `tags: +needs-decision -wontfix ~running(emoji)`, or "" when equal.
// Order is not compared. A nil desired list is unmanaged.
func diffTags(desired []TagConfig, live []DiscordForumTag) string {
	if desired == nil {
		return ""
	}
	liveByName := make(map[string]DiscordForumTag, len(live))
	for _, t := range live {
		liveByName[t.Name] = t
	}
	desiredNames := make(map[string]bool, len(desired))
	var added, removed, changed []string
	for _, d := range desired {
		desiredNames[d.Name] = true
		l, ok := liveByName[d.Name]
		if !ok {
			added = append(added, "+"+d.Name)
			continue
		}
		var what []string
		if d.Emoji != liveTagEmoji(l) {
			what = append(what, "emoji")
		}
		if d.Moderated != l.Moderated {
			what = append(what, "moderated")
		}
		if len(what) > 0 {
			changed = append(changed, fmt.Sprintf("~%s(%s)", d.Name, strings.Join(what, ",")))
		}
	}
	for _, l := range live {
		if !desiredNames[l.Name] {
			removed = append(removed, "-"+l.Name)
		}
	}
	parts := append(append(added, removed...), changed...)
	if len(parts) == 0 {
		return ""
	}
	return "tags: " + strings.Join(parts, " ")
}

// diffForum returns the forum-only diff lines for a channel.
func diffForum(desired ChannelConfig, live DiscordChannel) []string {
	var diffs []string
	if d := diffTags(desired.Tags, live.AvailableTags); d != "" {
		diffs = append(diffs, d)
	}
	if desired.DefaultSort != "" {
		liveSort := ""
		if live.DefaultSortOrder != nil {
			liveSort = forumSortToString[*live.DefaultSortOrder]
		}
		if desired.DefaultSort != liveSort {
			diffs = append(diffs, fmt.Sprintf("default_sort: %s -> %s", liveSort, desired.DefaultSort))
		}
	}
	if desired.DefaultLayout != "" {
		liveLayout := ""
		if live.DefaultForumLayout != nil {
			liveLayout = forumLayoutToString[*live.DefaultForumLayout]
		}
		if desired.DefaultLayout != liveLayout {
			diffs = append(diffs, fmt.Sprintf("default_layout: %s -> %s", liveLayout, desired.DefaultLayout))
		}
	}
	return diffs
}

// forumDetails puts the desired forum fields into plan-action details. Tags
// are stored as []TagConfig; readers go through detailTags so a JSON round
// trip of the plan ([]any of maps) still works.
func forumDetails(d map[string]any, ch ChannelConfig) {
	if ch.Tags != nil {
		d["tags"] = ch.Tags
	}
	if ch.DefaultSort != "" {
		d["default_sort"] = ch.DefaultSort
	}
	if ch.DefaultLayout != "" {
		d["default_layout"] = ch.DefaultLayout
	}
}

// detailTags decodes Details["tags"]; ok is false when the key is absent.
func detailTags(details map[string]any) ([]TagConfig, bool) {
	raw, present := details["tags"]
	if !present {
		return nil, false
	}
	if tags, ok := raw.([]TagConfig); ok {
		return tags, true
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, false
	}
	tags := []TagConfig{}
	if err := json.Unmarshal(data, &tags); err != nil {
		return nil, false
	}
	return tags, true
}

// buildAvailableTags renders the REPLACE-ALL available_tags payload: the full
// desired list, with the live id for tags matched by name and no id for new
// ones.
func buildAvailableTags(desired []TagConfig, live []DiscordForumTag) []map[string]any {
	idByName := make(map[string]string, len(live))
	for _, t := range live {
		idByName[t.Name] = t.ID
	}
	out := make([]map[string]any, 0, len(desired))
	for _, d := range desired {
		t := map[string]any{"name": d.Name, "moderated": d.Moderated}
		if id, ok := idByName[d.Name]; ok {
			t["id"] = id
		}
		emojiID, emojiName := emojiParts(d.Emoji)
		if emojiID != nil {
			t["emoji_id"] = *emojiID
		}
		if emojiName != nil {
			t["emoji_name"] = *emojiName
		}
		out = append(out, t)
	}
	return out
}

// forumPayload adds the forum fields of a plan action to a create/update
// payload. liveTags are the channel's current tags (nil on create).
func forumPayload(payload map[string]any, details map[string]any, liveTags []DiscordForumTag) {
	if tags, ok := detailTags(details); ok {
		payload["available_tags"] = buildAvailableTags(tags, liveTags)
	}
	if s, ok := details["default_sort"].(string); ok && s != "" {
		payload["default_sort_order"] = forumSortToInt[s]
	}
	if l, ok := details["default_layout"].(string); ok && l != "" {
		payload["default_forum_layout"] = forumLayoutToInt[l]
	}
}

// tagIDsByName maps tag name -> Discord id for a channel's tags.
func tagIDsByName(tags []DiscordForumTag) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Name] = t.ID
	}
	return m
}

func liveForumSort(ch DiscordChannel) string {
	if ch.DefaultSortOrder == nil {
		return ""
	}
	return forumSortToString[*ch.DefaultSortOrder]
}

func liveForumLayout(ch DiscordChannel) string {
	if ch.DefaultForumLayout == nil {
		return ""
	}
	return forumLayoutToString[*ch.DefaultForumLayout]
}

// discordStateMetadata is the generic-state metadata for a DiscordState. Tag
// ids travel here because reconcile.State has no per-provider fields.
func discordStateMetadata(ds *DiscordState) map[string]any {
	md := map[string]any{"guild_id": ds.GuildID}
	if len(ds.TagIDs) > 0 {
		md["tag_ids"] = ds.TagIDs
	}
	return md
}

// tagIDsFromMetadata is the inverse of discordStateMetadata; it also accepts
// the []any/map[string]any shape a JSON round trip produces.
func tagIDsFromMetadata(md map[string]any) map[string]map[string]string {
	raw, ok := md["tag_ids"]
	if !ok {
		return nil
	}
	if m, ok := raw.(map[string]map[string]string); ok {
		return m
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var m map[string]map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return m
}
