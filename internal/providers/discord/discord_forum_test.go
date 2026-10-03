package discord

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

const forumJSON = `{
  "id": "900", "type": 15, "guild_id": "1", "position": 0, "name": "board", "topic": "",
  "parent_id": "100", "nsfw": false, "rate_limit_per_user": 0,
  "available_tags": [
    {"id": "t1", "name": "running", "moderated": false, "emoji_id": null, "emoji_name": "🟢"},
    {"id": "t2", "name": "wontfix", "moderated": true, "emoji_id": null, "emoji_name": null},
    {"id": "t3", "name": "blocked", "moderated": false, "emoji_id": "555", "emoji_name": "stop"}
  ],
  "default_reaction_emoji": {"emoji_id": null, "emoji_name": "👍"},
  "default_sort_order": 1,
  "default_forum_layout": 2
}`

func liveForum(t *testing.T) DiscordChannel {
	t.Helper()
	var ch DiscordChannel
	if err := json.Unmarshal([]byte(forumJSON), &ch); err != nil {
		t.Fatal(err)
	}
	return ch
}

func forumCfg(ch ChannelConfig) *DiscordServerConfig {
	return &DiscordServerConfig{Guild: GuildConfig{ID: "1", Categories: []CategoryConfig{
		{Name: "work", Channels: []ChannelConfig{ch}},
	}}}
}

func TestForumDecode(t *testing.T) {
	ch := liveForum(t)
	if len(ch.AvailableTags) != 3 {
		t.Fatalf("tags = %d, want 3", len(ch.AvailableTags))
	}
	if got := liveTagEmoji(ch.AvailableTags[0]); got != "🟢" {
		t.Errorf("unicode emoji = %q", got)
	}
	if got := liveTagEmoji(ch.AvailableTags[2]); got != "stop:555" {
		t.Errorf("custom emoji = %q", got)
	}
	if !ch.AvailableTags[1].Moderated || ch.AvailableTags[1].ID != "t2" {
		t.Errorf("tag 2 = %+v", ch.AvailableTags[1])
	}
	if ch.DefaultReactionEmoji == nil || *ch.DefaultReactionEmoji.EmojiName != "👍" {
		t.Errorf("default reaction = %+v", ch.DefaultReactionEmoji)
	}
	if liveForumSort(ch) != "creation_date" || liveForumLayout(ch) != "gallery" {
		t.Errorf("sort/layout = %s/%s", liveForumSort(ch), liveForumLayout(ch))
	}
	// Non-forum channels are unaffected.
	var text DiscordChannel
	json.Unmarshal([]byte(`{"id":"1","type":0,"name":"x"}`), &text)
	if text.AvailableTags != nil || text.DefaultSortOrder != nil || text.DefaultForumLayout != nil {
		t.Errorf("text channel grew forum fields: %+v", text)
	}
}

func TestForumTagDiff(t *testing.T) {
	live := liveForum(t)
	want := []TagConfig{
		{Name: "running", Emoji: "🔵"},        // emoji changed
		{Name: "blocked", Emoji: "stop:555"}, // unchanged
		{Name: "needs-decision"},             // added; wontfix removed
	}
	got := diffTags(want, live.AvailableTags)
	if got != "tags: +needs-decision -wontfix ~running(emoji)" {
		t.Errorf("diff = %q", got)
	}
	if d := diffTags([]TagConfig{{Name: "running", Emoji: "🟢", Moderated: true}}, live.AvailableTags[:1]); d != "tags: ~running(moderated)" {
		t.Errorf("moderated diff = %q", d)
	}
	// Order alone is not a change; nil is unmanaged.
	reordered := []TagConfig{{Name: "blocked", Emoji: "stop:555"}, {Name: "wontfix", Moderated: true}, {Name: "running", Emoji: "🟢"}}
	if d := diffTags(reordered, live.AvailableTags); d != "" {
		t.Errorf("reorder diff = %q", d)
	}
	if d := diffTags(nil, live.AvailableTags); d != "" {
		t.Errorf("nil desired diff = %q", d)
	}
	// Sort/layout are compared through diffChannel.
	ch := ChannelConfig{Name: "board", Type: "forum", DefaultSort: "latest_activity", DefaultLayout: "gallery"}
	parent := "100"
	live.ParentID = &parent
	diffs := diffChannel(ch, live, "100")
	if len(diffs) != 1 || diffs[0] != "default_sort: creation_date -> latest_activity" {
		t.Errorf("diffChannel = %v", diffs)
	}
}

func TestForumTooManyTagsIsPlanError(t *testing.T) {
	var tags []TagConfig
	for i := 0; i < 21; i++ {
		tags = append(tags, TagConfig{Name: strings.Repeat("a", i+1)})
	}
	cfg := forumCfg(ChannelConfig{Name: "board", Type: "forum", Tags: tags})
	if _, err := computePlan(cfg, nil, nil); err == nil || !strings.Contains(err.Error(), "limit of 20") {
		t.Fatalf("computePlan err = %v", err)
	}
	if _, err := computePlanWithState(cfg, nil, nil, &DiscordState{}); err == nil {
		t.Fatal("computePlanWithState accepted 21 tags")
	}
	cfg = forumCfg(ChannelConfig{Name: "board", Type: "forum", Tags: tags[:20]})
	if _, err := computePlan(cfg, nil, nil); err != nil {
		t.Fatalf("20 tags rejected: %v", err)
	}
}

func TestTagsOnTextChannelIsPlanError(t *testing.T) {
	cfg := forumCfg(ChannelConfig{Name: "chat", Type: "text", Tags: []TagConfig{{Name: "x"}}})
	if _, err := computePlan(cfg, nil, nil); err == nil || !strings.Contains(err.Error(), "only valid on type: forum") {
		t.Fatalf("err = %v", err)
	}
	cfg = forumCfg(ChannelConfig{Name: "chat", Type: "text", DefaultSort: "creation_date"})
	if _, err := computePlan(cfg, nil, nil); err == nil {
		t.Fatal("default_sort on text channel accepted")
	}
	cfg = forumCfg(ChannelConfig{Name: "board", Type: "forum", DefaultLayout: "grid"})
	if _, err := computePlan(cfg, nil, nil); err == nil {
		t.Fatal("bad default_layout accepted")
	}
}

func TestForumApplyPayloadPreservesIDs(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		data, _ := io.ReadAll(r.Body)
		json.Unmarshal(data, &gotBody)
		w.Write([]byte(`{"id":"900","type":15,"available_tags":[` +
			`{"id":"t1","name":"running","moderated":false,"emoji_id":null,"emoji_name":"🔵"},` +
			`{"id":"t9","name":"needs-decision","moderated":false,"emoji_id":null,"emoji_name":null}]}`))
	}))
	defer srv.Close()
	client := &discordClient{token: "x", httpClient: srv.Client(), baseURL: srv.URL}

	live := liveForum(t)
	parent := "100"
	live.ParentID = &parent
	cat := DiscordChannel{ID: "100", Type: 4, Name: "work"}
	cfg := forumCfg(ChannelConfig{Name: "board", Type: "forum", Tags: []TagConfig{
		{Name: "running", Emoji: "🔵"}, {Name: "needs-decision"},
	}})
	plan, err := computePlan(cfg, []DiscordChannel{cat, live}, nil)
	if err != nil {
		t.Fatal(err)
	}
	results, err := applyPlan(client, plan, "1", nil, []DiscordChannel{cat, live})
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != "PATCH" || gotPath != "/channels/900" {
		t.Fatalf("request = %s %s", gotMethod, gotPath)
	}
	tags, _ := gotBody["available_tags"].([]any)
	if len(tags) != 2 {
		t.Fatalf("available_tags = %v", gotBody["available_tags"])
	}
	running := tags[0].(map[string]any)
	if running["name"] != "running" || running["id"] != "t1" || running["emoji_name"] != "🔵" {
		t.Errorf("matched tag = %v", running)
	}
	added := tags[1].(map[string]any)
	if _, hasID := added["id"]; hasID || added["name"] != "needs-decision" {
		t.Errorf("new tag must omit id: %v", added)
	}
	if len(results) != 1 || results[0].Status != "succeeded" {
		t.Fatalf("results = %+v", results)
	}
	if results[0].TagIDs["needs-decision"] != "t9" || results[0].TagIDs["running"] != "t1" {
		t.Errorf("recorded tag ids = %v", results[0].TagIDs)
	}
}

func TestForumApplyCreateOmitsIDs(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		json.Unmarshal(data, &gotBody)
		w.Write([]byte(`{"id":"901","type":15,"available_tags":[{"id":"n1","name":"a"}]}`))
	}))
	defer srv.Close()
	client := &discordClient{token: "x", httpClient: srv.Client(), baseURL: srv.URL}
	plan := &Plan{Actions: []PlanAction{{Action: "create", ResourceType: "channel", Name: "board",
		Details: channelCreateDetails("work", ChannelConfig{Name: "board", Type: "forum",
			Tags: []TagConfig{{Name: "a"}}, DefaultSort: "creation_date", DefaultLayout: "list"})}}}
	results, _ := applyPlan(client, plan, "1", nil, nil)
	tags := gotBody["available_tags"].([]any)
	if _, hasID := tags[0].(map[string]any)["id"]; hasID {
		t.Error("create sent a tag id")
	}
	if gotBody["default_sort_order"] != float64(1) || gotBody["default_forum_layout"] != float64(1) {
		t.Errorf("payload = %v", gotBody)
	}
	if results[0].TagIDs["a"] != "n1" {
		t.Errorf("tag ids = %v", results[0].TagIDs)
	}
}

// Plans may be JSON round-tripped between plan and apply; tags must survive.
func TestForumDetailsSurviveJSON(t *testing.T) {
	d := channelCreateDetails("work", ChannelConfig{Type: "forum", Tags: []TagConfig{{Name: "a", Emoji: "x", Moderated: true}}})
	data, _ := json.Marshal(d)
	var back map[string]any
	json.Unmarshal(data, &back)
	tags, ok := detailTags(back)
	if !ok || len(tags) != 1 || tags[0] != (TagConfig{Name: "a", Emoji: "x", Moderated: true}) {
		t.Errorf("tags = %+v ok=%v", tags, ok)
	}
}

func TestForumRoundTripLiveToConfigToPlanIsEmpty(t *testing.T) {
	live := &DiscordLiveState{Channels: []DiscordChannel{
		{ID: "100", Type: 4, Name: "work"},
		func() DiscordChannel { c := liveForum(t); return c }(),
	}}
	cfg := buildDiscordConfigFromLive(live, "1", "g", "")
	ch := cfg.Guild.Categories[0].Channels[0]
	if len(ch.Tags) != 3 || ch.DefaultSort != "creation_date" || ch.DefaultLayout != "gallery" {
		t.Fatalf("exported channel = %+v", ch)
	}
	// Through YAML, as snapshot writes it.
	data, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var reread DiscordServerConfig
	if err := yaml.Unmarshal(data, &reread); err != nil {
		t.Fatal(err)
	}
	for _, withState := range []bool{false, true} {
		var plan *Plan
		if withState {
			st := buildStateFromLive("1", &reread, live.Channels, nil, nil)
			plan, err = computePlanWithState(&reread, live.Channels, nil, st)
		} else {
			plan, err = computePlan(&reread, live.Channels, nil)
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range plan.Actions {
			if a.Action != "skip" {
				t.Errorf("withState=%v: unexpected action %s %s %v", withState, a.Action, a.Name, a.Details)
			}
		}
	}
}

func TestForumHCLRoundTrip(t *testing.T) {
	yamlCfg := forumCfg(ChannelConfig{Name: "board", Type: "forum", DefaultSort: "creation_date", DefaultLayout: "list",
		Tags: []TagConfig{{Name: "running", Emoji: "🟢"}, {Name: "wontfix", Moderated: true}}})
	yamlCfg.Reconciler.MaxAPICalls = 60
	hclText := discordConfigToHCL(yamlCfg)
	path := filepath.Join(t.TempDir(), "server.hcl")
	if err := os.WriteFile(path, []byte(hclText), 0o644); err != nil {
		t.Fatal(err)
	}
	parsed, err := parseHCLConfig(path)
	if err != nil {
		t.Fatalf("parse generated HCL: %v\n%s", err, hclText)
	}
	got := parsed.Guild.Categories[0].Channels[0]
	want := yamlCfg.Guild.Categories[0].Channels[0]
	if got.DefaultSort != want.DefaultSort || got.DefaultLayout != want.DefaultLayout || len(got.Tags) != 2 ||
		got.Tags[0] != want.Tags[0] || got.Tags[1] != want.Tags[1] {
		t.Errorf("hcl round trip = %+v", got)
	}
}

func TestForumStateTagIDsRoundTrip(t *testing.T) {
	live := []DiscordChannel{{ID: "100", Type: 4, Name: "work"}, liveForum(t)}
	st := buildStateFromLive("1", nil, live, nil, nil)
	addr := channelAddress("work", "board")
	if st.TagIDs[addr]["running"] != "t1" || st.TagIDs[addr]["blocked"] != "t3" {
		t.Fatalf("tag ids = %v", st.TagIDs)
	}
	// File round trip.
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Dir(statePath(root)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeState(root, st); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadState(root)
	if err != nil || loaded.TagIDs[addr]["wontfix"] != "t2" {
		t.Fatalf("loaded = %+v err=%v", loaded, err)
	}
	// Generic reconcile.State round trip, including a JSON hop.
	generic := discordStateToReconcileState(st)
	data, _ := json.Marshal(generic)
	var hop reconcile.State
	if err := json.Unmarshal(data, &hop); err != nil {
		t.Fatal(err)
	}
	if back := reconcileStateToDiscordState(&hop); back.TagIDs[addr]["running"] != "t1" {
		t.Errorf("generic round trip tag ids = %v", back.TagIDs)
	}
}
