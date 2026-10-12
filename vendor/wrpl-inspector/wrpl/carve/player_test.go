package carve

import (
	packetslot "github.com/maxsupermanhd/wrpl-inspector/v3/wrpl/packet/parser/slot"
	"testing"
)

func TestResultsDoNotDependOnSlots(t *testing.T) {
	r := map[string]any{"player": map[string]any{"userId": "123", "name": "left-player", "team": int64(2), "kills": int64(0), "score": int64(312)}, "matchingInfo": map[string]any{"123": map[string]any{"crafts_info": map[string]any{"array0": map[string]any{"name": "f_14d"}}}}}
	p := PlayersFromResults(r)
	if len(p) != 1 || p[0].PlayerID != 123 || p[0].Team != 2 || p[0].HasSlot || p[0].Crafts["0"] != "f_14d" {
		t.Fatalf("incorrect result-only player: %+v", p)
	}
	if p[0].Kills != 0 || p[0].Score != 312 || p[0].Deaths != -1 {
		t.Fatal("missing stats confused with zero")
	}
}

func TestSlotPlayerWithoutResultsHasUnknownStats(t *testing.T) {
	var slots [256]*packetslot.Player
	slots[4] = &packetslot.Player{}
	slots[4].Uid.Player_id = 123
	slots[4].Uid.Name = "slot-player"
	p, err := assemblePlayers(nil, slots)
	if err != nil || len(p) != 1 || !p[0].HasSlot || p[0].Slot != 4 || p[0].Kills != -1 || p[0].Score != -1 {
		t.Fatalf("incorrect missing-result stats: %+v %v", p, err)
	}
}

func TestPlayerWithUnavailableUIDIsRetained(t *testing.T) {
	p := PlayersFromResults(map[string]any{"player": map[string]any{"name": "unknown-id", "team": int64(2), "score": int64(10)}})
	if len(p) != 1 || p[0].PlayerID != 0 || p[0].Name != "unknown-id" || p[0].Team != 2 || p[0].Score != 10 {
		t.Fatalf("lost unidentified result: %+v", p)
	}
}
