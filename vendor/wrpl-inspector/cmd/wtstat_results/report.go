package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/maxsupermanhd/wrpl-inspector/v3/wrpl"
	"github.com/maxsupermanhd/wrpl-inspector/v3/wrpl/carve"
)

type vehicleInfo struct {
	Name      string
	Nation    string
	Operators []string
	Source    string
}

// Exact internal IDs only. The local catalog records the official Wiki source.
func readVehicleCatalog(path string) (map[string]vehicleInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	head, err := r.Read()
	if err != nil {
		return nil, err
	}
	cols := map[string]int{}
	for i, name := range head {
		cols[strings.TrimPrefix(name, "\ufeff")] = i
	}
	for _, key := range []string{"vehicle_id", "name", "research_country_code", "operator_countries_json", "source_url"} {
		if _, ok := cols[key]; !ok {
			return nil, fmt.Errorf("vehicle catalog missing column %s", key)
		}
	}
	ret := map[string]vehicleInfo{}
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		get := func(key string) string { return row[cols[key]] }
		id := get("vehicle_id")
		if _, exists := ret[id]; exists {
			return nil, fmt.Errorf("duplicate catalog vehicle ID %q", id)
		}
		v := vehicleInfo{Name: get("name"), Nation: get("research_country_code"), Source: get("source_url")}
		if raw := get("operator_countries_json"); raw != "" {
			if err := json.Unmarshal([]byte(raw), &v.Operators); err != nil {
				return nil, fmt.Errorf("vehicle %s operators: %w", id, err)
			}
		}
		ret[id] = v
	}
	return ret, nil
}

func projectPath(parts ...string) string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	return filepath.Join(append([]string{filepath.Dir(exe), ".."}, parts...)...)
}

func nullableText(s string) any {
	s = strings.TrimRight(s, "\x00")
	if s == "" {
		return nil
	}
	return s
}

func nullableStat(n int) any {
	if n < 0 {
		return nil
	}
	return n
}

func mapName(level string) any {
	b, err := os.ReadFile(projectPath("vendor", "WT-Plotter", "resources", "translations", "locations.json"))
	if err != nil {
		return nil
	}
	var locations []struct {
		Identifier string `json:"identifier"`
		English    string `json:"en"`
	}
	if json.Unmarshal(b, &locations) != nil {
		return nil
	}
	id := strings.TrimSuffix(filepath.Base(strings.TrimRight(level, "\x00")), ".bin")
	for _, location := range locations {
		if location.Identifier == id {
			return nullableText(location.English)
		}
	}
	return nil
}

// Added alongside the original header/results and carve fields, so WTStat's
// existing consumers keep their wire format. All new unknown values are null.
func matchReport(h wrpl.WRPLHeader, results map[string]any, players []carve.SessionPlayer, errors []string, catalogPath string) map[string]any {
	errors = append([]string{}, errors...)
	catalog, err := readVehicleCatalog(catalogPath)
	if err != nil {
		errors = append(errors, fmt.Sprintf("vehicle catalog unavailable: %v", err))
	}
	var started, duration any
	if h.StartTime != 0 {
		started = time.Unix(int64(h.StartTime), 0).UTC().Format(time.RFC3339)
	}
	switch v := results["timePlayed"].(type) {
	case float64:
		if v >= 0 {
			duration = v
		}
	case int64:
		if v >= 0 {
			duration = v
		}
	}
	status, _ := results["status"].(string)
	// A checkpoint BLK (e.g. part 1) has no final status and must not be
	// presented as end-of-match statistics.
	final := status == "success"
	if !final {
		errors = append(errors, "final results BLK unavailable; duration and final statistics are null")
		duration = nil
	}
	groups := map[int][]any{}
	unknown := []any{}
	missingCatalog := map[string]bool{}
	for _, p := range players {
		var uid, team, slot any
		if p.PlayerID != 0 {
			uid = strconv.FormatUint(p.PlayerID, 10)
		}
		if p.Team != 0 {
			team = int(p.Team)
		}
		if p.HasSlot {
			slot = int(p.Slot)
		}
		var vehicles []any
		if p.Crafts != nil {
			vehicles = []any{}
		}
		for _, key := range slices.Sorted(maps.Keys(p.Crafts)) {
			code := p.Crafts[key]
			v, found := catalog[code]
			if !found {
				missingCatalog[code] = true
			}
			vehicles = append(vehicles, map[string]any{
				"id": code, "name": nullableText(v.Name), "nation": nullableText(v.Nation),
				"operator_nations": v.Operators, "mapping_source": nullableText(v.Source),
				"source": "results.matchingInfo.crafts_info", "spawned": nil,
			})
		}
		stats := map[string]any{
			"kills": nullableStat(p.Kills), "ground_kills": nullableStat(p.GroundKills), "naval_kills": nullableStat(p.NavalKills),
			"deaths": nullableStat(p.Deaths), "assists": nullableStat(p.Assists), "score": nullableStat(p.Score),
			"ai_kills": nullableStat(p.AiKills), "ai_ground_kills": nullableStat(p.AiGroundKills), "ai_naval_kills": nullableStat(p.AiNavalKills),
			"team_kills": nullableStat(p.TeamKills), "capture_zones": nullableStat(p.CaptureZone),
		}
		if !final {
			for k := range stats {
				stats[k] = nil
			}
		}
		player := map[string]any{"uid": uid, "name": nullableText(p.Name), "team_id": team, "slot": slot,
			"vehicles": vehicles, "actual_vehicles": nil, "statistics": stats}
		if team == nil {
			unknown = append(unknown, player)
		} else {
			groups[int(p.Team)] = append(groups[int(p.Team)], player)
		}
	}
	if len(missingCatalog) > 0 {
		errors = append(errors, "vehicle IDs missing from local catalog: "+strings.Join(slices.Sorted(maps.Keys(missingCatalog)), ", "))
	}
	teams := []any{}
	for _, id := range slices.Sorted(maps.Keys(groups)) {
		teams = append(teams, map[string]any{"team_id": id, "players": groups[id]})
	}
	return map[string]any{
		"schema_version": 1, "session_id": h.SessionHEX(), "start_time_utc": started,
		"map":              map[string]any{"name": mapName(string(h.Raw_Level[:])), "level": nullableText(string(h.Raw_Level[:])), "location": nullableText(string(h.Raw_LocName[:]))},
		"duration_seconds": duration, "replay_version": fmt.Sprintf("0x%X", h.Version),
		"final_results_available": final, "statistics_source": "results BLK",
		"teams": teams, "unassigned_players": unknown, "errors": errors,
		"unsupported_fields": []string{"players.actual_vehicles", "players.vehicles.spawned", "battle_log.assist_events"},
	}
}
