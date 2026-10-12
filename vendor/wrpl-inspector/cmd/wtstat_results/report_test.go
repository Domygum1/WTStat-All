package main

import (
	"github.com/maxsupermanhd/wrpl-inspector/v3/wrpl"
	"github.com/maxsupermanhd/wrpl-inspector/v3/wrpl/carve"
	"os"
	"path/filepath"
	"testing"
)

func TestReportNullsAndExactVehicleMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vehicles.csv")
	if err := os.WriteFile(path, []byte("vehicle_id,name,research_country_code,operator_countries_json,source_url\nf_14d,F-14D,usa,[],https://wiki.warthunder.com/unit/f_14d\n"), 0600); err != nil {
		t.Fatal(err)
	}
	r := map[string]any{"status": "success", "timePlayed": float64(123.5), "player": []any{map[string]any{"userId": "123", "name": "test", "team": int64(1), "kills": int64(0)}}, "matchingInfo": map[string]any{"123": map[string]any{"crafts_info": map[string]any{"array0": map[string]any{"name": "f_14d"}, "array1": map[string]any{"name": "f14d"}}}}}
	m := matchReport(wrpl.WRPLHeader{StartTime: 1}, r, carve.PlayersFromResults(r), nil, path)
	p := m["teams"].([]any)[0].(map[string]any)["players"].([]any)[0].(map[string]any)
	v := p["vehicles"].([]any)
	if v[0].(map[string]any)["nation"] != "usa" || v[1].(map[string]any)["nation"] != nil {
		t.Fatal("vehicle mapping must be exact")
	}
	s := p["statistics"].(map[string]any)
	if s["kills"] != 0 || s["deaths"] != nil || p["slot"] != nil || p["actual_vehicles"] != nil {
		t.Fatal("unknown data must be null")
	}
	if m["start_time_utc"] != "1970-01-01T00:00:01Z" || m["duration_seconds"] != 123.5 {
		t.Fatal("time/duration mismatch")
	}
	delete(r, "status")
	m = matchReport(wrpl.WRPLHeader{}, r, carve.PlayersFromResults(r), nil, path)
	p = m["teams"].([]any)[0].(map[string]any)["players"].([]any)[0].(map[string]any)
	if m["duration_seconds"] != nil || p["statistics"].(map[string]any)["kills"] != nil {
		t.Fatal("checkpoint stats reported as final")
	}
}
