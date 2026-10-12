package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/maxsupermanhd/wrpl-inspector/v3/wrpl"
	"github.com/maxsupermanhd/wrpl-inspector/v3/wrpl/carve"
	packetecs2 "github.com/maxsupermanhd/wrpl-inspector/v3/wrpl/packet/parser/ecs2"
	"os"
	"path/filepath"
)

func main() {
	if len(os.Args) == 4 && os.Args[1] == "--carve" {
		carveDir(os.Args[2], os.Args[3], "carve", projectPath("data", "wiki_vehicles.csv"))
		return
	}
	if (len(os.Args) == 3 || len(os.Args) == 4) && os.Args[1] == "--json" {
		catalog := projectPath("data", "wiki_vehicles.csv")
		if len(os.Args) == 4 {
			catalog = os.Args[3]
		}
		carveDir(os.Args[2], projectPath("vendor", "wrpl-inspector", "data", "ecshashes.json"), "json", catalog)
		return
	}
	if len(os.Args) == 3 && os.Args[1] == "--battle-log" {
		carveDir(os.Args[2], projectPath("vendor", "wrpl-inspector", "data", "ecshashes.json"), "battle-log", projectPath("data", "wiki_vehicles.csv"))
		return
	}
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: wtstat_results FILE... | --carve DIR HASHES | --json DIR [VEHICLE_CSV] | --battle-log DIR")
		os.Exit(2)
	}
	data := map[string]any{}
	var header *wrpl.WRPLHeader
	var results map[string]any
	resultsPart := -1
	for _, path := range os.Args[1:] {
		f, err := os.Open(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		r, err := wrpl.OpenReplay(f, true, false, true)
		f.Close()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if header != nil && header.SessionID != r.Header.SessionID {
			fail(fmt.Errorf("files belong to different replay sessions"))
		}
		if header == nil || r.Header.ReplayPartNumber < header.ReplayPartNumber {
			h := r.Header
			header = &h
		}
		if _, ok := data["header"]; !ok {
			data["header"] = map[string]any{
				"session_id": r.Header.SessionHEX(), "start_time": r.Header.StartTime,
				"level": string(r.Header.Raw_Level[:]), "location": string(r.Header.Raw_LocName[:]),
				"version": r.Header.Version,
			}
		}
		if len(r.Results) > 0 && int(r.Header.ReplayPartNumber) >= resultsPart {
			v, err := wrpl.ParseBlk(r.Results)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			data["results"] = v
			results, resultsPart = v, int(r.Header.ReplayPartNumber)
		}
	}
	data["match"] = matchReport(*header, results, carve.PlayersFromResults(results), nil, projectPath("data", "wiki_vehicles.csv"))
	if err := json.NewEncoder(os.Stdout).Encode(data); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func carveDir(dir, hashes, format, catalogPath string) {
	b, err := os.ReadFile(hashes)
	if err != nil {
		fail(err)
	}
	h, err := packetecs2.ReadComponentHashMaps(bytes.NewReader(b))
	if err != nil {
		fail(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		fail(err)
	}
	readers := map[int]*wrpl.ReplayReader{}
	files := []*os.File{}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
		for _, f := range files {
			f.Close()
		}
	}()
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".wrpl" {
			continue
		}
		f, err := os.Open(filepath.Join(dir, e.Name()))
		if err != nil {
			fail(err)
		}
		files = append(files, f)
		r, err := wrpl.OpenReplay(f, true, true, true)
		if err != nil {
			fail(err)
		}
		if !r.Header.IsServer() {
			fail(fmt.Errorf("%s is not a server replay part", e.Name()))
		}
		if _, exists := readers[int(r.Header.ReplayPartNumber)]; exists {
			fail(fmt.Errorf("duplicate replay part %d", r.Header.ReplayPartNumber))
		}
		readers[int(r.Header.ReplayPartNumber)] = r
	}
	c, err := carve.CarveReplay(readers, *h)
	if err != nil {
		fail(err)
	}
	vehicles := map[uint64]map[string]bool{}
	for _, e := range c.Entities {
		if e.PlayerID == 0 || e.ModelName == "" {
			continue
		}
		if vehicles[e.PlayerID] == nil {
			vehicles[e.PlayerID] = map[string]bool{}
		}
		vehicles[e.PlayerID][e.ModelName] = true
	}
	out := map[string]any{"session_id": fmt.Sprintf("%016x", c.SessionID), "start_time": c.TimeStarted, "level": c.Mission.Level, "players": c.Players, "spawned": vehicles, "kills": c.Kills, "errors": c.CarveErrors}
	out["match"] = matchReport(readers[0].Header, c.Results, c.Players, c.CarveErrors, catalogPath)
	events := battleLog(c)
	if format == "json" {
		out = out["match"].(map[string]any)
		out["battle_log"] = events
	} else if format == "battle-log" {
		out = map[string]any{"session_id": fmt.Sprintf("%016x", c.SessionID), "start_time_utc": out["match"].(map[string]any)["start_time_utc"], "events": events, "errors": c.CarveErrors}
	} else {
		out["battle_log"] = events
	}
	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		fail(err)
	}
}

func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
