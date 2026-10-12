package carve

import (
	"maps"
	"slices"
	"strconv"
	"strings"

	packetslot "github.com/maxsupermanhd/wrpl-inspector/v3/wrpl/packet/parser/slot"
)

type SessionPlayer struct {
	Slot     uint8
	HasSlot  bool `json:"-"`
	PlayerID uint64
	Name     string
	ClanTag  string
	Team     byte

	Crafts map[string]string

	SessionPlayerPerformance
}

type SessionPlayerPerformance struct {
	SquadID             int  `json:"squadId"`
	AutoSquad           bool `json:"autoSquad"`
	Deaths              int  `json:"deaths"`
	AiGroundKills       int  `json:"aiGroundKills"`
	AiKills             int  `json:"aiKills"`
	AiNavalKills        int  `json:"aiNavalKills"`
	Assists             int  `json:"assists"`
	AwardDamage         int  `json:"awardDamage"`
	CaptureZone         int  `json:"captureZone"`
	DamageZone          int  `json:"damageZone"`
	GroundKills         int  `json:"groundKills"`
	HumanKills          int  `json:"humanKills"`
	Kills               int  `json:"kills"`
	MissileEvades       int  `json:"missileEvades"`
	NavalKills          int  `json:"navalKills"`
	Score               int  `json:"score"`
	ShellIntgerceptions int  `json:"shellIntgerceptions"`
	TeamKills           int  `json:"teamKills"`
}

func assemblePlayers(results map[string]any, slots [256]*packetslot.Player) (ret []SessionPlayer, err error) {
	resultsPlayers := []any{}
	resultsMatchingInfo := map[string]any{}
	if results != nil {
		resultsPlayers, _ = results["player"].([]any)
		if single, ok := results["player"].(map[string]any); ok {
			resultsPlayers = []any{single}
		}
		resultsMatchingInfo, _ = results["matchingInfo"].(map[string]any)
	}
	byUID := map[uint64]SessionPlayer{}
	for slot, p := range slots {
		if p == nil {
			continue
		}
		if p.Uid.Name == "" {
			continue
		}
		if p.Uid.Player_id == 0 {
			continue
		}
		player := SessionPlayer{
			Slot:     uint8(slot),
			HasSlot:  true,
			PlayerID: uint64(p.Uid.Player_id),
			Name:     p.Uid.Name,
			ClanTag:  p.ClanTag,
			Team:     p.Team,
		}
		if p.RealNick != "" {
			player.Name = p.RealNick
		}
		byUID[player.PlayerID] = player
	}
	// Results BLK is independently usable even if slot/ECS decoding fails.
	// Preserve result-only players, including those who left before the end.
	rows := map[uint64]map[string]any{}
	for _, r := range resultsPlayers {
		r2, ok := r.(map[string]any)
		if !ok {
			continue
		}
		pidStr, _ := r2["userId"].(string)
		pid, parseErr := strconv.ParseUint(pidStr, 10, 64)
		if parseErr != nil || pid == 0 {
			ret = append(ret, applyPlayerResult(SessionPlayer{}, r2))
			continue
		}
		player := byUID[pid]
		player.PlayerID = pid
		byUID[pid] = player
		rows[pid] = r2
	}
	for _, pid := range slices.Sorted(maps.Keys(byUID)) {
		player := byUID[pid]
		r2 := rows[pid]
		player = applyPlayerResult(player, r2)
		for rid, r := range resultsMatchingInfo {
			r2, ok := r.(map[string]any)
			if !ok {
				continue
			}
			pid, err := strconv.ParseUint(rid, 10, 64)
			if err != nil {
				continue
			}
			if player.PlayerID != pid {
				continue
			}
			craftsInfo, ok := r2["crafts_info"].(map[string]any)
			if !ok {
				continue
			}
			player.Crafts = map[string]string{}
			for _, k := range slices.Sorted(maps.Keys(craftsInfo)) {
				craft, ok := craftsInfo[k].(map[string]any)
				if !ok {
					continue
				}
				k2 := strings.TrimPrefix(k, "array")
				_, err := strconv.Atoi(k2)
				if err != nil {
					continue
				}
				craftName, ok := craft["name"].(string)
				if !ok {
					continue
				}
				player.Crafts[k2] = craftName
			}
		}
		ret = append(ret, player)
	}
	return
}

// PlayersFromResults exposes the same assembly without requiring packet decoding.
func PlayersFromResults(results map[string]any) []SessionPlayer {
	players, _ := assemblePlayers(results, [256]*packetslot.Player{})
	return players
}

func applyPlayerResult(player SessionPlayer, r2 map[string]any) SessionPlayer {
	player.Name = getMapStringAnyValue(r2, player.Name, "name")
	player.ClanTag = getMapStringAnyValue(r2, player.ClanTag, "clanTag")
	team := getMapStringAnyValue(r2, int64(player.Team), "team")
	if team >= 0 && team <= 255 {
		player.Team = byte(team)
	}
	player.SessionPlayerPerformance.SquadID = int(getMapStringAnyValue(r2, int64(-1), "squadId"))
	player.SessionPlayerPerformance.AutoSquad = getMapStringAnyValue(r2, false, "autoSquad")
	player.SessionPlayerPerformance.Deaths = int(getMapStringAnyValue(r2, int64(-1), "deaths"))
	player.SessionPlayerPerformance.AiGroundKills = int(getMapStringAnyValue(r2, int64(-1), "aiGroundKills"))
	player.SessionPlayerPerformance.AiKills = int(getMapStringAnyValue(r2, int64(-1), "aiKills"))
	player.SessionPlayerPerformance.AiNavalKills = int(getMapStringAnyValue(r2, int64(-1), "aiNavalKills"))
	player.SessionPlayerPerformance.Assists = int(getMapStringAnyValue(r2, int64(-1), "assists"))
	player.SessionPlayerPerformance.AwardDamage = int(getMapStringAnyValue(r2, int64(-1), "awardDamage"))
	player.SessionPlayerPerformance.CaptureZone = int(getMapStringAnyValue(r2, int64(-1), "captureZone"))
	player.SessionPlayerPerformance.DamageZone = int(getMapStringAnyValue(r2, int64(-1), "damageZone"))
	player.SessionPlayerPerformance.GroundKills = int(getMapStringAnyValue(r2, int64(-1), "groundKills"))
	player.SessionPlayerPerformance.HumanKills = int(getMapStringAnyValue(r2, int64(-1), "humanKills"))
	player.SessionPlayerPerformance.Kills = int(getMapStringAnyValue(r2, int64(-1), "kills"))
	player.SessionPlayerPerformance.MissileEvades = int(getMapStringAnyValue(r2, int64(-1), "missileEvades"))
	player.SessionPlayerPerformance.NavalKills = int(getMapStringAnyValue(r2, int64(-1), "navalKills"))
	player.SessionPlayerPerformance.Score = int(getMapStringAnyValue(r2, int64(-1), "score"))
	player.SessionPlayerPerformance.ShellIntgerceptions = int(getMapStringAnyValue(r2, int64(-1), "shellIntgerceptions"))
	player.SessionPlayerPerformance.TeamKills = int(getMapStringAnyValue(r2, int64(-1), "teamKills"))
	return player
}
