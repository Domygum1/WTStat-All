package main

import (
	"slices"
	"strconv"

	"github.com/maxsupermanhd/wrpl-inspector/v3/wrpl/carve"
)

func accountID(id uint64) any {
	if id == 0 {
		return nil
	}
	return strconv.FormatUint(id, 10)
}

// battleLog contains every battle event currently decoded by carve. Entity
// numbers and weapon ownership are kept distinct from verified player deaths.
func battleLog(c *carve.CarvedReplay) []map[string]any {
	events := []map[string]any{}
	names := map[uint64]string{}
	slots := map[uint32]carve.SessionPlayer{}
	for _, player := range c.Players {
		names[player.PlayerID] = player.Name
		if player.HasSlot {
			slots[uint32(player.Slot)] = player
		}
	}
	name := func(id uint64) any { return nullableText(names[id]) }
	for _, k := range c.Kills {
		var attackerSlot, victimEntity, attackerEntity, ownerSlot, ownerID, ownerName any
		if k.HasKillerPid && k.KillerPid < 256 {
			attackerSlot = k.KillerPid
		}
		if k.HasKillerUid && k.KillerECSUid != 0x7ff {
			attackerEntity = k.KillerECSUid
		}
		if k.HasVictimUid && k.VictimECSUid != 0x7ff {
			victimEntity = k.VictimECSUid
		}
		if k.HasVictimPid && k.VictimPid < 256 {
			ownerSlot = k.VictimPid
			if player, ok := slots[k.VictimPid]; ok {
				ownerID, ownerName = accountID(player.PlayerID), nullableText(player.Name)
			}
		}
		targetKind := "unresolved_entity"
		if k.VictimID != 0 {
			targetKind = "player_unit"
		} else if victimEntity == nil && ownerSlot != nil {
			targetKind = "weapon_or_component"
		} else if k.VictimModel != "" {
			targetKind = "entity"
		}
		events = append(events, map[string]any{
			"time_ms": k.Time, "event_type": "kill", "target_kind": targetKind,
			"attacker_uid": accountID(k.KillerID), "attacker_name": name(k.KillerID),
			"attacker_uid_source": nullableText(k.KillerResolution),
			"attacker_slot":       attackerSlot, "attacker_entity_uid": attackerEntity,
			"target_uid": accountID(k.VictimID), "target_name": name(k.VictimID),
			"target_uid_source": nullableText(k.VictimResolution),
			"target_entity_uid": victimEntity, "target_model": nullableText(k.VictimModel),
			"weapon_owner_slot": ownerSlot, "weapon_owner_uid": ownerID, "weapon_owner_name": ownerName,
			"attacker_model": nullableText(k.KillerModel), "weapon": nullableText(k.Weapon),
			"raw_death_type": k.DeathType,
		})
	}
	for _, d := range c.DamageReports {
		kind := "critical_damage"
		if d.Variant == carve.DamageVariantSevere {
			kind = "severe_damage"
		}
		events = append(events, map[string]any{
			"time_ms": d.Time, "event_type": kind,
			"attacker_uid": accountID(d.OffenderID), "attacker_name": name(d.OffenderID),
			"target_uid": accountID(d.OffendedID), "target_name": name(d.OffendedID),
			"attacker_model": nullableText(d.OffenderModel), "target_model": nullableText(d.OffendedModel),
			"caused_fire": d.CausedFire,
		})
	}
	for _, award := range c.Awards {
		events = append(events, map[string]any{
			"time_ms": award.Time, "event_type": "award", "actor_uid": accountID(award.PlayerID),
			"actor_name": name(award.PlayerID), "award_name": nullableText(award.AwardName),
		})
	}
	for _, msg := range c.ChatMessages {
		events = append(events, map[string]any{
			"time_ms": msg.Time, "event_type": "chat", "sender": nullableText(msg.Sender),
			"message": msg.Message, "channel": msg.Channel,
		})
	}
	slices.SortStableFunc(events, func(a, b map[string]any) int {
		aTime, bTime := a["time_ms"].(uint32), b["time_ms"].(uint32)
		if aTime < bTime {
			return -1
		}
		if aTime > bTime {
			return 1
		}
		return 0
	})
	return events
}
