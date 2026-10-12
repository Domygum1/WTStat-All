package carve

import (
	"github.com/maxsupermanhd/wrpl-inspector/v3/wrpl/game"
	packetecs2 "github.com/maxsupermanhd/wrpl-inspector/v3/wrpl/packet/parser/ecs2"
	packetkill "github.com/maxsupermanhd/wrpl-inspector/v3/wrpl/packet/parser/kill"
	packetslot "github.com/maxsupermanhd/wrpl-inspector/v3/wrpl/packet/parser/slot"
	"slices"
)

type SessionKill struct {
	Time              uint32
	KillerPid         uint32
	HasKillerPid      bool
	KillerECSUid      uint16
	HasKillerUid      bool
	VictimECSUid      uint16
	HasVictimUid      bool
	VictimPid         uint32
	HasVictimPid      bool
	DeathType         int32
	KillerID          uint64
	KillerResolution  string
	KillerModel       string
	KillerEntityIndex uint32
	KillerPosition    *game.SpaceTime
	Weapon            string
	VictimID          uint64
	VictimResolution  string
	VictimModel       string
	VictimEntityIndex uint32
	VictimPosition    *game.SpaceTime
}

func assembleKills(ecs *packetecs2.EntityManager, players [256]*packetslot.Player, kills *packetkill.PacketKillParser) (ret []SessionKill, err error) {
	// An ECS UID can be reused after a vehicle death. Only use attacker
	// observations made before the victim event, within that entity's life.
	uidOwner := map[uint16]uint64{}
	ambiguous := map[uint16]bool{}
	chronological := slices.Clone(kills.Kills)
	slices.SortStableFunc(chronological, func(a, b packetkill.KillEntry) int {
		if a.CurrentTime < b.CurrentTime {
			return -1
		}
		if a.CurrentTime > b.CurrentTime {
			return 1
		}
		return 0
	})
	for _, k := range chronological {
		entry := SessionKill{
			Time:           k.CurrentTime,
			KillerPid:      k.KillerPid,
			HasKillerPid:   k.HasKillerPid,
			KillerECSUid:   k.KillerUid,
			HasKillerUid:   k.HasKillerUid,
			VictimECSUid:   k.VictimUid,
			HasVictimUid:   k.HasVictimUid,
			VictimPid:      k.VictimPid,
			HasVictimPid:   k.HasVictimPid,
			DeathType:      k.DeathType,
			Weapon:         k.PlayerWeapon,
			KillerPosition: k.ResolvedKillerPosition,
			VictimPosition: k.ResolvedVictimPosition,
		}
		entry.KillerID, entry.KillerModel = resolveEntityDetails(players, k.ResolvedKiller)
		if entry.KillerID != 0 {
			entry.KillerResolution = "ecs"
		}
		if k.HasKillerPid && k.KillerPid < uint32(len(players)) && players[k.KillerPid] != nil {
			fromSlot := players[k.KillerPid].Uid.Player_id
			if entry.KillerID != 0 && entry.KillerID != fromSlot {
				entry.KillerID = 0
				entry.KillerResolution = "conflict"
			} else {
				entry.KillerID = fromSlot
				if entry.KillerResolution == "ecs" {
					entry.KillerResolution = "ecs+slot"
				} else {
					entry.KillerResolution = "slot"
				}
			}
		}
		entry.KillerEntityIndex = resolveEntityToEntityIndex(ecs.Entities, k.ResolvedKiller)
		entry.VictimID, entry.VictimModel = resolveEntityDetails(players, k.ResolvedVictim)
		if entry.VictimID != 0 {
			entry.VictimResolution = "ecs"
		}
		if fromUID, exists := uidOwner[k.VictimUid]; k.HasVictimUid && exists && !ambiguous[k.VictimUid] {
			if entry.VictimID != 0 && entry.VictimID != fromUID {
				entry.VictimID = 0
				entry.VictimResolution = "conflict"
			} else {
				entry.VictimID = fromUID
				if entry.VictimResolution == "" {
					entry.VictimResolution = "prior_attacker_uid"
				}
			}
		}
		if k.HasVictimUid && k.VictimUid != 0x7ff {
			delete(uidOwner, k.VictimUid)
			delete(ambiguous, k.VictimUid)
		}
		if k.HasKillerPid && k.HasKillerUid && k.KillerPid < uint32(len(players)) && k.KillerUid != 0x7ff && players[k.KillerPid] != nil {
			owner := players[k.KillerPid].Uid.Player_id
			if owner != 0 {
				if previous, exists := uidOwner[k.KillerUid]; exists && previous != owner {
					ambiguous[k.KillerUid] = true
				}
				uidOwner[k.KillerUid] = owner
			}
		}
		entry.VictimEntityIndex = resolveEntityToEntityIndex(ecs.Entities, k.ResolvedVictim)
		ret = append(ret, entry)
	}
	return
}
