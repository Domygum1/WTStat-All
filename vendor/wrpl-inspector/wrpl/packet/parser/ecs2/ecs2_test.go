package packetecs2

import (
	"testing"

	"github.com/maxsupermanhd/wrpl-inspector/v3/wrpl/danet"
	"github.com/maxsupermanhd/wrpl-inspector/v3/wrpl/packet"
)

func TestConstructionLayouts(t *testing.T) {
	for _, raw := range [][]byte{{0, 0}, {0, 0, 0}} {
		p := NewPacketECSParser(ComponentHashMaps{})
		p.TemplateDefs[0] = &Template{ID: 0, Name: "empty"}
		msg := append([]byte{5, 0, byte(len(raw))}, raw...)
		_, err := p.ParseECSConstructMessage(danet.NewBitReader(msg))
		if err != nil {
			t.Fatalf("layout %x: %v", raw, err)
		}
		if _, ok := p.Mgr.GetEntity(EntityID(1)); !ok {
			t.Fatalf("layout %x lost entity", raw)
		}
		if p.IncompleteConstructions != 0 {
			t.Fatalf("layout %x marked incomplete", raw)
		}
	}
}

func TestDestructionClearsUIDMapping(t *testing.T) {
	p := NewPacketECSParser(ComponentHashMaps{})
	e := &Entity{}
	e.Data.AddComponent(&Component{Value: int32(42)}, "uid")
	p.Mgr.AddEntity(EntityID(1), e)
	if _, ok := p.Mgr.GetEntityByUid(42); !ok {
		t.Fatal("missing UID mapping")
	}
	_, err := p.Parse(&packet.Packet{PacketType: 6, PacketPayload: []byte{0x26, 0, 5, 0}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.Mgr.GetEntityByUid(42); ok {
		t.Fatal("destroyed entity retained in UID mapping")
	}
	if _, ok := p.Mgr.GetEntity(EntityID(1)); ok {
		t.Fatal("destroyed entity retained by index")
	}
}
