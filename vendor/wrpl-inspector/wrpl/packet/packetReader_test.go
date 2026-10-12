package packet

import (
	"bytes"
	"testing"
)

func TestEmptyPacketClearsPreviousPayload(t *testing.T) {
	r := NewPacketStreamReader(bytes.NewReader([]byte{0x83, 0x14, 0, 42, 0x82, 0x15, 0}))
	p := &Packet{}
	if _, err := r.ReadPacket(p); err != nil {
		t.Fatal(err)
	}
	if len(p.PacketPayload) != 1 {
		t.Fatal("missing first payload")
	}
	if _, err := r.ReadPacket(p); err != nil {
		t.Fatal(err)
	}
	if len(p.PacketPayload) != 0 || p.PacketType != 5 {
		t.Fatal("empty next-segment packet reused stale data")
	}
}

func TestMalformedPacketsReturnErrors(t *testing.T) {
	for _, b := range [][]byte{{0x81}, {0x82, 4, 0}, {0x40}, {0x87, 4, 0, 0, 0, 0, 0}} {
		r := NewPacketStreamReader(bytes.NewReader(b))
		end, err := r.ReadPacket(&Packet{})
		if err == nil || end {
			t.Fatalf("accepted truncated/undersized packet %x", b)
		}
	}
}
