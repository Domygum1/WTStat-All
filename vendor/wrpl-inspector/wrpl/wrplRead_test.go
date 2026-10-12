package wrpl

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/klauspost/compress/zlib"
	"github.com/klauspost/compress/zstd"
)

func replayFixture(t *testing.T, codec string, truncate bool) ([]byte, []byte) {
	t.Helper()
	payload := []byte("packet data, not BLK")
	var compressed []byte
	if codec == "zstd" {
		w, err := zstd.NewWriter(nil)
		if err != nil {
			t.Fatal(err)
		}
		compressed = w.EncodeAll(payload, nil)
		// Recent streams can contain concatenated zstd frames.
		compressed = append(compressed, w.EncodeAll(payload, nil)...)
		payload = append(payload, payload...)
		w.Close()
	} else {
		var out bytes.Buffer
		w := zlib.NewWriter(&out)
		w.Write(payload)
		w.Close()
		compressed = out.Bytes()
	}
	if truncate {
		compressed = compressed[:len(compressed)-3]
	}
	h := WRPLHeader{Magic: [4]byte{0xe5, 0xac, 0, 0x10}, SettingsBLKSize: 3, ResultsBlkOffset: int32(1237 + len(compressed))}
	var b bytes.Buffer
	if err := binary.Write(&b, binary.LittleEndian, h); err != nil {
		t.Fatal(err)
	}
	b.Write(make([]byte, 1234-b.Len()))
	b.Write([]byte{1, 2, 3})
	b.Write(compressed)
	b.Write([]byte{1, 42, 99})
	return b.Bytes(), payload
}

func TestPacketStreamStopsBeforeResults(t *testing.T) {
	for _, codec := range []string{"zstd", "zlib"} {
		t.Run(codec, func(t *testing.T) {
			data, want := replayFixture(t, codec, false)
			r, err := OpenReplay(bytes.NewReader(data), true, true, true)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got, err := io.ReadAll(r.PacketStream)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("packets %q, error %v", got, err)
			}
			if !bytes.Equal(r.Settings, []byte{1, 2, 3}) || !bytes.Equal(r.Results, []byte{1, 42, 99}) {
				t.Fatal("settings/results boundaries incorrect")
			}
		})
	}
}

func TestTruncatedStreamIsNotSuccessful(t *testing.T) {
	data, _ := replayFixture(t, "zstd", true)
	r, err := OpenReplay(bytes.NewReader(data), false, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = io.ReadAll(r.PacketStream); err == nil {
		t.Fatal("truncated zstd accepted")
	}
}

func TestRejectInvalidResultsOffset(t *testing.T) {
	data, _ := replayFixture(t, "zstd", false)
	for _, offset := range []uint32{100, uint32(len(data) + 1), 0xffffffff} {
		binary.LittleEndian.PutUint32(data[684:], offset)
		if _, err := OpenReplay(bytes.NewReader(data), true, true, true); err == nil {
			t.Fatalf("accepted offset %d", offset)
		}
	}
}
