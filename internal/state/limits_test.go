package state

import (
	"os"
	"reflect"
	"testing"
)

func TestLimitsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := map[string]float64{"Ethernet 3": 10, "Wi-Fi": 2.5}
	if err := SaveLimits(dir, want); err != nil {
		t.Fatal(err)
	}
	got, err := LoadLimits(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadLimits = %v, want %v", got, want)
	}
}

func TestLoadLimitsWithoutAFile(t *testing.T) {
	got, err := LoadLimits(t.TempDir())
	if err != nil {
		t.Fatalf("a missing limits file must not be an error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want no limits, got %v", got)
	}
}

func TestLoadLimitsSanitises(t *testing.T) {
	dir := t.TempDir()
	raw := `{"version":1,"limits":{"neg":-1,"zero":0,"huge":1e9,"ok":5,"":7}}`
	if err := os.WriteFile(LimitsPath(dir), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := LoadLimits(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"ok": 5}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadLimits = %v, want %v", got, want)
	}
}

func TestLoadLimitsBrokenFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(LimitsPath(dir), []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadLimits(dir); err == nil {
		t.Fatal("want an error for a broken limits file")
	}
}
