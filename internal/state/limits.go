package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const limitsVersion = 1

type limitsFile struct {
	Version int                `json:"version"`
	Limits  map[string]float64 `json:"limits"`
}

// LimitsPath is where the per-adapter speed limits live, next to the status file.
func LimitsPath(dir string) string { return filepath.Join(dir, "limits.json") }

// LoadLimits reads the saved caps in MByte/s. A missing file means no caps, not an error.
func LoadLimits(dir string) (map[string]float64, error) {
	b, err := os.ReadFile(LimitsPath(dir))
	if errors.Is(err, os.ErrNotExist) {
		return map[string]float64{}, nil
	}
	if err != nil {
		return nil, err
	}
	var f limitsFile
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("%s: %w", LimitsPath(dir), err)
	}
	limits := make(map[string]float64, len(f.Limits))
	for name, mbPerSec := range f.Limits {
		if name != "" && mbPerSec > 0 && mbPerSec <= 1e6 {
			limits[name] = mbPerSec
		}
	}
	return limits, nil
}

// SaveLimits writes the caps atomically, so a crash mid-write cannot leave a broken file.
func SaveLimits(dir string, limits map[string]float64) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(limitsFile{Version: limitsVersion, Limits: limits}, "", "  ")
	if err != nil {
		return err
	}
	tmp := LimitsPath(dir) + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, LimitsPath(dir))
}
