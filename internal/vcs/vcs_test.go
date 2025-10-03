package vcs

import (
	"runtime/debug"
	"strings"
	"testing"
)

func TestVersionFromBuildInfo(t *testing.T) {
	tests := []struct {
		name string
		bi   *debug.BuildInfo
		ok   bool
		want string
	}{
		{
			name: "with time, revision, and not modified",
			bi: &debug.BuildInfo{
				Settings: []debug.BuildSetting{
					{Key: "vcs.time", Value: "2024-01-15T10:30:00Z"},
					{Key: "vcs.revision", Value: "abc123"},
					{Key: "vcs.modified", Value: "false"},
				},
			},
			ok:   true,
			want: "2024-01-15T10:30:00Z-abc123",
		},
		{
			name: "with time, revision, and modified",
			bi: &debug.BuildInfo{
				Settings: []debug.BuildSetting{
					{Key: "vcs.time", Value: "2024-01-15T10:30:00Z"},
					{Key: "vcs.revision", Value: "abc123"},
					{Key: "vcs.modified", Value: "true"},
				},
			},
			ok:   true,
			want: "2024-01-15T10:30:00Z-abc123+dirty",
		},
		{
			name: "with partial settings - only revision",
			bi: &debug.BuildInfo{
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: "def456"},
				},
			},
			ok:   true,
			want: "-def456",
		},
		{
			name: "with partial settings - only time",
			bi: &debug.BuildInfo{
				Settings: []debug.BuildSetting{
					{Key: "vcs.time", Value: "2024-01-15T10:30:00Z"},
				},
			},
			ok:   true,
			want: "2024-01-15T10:30:00Z-",
		},
		{
			name: "with no vcs settings",
			bi: &debug.BuildInfo{
				Settings: []debug.BuildSetting{
					{Key: "other.setting", Value: "value"},
				},
			},
			ok:   true,
			want: "-",
		},
		{
			name: "with no build info (ok=false)",
			bi:   nil,
			ok:   false,
			want: "-",
		},
		{
			name: "with nil build info but ok=true",
			bi:   nil,
			ok:   true,
			want: "-",
		},
		{
			name: "with empty settings",
			bi: &debug.BuildInfo{
				Settings: []debug.BuildSetting{},
			},
			ok:   true,
			want: "-",
		},
		{
			name: "with modified=false explicitly",
			bi: &debug.BuildInfo{
				Settings: []debug.BuildSetting{
					{Key: "vcs.time", Value: "2024-01-15T10:30:00Z"},
					{Key: "vcs.revision", Value: "xyz789"},
					{Key: "vcs.modified", Value: "false"},
				},
			},
			ok:   true,
			want: "2024-01-15T10:30:00Z-xyz789",
		},
		{
			name: "with modified=invalid value",
			bi: &debug.BuildInfo{
				Settings: []debug.BuildSetting{
					{Key: "vcs.time", Value: "2024-01-15T10:30:00Z"},
					{Key: "vcs.revision", Value: "xyz789"},
					{Key: "vcs.modified", Value: "invalid"},
				},
			},
			ok:   true,
			want: "2024-01-15T10:30:00Z-xyz789",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := versionFromBuildInfo(tt.bi, tt.ok)
			if got != tt.want {
				t.Errorf("versionFromBuildInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVersion(t *testing.T) {
	// Test the actual Version() function
	version := Version()

	// Should not be empty
	if version == "" {
		t.Error("Version() should not return empty string")
	}

	// Should contain a hyphen
	if !strings.Contains(version, "-") {
		t.Errorf("Version() should contain a hyphen separator, got: %s", version)
	}

	// Log the actual version for debugging
	t.Logf("Actual version string: %s", version)
}
