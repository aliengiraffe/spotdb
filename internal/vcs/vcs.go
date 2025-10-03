package vcs

import (
	"fmt"
	"runtime/debug"
)

// Version returns the VCS version information.
func Version() string {
	bi, ok := debug.ReadBuildInfo()
	return versionFromBuildInfo(bi, ok)
}

// versionFromBuildInfo extracts version info from build info
// This is exported for testing purposes.
func versionFromBuildInfo(bi *debug.BuildInfo, ok bool) string {
	var (
		time     string
		revision string
		modified bool
	)

	if ok && bi != nil {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.time":
				time = s.Value
			case "vcs.revision":
				revision = s.Value
			case "vcs.modified":
				if s.Value == "true" {
					modified = true
				}
			}
		}
	}

	if modified {
		return fmt.Sprintf("%s-%s+dirty", time, revision)
	}

	return fmt.Sprintf("%s-%s", time, revision)
}
