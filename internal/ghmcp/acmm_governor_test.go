package ghmcp

import (
	"os"
	"testing"
)

// TestMain keeps the ACMM production traffic governor out of the upstream
// ghmcp unit-test process. Those tests deliberately run many independent local
// HTTP servers in parallel; applying one process-wide production governor to
// those unrelated fixtures serializes them and can exhaust their per-test
// deadlines. Governor behavior itself is covered in pkg/http/transport.
func TestMain(m *testing.M) {
	previous, existed := os.LookupEnv("ACMM_GITHUB_MCP_GOVERNOR_ENABLED")
	if err := os.Setenv("ACMM_GITHUB_MCP_GOVERNOR_ENABLED", "false"); err != nil {
		panic(err)
	}

	code := m.Run()

	if existed {
		if err := os.Setenv("ACMM_GITHUB_MCP_GOVERNOR_ENABLED", previous); err != nil {
			panic(err)
		}
	} else {
		if err := os.Unsetenv("ACMM_GITHUB_MCP_GOVERNOR_ENABLED"); err != nil {
			panic(err)
		}
	}

	os.Exit(code)
}
