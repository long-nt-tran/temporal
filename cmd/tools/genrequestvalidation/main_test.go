package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	apiregistry "go.temporal.io/api/temporalproto/registry"
)

func TestGeneratorAndFreshness(t *testing.T) {
	out := filepath.Join(t.TempDir(), "services_gen.go")
	require.NoError(t, run(out, false))
	require.NoError(t, run(out, true))
	generated, err := os.ReadFile(out)
	require.NoError(t, err)
	for _, service := range apiregistry.Services() {
		require.Contains(t, string(generated), "func Wrap"+string(service.Name()))
		for i := range service.Methods().Len() {
			method := service.Methods().Get(i)
			if !method.IsStreamingClient() && !method.IsStreamingServer() {
				require.Contains(t, string(generated), "/"+string(service.FullName())+"/"+string(method.Name()))
			}
		}
	}
	require.NoError(t, os.WriteFile(out, append(generated, '\n'), 0o600))
	require.ErrorContains(t, run(out, true), "is stale")
}
