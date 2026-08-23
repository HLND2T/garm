//go:build windows

// Copyright 2026 Cloudbase Solutions SRL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExternalExecutableAcceptsCaseInsensitivePATHEXT(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "provider.exe")
	require.NoError(t, os.WriteFile(path, nil, 0o600))

	t.Setenv("PATHEXT", ".COM;.EXE;.BAT")
	cfg := External{ProviderExecutable: path}
	require.NoError(t, cfg.Validate())
}
