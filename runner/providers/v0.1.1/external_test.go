// Copyright 2026 Cloudbase Solutions SRL
//
// Licensed under the Apache License, Version 2.0 (the "License");

package v011

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cloudbase/garm/config"
	"github.com/cloudbase/garm/runner/common"
)

func TestMain(m *testing.M) {
	if capturePath := os.Getenv("GARM_TEST_ENV_CAPTURE"); capturePath != "" {
		payload := os.Getenv("GARM_INSTANCE_ID") + "|" + os.Getenv("GARM_INSTANCE_NAME")
		if err := os.WriteFile(capturePath, []byte(payload), 0o600); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestDeleteInstancePassesInstanceIdentity(t *testing.T) {
	capturePath := filepath.Join(t.TempDir(), "delete-environment.txt")
	provider := &external{
		controllerID: "controller-a",
		cfg:          &config.Provider{Name: "test-provider"},
		execPath:     os.Args[0],
		environmentVariables: []string{
			"GARM_TEST_ENV_CAPTURE=" + capturePath,
		},
	}

	err := provider.DeleteInstance(context.Background(), "vm-01", common.DeleteInstanceParams{InstanceName: "runner-a"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(capturePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "vm-01|runner-a" {
		t.Fatalf("captured delete identity = %q, want %q", payload, "vm-01|runner-a")
	}
}
