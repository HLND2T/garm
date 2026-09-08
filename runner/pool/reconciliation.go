// Copyright 2026 Cloudbase Solutions SRL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pool

import (
	"errors"
	"fmt"
	"log/slog"

	runnerErrors "github.com/cloudbase/garm-provider-common/errors"
	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/locking"
	"github.com/cloudbase/garm/params"
)

func (r *basePoolManager) reconcileRunnerStatuses(runners []forgeRunner) error {
	var errs []error
	for _, runner := range runners {
		if runner.Status != string(params.RunnerActive) || runner.ID <= 0 ||
			!isManagedRunner(labelsFromRunner(runner), r.controllerInfo.ControllerID.String()) {
			continue
		}
		if err := r.reconcileRunnerStatus(runner); err != nil {
			errs = append(errs, fmt.Errorf("failed to reconcile runner %s: %w", runner.Name, err))
		}
	}
	return errors.Join(errs...)
}

func (r *basePoolManager) reconcileRunnerStatus(runner forgeRunner) error {
	if !locking.TryLock(runner.Name, r.consumerID) {
		slog.DebugContext(r.ctx, "busy runner reconciliation deferred: instance locked", "runner_name", runner.Name)
		return nil
	}
	defer locking.Unlock(runner.Name, false)

	instance, err := r.store.GetInstance(r.ctx, runner.Name)
	if errors.Is(err, runnerErrors.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if instance.ScaleSetID != 0 || instance.PoolID == "" || instance.Status != commonParams.InstanceRunning {
		return nil
	}
	switch instance.RunnerStatus {
	case params.RunnerIdle, params.RunnerPending, params.RunnerInstalling:
	default:
		return nil
	}
	if instance.AgentID != 0 && instance.AgentID != runner.ID {
		slog.WarnContext(r.ctx, "busy runner reconciliation skipped: agent ID mismatch",
			"runner_name", runner.Name, "local_agent_id", instance.AgentID, "remote_agent_id", runner.ID)
		return nil
	}
	// Webhooks do not take the instance lock. The store must atomically recheck
	// identity, pool ownership and lifecycle before recording this observation.
	changed, err := r.store.ReconcileInstanceRunnerActive(r.ctx, r.entity, instance, runner.ID)
	if err != nil {
		return err
	}
	if changed {
		slog.InfoContext(r.ctx, "reconciled busy runner status",
			"runner_name", runner.Name, "pool_id", instance.PoolID,
			"agent_id", runner.ID, "previous_status", instance.RunnerStatus)
	}
	return nil
}
