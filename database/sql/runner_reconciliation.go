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

package sql

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	runnerErrors "github.com/cloudbase/garm-provider-common/errors"
	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/params"
)

func (s *sqlDatabase) ReconcileInstanceRunnerActive(ctx context.Context, entity params.ForgeEntity, instance params.Instance, agentID int64) (bool, error) {
	if instance.ScaleSetID != 0 || instance.PoolID == "" || agentID <= 0 || (instance.AgentID != 0 && instance.AgentID != agentID) {
		return false, nil
	}
	id, err := uuid.Parse(instance.ID)
	if err != nil {
		return false, fmt.Errorf("invalid instance ID: %w", err)
	}

	var changed bool
	var updated params.Instance
	err = s.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		pool, err := s.getEntityPool(tx, entity.EntityType, entity.ID, instance.PoolID)
		if errors.Is(err, runnerErrors.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("error checking instance pool: %w", err)
		}
		// The snapshot can predate a completion, deletion or registration callback.
		// Keep every mutable precondition in the UPDATE itself, including when
		// the callback assigns an agent ID that was missing in our snapshot.
		result := tx.Model(&Instance{}).
			Where("id = ? AND name = ? AND pool_id = ? AND scale_set_fk_id IS NULL", id, instance.Name, pool.ID).
			Where("agent_id = ? OR agent_id = 0", agentID).
			Where("status = ?", commonParams.InstanceRunning).
			Where("runner_status IN ?", []params.RunnerStatus{params.RunnerIdle, params.RunnerPending, params.RunnerInstalling}).
			Update("runner_status", params.RunnerActive)
		if result.Error != nil {
			return fmt.Errorf("error reconciling busy runner: %w", result.Error)
		}
		if result.RowsAffected == 0 {
			return nil
		}
		current, err := s.getInstance(ctx, tx, instance.ID, "Pool", "ScaleSet")
		if err != nil {
			return err
		}
		updated, err = s.sqlToParamsInstance(current)
		if err != nil {
			return err
		}
		changed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	if changed {
		s.sendNotify(common.InstanceEntityType, common.UpdateOperation, updated)
	}
	return changed, nil
}
