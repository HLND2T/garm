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

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/params"
)

// ReconcileQueuedJob only advances an existing, entity-owned queued snapshot.
// It deliberately does not bind or change the runner associated with the job.
func (s *sqlDatabase) ReconcileQueuedJob(ctx context.Context, entity params.ForgeEntity, observed params.Job) (bool, error) {
	if observed.ID == 0 || observed.WorkflowJobID <= 0 || observed.ScaleSetJobID != "" ||
		(observed.Status != string(params.JobStatusInProgress) && observed.Status != string(params.JobStatusCompleted)) {
		return false, nil
	}
	entityID, err := uuid.Parse(entity.ID)
	if err != nil {
		return false, err
	}
	var updated params.Job
	changed := false
	err = s.conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		q := tx.Model(&WorkflowJob{}).Where("id = ? AND workflow_job_id = ? AND status = ?", observed.ID, observed.WorkflowJobID, string(params.JobStatusQueued)).
			Where("repository_owner = ? AND repository_name = ?", observed.RepositoryOwner, observed.RepositoryName)
		switch entity.EntityType {
		case params.ForgeEntityTypeOrganization:
			q = q.Where("org_id = ?", entityID)
		case params.ForgeEntityTypeRepository:
			q = q.Where("repo_id = ?", entityID)
		case params.ForgeEntityTypeEnterprise:
			q = q.Where("enterprise_id = ?", entityID)
		case params.ForgeEntityTypeInstance:
			q = q.Where("forge_instance_id = ?", entityID)
		default:
			return nil
		}
		result := q.Updates(map[string]any{
			"status": observed.Status, "action": observed.Status,
			"conclusion": observed.Conclusion, "started_at": observed.StartedAt, "completed_at": observed.CompletedAt,
		})
		if result.Error != nil || result.RowsAffected == 0 {
			return result.Error
		}
		var row WorkflowJob
		if err := tx.Preload("Instance").First(&row, observed.ID).Error; err != nil {
			return err
		}
		var err error
		updated, err = sqlWorkflowJobToParamsJob(row)
		changed = err == nil
		return err
	})
	if err == nil && changed {
		s.sendNotify(common.JobEntityType, common.UpdateOperation, updated)
	}
	return changed, err
}
