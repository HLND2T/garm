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
	"github.com/google/uuid"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/params"
)

func (s *InstancesTestSuite) TestReconcileInstanceRunnerActive() {
	entity, err := s.Fixtures.Org.GetEntity()
	s.Require().NoError(err)
	const agentID int64 = 12345
	for _, status := range []params.RunnerStatus{params.RunnerIdle, params.RunnerPending, params.RunnerInstalling} {
		s.Run(string(status), func() {
			before, err := s.Store.ForceUpdateInstance(s.adminCtx, s.Fixtures.Instances[0].Name, params.UpdateInstanceParams{
				Status: commonParams.InstanceRunning, RunnerStatus: status, AgentID: agentID,
				ProviderFault: []byte("preserve provider diagnostics"),
			})
			s.Require().NoError(err)
			changed, err := s.Store.ReconcileInstanceRunnerActive(s.adminCtx, entity, before, agentID)
			s.Require().NoError(err)
			s.True(changed)
			after, err := s.Store.GetInstance(s.adminCtx, before.Name)
			s.Require().NoError(err)
			s.Equal(params.RunnerActive, after.RunnerStatus)
			s.Equal(before.ProviderFault, after.ProviderFault)
			s.Equal(before.Status, after.Status)
			s.Equal(before.AgentID, after.AgentID)
			changed, err = s.Store.ReconcileInstanceRunnerActive(s.adminCtx, entity, before, agentID)
			s.Require().NoError(err)
			s.False(changed)
			again, err := s.Store.GetInstance(s.adminCtx, before.Name)
			s.Require().NoError(err)
			s.Equal(after.UpdatedAt, again.UpdatedAt)
		})
	}
}

func (s *InstancesTestSuite) TestReconcileInstanceRunnerActiveRejectsStaleSnapshot() {
	entity, err := s.Fixtures.Org.GetEntity()
	s.Require().NoError(err)
	const agentID int64 = 12345
	tests := []struct {
		name         string
		status       commonParams.InstanceStatus
		runnerStatus params.RunnerStatus
	}{
		{"completed", commonParams.InstanceRunning, params.RunnerTerminated},
		{"failed", commonParams.InstanceRunning, params.RunnerFailed},
		{"unknown", commonParams.InstanceRunning, params.RunnerUnknown},
		{"offline", commonParams.InstanceRunning, params.RunnerOffline},
		{"pending_delete", commonParams.InstancePendingDelete, params.RunnerIdle},
		{"pending_force_delete", commonParams.InstancePendingForceDelete, params.RunnerIdle},
		{"deleting", commonParams.InstanceDeleting, params.RunnerIdle},
		{"deleted", commonParams.InstanceDeleted, params.RunnerIdle},
		{"stopped", commonParams.InstanceStopped, params.RunnerIdle},
		{"creating", commonParams.InstanceCreating, params.RunnerPending},
		{"error", commonParams.InstanceError, params.RunnerIdle},
	}
	for _, tc := range tests {
		s.Run(tc.name, func() {
			snapshot, err := s.Store.ForceUpdateInstance(s.adminCtx, s.Fixtures.Instances[0].Name, params.UpdateInstanceParams{
				Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle, AgentID: agentID,
			})
			s.Require().NoError(err)
			before, err := s.Store.ForceUpdateInstance(s.adminCtx, snapshot.Name, params.UpdateInstanceParams{
				Status: tc.status, RunnerStatus: tc.runnerStatus,
			})
			s.Require().NoError(err)
			changed, err := s.Store.ReconcileInstanceRunnerActive(s.adminCtx, entity, snapshot, agentID)
			s.Require().NoError(err)
			s.False(changed)
			after, err := s.Store.GetInstance(s.adminCtx, snapshot.Name)
			s.Require().NoError(err)
			s.Equal(before.RunnerStatus, after.RunnerStatus)
			s.Equal(before.Status, after.Status)
			s.Equal(before.UpdatedAt, after.UpdatedAt)
		})
	}
}

func (s *InstancesTestSuite) TestReconcileInstanceRunnerActiveChecksIdentity() {
	entity, err := s.Fixtures.Org.GetEntity()
	s.Require().NoError(err)
	const agentID int64 = 12345
	before, err := s.Store.ForceUpdateInstance(s.adminCtx, s.Fixtures.Instances[0].Name, params.UpdateInstanceParams{
		Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle, AgentID: agentID,
	})
	s.Require().NoError(err)
	for _, field := range []string{"id", "name", "pool", "scaleset", "agent", "entity", "missing_agent"} {
		s.Run(field, func() {
			expected, owner, remoteID := before, entity, agentID
			switch field {
			case "id":
				expected.ID = uuid.NewString()
			case "name":
				expected.Name = "different-runner"
			case "pool":
				expected.PoolID = uuid.NewString()
			case "scaleset":
				expected.ScaleSetID = 1
			case "agent":
				remoteID++
			case "entity":
				owner.ID = uuid.NewString()
			case "missing_agent":
				remoteID = 0
			}
			changed, err := s.Store.ReconcileInstanceRunnerActive(s.adminCtx, owner, expected, remoteID)
			s.Require().NoError(err)
			s.False(changed)
		})
	}
	// A callback assigning a different agent after the snapshot must also be respected.
	_, err = s.Store.ForceUpdateInstance(s.adminCtx, before.Name, params.UpdateInstanceParams{AgentID: agentID + 1})
	s.Require().NoError(err)
	changed, err := s.Store.ReconcileInstanceRunnerActive(s.adminCtx, entity, before, agentID)
	s.Require().NoError(err)
	s.False(changed)
}

func (s *InstancesTestSuite) TestReconcileInstanceRunnerActiveWithoutAgentID() {
	entity, err := s.Fixtures.Org.GetEntity()
	s.Require().NoError(err)
	instance, err := s.Store.CreateInstance(s.adminCtx, s.Fixtures.Pool.ID, params.CreateInstanceParams{
		Name: "busy-without-agent-id", Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerPending,
	})
	s.Require().NoError(err)
	changed, err := s.Store.ReconcileInstanceRunnerActive(s.adminCtx, entity, instance, 12345)
	s.Require().NoError(err)
	s.True(changed)
	after, err := s.Store.GetInstance(s.adminCtx, instance.Name)
	s.Require().NoError(err)
	s.Equal(int64(0), after.AgentID)
	s.Equal(params.RunnerActive, after.RunnerStatus)
}
