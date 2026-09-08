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

//go:build testing

package pool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/google/go-github/v84/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	dbCommon "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/locking"
	"github.com/cloudbase/garm/params"
	runnerMocks "github.com/cloudbase/garm/runner/common/mocks"
	"github.com/cloudbase/garm/util/github/scalesets"
)

func init() {
	locker, err := locking.NewLocalLocker(context.Background(), nil)
	if err != nil {
		panic(err)
	}
	if err := locking.RegisterLocker(locker); err != nil {
		panic(err)
	}
}

func TestListRunnersWithPaginationPreservesBusy(t *testing.T) {
	client := runnerMocks.NewGithubClient(t)
	statuses := []string{string(params.RunnerOnline), string(params.RunnerOnline), string(params.RunnerOffline), "unexpected"}
	busy := []bool{true, false, true, true}
	for i, status := range statuses {
		page := i + 1
		if i == 0 {
			page = 0
		}
		next := i + 2
		if i == len(statuses)-1 {
			next = 0
		}
		client.On("ListEntityRunners", mock.Anything, mock.MatchedBy(func(opts *github.ListRunnersOptions) bool {
			return opts.Page == page && opts.PerPage == 100
		})).Return(&github.Runners{Runners: []*github.Runner{{
			ID: github.Ptr(int64(i + 1)), Name: github.Ptr(fmt.Sprintf("runner-%d", i)),
			Status: github.Ptr(status), Busy: github.Ptr(busy[i]),
			Labels: []*github.RunnerLabels{{Name: github.Ptr("self-hosted")}},
		}}}, &github.Response{NextPage: next}, nil).Once()
	}
	mgr := &basePoolManager{ctx: context.Background(), ghcli: client}
	runners, err := mgr.GetGithubRunners()
	require.NoError(t, err)
	require.Len(t, runners, len(statuses))
	for i, status := range []params.RunnerStatus{params.RunnerActive, params.RunnerIdle, params.RunnerOffline, params.RunnerUnknown} {
		require.Equal(t, string(status), runners[i].Status)
		require.Equal(t, []RunnerLabels{{Name: "self-hosted"}}, runners[i].Labels)
		require.Equal(t, int64(i+1), runners[i].ID)
	}
}

func (s *PoolStressTestSuite) busySnapshot(instance params.Instance) forgeRunner {
	return forgeRunner{ID: 12345, Name: instance.Name, Status: string(params.RunnerActive), Labels: []RunnerLabels{{Name: s.mgr.controllerLabel()}}}
}

func (s *PoolStressTestSuite) TestReconcileRunnerStatusesWithoutQueuedWebhook() {
	s.setupProviderMocks()
	pool := s.pool
	pool.MinIdleRunners = 1
	pool.MaxRunners = 2
	// Enforce the same limit in the real store used by AddRunner.
	_, err := s.store.UpdateEntityPool(s.adminCtx, s.entity, pool.ID, params.UpdatePoolParams{MaxRunners: github.Ptr(uint(2)), MinIdleRunners: github.Ptr(uint(1))})
	s.Require().NoError(err)
	instance, err := s.store.CreateInstance(s.adminCtx, pool.ID, params.CreateInstanceParams{
		Name: "busy-with-missing-webhook", Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle, AgentID: 12345,
	})
	s.Require().NoError(err)
	jobs, err := s.store.ListAllJobs(s.adminCtx)
	s.Require().NoError(err)
	s.Empty(jobs)
	s.Require().NoError(s.mgr.ensureIdleRunnersForOnePool(pool))
	instances, err := s.store.ListPoolInstances(s.adminCtx, pool.ID, false)
	s.Require().NoError(err)
	s.Len(instances, 1)
	for range 2 {
		s.Require().NoError(s.mgr.reconcileRunnerStatuses([]forgeRunner{s.busySnapshot(instance)}))
		s.Require().NoError(s.mgr.ensureIdleRunnersForOnePool(pool))
	}
	instances, err = s.store.ListPoolInstances(s.adminCtx, pool.ID, false)
	s.Require().NoError(err)
	s.Len(instances, 2)
	active, err := s.store.GetInstance(s.adminCtx, instance.Name)
	s.Require().NoError(err)
	s.Equal(params.RunnerActive, active.RunnerStatus)
	// Completion still wins when it arrives after reconciliation.
	job := s.makeWorkflowJob(9876, "completed", "completed", instance.Name, []string{"self-hosted", "linux", "x64"})
	s.Require().NoError(s.mgr.HandleWorkflowJob(job))
	s.Require().NoError(s.mgr.reconcileRunnerStatuses([]forgeRunner{s.busySnapshot(instance)}))
	completed, err := s.store.GetInstance(s.adminCtx, instance.Name)
	s.Require().NoError(err)
	s.Equal(params.RunnerTerminated, completed.RunnerStatus)
	s.Equal(commonParams.InstancePendingDelete, completed.Status)

	// Once the job finishes, the normal provider deletion path must leave only
	// the replacement idle runner. Reconciliation must not keep spent capacity.
	s.providerMock.On("DeleteInstance", mock.Anything, instance.Name, mock.Anything).Return(nil).Once()
	s.Require().NoError(s.mgr.deletePendingInstances())
	const deletionTimeout = 10 * time.Second
	const deletionPollInterval = 20 * time.Millisecond
	s.Require().Eventually(func() bool {
		remaining, listErr := s.store.ListPoolInstances(s.adminCtx, pool.ID, false)
		return listErr == nil && len(remaining) == 1 && remaining[0].Name != instance.Name
	}, deletionTimeout, deletionPollInterval)
	remaining, err := s.store.ListPoolInstances(s.adminCtx, pool.ID, false)
	s.Require().NoError(err)
	s.Require().Len(remaining, 1)
	_, err = s.store.ForceUpdateInstance(s.adminCtx, remaining[0].Name, params.UpdateInstanceParams{
		Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle,
	})
	s.Require().NoError(err)
	for range 2 {
		s.Require().NoError(s.mgr.reconcileRunnerStatuses([]forgeRunner{s.busySnapshot(instance)}))
		s.Require().NoError(s.mgr.ensureIdleRunnersForOnePool(pool))
	}
	remaining, err = s.store.ListPoolInstances(s.adminCtx, pool.ID, false)
	s.Require().NoError(err)
	s.Require().Len(remaining, 1, "completed work must settle at min_idle=1")
	s.Equal(params.RunnerIdle, remaining[0].RunnerStatus)
}

func (s *PoolStressTestSuite) TestReconcileRunnerStatusesFiltersAndLocks() {
	instance, err := s.store.CreateInstance(s.adminCtx, s.pool.ID, params.CreateInstanceParams{
		Name: "busy-filtered", Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle, AgentID: 12345,
	})
	s.Require().NoError(err)
	for _, scenario := range []string{string(params.RunnerIdle), string(params.RunnerOffline), string(params.RunnerUnknown), "foreign_controller", "foreign_entity", "agent_mismatch", "locked", "missing"} {
		s.Run(scenario, func() {
			runner := s.busySnapshot(instance)
			switch scenario {
			case string(params.RunnerIdle), string(params.RunnerOffline), string(params.RunnerUnknown):
				runner.Status = scenario
			case "foreign_controller":
				runner.Labels = []RunnerLabels{{Name: "runner-controller-id-other"}}
			case "foreign_entity":
				original := s.mgr.entity
				s.mgr.entity.ID = uuid.NewString()
				defer func() { s.mgr.entity = original }()
			case "agent_mismatch":
				runner.ID++
			case "locked":
				s.Require().True(locking.TryLock(instance.Name, "other-operation"))
				defer locking.Unlock(instance.Name, false)
			case "missing":
				runner.Name = "missing"
			}
			s.Require().NoError(s.mgr.reconcileRunnerStatuses([]forgeRunner{runner}))
			after, err := s.store.GetInstance(s.adminCtx, instance.Name)
			s.Require().NoError(err)
			s.Equal(params.RunnerIdle, after.RunnerStatus)
		})
	}
	s.Require().NoError(s.mgr.reconcileRunnerStatuses([]forgeRunner{s.busySnapshot(instance)}))
	s.Require().True(locking.TryLock(instance.Name, "verify-release"))
	locking.Unlock(instance.Name, false)
}

func (s *PoolStressTestSuite) TestRunnerCleanupReconcilesBusySnapshot() {
	instance, err := s.store.CreateInstance(s.adminCtx, s.pool.ID, params.CreateInstanceParams{
		Name: "busy-through-cleanup", Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle, AgentID: 12345,
	})
	s.Require().NoError(err)
	s.ghcliMock.On("ListEntityRunners", mock.Anything, mock.Anything).Return(&github.Runners{Runners: []*github.Runner{{
		ID: github.Ptr(int64(12345)), Name: github.Ptr(instance.Name), Status: github.Ptr("online"), Busy: github.Ptr(true),
		Labels: []*github.RunnerLabels{{Name: github.Ptr(s.mgr.controllerLabel())}},
	}}}, &github.Response{Response: &http.Response{StatusCode: http.StatusOK}}, nil).Once()
	s.Require().NoError(s.mgr.runnerCleanup())
	after, err := s.store.GetInstance(s.adminCtx, instance.Name)
	s.Require().NoError(err)
	s.Equal(params.RunnerActive, after.RunnerStatus)
}

// Interpose a concurrent lifecycle write immediately before the real store update.
type reconciliationHookStore struct {
	dbCommon.Store
	reconcile func(context.Context, params.ForgeEntity, params.Instance, int64) (bool, error)
	listCalls int
}

func (s *reconciliationHookStore) ReconcileInstanceRunnerActive(ctx context.Context, entity params.ForgeEntity, instance params.Instance, agentID int64) (bool, error) {
	return s.reconcile(ctx, entity, instance, agentID)
}

func (s *reconciliationHookStore) ListEntityInstances(ctx context.Context, entity params.ForgeEntity) ([]params.Instance, error) {
	s.listCalls++
	return s.Store.ListEntityInstances(ctx, entity)
}

func (s *PoolStressTestSuite) TestReconcileRunnerStatusesCompletionWinsDuringUpdate() {
	instance, err := s.store.CreateInstance(s.adminCtx, s.pool.ID, params.CreateInstanceParams{
		Name: "completed-during-reconcile", Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle, AgentID: 12345,
	})
	s.Require().NoError(err)
	s.mgr.store = &reconciliationHookStore{Store: s.store, reconcile: func(ctx context.Context, entity params.ForgeEntity, expected params.Instance, agentID int64) (bool, error) {
		job := s.makeWorkflowJob(9877, "completed", "completed", instance.Name, []string{"self-hosted", "linux", "x64"})
		s.Require().NoError(s.mgr.HandleWorkflowJob(job))
		return s.store.ReconcileInstanceRunnerActive(ctx, entity, expected, agentID)
	}}
	s.Require().NoError(s.mgr.reconcileRunnerStatuses([]forgeRunner{s.busySnapshot(instance)}))
	after, err := s.store.GetInstance(s.adminCtx, instance.Name)
	s.Require().NoError(err)
	s.Equal(params.RunnerTerminated, after.RunnerStatus)
	s.Equal(commonParams.InstancePendingDelete, after.Status)
}

func (s *PoolStressTestSuite) TestRunnerCleanupContinuesAfterReconciliationError() {
	injectedErr := errors.New("injected busy update failure")
	store := &reconciliationHookStore{Store: s.store, reconcile: func(ctx context.Context, entity params.ForgeEntity, instance params.Instance, agentID int64) (bool, error) {
		if instance.Name == "first-busy-runner" {
			return false, injectedErr
		}
		return s.store.ReconcileInstanceRunnerActive(ctx, entity, instance, agentID)
	}}
	s.mgr.store = store
	remote := &github.Runners{}
	for i, name := range []string{"first-busy-runner", "second-busy-runner"} {
		agentID := int64(12345 + i)
		_, err := s.store.CreateInstance(s.adminCtx, s.pool.ID, params.CreateInstanceParams{
			Name: name, Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle, AgentID: agentID,
		})
		s.Require().NoError(err)
		remote.Runners = append(remote.Runners, &github.Runner{
			ID: github.Ptr(agentID), Name: github.Ptr(name), Status: github.Ptr("online"), Busy: github.Ptr(true),
			Labels: []*github.RunnerLabels{{Name: github.Ptr(s.mgr.controllerLabel())}},
		})
	}
	s.ghcliMock.On("ListEntityRunners", mock.Anything, mock.Anything).Return(remote, &github.Response{}, nil).Once()
	s.ErrorIs(s.mgr.runnerCleanup(), injectedErr)
	s.Equal(2, store.listCalls, "both reap and orphan cleanup must still run")
	after, err := s.store.GetInstance(s.adminCtx, "second-busy-runner")
	s.Require().NoError(err)
	s.Equal(params.RunnerActive, after.RunnerStatus)
	for _, runner := range remote.Runners {
		s.Require().True(locking.TryLock(runner.GetName(), "verify-release"))
		locking.Unlock(runner.GetName(), false)
	}
}

func (s *PoolStressTestSuite) TestRunnerCleanupDoesNotUseFailedSnapshot() {
	instance, err := s.store.CreateInstance(s.adminCtx, s.pool.ID, params.CreateInstanceParams{
		Name: "api-unavailable", Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle, AgentID: 12345,
	})
	s.Require().NoError(err)
	injectedErr := errors.New("GitHub unavailable")
	s.ghcliMock.On("ListEntityRunners", mock.Anything, mock.Anything).Return((*github.Runners)(nil), &github.Response{Response: &http.Response{StatusCode: http.StatusServiceUnavailable}}, injectedErr).Once()
	s.ErrorIs(s.mgr.runnerCleanup(), injectedErr)
	after, err := s.store.GetInstance(s.adminCtx, instance.Name)
	s.Require().NoError(err)
	s.Equal(instance.RunnerStatus, after.RunnerStatus)
	s.Equal(instance.Status, after.Status)
	s.Equal(instance.UpdatedAt, after.UpdatedAt)
}

func (s *PoolStressTestSuite) TestGetGithubRunnersSupplementsScaleSetBusyState() {
	for _, restFails := range []bool{false, true} {
		s.Run(fmt.Sprintf("rest_failure=%t", restFails), func() {
			client := runnerMocks.NewGithubClient(s.T())
			var serviceURL string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				var payload any
				if req.URL.Path == "/actions/runner-registration" {
					payload = params.ActionsServiceAdminInfoResponse{URL: serviceURL, Token: "test-admin-token"}
				} else {
					// The real scale-set list can omit busy even while REST reports it.
					payload = params.RunnerReferenceList{Count: 2, RunnerReferences: []params.RunnerReference{
						{ID: 1, Name: "working", Status: string(params.RunnerOnline)},
						{ID: 2, Name: "missing-from-rest-page", Status: string(params.RunnerOnline)},
					}}
				}
				if err := json.NewEncoder(w).Encode(payload); err != nil {
					s.T().Error(err)
				}
			}))
			defer server.Close()
			serviceURL = server.URL
			baseURL, err := url.Parse(serviceURL)
			s.Require().NoError(err)
			client.On("GetEntity").Return(s.entity)
			client.On("GithubBaseURL").Return(baseURL)
			client.On("CreateEntityRegistrationToken", mock.Anything).Return(&github.RegistrationToken{
				Token: github.Ptr("test-registration-token"), ExpiresAt: &github.Timestamp{Time: time.Now().Add(time.Hour)},
			}, &github.Response{}, nil).Once()
			if restFails {
				client.On("ListEntityRunners", mock.Anything, mock.Anything).Return((*github.Runners)(nil), (*github.Response)(nil), errors.New("REST unavailable")).Once()
			} else {
				client.On("ListEntityRunners", mock.Anything, mock.Anything).Return(&github.Runners{Runners: []*github.Runner{
					{ID: github.Ptr(int64(1)), Name: github.Ptr("working"), Status: github.Ptr(string(params.RunnerOnline)), Busy: github.Ptr(true)},
					{ID: github.Ptr(int64(3)), Name: github.Ptr("missing-from-scaleset-list"), Status: github.Ptr(string(params.RunnerOnline)), Busy: github.Ptr(true)},
				}}, &github.Response{}, nil).Once()
			}
			ss, err := scalesets.NewClient(client)
			s.Require().NoError(err)
			mgr := &basePoolManager{ctx: s.adminCtx, entity: s.entity, ghcli: client, scaleSetClient: ss}
			runners, err := mgr.GetGithubRunners()
			s.Require().NoError(err)
			byName := make(map[string]forgeRunner)
			for _, runner := range runners {
				byName[runner.Name] = runner
			}
			s.Contains(byName, "missing-from-rest-page", "REST pagination gaps must not remove cleanup snapshot entries")
			if restFails {
				s.Len(runners, 2)
				s.Equal(string(params.RunnerIdle), byName["working"].Status)
			} else {
				s.Len(runners, 3)
				s.Equal(string(params.RunnerActive), byName["working"].Status)
				s.Equal(string(params.RunnerActive), byName["missing-from-scaleset-list"].Status)
			}
		})
	}
}
