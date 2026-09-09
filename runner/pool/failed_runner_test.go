//go:build testing

package pool

import (
	"errors"
	"fmt"

	"github.com/stretchr/testify/mock"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/params"
)

func (s *PoolStressTestSuite) TestFailedRunnerCleanupPreservesStateOnGitHubError() {
	instance, err := s.store.CreateInstance(s.adminCtx, s.pool.ID, params.CreateInstanceParams{
		Name: "failed-github-unavailable", Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerFailed, AgentID: 12345,
	})
	s.Require().NoError(err)
	s.ghcliMock.On("ListEntityRunners", mock.Anything, mock.Anything).Return(nil, nil, errors.New("GitHub unavailable")).Once()
	s.Require().Error(s.mgr.runnerCleanup())
	current, err := s.store.GetInstance(s.adminCtx, instance.Name)
	s.Require().NoError(err)
	s.Equal(commonParams.InstanceRunning, current.Status)
	s.ghcliMock.AssertNotCalled(s.T(), "RemoveEntityRunner", mock.Anything, mock.Anything)
}

func (s *PoolStressTestSuite) TestMinIdleCountsOnlyViableWorkers() {
	s.setupProviderMocks()
	pool := s.pool
	pool.MinIdleRunners = 1
	pool.MaxRunners = 2
	for i, tc := range []struct {
		status commonParams.InstanceStatus
		runner params.RunnerStatus
		want   int
	}{
		{commonParams.InstanceRunning, params.RunnerIdle, 1},
		{commonParams.InstanceRunning, params.RunnerInstalling, 1},
		{commonParams.InstanceCreating, params.RunnerPending, 1},
		{commonParams.InstancePendingCreate, params.RunnerPending, 1},
		{commonParams.InstanceRunning, params.RunnerActive, 2},
		{commonParams.InstanceRunning, params.RunnerFailed, 2},
		{commonParams.InstanceRunning, params.RunnerUnknown, 2},
		{commonParams.InstanceRunning, params.RunnerTerminated, 2},
		{commonParams.InstancePendingDelete, params.RunnerIdle, 2},
		{commonParams.InstanceError, params.RunnerPending, 2},
	} {
		s.Run(fmt.Sprintf("%s-%s", tc.status, tc.runner), func() {
			_, err := s.store.CreateInstance(s.adminCtx, pool.ID, params.CreateInstanceParams{
				Name: fmt.Sprintf("capacity-%d", i), Status: tc.status, RunnerStatus: tc.runner,
			})
			s.Require().NoError(err)
			s.Require().NoError(s.mgr.ensureIdleRunnersForOnePool(pool))
			instances, err := s.store.ListPoolInstances(s.adminCtx, pool.ID, false)
			s.Require().NoError(err)
			s.Len(instances, tc.want)
			for _, instance := range instances {
				s.Require().NoError(s.store.DeleteInstance(s.adminCtx, pool.ID, instance.Name))
			}
		})
	}
}

func (s *PoolStressTestSuite) TestFailedRunnerDoesNotSatisfyMinIdle() {
	s.setupProviderMocks()
	pool := s.pool
	pool.MinIdleRunners = 1
	pool.MaxRunners = 2
	_, err := s.store.CreateInstance(s.adminCtx, pool.ID, params.CreateInstanceParams{
		Name: "failed-bootstrap", Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerFailed,
	})
	s.Require().NoError(err)
	s.Require().NoError(s.mgr.ensureIdleRunnersForOnePool(pool))
	instances, err := s.store.ListPoolInstances(s.adminCtx, pool.ID, false)
	s.Require().NoError(err)
	s.Require().Len(instances, 2)
	// A pending replacement counts toward min-idle; repeated scans do not flood the pool.
	s.Require().NoError(s.mgr.ensureIdleRunnersForOnePool(pool))
	instances, err = s.store.ListPoolInstances(s.adminCtx, pool.ID, false)
	s.Require().NoError(err)
	s.Len(instances, 2)
}

func (s *PoolStressTestSuite) TestFailedRunnerStillCountsTowardMaxCapacity() {
	s.setupProviderMocks()
	pool := s.pool
	pool.MinIdleRunners = 1
	pool.MaxRunners = 1
	_, err := s.store.CreateInstance(s.adminCtx, pool.ID, params.CreateInstanceParams{
		Name: "failed-at-capacity", Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerFailed,
	})
	s.Require().NoError(err)
	s.Require().NoError(s.mgr.ensureIdleRunnersForOnePool(pool))
	instances, err := s.store.ListPoolInstances(s.adminCtx, pool.ID, false)
	s.Require().NoError(err)
	s.Len(instances, 1)
}

func (s *PoolStressTestSuite) TestReapExplicitFailureBeforeBootstrapTimeout() {
	s.setupProviderMocks()
	for i, tc := range []struct {
		local      params.RunnerStatus
		remote     string
		wantDelete bool
	}{
		{params.RunnerFailed, "offline", true},
		{params.RunnerFailed, "", true},
		{params.RunnerFailed, "active", false},
		{params.RunnerFailed, "idle", false},
		{params.RunnerFailed, "unknown", false},
		{params.RunnerInstalling, "offline", false},
		{params.RunnerPending, "", false},
		{params.RunnerIdle, "offline", false},
	} {
		s.Run(fmt.Sprintf("%s-%s", tc.local, tc.remote), func() {
			instance, err := s.store.CreateInstance(s.adminCtx, s.pool.ID, params.CreateInstanceParams{
				Name: fmt.Sprintf("bootstrap-%d", i), Status: commonParams.InstanceRunning, RunnerStatus: tc.local, AgentID: 12345,
			})
			s.Require().NoError(err)
			var runners []forgeRunner
			if tc.remote != "" {
				runners = []forgeRunner{{ID: 12345, Name: instance.Name, Status: tc.remote, Labels: []RunnerLabels{{Name: s.mgr.controllerLabel()}}}}
			}
			s.Require().NoError(s.mgr.reapTimedOutRunners(runners))
			current, err := s.store.GetInstance(s.adminCtx, instance.Name)
			s.Require().NoError(err)
			want := commonParams.InstanceRunning
			if tc.wantDelete {
				want = commonParams.InstancePendingDelete
			}
			s.Equal(want, current.Status)
			s.Require().NoError(s.store.DeleteInstance(s.adminCtx, s.pool.ID, instance.Name))
		})
	}
}
