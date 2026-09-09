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
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/go-github/v84/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	dbMocks "github.com/cloudbase/garm/database/common/mocks"
	"github.com/cloudbase/garm/params"
	"github.com/cloudbase/garm/runner/common"
	runnerMocks "github.com/cloudbase/garm/runner/common/mocks"
)

func (s *PoolStressTestSuite) TestQueuedDemandReconciliation() {
	for i, status := range []string{string(params.JobStatusQueued), string(params.JobStatusInProgress), string(params.JobStatusCompleted), "unexpected", ""} {
		s.Run(status, func() {
			s.mgr.jobChecks = &jobCheckCoordinator{}
			job := params.Job{
				WorkflowJobID: int64(778800 + i), Status: string(params.JobStatusQueued), Action: string(params.JobStatusQueued),
				RepositoryOwner: "test-owner", RepositoryName: "test-repo", RepoID: github.Ptr(uuid.MustParse(s.entity.ID)),
				Labels: []string{"self-hosted", "linux", "x64"},
			}
			job, err := s.store.CreateOrUpdateJob(s.adminCtx, job)
			s.Require().NoError(err)
			job.CreatedAt = time.Now().Add(-2 * time.Minute)
			job.UpdatedAt = job.CreatedAt
			s.ghcliMock.On("GetWorkflowJobByID", mock.Anything, job.RepositoryOwner, job.RepositoryName, job.WorkflowJobID).
				Return(&github.WorkflowJob{ID: github.Ptr(job.WorkflowJobID), Status: github.Ptr(status), Conclusion: github.Ptr("success")}, (*github.Response)(nil), nil).Once()
			s.Equal(status == string(params.JobStatusQueued), s.mgr.queuedDemandIsCurrent(job))
			// Both callers share the cached observation, without a second request.
			s.Equal(status == string(params.JobStatusQueued), s.mgr.queuedDemandIsCurrent(job))
			actual, err := s.store.GetJobByID(s.adminCtx, job.WorkflowJobID)
			s.Require().NoError(err)
			expected := string(params.JobStatusQueued)
			if status == string(params.JobStatusCompleted) || status == string(params.JobStatusInProgress) {
				expected = status
			}
			s.Equal(expected, actual.Status)
			if status != string(params.JobStatusQueued) {
				// Stale watcher snapshot must not drive provider creation.
				s.mgr.jobs = map[int64]params.Job{job.WorkflowJobID: job}
				s.Require().NoError(s.mgr.consumeQueuedJobs())
				instances, err := s.store.ListPoolInstances(s.adminCtx, s.pool.ID, false)
				s.Require().NoError(err)
				s.Empty(instances)
			}
		})
	}
}

func (s *PoolStressTestSuite) TestQueuedCheckErrorsAndBudget() {
	for _, code := range []int{404, 403, 429, 500} {
		s.mgr.jobChecks = &jobCheckCoordinator{}
		job := params.Job{WorkflowJobID: int64(code), RepositoryOwner: "owner", RepositoryName: "repo", CreatedAt: time.Now().Add(-time.Hour)}
		s.ghcliMock.On("GetWorkflowJobByID", mock.Anything, "owner", "repo", job.WorkflowJobID).
			Return((*github.WorkflowJob)(nil), &github.Response{Response: &http.Response{StatusCode: code, Header: http.Header{"Retry-After": []string{"120"}}}}, errors.New("unavailable")).Once()
		s.False(s.mgr.queuedDemandIsCurrent(job))
		s.False(s.mgr.queuedDemandIsCurrent(job))
		if code == 403 || code == 429 {
			s.True(time.Until(s.mgr.jobChecks.blockedUntil) > time.Minute)
		}
	}
	s.mgr.jobChecks = &jobCheckCoordinator{window: time.Now(), requests: jobCheckBudget}
	s.False(s.mgr.queuedDemandIsCurrent(params.Job{WorkflowJobID: 99, RepositoryOwner: "owner", RepositoryName: "repo"}))
	// Fresh webhook demand does not require REST availability.
	s.True(s.mgr.queuedDemandIsCurrent(params.Job{CreatedAt: time.Now()}))
}

func TestQueuedChecksDeduplicateConcurrentCallers(t *testing.T) {
	client := runnerMocks.NewGithubClient(t)
	coordinator := &jobCheckCoordinator{}
	entered, release := make(chan struct{}), make(chan struct{})
	job := params.Job{WorkflowJobID: 555, RepositoryOwner: "owner", RepositoryName: "repo"}
	client.On("GetWorkflowJobByID", mock.Anything, "owner", "repo", int64(555)).
		Run(func(mock.Arguments) { close(entered); <-release }).
		Return(&github.WorkflowJob{ID: github.Ptr(int64(555)), Status: github.Ptr(string(params.JobStatusQueued))}, (*github.Response)(nil), nil).Once()
	mgr := &basePoolManager{ctx: t.Context(), ghcli: client, jobChecks: coordinator}
	done := make(chan *github.WorkflowJob, 1)
	go func() { done <- mgr.checkQueuedJob(job) }()
	<-entered
	require.Nil(t, mgr.checkQueuedJob(job))
	close(release)
	require.Equal(t, string(params.JobStatusQueued), (<-done).GetStatus())
	require.Equal(t, string(params.JobStatusQueued), mgr.checkQueuedJob(job).GetStatus())
}

func TestQueuedChecksReserveBudgetForOtherEntities(t *testing.T) {
	c := &jobCheckCoordinator{window: time.Now(), requests: jobCheckBudget}
	client := runnerMocks.NewGithubClient(t)
	first := &basePoolManager{ctx: t.Context(), ghcli: client, jobChecks: c}
	second := &basePoolManager{ctx: t.Context(), ghcli: client, jobChecks: c}
	older := params.Job{WorkflowJobID: 100, RepositoryOwner: "owner", RepositoryName: "other-repo"}
	newer := params.Job{WorkflowJobID: 101, RepositoryOwner: "owner", RepositoryName: "repo"}
	require.Nil(t, second.checkQueuedJob(older))
	c.window = time.Now().Add(-time.Minute)
	// Even if first runs first after budget reset, the older waiter gets priority.
	require.Nil(t, first.checkQueuedJob(newer))
	client.On("GetWorkflowJobByID", mock.Anything, "owner", "other-repo", int64(100)).
		Return(&github.WorkflowJob{ID: github.Ptr(int64(100)), Status: github.Ptr(string(params.JobStatusQueued))}, (*github.Response)(nil), nil).Once()
	require.NotNil(t, second.checkQueuedJob(older))
	client.On("GetWorkflowJobByID", mock.Anything, "owner", "repo", int64(101)).
		Return(&github.WorkflowJob{ID: github.Ptr(int64(101)), Status: github.Ptr(string(params.JobStatusQueued))}, (*github.Response)(nil), nil).Once()
	require.NotNil(t, first.checkQueuedJob(newer))
}

func (s *PoolStressTestSuite) TestQueuedDemandStillCreatesRunner() {
	s.setupProviderMocks()
	job, err := s.store.CreateOrUpdateJob(s.adminCtx, params.Job{
		WorkflowJobID: 990012, Status: string(params.JobStatusQueued), Action: string(params.JobStatusQueued),
		RepositoryOwner: "test-owner", RepositoryName: "test-repo", RepoID: github.Ptr(uuid.MustParse(s.entity.ID)),
		Labels: []string{"self-hosted", "linux", "x64"},
	})
	s.Require().NoError(err)
	job.CreatedAt, job.UpdatedAt = time.Now().Add(-2*time.Minute), time.Now().Add(-2*time.Minute)
	s.mgr.jobs = map[int64]params.Job{job.WorkflowJobID: job}
	s.ghcliMock.On("GetWorkflowJobByID", mock.Anything, job.RepositoryOwner, job.RepositoryName, job.WorkflowJobID).
		Return(&github.WorkflowJob{ID: github.Ptr(job.WorkflowJobID), Status: github.Ptr(string(params.JobStatusQueued))}, (*github.Response)(nil), nil).Once()
	s.Require().NoError(s.mgr.consumeQueuedJobs())
	instances, err := s.store.ListPoolInstances(s.adminCtx, s.pool.ID, false)
	s.Require().NoError(err)
	s.Len(instances, 1)
}

func TestJobObservationIdentityAndRetryHeaders(t *testing.T) {
	require.False(t, jobResponseRateLimited(&github.Response{Response: &http.Response{StatusCode: http.StatusForbidden}}))
	job := params.Job{WorkflowJobID: 123, RunID: 456}
	remote := &github.WorkflowJob{ID: github.Ptr(int64(123)), RunID: github.Ptr(int64(456)), Status: github.Ptr(string(params.JobStatusQueued))}
	require.True(t, validJobObservation(job, remote))
	remote.ID = github.Ptr(int64(124))
	require.False(t, validJobObservation(job, remote))
	remote.ID, remote.RunID = github.Ptr(int64(123)), github.Ptr(int64(457))
	require.False(t, validJobObservation(job, remote))
	require.False(t, validJobObservation(job, nil))
	now := time.Now().UTC().Truncate(time.Second)
	response := &github.Response{Response: &http.Response{Header: http.Header{"Retry-After": []string{now.Add(3 * time.Minute).UTC().Format(http.TimeFormat)}}}}
	require.Equal(t, now.Add(3*time.Minute), jobRetryDeadline(response, now, now.Add(time.Minute)))
}

func TestBackgroundJobScanUsesMinuteIntervalAndSharedBudget(t *testing.T) {
	require.Equal(t, time.Minute, common.PoolStaleJobReconcileInterval)
	store := dbMocks.NewStore(t)
	client := runnerMocks.NewGithubClient(t)
	mgr := &basePoolManager{ctx: t.Context(), store: store, ghcli: client, jobChecks: &jobCheckCoordinator{}}
	jobs := make([]params.Job, jobCheckBudget+1)
	for i := range jobs {
		jobs[i] = params.Job{ID: int64(i + 1), WorkflowJobID: int64(i + 1), RepositoryOwner: "owner", RepositoryName: "repo", CreatedAt: time.Now().Add(-2 * time.Minute)}
		if i < jobCheckBudget {
			client.On("GetWorkflowJobByID", mock.Anything, "owner", "repo", int64(i+1)).
				Return(&github.WorkflowJob{ID: github.Ptr(int64(i + 1)), Status: github.Ptr(string(params.JobStatusCompleted))}, (*github.Response)(nil), nil).Once()
		}
	}
	store.On("ListEntityJobsByStatus", mock.Anything, mgr.entity.EntityType, mgr.entity.ID, params.JobStatusQueued).Return(jobs, nil).Once()
	store.On("ReconcileQueuedJob", mock.Anything, mgr.entity, mock.Anything).Return(true, nil).Times(jobCheckBudget)
	require.NoError(t, mgr.reconcileStaleJobs())
	require.Equal(t, jobCheckBudget, mgr.jobChecks.requests)
}

func (s *PoolStressTestSuite) TestQueuedReconciliationPreservesIdentityAndTerminalState() {
	job, err := s.store.CreateOrUpdateJob(s.adminCtx, params.Job{
		WorkflowJobID: 990011, Status: string(params.JobStatusQueued), Action: string(params.JobStatusQueued),
		RepositoryOwner: "test-owner", RepositoryName: "test-repo", RepoID: github.Ptr(uuid.MustParse(s.entity.ID)),
		Name: "preserve-name", Labels: []string{"keep-label"},
	})
	s.Require().NoError(err)
	job.Status = string(params.JobStatusCompleted)
	job.Conclusion = "success"
	wrongEntity := s.entity
	wrongEntity.ID = uuid.NewString()
	changed, err := s.store.ReconcileQueuedJob(s.adminCtx, wrongEntity, job)
	s.Require().NoError(err)
	s.False(changed)
	wrongJob := job
	wrongJob.ID++
	changed, err = s.store.ReconcileQueuedJob(s.adminCtx, s.entity, wrongJob)
	s.Require().NoError(err)
	s.False(changed)
	changed, err = s.store.ReconcileQueuedJob(s.adminCtx, s.entity, job)
	s.Require().NoError(err)
	s.True(changed)
	job.Status = string(params.JobStatusInProgress)
	changed, err = s.store.ReconcileQueuedJob(s.adminCtx, s.entity, job)
	s.Require().NoError(err)
	s.False(changed)
	actual, err := s.store.GetJobByID(s.adminCtx, job.WorkflowJobID)
	s.Require().NoError(err)
	s.Equal(string(params.JobStatusCompleted), actual.Status)
	s.Equal("success", actual.Conclusion)
	s.Equal("preserve-name", actual.Name)
	s.Equal([]string{"keep-label"}, actual.Labels)
}
