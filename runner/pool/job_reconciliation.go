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
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/go-github/v84/github"

	"github.com/cloudbase/garm/params"
)

const (
	jobCheckGrace      = time.Minute
	jobCheckTTL        = time.Minute
	jobCheckBudget     = 5
	jobCheckTimeout    = 10 * time.Second
	jobCheckMaxBackoff = 15 * time.Minute
	jobCheckRetention  = time.Hour
)

type jobCheckEntry struct {
	checked      time.Time
	next         time.Time
	job          *github.WorkflowJob
	inflight     bool
	backoff      time.Duration
	waitingOrder uint64
	lastSeen     time.Time
}

// The controller-wide budget is shared by all entities and both callers. This
// deliberately caps aggregate traffic rather than multiplying a per-entity cap.
type jobCheckCoordinator struct {
	mu           sync.Mutex
	entries      map[string]jobCheckEntry
	window       time.Time
	requests     int
	blockedUntil time.Time
	sequence     uint64
}

var sharedJobChecks = &jobCheckCoordinator{}

func (r *basePoolManager) jobCheckState() *jobCheckCoordinator {
	if r.jobChecks != nil {
		return r.jobChecks
	}
	return sharedJobChecks
}

func (r *basePoolManager) jobCheckKey(job params.Job) string {
	return fmt.Sprintf("%s/%s/%s/%d", r.entity.Credentials.Endpoint.Name, job.RepositoryOwner, job.RepositoryName, job.WorkflowJobID)
}

func (r *basePoolManager) checkQueuedJob(job params.Job) *github.WorkflowJob {
	if job.WorkflowJobID <= 0 || job.RepositoryOwner == "" || job.RepositoryName == "" {
		return nil
	}
	c := r.jobCheckState()
	key := r.jobCheckKey(job)
	now := time.Now()
	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[string]jobCheckEntry)
	}
	for k, entry := range c.entries {
		if !entry.inflight && now.Sub(entry.checked) > jobCheckRetention && now.Sub(entry.lastSeen) > jobCheckRetention {
			delete(c.entries, k)
		}
	}
	entry := c.entries[key]
	if entry.inflight || now.Before(entry.next) {
		c.mu.Unlock()
		return entry.job
	}
	entry.lastSeen = now
	if entry.waitingOrder == 0 {
		c.sequence++
		entry.waitingOrder = c.sequence
	}
	c.entries[key] = entry
	// Reserve the next available slot for the oldest active waiter, including
	// waiters from other entity managers. Drop reservations from stopped callers.
	for otherKey, other := range c.entries {
		if otherKey != key && other.waitingOrder != 0 &&
			now.Sub(other.lastSeen) < 2*jobCheckTTL && other.waitingOrder < entry.waitingOrder {
			c.mu.Unlock()
			return nil
		}
	}
	if now.Sub(c.window) >= time.Minute {
		c.window, c.requests = now, 0
	}
	if now.Before(c.blockedUntil) || c.requests >= jobCheckBudget {
		c.mu.Unlock()
		return nil
	}
	c.requests++
	entry.inflight, entry.job = true, nil
	entry.waitingOrder = 0
	c.entries[key] = entry
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(r.ctx, jobCheckTimeout)
	defer cancel()
	remote, response, err := r.ghcli.GetWorkflowJobByID(ctx, job.RepositoryOwner, job.RepositoryName, job.WorkflowJobID)
	valid := err == nil && validJobObservation(job, remote)
	now = time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.inflight, entry.checked = false, now
	if valid {
		entry.job, entry.backoff, entry.next = remote, 0, now.Add(jobCheckTTL)
	} else {
		entry.backoff *= 2
		if entry.backoff < jobCheckTTL {
			entry.backoff = jobCheckTTL
		}
		if entry.backoff > jobCheckMaxBackoff {
			entry.backoff = jobCheckMaxBackoff
		}
		entry.next = now.Add(entry.backoff)
		// 404 can conceal authorization errors. Never infer a terminal job.
		slog.WarnContext(r.ctx, "could not verify queued demand; deferring expansion", "job_id", job.WorkflowJobID, "error", err)
		if jobResponseRateLimited(response) {
			until := jobRetryDeadline(response, now, entry.next)
			if until.After(c.blockedUntil) {
				c.blockedUntil = until
			}
			entry.next = until
		}
	}
	c.entries[key] = entry
	return entry.job
}

func jobRetryDeadline(response *github.Response, now, until time.Time) time.Time {
	if response.Response == nil {
		return until
	}
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
		if retry := now.Add(time.Duration(seconds) * time.Second); retry.After(until) {
			until = retry
		}
	} else if date, err := http.ParseTime(response.Header.Get("Retry-After")); err == nil && date.After(until) {
		until = date
	}
	if reset, err := strconv.ParseInt(response.Header.Get("X-RateLimit-Reset"), 10, 64); err == nil && response.Header.Get("X-RateLimit-Remaining") == "0" && time.Unix(reset, 0).After(until) {
		until = time.Unix(reset, 0)
	}
	return until
}

func jobResponseRateLimited(response *github.Response) bool {
	if response == nil || response.Response == nil {
		return false
	}
	return response.StatusCode == http.StatusTooManyRequests ||
		(response.StatusCode == http.StatusForbidden &&
			(response.Header.Get("Retry-After") != "" || response.Header.Get("X-RateLimit-Remaining") == "0"))
}

func validJobObservation(job params.Job, remote *github.WorkflowJob) bool {
	if remote == nil || remote.GetID() != job.WorkflowJobID || (job.RunID != 0 && remote.GetRunID() != job.RunID) {
		return false
	}
	switch remote.GetStatus() {
	case string(params.JobStatusQueued), string(params.JobStatusInProgress), string(params.JobStatusCompleted):
		return true
	default:
		return false
	}
}

// queuedDemandIsCurrent validates stale demand, without changing runner state.
func (r *basePoolManager) queuedDemandIsCurrent(job params.Job) bool {
	if time.Since(job.CreatedAt) < jobCheckGrace {
		return true
	}
	remote := r.checkQueuedJob(job)
	if remote == nil {
		return false
	}
	if remote.GetStatus() == string(params.JobStatusQueued) {
		return true
	}
	job.Status, job.Action = remote.GetStatus(), remote.GetStatus()
	job.Conclusion = remote.GetConclusion()
	if remote.StartedAt != nil {
		job.StartedAt = remote.StartedAt.Time
	}
	if remote.CompletedAt != nil {
		job.CompletedAt = remote.CompletedAt.Time
	}
	changed, err := r.store.ReconcileQueuedJob(r.ctx, r.entity, job)
	if err != nil {
		slog.ErrorContext(r.ctx, "failed to reconcile queued job", "job_id", job.WorkflowJobID, "error", err)
	} else if changed {
		slog.InfoContext(r.ctx, "reconciled queued job status", "job_id", job.WorkflowJobID, "github_status", job.Status)
	}
	return false
}

func (r *basePoolManager) reconcileStaleJobs() error {
	jobs, err := r.store.ListEntityJobsByStatus(r.ctx, r.entity.EntityType, r.entity.ID, params.JobStatusQueued)
	if err != nil {
		return err
	}
	c := r.jobCheckState()
	c.mu.Lock()
	// A recently checked queued job must not starve never-checked demand.
	sort.SliceStable(jobs, func(i, j int) bool {
		left, right := c.entries[r.jobCheckKey(jobs[i])].checked, c.entries[r.jobCheckKey(jobs[j])].checked
		if left.Equal(right) {
			return jobs[i].CreatedAt.Before(jobs[j].CreatedAt)
		}
		return left.Before(right)
	})
	c.mu.Unlock()
	for _, job := range jobs {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}
		r.queuedDemandIsCurrent(job)
	}
	return nil
}
