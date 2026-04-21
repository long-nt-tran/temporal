package nexusoperation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	"pgregory.net/rapid"
)

// CancellationMachine tests the Cancellation state machine in isolation.
// The parent Operation is pinned in STARTED so transitionCancellationRescheduled
// can resolve the endpoint via ParentPtr without requiring a full CHASM tree.
// The UNSPECIFIED → SCHEDULED entry transition is covered by TestNexusOperationSPBT.
type CancellationMachine struct {
	model cancelModel
	c     *Cancellation
	op    *Operation
	ctx   *chasm.MockMutableContext
	now   time.Time
}

func (m *CancellationMachine) freshCtx(t *rapid.T) *chasm.MockMutableContext {
	delta := rapid.IntRange(1, 60).Draw(t, "timeDeltaSec")
	m.now = m.now.Add(time.Duration(delta) * time.Second)
	m.ctx = newMockCtx(m.now)
	return m.ctx
}

// check verifies invariants. Called after every step and at end of test.
func (m *CancellationMachine) check(t *rapid.T) {
	t.Helper()
	c := m.c

	require.Equal(t, m.model.status, c.Status)
	require.Equal(t, m.model.attempt, c.Attempt)
	require.Equal(t, expectedLifecycleCancel(m.model.status), c.LifecycleState(m.ctx))

	inBackoff := m.model.status == nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF
	require.Equal(t, inBackoff, c.NextAttemptScheduleTime != nil,
		"NextAttemptScheduleTime set=%v but status=%v", c.NextAttemptScheduleTime != nil, m.model.status)

	if !m.model.lastFail {
		require.Nil(t, c.LastAttemptFailure)
	} else {
		require.NotNil(t, c.LastAttemptFailure)
	}

	if !m.model.lastTimeSet {
		require.Nil(t, c.LastAttemptCompleteTime)
	} else {
		require.NotNil(t, c.LastAttemptCompleteTime)
	}

	if isTerminalCancel(m.model.status) {
		require.True(t, c.LifecycleState(m.ctx).IsClosed())
	}
}

// ── Action implementations ────────────────────────────────────────────────────

func (m *CancellationMachine) attemptFailed(t *rapid.T) {
	ctx := m.freshCtx(t)

	failure := &failurepb.Failure{Message: "transient cancel error"}
	err := transitionCancellationAttemptFailed.Apply(m.c, ctx, EventCancellationAttemptFailed{
		Failure:     failure,
		RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
	})
	require.NoError(t, err)

	require.Len(t, ctx.Tasks, 1)
	bt, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.CancellationBackoffTask)
	require.True(t, ok, "expected CancellationBackoffTask")
	require.Equal(t, m.model.attempt, bt.Attempt)
	require.True(t, ctx.Tasks[0].Attributes.ScheduledTime.After(m.now))
	require.Equal(t, ctx.Tasks[0].Attributes.ScheduledTime, m.c.NextAttemptScheduleTime.AsTime())

	m.model.status = nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF
	m.model.nextSet = true
	m.model.lastFail = true
	m.model.lastTimeSet = true
}

func (m *CancellationMachine) rescheduled(t *rapid.T) {
	ctx := m.freshCtx(t)

	prevAttempt := m.model.attempt
	err := transitionCancellationRescheduled.Apply(m.c, ctx, EventCancellationRescheduled{})
	require.NoError(t, err)

	require.Len(t, ctx.Tasks, 1)
	ct, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.CancellationTask)
	require.True(t, ok, "expected CancellationTask")
	require.Equal(t, prevAttempt+1, ct.Attempt)
	require.Equal(t, m.op.GetEndpoint(), ctx.Tasks[0].Attributes.Destination,
		"destination must come from parent operation endpoint")

	m.model.status = nexusoperationpb.CANCELLATION_STATUS_SCHEDULED
	m.model.attempt = prevAttempt + 1
	m.model.nextSet = false
}

func (m *CancellationMachine) succeeded(t *rapid.T) {
	ctx := m.freshCtx(t)

	err := TransitionCancellationSucceeded.Apply(m.c, ctx, EventCancellationSucceeded{})
	require.NoError(t, err)
	require.Empty(t, ctx.Tasks, "terminal transition must emit no tasks")

	m.model.status = nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED
	m.model.lastFail = false // TransitionCancellationSucceeded explicitly clears failure
	m.model.lastTimeSet = true
}

func (m *CancellationMachine) failed(t *rapid.T) {
	ctx := m.freshCtx(t)

	err := TransitionCancellationFailed.Apply(m.c, ctx, EventCancellationFailed{
		Failure: &failurepb.Failure{Message: "permanent cancel error"},
	})
	require.NoError(t, err)
	require.Empty(t, ctx.Tasks, "terminal transition must emit no tasks")

	m.model.status = nexusoperationpb.CANCELLATION_STATUS_FAILED
	m.model.lastFail = true
	m.model.lastTimeSet = true
}

// step selects and runs one valid action for the current state.
// Uses dynamic action selection so rapid never draws an action that would skip,
// which avoids the "can't find a valid action" panic on minimal data.
func (m *CancellationMachine) step(t *rapid.T) {
	t.Helper()

	type action struct {
		name string
		fn   func(*rapid.T)
	}

	var valid []action
	switch m.model.status {
	case nexusoperationpb.CANCELLATION_STATUS_SCHEDULED:
		valid = []action{
			{"AttemptFailed", m.attemptFailed},
			{"Succeeded", m.succeeded},
			{"Failed", m.failed},
		}
	case nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF:
		valid = []action{
			{"Rescheduled", m.rescheduled},
		}
	}

	if len(valid) == 0 {
		return // terminal — caller's loop will break
	}

	chosen := rapid.SampledFrom(valid).Draw(t, "action")
	t.Log("action:", chosen.name)
	chosen.fn(t)
}

func TestCancellationSPBT(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		op := newTestOperation()
		op.Status = nexusoperationpb.OPERATION_STATUS_STARTED

		// Start in SCHEDULED / attempt=1 so that valid actions exist from step 1.
		c := newCancellation(&nexusoperationpb.CancellationState{
			Status:  nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
			Attempt: 1,
		})
		c.Operation = chasm.NewMockParentPtr(op)

		now := defaultTime
		m := &CancellationMachine{
			model: cancelModel{
				status:  nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
				attempt: 1,
			},
			c:   c,
			op:  op,
			ctx: newMockCtx(now),
			now: now,
		}

		// Explicit bounded loop so the test terminates cleanly at terminal states
		// without relying on t.Skip / t.Repeat, which panics when all actions skip.
		steps := rapid.IntRange(1, 50).Draw(t, "steps")
		for range steps {
			m.check(t)
			if isTerminalCancel(m.model.status) {
				break
			}
			m.step(t)
		}
		m.check(t)
	})
}
