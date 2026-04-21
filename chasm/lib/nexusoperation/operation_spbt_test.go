package nexusoperation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/types/known/durationpb"
	"pgregory.net/rapid"
)

// NexusOperationMachine is the rapid state machine under test.
// It drives the real Operation and Cancellation implementations and keeps
// an opModel as the reference to check invariants against after every action.
type NexusOperationMachine struct {
	model        opModel
	op           *Operation
	cancellation *Cancellation // non-nil once RequestCancellation fires
	ctx          *chasm.MockMutableContext
	now          time.Time
}

// freshCtx advances the clock by a generated amount and returns a clean context
// whose Tasks slice is empty, so per-action task assertions are unambiguous.
func (m *NexusOperationMachine) freshCtx(t *rapid.T) *chasm.MockMutableContext {
	delta := rapid.IntRange(1, 60).Draw(t, "timeDeltaSec")
	m.now = m.now.Add(time.Duration(delta) * time.Second)
	m.ctx = newMockCtx(m.now)
	return m.ctx
}

// check verifies invariants. Called after every step and at end of test.
func (m *NexusOperationMachine) check(t *rapid.T) {
	t.Helper()
	op := m.op

	require.Equal(t, m.model.status, op.Status)
	require.Equal(t, m.model.attempt, op.Attempt)
	require.Equal(t, expectedLifecycleOp(m.model.status), op.LifecycleState(m.ctx))

	// NextAttemptScheduleTime must be set iff in BACKING_OFF
	inBackoff := m.model.status == nexusoperationpb.OPERATION_STATUS_BACKING_OFF
	require.Equal(t, inBackoff, op.NextAttemptScheduleTime != nil,
		"NextAttemptScheduleTime set=%v but status=%v", op.NextAttemptScheduleTime != nil, m.model.status)

	require.Equal(t, m.model.closedTimeSet, op.ClosedTime != nil)

	if m.model.startedTimeSet {
		require.NotNil(t, op.StartedTime)
	}
	if m.model.operationTokenSet {
		require.NotEmpty(t, op.OperationToken)
	}

	if !m.model.lastAttemptFailureSet {
		require.Nil(t, op.LastAttemptFailure)
	} else {
		require.NotNil(t, op.LastAttemptFailure)
	}

	if m.model.hasCancellation {
		c, ok := op.Cancellation.TryGet(m.ctx)
		require.True(t, ok, "Cancellation field must be set after RequestCancellation")
		require.Equal(t, m.model.cancelStatus, c.Status)
		require.Equal(t, m.model.cancelAttempt, c.Attempt)
		require.Equal(t, expectedLifecycleCancel(m.model.cancelStatus), c.LifecycleState(m.ctx))

		cancelInBackoff := m.model.cancelStatus == nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF
		require.Equal(t, cancelInBackoff, c.NextAttemptScheduleTime != nil,
			"cancel NextAttemptScheduleTime set=%v but cancelStatus=%v",
			c.NextAttemptScheduleTime != nil, m.model.cancelStatus)

		if !m.model.cancelLastFail {
			require.Nil(t, c.LastAttemptFailure)
		} else {
			require.NotNil(t, c.LastAttemptFailure)
		}
	}
}

// ── Actions ───────────────────────────────────────────────────────────────────


func (m *NexusOperationMachine) AttemptFailed(t *rapid.T) {
	if m.model.status != nexusoperationpb.OPERATION_STATUS_SCHEDULED {
		t.Skip("not SCHEDULED")
	}
	ctx := m.freshCtx(t)

	failure := &failurepb.Failure{Message: "retryable error"}
	err := transitionAttemptFailed.Apply(m.op, ctx, EventAttemptFailed{
		Failure:     failure,
		RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
	})
	require.NoError(t, err)

	require.Len(t, ctx.Tasks, 1)
	bt, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.InvocationBackoffTask)
	require.True(t, ok, "expected InvocationBackoffTask")
	require.Equal(t, m.model.attempt, bt.Attempt)
	require.True(t, ctx.Tasks[0].Attributes.ScheduledTime.After(m.now))

	m.model.status = nexusoperationpb.OPERATION_STATUS_BACKING_OFF
	m.model.nextAttemptScheduleTimeSet = true
	m.model.lastAttemptFailureSet = true
}

func (m *NexusOperationMachine) Rescheduled(t *rapid.T) {
	if m.model.status != nexusoperationpb.OPERATION_STATUS_BACKING_OFF {
		t.Skip("not BACKING_OFF")
	}
	ctx := m.freshCtx(t)

	prevAttempt := m.model.attempt
	err := transitionRescheduled.Apply(m.op, ctx, EventRescheduled{})
	require.NoError(t, err)

	require.Len(t, ctx.Tasks, 1)
	inv, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.InvocationTask)
	require.True(t, ok, "expected InvocationTask")
	require.Equal(t, prevAttempt+1, inv.Attempt)

	m.model.status = nexusoperationpb.OPERATION_STATUS_SCHEDULED
	m.model.attempt = prevAttempt + 1
	m.model.nextAttemptScheduleTimeSet = false
	// lastAttemptFailureSet intentionally NOT cleared: Rescheduled does not reset it
}

func (m *NexusOperationMachine) Started(t *rapid.T) {
	if m.model.status != nexusoperationpb.OPERATION_STATUS_SCHEDULED &&
		m.model.status != nexusoperationpb.OPERATION_STATUS_BACKING_OFF {
		t.Skip("not SCHEDULED or BACKING_OFF")
	}
	ctx := m.freshCtx(t)

	token := rapid.StringN(1, 64, -1).Draw(t, "opToken")
	err := TransitionStarted.Apply(m.op, ctx, EventStarted{OperationToken: token})
	require.NoError(t, err)

	if m.op.StartToCloseTimeout != nil && m.op.StartToCloseTimeout.AsDuration() != 0 {
		var found bool
		for _, task := range ctx.Tasks {
			if _, ok := task.Payload.(*nexusoperationpb.StartToCloseTimeoutTask); ok {
				found = true
			}
		}
		require.True(t, found, "StartToCloseTimeoutTask must be emitted when s2c timeout set")
	}

	// Pending cancellation (UNSPECIFIED) is scheduled immediately on start
	if m.model.hasCancellation && m.model.cancelStatus == nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED {
		var found bool
		for _, task := range ctx.Tasks {
			if _, ok := task.Payload.(*nexusoperationpb.CancellationTask); ok {
				found = true
			}
		}
		require.True(t, found, "CancellationTask must be emitted when pending cancellation exists on Started")
		m.model.cancelStatus = nexusoperationpb.CANCELLATION_STATUS_SCHEDULED
		m.model.cancelAttempt++
	}

	m.model.status = nexusoperationpb.OPERATION_STATUS_STARTED
	m.model.nextAttemptScheduleTimeSet = false
	m.model.operationTokenSet = true
	m.model.startedTimeSet = true
	m.model.lastAttemptFailureSet = false // TransitionStarted clears LastAttemptFailure
}

func (m *NexusOperationMachine) Succeeded(t *rapid.T) {
	if !TransitionSucceeded.Possible(m.op) {
		t.Skip("TransitionSucceeded not possible")
	}
	ctx := m.freshCtx(t)

	err := TransitionSucceeded.Apply(m.op, ctx, EventSucceeded{})
	require.NoError(t, err)
	require.Empty(t, ctx.Tasks, "terminal transition must emit no tasks")

	m.model.status = nexusoperationpb.OPERATION_STATUS_SUCCEEDED
	m.model.nextAttemptScheduleTimeSet = false
	m.model.closedTimeSet = true
}

func (m *NexusOperationMachine) Failed(t *rapid.T) {
	if !TransitionFailed.Possible(m.op) {
		t.Skip("TransitionFailed not possible")
	}
	ctx := m.freshCtx(t)

	prevStatus := m.op.Status
	err := TransitionFailed.Apply(m.op, ctx, EventFailed{Failure: &failurepb.Failure{Message: "non-retryable"}})
	require.NoError(t, err)
	require.Empty(t, ctx.Tasks, "terminal transition must emit no tasks")

	m.model.status = nexusoperationpb.OPERATION_STATUS_FAILED
	m.model.nextAttemptScheduleTimeSet = false
	m.model.closedTimeSet = true
	// resolveUnsuccessfully records LastAttemptFailure only when coming from SCHEDULED
	if prevStatus == nexusoperationpb.OPERATION_STATUS_SCHEDULED {
		m.model.lastAttemptFailureSet = true
	}
}

func (m *NexusOperationMachine) Canceled(t *rapid.T) {
	if !TransitionCanceled.Possible(m.op) {
		t.Skip("TransitionCanceled not possible")
	}
	ctx := m.freshCtx(t)

	prevStatus := m.op.Status
	err := TransitionCanceled.Apply(m.op, ctx, EventCanceled{Failure: &failurepb.Failure{Message: "canceled"}})
	require.NoError(t, err)
	require.Empty(t, ctx.Tasks, "terminal transition must emit no tasks")

	m.model.status = nexusoperationpb.OPERATION_STATUS_CANCELED
	m.model.nextAttemptScheduleTimeSet = false
	m.model.closedTimeSet = true
	if prevStatus == nexusoperationpb.OPERATION_STATUS_SCHEDULED {
		m.model.lastAttemptFailureSet = true
	}
}

func (m *NexusOperationMachine) TimedOut(t *rapid.T) {
	if !TransitionTimedOut.Possible(m.op) {
		t.Skip("TransitionTimedOut not possible")
	}
	ctx := m.freshCtx(t)

	err := TransitionTimedOut.Apply(m.op, ctx, EventTimedOut{})
	require.NoError(t, err)
	require.Empty(t, ctx.Tasks, "terminal transition must emit no tasks")

	m.model.status = nexusoperationpb.OPERATION_STATUS_TIMED_OUT
	m.model.nextAttemptScheduleTimeSet = false
	m.model.closedTimeSet = true
}

// RequestCancellation calls op.Cancel and wires the Cancellation.Operation ParentPtr,
// which a real CHASM tree does automatically after the transaction closes.
func (m *NexusOperationMachine) RequestCancellation(t *rapid.T) {
	if !TransitionCanceled.Possible(m.op) {
		t.Skip("operation already completed")
	}
	if m.model.hasCancellation {
		t.Skip("cancellation already requested")
	}
	ctx := m.freshCtx(t)

	err := m.op.Cancel(ctx, nil)
	require.NoError(t, err)

	c, ok := m.op.Cancellation.TryGet(ctx)
	require.True(t, ok, "Cancellation field must be set after Cancel()")
	c.Operation = chasm.NewMockParentPtr(m.op)
	m.cancellation = c

	m.model.hasCancellation = true
	if m.op.Status == nexusoperationpb.OPERATION_STATUS_STARTED {
		// Cancel() calls TransitionCancellationScheduled immediately when op is STARTED
		m.model.cancelStatus = nexusoperationpb.CANCELLATION_STATUS_SCHEDULED
		m.model.cancelAttempt = 1
	} else {
		// Cancellation waits in UNSPECIFIED until operation reaches STARTED
		m.model.cancelStatus = nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED
		m.model.cancelAttempt = 0
	}
}

// RequestCancellationAgain verifies the second Cancel() call is rejected with
// ErrCancellationAlreadyRequested.
func (m *NexusOperationMachine) RequestCancellationAgain(t *rapid.T) {
	if !m.model.hasCancellation {
		t.Skip("no existing cancellation")
	}
	if isTerminalOp(m.model.status) {
		// When terminal, Cancel() returns ErrOperationAlreadyCompleted instead
		t.Skip("operation already terminal")
	}
	ctx := m.freshCtx(t)

	err := m.op.Cancel(ctx, nil)
	require.ErrorIs(t, err, ErrCancellationAlreadyRequested)
}

// CancelWhenTerminal verifies Cancel() on a completed operation returns
// ErrOperationAlreadyCompleted.
func (m *NexusOperationMachine) CancelWhenTerminal(t *rapid.T) {
	if !isTerminalOp(m.model.status) {
		t.Skip("operation not terminal")
	}
	ctx := m.freshCtx(t)

	err := m.op.Cancel(ctx, nil)
	require.ErrorIs(t, err, ErrOperationAlreadyCompleted)
}

func (m *NexusOperationMachine) CancellationSucceeded(t *rapid.T) {
	if !m.model.hasCancellation ||
		m.model.cancelStatus != nexusoperationpb.CANCELLATION_STATUS_SCHEDULED {
		t.Skip("cancellation not SCHEDULED")
	}
	ctx := m.freshCtx(t)

	err := TransitionCancellationSucceeded.Apply(m.cancellation, ctx, EventCancellationSucceeded{})
	require.NoError(t, err)
	require.Empty(t, ctx.Tasks, "terminal cancellation transition must emit no tasks")

	m.model.cancelStatus = nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED
	m.model.cancelLastFail = false // TransitionCancellationSucceeded explicitly clears failure
}

func (m *NexusOperationMachine) CancellationAttemptFailed(t *rapid.T) {
	if !m.model.hasCancellation ||
		m.model.cancelStatus != nexusoperationpb.CANCELLATION_STATUS_SCHEDULED {
		t.Skip("cancellation not SCHEDULED")
	}
	ctx := m.freshCtx(t)

	failure := &failurepb.Failure{Message: "transient cancel error"}
	err := transitionCancellationAttemptFailed.Apply(m.cancellation, ctx, EventCancellationAttemptFailed{
		Failure:     failure,
		RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
	})
	require.NoError(t, err)

	require.Len(t, ctx.Tasks, 1)
	bt, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.CancellationBackoffTask)
	require.True(t, ok, "expected CancellationBackoffTask")
	require.Equal(t, m.model.cancelAttempt, bt.Attempt)
	require.True(t, ctx.Tasks[0].Attributes.ScheduledTime.After(m.now))

	m.model.cancelStatus = nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF
	m.model.cancelNextSet = true
	m.model.cancelLastFail = true
}

func (m *NexusOperationMachine) CancellationRescheduled(t *rapid.T) {
	if !m.model.hasCancellation ||
		m.model.cancelStatus != nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF {
		t.Skip("cancellation not BACKING_OFF")
	}
	ctx := m.freshCtx(t)

	prevAttempt := m.model.cancelAttempt
	err := transitionCancellationRescheduled.Apply(m.cancellation, ctx, EventCancellationRescheduled{})
	require.NoError(t, err)

	require.Len(t, ctx.Tasks, 1)
	ct, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.CancellationTask)
	require.True(t, ok, "expected CancellationTask")
	require.Equal(t, prevAttempt+1, ct.Attempt)

	m.model.cancelStatus = nexusoperationpb.CANCELLATION_STATUS_SCHEDULED
	m.model.cancelAttempt = prevAttempt + 1
	m.model.cancelNextSet = false
}

func (m *NexusOperationMachine) CancellationFailed(t *rapid.T) {
	if !m.model.hasCancellation ||
		!TransitionCancellationFailed.Possible(m.cancellation) {
		t.Skip("CancellationFailed not possible")
	}
	ctx := m.freshCtx(t)

	err := TransitionCancellationFailed.Apply(m.cancellation, ctx, EventCancellationFailed{
		Failure: &failurepb.Failure{Message: "permanent cancel failure"},
	})
	require.NoError(t, err)
	require.Empty(t, ctx.Tasks, "terminal cancellation transition must emit no tasks")

	m.model.cancelStatus = nexusoperationpb.CANCELLATION_STATUS_FAILED
	m.model.cancelLastFail = true
}

// dispatch samples only from currently-valid actions, avoiding rapid's
// "can't find a valid action" panic when the alphabetically-first action
// is invalid for the current state (e.g. BACKING_OFF, terminal).
func (m *NexusOperationMachine) dispatch(t *rapid.T) {
	t.Helper()

	type action struct {
		name string
		fn   func(*rapid.T)
	}

	// Terminal transitions valid from multiple states.
	terminals := []action{
		{"Succeeded", m.Succeeded},
		{"Failed", m.Failed},
		{"Canceled", m.Canceled},
		{"TimedOut", m.TimedOut},
	}

	var valid []action
	switch m.model.status {
	case nexusoperationpb.OPERATION_STATUS_SCHEDULED:
		valid = append(valid, action{"AttemptFailed", m.AttemptFailed})
		valid = append(valid, action{"Started", m.Started})
		valid = append(valid, terminals...)
	case nexusoperationpb.OPERATION_STATUS_BACKING_OFF:
		valid = append(valid, action{"Rescheduled", m.Rescheduled})
		valid = append(valid, action{"Started", m.Started})
		valid = append(valid, terminals...)
	case nexusoperationpb.OPERATION_STATUS_STARTED:
		valid = append(valid, terminals...)
	}

	// Cancellation actions, available from any non-terminal state.
	if !isTerminalOp(m.model.status) {
		if !m.model.hasCancellation {
			valid = append(valid, action{"RequestCancellation", m.RequestCancellation})
		} else if !isTerminalOp(m.model.status) {
			valid = append(valid, action{"RequestCancellationAgain", m.RequestCancellationAgain})
		}
	}

	// Cancellation sub-machine actions.
	if m.model.hasCancellation {
		switch m.model.cancelStatus {
		case nexusoperationpb.CANCELLATION_STATUS_SCHEDULED:
			valid = append(valid,
				action{"CancellationSucceeded", m.CancellationSucceeded},
				action{"CancellationAttemptFailed", m.CancellationAttemptFailed},
				action{"CancellationFailed", m.CancellationFailed},
			)
		case nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF:
			valid = append(valid, action{"CancellationRescheduled", m.CancellationRescheduled})
		}
	}

	if len(valid) == 0 {
		return // terminal or no-op state — caller's loop will break
	}

	chosen := rapid.SampledFrom(valid).Draw(t, "action")
	t.Log("action:", chosen.name)
	chosen.fn(t)
}

func TestNexusOperationSPBT(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		now := defaultTime
		op := newTestOperation()

		// Start in SCHEDULED state. The UNSPECIFIED → SCHEDULED entry transition is
		// covered by TestTransitionScheduled in operation_statemachine_test.go.
		op.ScheduleToCloseTimeout = durationpb.New(defaultScheduleToCloseTimeout)
		ctx := newMockCtx(now)
		if err := TransitionScheduled.Apply(op, ctx, EventScheduled{}); err != nil {
			t.Fatal("setup: TransitionScheduled failed:", err)
		}

		m := &NexusOperationMachine{
			model: opModel{
				status:  nexusoperationpb.OPERATION_STATUS_SCHEDULED,
				attempt: 1,
			},
			op:  op,
			ctx: newMockCtx(now),
			now: now,
		}

		steps := rapid.IntRange(1, 50).Draw(t, "steps")
		for range steps {
			m.check(t)
			if isTerminalOp(m.model.status) {
				break
			}
			m.dispatch(t)
		}
		m.check(t)
	})
}
