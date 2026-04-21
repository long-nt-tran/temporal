package nexusoperation

import (
	"time"

	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

// opModel is the reference model for an Operation. It tracks expected field presence
// alongside status so that Check() can verify invariants without needing to reimplement
// transition logic.
type opModel struct {
	status  nexusoperationpb.OperationStatus
	attempt int32

	// field-presence flags
	nextAttemptScheduleTimeSet bool
	operationTokenSet          bool
	closedTimeSet              bool
	startedTimeSet             bool
	lastAttemptFailureSet      bool // only set when transitioning terminal from SCHEDULED

	// cancellation sub-state
	hasCancellation bool
	cancelStatus    nexusoperationpb.CancellationStatus
	cancelAttempt   int32
	cancelNextSet   bool
	cancelLastFail  bool // whether LastAttemptFailure should be non-nil
}

// cancelModel is the reference model for a standalone Cancellation state machine test.
type cancelModel struct {
	status    nexusoperationpb.CancellationStatus
	attempt   int32
	nextSet   bool
	lastFail  bool
	lastTimeSet bool
}

func newMockCtx(now time.Time) *chasm.MockMutableContext {
	return &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return now },
		},
	}
}

func isTerminalOp(s nexusoperationpb.OperationStatus) bool {
	switch s {
	case nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
		nexusoperationpb.OPERATION_STATUS_FAILED,
		nexusoperationpb.OPERATION_STATUS_CANCELED,
		nexusoperationpb.OPERATION_STATUS_TIMED_OUT:
		return true
	}
	return false
}

func isTerminalCancel(s nexusoperationpb.CancellationStatus) bool {
	switch s {
	case nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED,
		nexusoperationpb.CANCELLATION_STATUS_FAILED,
		nexusoperationpb.CANCELLATION_STATUS_TIMED_OUT:
		return true
	}
	return false
}

func expectedLifecycleOp(s nexusoperationpb.OperationStatus) chasm.LifecycleState {
	switch s {
	case nexusoperationpb.OPERATION_STATUS_SUCCEEDED:
		return chasm.LifecycleStateCompleted
	case nexusoperationpb.OPERATION_STATUS_FAILED,
		nexusoperationpb.OPERATION_STATUS_CANCELED,
		nexusoperationpb.OPERATION_STATUS_TIMED_OUT:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

func expectedLifecycleCancel(s nexusoperationpb.CancellationStatus) chasm.LifecycleState {
	switch s {
	case nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED:
		return chasm.LifecycleStateCompleted
	case nexusoperationpb.CANCELLATION_STATUS_FAILED,
		nexusoperationpb.CANCELLATION_STATUS_TIMED_OUT:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}
