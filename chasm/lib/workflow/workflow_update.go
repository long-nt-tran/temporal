package workflow

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// WorkflowUpdate carries per-update callback state inside the CHASM workflow
// component.
//
// Callbacks are persisted her here only via UpdateAccepted or via
// UpdateAdmitted apply during reset/reapply.
// Rejections never persist callbacks so the "no persistence writes on reject"
// property is preserved.
//
// Lifetime: when a child Callback reaches a terminal status it removes
// itself via the RemoveCallback function implemented here. Once Callbacks is
// empty, the WorkflowUpdate entry removes itself from Workflow.Updates
// using the Parent pointer. As a result, workflow.Updates does not
// accumulate finished update callbacks over time.
type WorkflowUpdate struct {
	chasm.UnimplementedComponent

	*workflowpb.UpdateState

	// MSPointer is a special in-memory field for accessing the underlying mutable state.
	chasm.MSPointer

	// Parent points back to the enclosing Workflow component so this
	// update can drop itself from Workflow.Updates when its last callback
	// terminates.
	Parent chasm.ParentPtr[*Workflow]

	// Callbacks holds completion callbacks attached to this update. Each callback
	// removes itself on terminal transition via WorkflowUpdate.RemoveCallback.
	Callbacks chasm.Map[string, *callback.Callback]
}

func NewWorkflowUpdate(
	_ chasm.MutableContext, updateID string, msPointer chasm.MSPointer,
) *WorkflowUpdate {
	return &WorkflowUpdate{
		UpdateState: &workflowpb.UpdateState{
			UpdateId: updateID,
		},
		MSPointer: msPointer,
	}
}

// RemoveCallback drops a terminated callback from this update's Callbacks
// map and, if no callbacks remain, removes the update entry itself from the
// parent Workflow. Implements callback.CallbackParent, invoked by Callback
// from saveResult on terminal transition.
func (u *WorkflowUpdate) RemoveCallback(ctx chasm.MutableContext, c *callback.Callback) {
	for id, field := range u.Callbacks {
		if field.Get(ctx) == c {
			delete(u.Callbacks, id)
			break
		}
	}
	if len(u.Callbacks) > 0 {
		return
	}
	// Last callback gone: drop ourselves from Workflow.Updates so the
	// parent map doesn't carry an empty entry forever.
	if w, ok := u.Parent.TryGet(ctx); ok {
		delete(w.Updates, u.UpdateId)
	}
}

func (u *WorkflowUpdate) LifecycleState(
	_ chasm.Context,
) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (u *WorkflowUpdate) GetNexusCompletion(
	ctx chasm.Context,
	requestID string,
) (nexusrpc.CompleteOperationOptions, error) {
	// If the update was rejected, return the rejection failure directly instead
	// of looking up a completion event that doesn't exist.
	if rf := u.GetRejectionFailure(); rf != nil {
		f, err := commonnexus.TemporalFailureToNexusFailure(rf)
		if err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		opErr := &nexus.OperationError{
			Message: "update rejected",
			State:   nexus.OperationStateFailed,
			Cause:   &nexus.FailureError{Failure: f},
		}
		if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		return nexusrpc.CompleteOperationOptions{
			Error: opErr,
		}, nil
	}

	// Retrieve the completion data from the underlying mutable state via MSPointer
	return u.GetNexusUpdateCompletion(ctx, u.UpdateId, requestID)
}
