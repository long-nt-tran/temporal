package workflow

import (
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/service/history/historybuilder"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Workflow struct {
	chasm.UnimplementedComponent

	// For now, workflow state is managed by mutable_state_impl, not CHASM engine, leaving it empty as CHASM expects a
	// state object.
	*emptypb.Empty

	// MSPointer is a special in-memory field for accessing the underlying mutable state.
	chasm.MSPointer

	// Callbacks map is used to store the callbacks for the workflow.
	Callbacks chasm.Map[string, *callback.Callback]

	// Operations map is used to store the Nexus operations for the workflow, keyed by scheduled event ID.
	Operations chasm.Map[int64, *nexusoperation.Operation]

	// Updates indexed by update ID, used to store the update components.
	Updates chasm.Map[string, *WorkflowUpdate]
}

func NewWorkflow(
	_ chasm.MutableContext,
	msPointer chasm.MSPointer,
) *Workflow {
	return &Workflow{
		MSPointer: msPointer,
	}
}

func (w *Workflow) LifecycleState(
	_ chasm.Context,
) chasm.LifecycleState {
	// NOTE: closeTransactionHandleRootLifecycleChange() is bypassed in tree.go
	//
	// NOTE: detached mode is not implemented yet, so always return Running here.
	// Otherwise, tasks for callback component can't be executed after workflow is closed.
	return chasm.LifecycleStateRunning
}

func (w *Workflow) ContextMetadata(_ chasm.Context) map[string]string {
	// TODO: Export workflow metadata from the CHASM workflow root instead of CloseTransaction().
	return nil
}

func (w *Workflow) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewInternal("workflow root Terminate should not be called")
}

// HasPendingCloseCallbacks returns true if there is any workflow-level
// callback or update entry that may need to fire on workflow close. Used as a
// fast-path check to avoid the writable-component allocation when there is
// nothing to schedule.
func (w *Workflow) HasPendingCloseCallbacks() bool {
	return len(w.Callbacks) > 0 || w.HasPendingUpdateCallbacks()
}

// HasPendingUpdateCallbacksFor returns true iff the given update has callbacks
// registered.
func (w *Workflow) HasPendingUpdateCallbacksFor(updateID string) bool {
	_, ok := w.Updates[updateID]
	return ok
}

// HasPendingUpdateCallbacks returns true if any update has callbacks registered.
// Used when callers want to schedule update callbacks but not workflow-level
// callbacks (e.g., on continue-as-new).
func (w *Workflow) HasPendingUpdateCallbacks() bool {
	return len(w.Updates) > 0
}

// ScheduleCloseCallbacks transitions all workflow-level and update-level
// "WorkflowClosed" callbacks from STANDBY to SCHEDULED. Workflow-level and
// update-level scheduling are independent: failure of one does not stop the
// other; the errors are joined.
func (w *Workflow) ScheduleCloseCallbacks(ctx chasm.MutableContext) error {
	wfErr := callback.ScheduleStandbyCallbacks(ctx, w.Callbacks)
	updErr := w.ScheduleAllUpdateCloseCallbacks(ctx)
	return errors.Join(wfErr, updErr)
}

// ScheduleAllUpdateCloseCallbacks schedules callbacks for every update without
// touching workflow-level callbacks. This is used when the workflow continues
// to a new run (ContinueAsNew, retry, cron): workflow-level callbacks are
// inherited by the new run, but update callbacks must fire now because the
// update was aborted on the old run. Errors from individual updates are
// joined so a single failing update does not prevent others from being
// scheduled.
func (w *Workflow) ScheduleAllUpdateCloseCallbacks(ctx chasm.MutableContext) error {
	var errs []error
	for _, updateField := range w.Updates {
		if err := callback.ScheduleStandbyCallbacks(ctx, updateField.Get(ctx).Callbacks); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ScheduleUpdateCallbacks schedules callbacks for a single updateID if it
// exists. Returns NotFound if the update has no callbacks registered.
func (w *Workflow) ScheduleUpdateCallbacks(ctx chasm.MutableContext, updateID string) error {
	update, exists := w.Updates[updateID]
	if !exists {
		return serviceerror.NewNotFoundf("update with ID %s not found", updateID)
	}
	return callback.ScheduleStandbyCallbacks(ctx, update.Get(ctx).Callbacks)
}

// RejectUpdate stores the rejection failure on the WorkflowUpdate component
// and schedules any registered callbacks. Used when a reapplied update
// (after reset) is rejected by the worker's validator: the callbacks need to
// deliver the rejection failure to the caller.
func (w *Workflow) RejectUpdate(ctx chasm.MutableContext, updateID string, rejectionFailure *failurepb.Failure) error {
	updateField, exists := w.Updates[updateID]
	if !exists {
		return nil // no callbacks registered for this update
	}

	upd := updateField.Get(ctx)
	upd.RejectionFailure = rejectionFailure

	return callback.ScheduleStandbyCallbacks(ctx, upd.Callbacks)
}

// totalCallbackCount returns the total number of callbacks across workflow-level
// and all update-level callback maps.
func (w *Workflow) totalCallbackCount(ctx chasm.Context) int {
	count := len(w.Callbacks)
	for _, updateField := range w.Updates {
		count += len(updateField.Get(ctx).Callbacks)
	}
	return count
}

// checkWorkflowCallbackLimit returns an error if adding newCount callbacks would
// exceed the per-workflow maximum.
func (w *Workflow) checkWorkflowCallbackLimit(ctx chasm.Context, newCount, maxCallbacksPerWorkflow int) error {
	current := w.totalCallbackCount(ctx)
	if newCount+current > maxCallbacksPerWorkflow {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d callbacks to a workflow (%d callbacks already attached)",
			maxCallbacksPerWorkflow,
			current,
		)
	}
	return nil
}

// addCallbacksToMap converts common callbacks to CHASM callback components and
// inserts them into target, keyed by "<requestID>-<index>". target must be
// non-nil.
//
// All callbacks are validated up front, so target is not mutated unless every
// callback can be converted successfully (atomic from the caller's POV).
func addCallbacksToMap(
	ctx chasm.MutableContext,
	target chasm.Map[string, *callback.Callback],
	requestID string,
	eventTime *timestamppb.Timestamp,
	completionCallbacks []*commonpb.Callback,
) error {
	chasmCBs := make([]*callbackspb.Callback, len(completionCallbacks))
	for i, cb := range completionCallbacks {
		chasmCB := &callbackspb.Callback{Links: cb.GetLinks()}
		switch variant := cb.Variant.(type) {
		case *commonpb.Callback_Nexus_:
			chasmCB.Variant = &callbackspb.Callback_Nexus_{
				Nexus: &callbackspb.Callback_Nexus{
					Url:    variant.Nexus.GetUrl(),
					Header: variant.Nexus.GetHeader(),
				},
			}
		default:
			return serviceerror.NewInvalidArgumentf("unsupported callback variant: %T", variant)
		}
		chasmCBs[i] = chasmCB
	}

	for idx, chasmCB := range chasmCBs {
		// requestID (unique per API call) + idx (position within the request) ensures unique, idempotent callback IDs.
		// Unlike HSM callbacks, CHASM replicates entire trees rather than replaying events, so deterministic
		// cross-cluster IDs based on event version are not needed.
		id := fmt.Sprintf("%s-%d", requestID, idx)
		if _, exists := target[id]; exists {
			// Already registered, skip to avoid overwriting.
			continue
		}
		callbackObj := callback.NewCallback(requestID, eventTime, &callbackspb.CallbackState{}, chasmCB)
		target[id] = chasm.NewComponentField(ctx, callbackObj)
	}
	return nil
}

// AddCompletionCallbacks creates completion callbacks using the CHASM implementation.
// maxCallbacksPerWorkflow is the configured maximum number of callbacks allowed per workflow.
func (w *Workflow) AddCompletionCallbacks(
	ctx chasm.MutableContext,
	eventTime *timestamppb.Timestamp,
	requestID string,
	completionCallbacks []*commonpb.Callback,
	maxCallbacksPerWorkflow int,
) error {
	if err := w.checkWorkflowCallbackLimit(ctx, len(completionCallbacks), maxCallbacksPerWorkflow); err != nil {
		return err
	}

	if w.Callbacks == nil {
		w.Callbacks = make(chasm.Map[string, *callback.Callback], len(completionCallbacks))
	}
	return addCallbacksToMap(ctx, w.Callbacks, requestID, eventTime, completionCallbacks)
}

// AddUpdateCompletionCallbacks creates completion callbacks using the CHASM implementation.
// maxCallbacksPerWorkflow is the configured maximum number of callbacks allowed per workflow.
// maxCallbacksPerUpdateID is the configured maximum number of callbacks allowed per update ID.
func (w *Workflow) AddUpdateCompletionCallbacks(
	ctx chasm.MutableContext,
	eventTime *timestamppb.Timestamp,
	updateID string,
	requestID string,
	completionCallbacks []*commonpb.Callback,
	maxCallbacksPerWorkflow int,
	maxCallbacksPerUpdateID int,
) error {
	if err := w.checkWorkflowCallbackLimit(ctx, len(completionCallbacks), maxCallbacksPerWorkflow); err != nil {
		return err
	}

	if w.Updates == nil {
		w.Updates = make(chasm.Map[string, *WorkflowUpdate], 1)
	}
	if _, ok := w.Updates[updateID]; !ok {
		workflowUpdateObj := NewWorkflowUpdate(ctx, updateID, w.MSPointer)
		workflowUpdateObj.Callbacks = make(chasm.Map[string, *callback.Callback], len(completionCallbacks))
		w.Updates[updateID] = chasm.NewComponentField(ctx, workflowUpdateObj)
	}

	update := w.Updates[updateID].Get(ctx)
	if update.Callbacks == nil {
		update.Callbacks = make(chasm.Map[string, *callback.Callback], len(completionCallbacks))
	}

	currentCallbackCount := len(update.Callbacks)
	if len(completionCallbacks)+currentCallbackCount > maxCallbacksPerUpdateID {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d callbacks to update %q (%d callbacks already attached)",
			maxCallbacksPerUpdateID,
			updateID,
			currentCallbackCount,
		)
	}

	return addCallbacksToMap(ctx, update.Callbacks, requestID, eventTime, completionCallbacks)
}


// addAndApplyHistoryEvent adds a history event to the workflow and applies the corresponding event definition,
// looked up by Go type. This is the preferred way to add and apply events as it provides go-to-definition navigation.
func addAndApplyHistoryEvent[D EventDefinition](
	w *Workflow,
	ctx chasm.MutableContext,
	setAttributes func(*historypb.HistoryEvent),
) (*historypb.HistoryEvent, error) {
	def, ok := eventDefinitionByGoType[D](workflowContextFromChasm(ctx).registry)
	if !ok {
		return nil, serviceerror.NewInternalf("no event definition registered for Go type %T", (*D)(nil))
	}
	event := w.AddHistoryEvent(def.Type(), setAttributes)
	return event, def.Apply(ctx, w, event)
}

// HasAnyBufferedEvent returns true if the workflow has any buffered event matching the given filter.
func (w *Workflow) HasAnyBufferedEvent(filter historybuilder.BufferedEventFilter) bool {
	return w.MSPointer.HasAnyBufferedEvent(filter)
}

func (w *Workflow) WorkflowTypeName() string {
	return w.GetWorkflowTypeName()
}
