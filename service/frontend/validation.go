package frontend

import (
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/validation"
)

type validatedWorkflowHandler struct {
	workflowservice.WorkflowServiceServer
	lifecycle Handler
}

func (h *validatedWorkflowHandler) Start()             { h.lifecycle.Start() }
func (h *validatedWorkflowHandler) Stop()              { h.lifecycle.Stop() }
func (h *validatedWorkflowHandler) GetConfig() *Config { return h.lifecycle.GetConfig() }

func validateWorkflowHandler(handler Handler, registry *validation.Registry) Handler {
	return &validatedWorkflowHandler{WorkflowServiceServer: validation.WrapWorkflowService(handler, registry), lifecycle: handler}
}

type validatedOperatorHandler struct {
	operatorservice.OperatorServiceServer
	lifecycle OperatorHandler
}

func (h *validatedOperatorHandler) Start() { h.lifecycle.Start() }
func (h *validatedOperatorHandler) Stop()  { h.lifecycle.Stop() }

func validateOperatorHandler(handler OperatorHandler, registry *validation.Registry) OperatorHandler {
	return &validatedOperatorHandler{OperatorServiceServer: validation.WrapOperatorService(handler, registry), lifecycle: handler}
}
