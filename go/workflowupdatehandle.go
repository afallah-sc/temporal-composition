package main

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

type compositeWorkflowUpdateHandle struct {
	temporalWorkflowUpdateHandle client.WorkflowUpdateHandle
	snapWorkflowUpdateHandle     client.WorkflowUpdateHandle
}

func (c *compositeWorkflowUpdateHandle) WorkflowID() string {
	temporalWorkflowId := c.temporalWorkflowUpdateHandle.WorkflowID()
	if temporalWorkflowId != "" {
		return temporalWorkflowId
	}

	return c.snapWorkflowUpdateHandle.WorkflowID()
}

func (c *compositeWorkflowUpdateHandle) RunID() string {
	temporalRunId := c.temporalWorkflowUpdateHandle.RunID()
	if temporalRunId != "" {
		return temporalRunId
	}

	return c.snapWorkflowUpdateHandle.RunID()
}

func (c *compositeWorkflowUpdateHandle) UpdateID() string {
	temporalUpdateId := c.temporalWorkflowUpdateHandle.UpdateID()
	if temporalUpdateId != "" {
		return temporalUpdateId
	}

	return c.snapWorkflowUpdateHandle.UpdateID()
}

func (c *compositeWorkflowUpdateHandle) Get(ctx context.Context, valuePtr interface{}) error {
	err := c.temporalWorkflowUpdateHandle.Get(ctx, valuePtr)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return err
		}
	} else {
		return nil
	}

	return c.snapWorkflowUpdateHandle.Get(ctx, valuePtr)
}
