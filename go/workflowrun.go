package main

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

type compositeWorkflowRun struct {
	temporalWorkflowRun client.WorkflowRun
	snapWorkflowRun     client.WorkflowRun
}

func (c *compositeWorkflowRun) GetID() string {
	temporalWorkflowId := c.temporalWorkflowRun.GetID()
	if temporalWorkflowId != "" {
		return temporalWorkflowId
	}

	return c.snapWorkflowRun.GetID()
}

func (c *compositeWorkflowRun) GetRunID() string {
	temporalRunId := c.temporalWorkflowRun.GetRunID()
	if temporalRunId != "" {
		return temporalRunId
	}

	return c.snapWorkflowRun.GetRunID()
}

func (c *compositeWorkflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	err := c.temporalWorkflowRun.Get(ctx, valuePtr)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return err
		}
	} else {
		return nil
	}

	return c.snapWorkflowRun.Get(ctx, valuePtr)
}

func (c *compositeWorkflowRun) GetWithOptions(ctx context.Context, valuePtr interface{}, options client.WorkflowRunGetOptions) error {
	err := c.temporalWorkflowRun.GetWithOptions(ctx, valuePtr, options)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return err
		}
	} else {
		return nil
	}

	return c.snapWorkflowRun.GetWithOptions(ctx, valuePtr, options)
}
