package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

const cloudTemporal = "F|c|B|t|G"
const snapTemporal = "F|s|B|t|G"

type compositeClient struct {
	temporalClient client.Client
	snapClient     client.Client
}

func (c *compositeClient) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	return c.temporalClient.ExecuteWorkflow(ctx, options, workflow, args...)
}

func (c *compositeClient) GetWorkflow(ctx context.Context, workflowID string, runID string) client.WorkflowRun {
	return &compositeWorkflowRun{
		snapWorkflowRun:     c.snapClient.GetWorkflow(ctx, workflowID, runID),
		temporalWorkflowRun: c.temporalClient.GetWorkflow(ctx, workflowID, runID),
	}
}

func (c *compositeClient) SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error {
	err := c.temporalClient.SignalWorkflow(ctx, workflowID, runID, signalName, arg)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return err
		}
	}

	return c.snapClient.SignalWorkflow(ctx, workflowID, runID, signalName, arg)
}

func (c *compositeClient) SignalWithStartWorkflow(ctx context.Context, workflowID string, signalName string, signalArg interface{}, options client.StartWorkflowOptions, workflow interface{}, workflowArgs ...interface{}) (client.WorkflowRun, error) {
	err := c.temporalClient.SignalWorkflow(ctx, workflowID, "", signalName, signalArg)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return nil, err
		}
	} else {
		return c.temporalClient.GetWorkflow(ctx, workflowID, ""), nil
	}

	err = c.snapClient.SignalWorkflow(ctx, workflowID, "", signalName, signalArg)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return nil, err
		}
	} else {
		return c.snapClient.GetWorkflow(ctx, workflowID, ""), nil
	}

	return c.temporalClient.SignalWithStartWorkflow(ctx, workflowID, signalName, signalArg, options, workflow, workflowArgs...)
}

func (c *compositeClient) CancelWorkflow(ctx context.Context, workflowID string, runID string) error {
	err := c.temporalClient.CancelWorkflow(ctx, workflowID, runID)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return err
		}
	} else {
		return nil
	}

	return c.snapClient.CancelWorkflow(ctx, workflowID, runID)
}

func (c *compositeClient) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string, details ...interface{}) error {
	err := c.temporalClient.TerminateWorkflow(ctx, workflowID, runID, reason, details...)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return err
		}
	} else {
		return nil
	}

	return c.snapClient.TerminateWorkflow(ctx, workflowID, runID, reason, details...)
}

func (c *compositeClient) GetWorkflowHistory(ctx context.Context, workflowID string, runID string, isLongPoll bool, filterType enumspb.HistoryEventFilterType) client.HistoryEventIterator {
	return &compositeHistoryIterator{
		temporalHistoryIterator: c.temporalClient.GetWorkflowHistory(ctx, workflowID, runID, isLongPoll, filterType),
		snapHistoryIterator:     c.snapClient.GetWorkflowHistory(ctx, workflowID, runID, isLongPoll, filterType),
	}
}

func (c *compositeClient) CompleteActivity(ctx context.Context, taskToken []byte, result interface{}, err error) error {
	err = c.temporalClient.CompleteActivity(ctx, taskToken, result, err)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return err
		}
	} else {
		return nil
	}

	return c.snapClient.CompleteActivity(ctx, taskToken, result, err)
}

func (c *compositeClient) CompleteActivityByID(ctx context.Context, namespace, workflowID, runID, activityID string, result interface{}, err error) error {
	err = c.temporalClient.CompleteActivityByID(ctx, namespace, workflowID, runID, activityID, result, err)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return err
		}
	} else {
		return nil
	}

	return c.snapClient.CompleteActivityByID(ctx, namespace, workflowID, runID, activityID, result, err)
}

func (c *compositeClient) RecordActivityHeartbeat(ctx context.Context, taskToken []byte, details ...interface{}) error {
	err := c.temporalClient.RecordActivityHeartbeat(ctx, taskToken, details...)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return err
		}
	} else {
		return nil
	}

	return c.snapClient.RecordActivityHeartbeat(ctx, taskToken, details...)
}

func (c *compositeClient) RecordActivityHeartbeatByID(ctx context.Context, namespace, workflowID, runID, activityID string, details ...interface{}) error {
	err := c.temporalClient.RecordActivityHeartbeatByID(ctx, namespace, workflowID, runID, activityID, details...)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return err
		}
	} else {
		return nil
	}

	return c.snapClient.RecordActivityHeartbeatByID(ctx, namespace, workflowID, runID, activityID, details...)
}

func (c *compositeClient) ListClosedWorkflow(ctx context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	nextPageToken := string(request.NextPageToken)

	cloudTemporalIndex := strings.Index(nextPageToken, cloudTemporal)
	snapTemporalIndex := strings.Index(nextPageToken, snapTemporal)

	if cloudTemporalIndex == -1 && snapTemporalIndex == -1 {
		ctResp, err := c.temporalClient.ListClosedWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		stResp, err := c.snapClient.ListClosedWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		return &workflowservice.ListClosedWorkflowExecutionsResponse{
			Executions:    append(ctResp.Executions, stResp.Executions...),
			NextPageToken: []byte(constructToken(string(ctResp.NextPageToken), string(stResp.NextPageToken))),
		}, nil
	} else if cloudTemporalIndex == 0 {
		resp, err := c.temporalClient.ListClosedWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		snapTemporalToken := ""
		if snapTemporalIndex != -1 {
			snapTemporalToken = nextPageToken[snapTemporalIndex+len(snapTemporal):]
		}
		resp.NextPageToken = []byte(constructToken(string(resp.NextPageToken), snapTemporalToken))
		return resp, nil
	} else {
		resp, err := c.snapClient.ListClosedWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		resp.NextPageToken = []byte(constructToken("", string(resp.NextPageToken)))
		return resp, nil
	}
}

func (c *compositeClient) ListOpenWorkflow(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	nextPageToken := string(request.NextPageToken)

	cloudTemporalIndex := strings.Index(nextPageToken, cloudTemporal)
	snapTemporalIndex := strings.Index(nextPageToken, snapTemporal)

	if cloudTemporalIndex == -1 && snapTemporalIndex == -1 {
		ctResp, err := c.temporalClient.ListOpenWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		stResp, err := c.snapClient.ListOpenWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		return &workflowservice.ListOpenWorkflowExecutionsResponse{
			Executions:    append(ctResp.Executions, stResp.Executions...),
			NextPageToken: []byte(constructToken(string(ctResp.NextPageToken), string(stResp.NextPageToken))),
		}, nil
	} else if cloudTemporalIndex == 0 {
		resp, err := c.temporalClient.ListOpenWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		snapTemporalToken := ""
		if snapTemporalIndex != -1 {
			snapTemporalToken = nextPageToken[snapTemporalIndex+len(snapTemporal):]
		}
		resp.NextPageToken = []byte(constructToken(string(resp.NextPageToken), snapTemporalToken))
		return resp, nil
	} else {
		resp, err := c.snapClient.ListOpenWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		resp.NextPageToken = []byte(constructToken("", string(resp.NextPageToken)))
		return resp, nil
	}
}

func (c *compositeClient) ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	nextPageToken := string(request.NextPageToken)

	cloudTemporalIndex := strings.Index(nextPageToken, cloudTemporal)
	snapTemporalIndex := strings.Index(nextPageToken, snapTemporal)

	if cloudTemporalIndex == -1 && snapTemporalIndex == -1 {
		ctResp, err := c.temporalClient.ListWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		stResp, err := c.snapClient.ListWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		return &workflowservice.ListWorkflowExecutionsResponse{
			Executions:    append(ctResp.Executions, stResp.Executions...),
			NextPageToken: []byte(constructToken(string(ctResp.NextPageToken), string(stResp.NextPageToken))),
		}, nil
	} else if cloudTemporalIndex == 0 {
		resp, err := c.temporalClient.ListWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		snapTemporalToken := ""
		if snapTemporalIndex != -1 {
			snapTemporalToken = nextPageToken[snapTemporalIndex+len(snapTemporal):]
		}
		resp.NextPageToken = []byte(constructToken(string(resp.NextPageToken), snapTemporalToken))
		return resp, nil
	} else {
		resp, err := c.snapClient.ListWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		resp.NextPageToken = []byte(constructToken("", string(resp.NextPageToken)))
		return resp, nil
	}
}

func (c *compositeClient) ListArchivedWorkflow(ctx context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	nextPageToken := string(request.NextPageToken)

	cloudTemporalIndex := strings.Index(nextPageToken, cloudTemporal)
	snapTemporalIndex := strings.Index(nextPageToken, snapTemporal)

	if cloudTemporalIndex == -1 && snapTemporalIndex == -1 {
		ctResp, err := c.temporalClient.ListArchivedWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		stResp, err := c.snapClient.ListArchivedWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		return &workflowservice.ListArchivedWorkflowExecutionsResponse{
			Executions:    append(ctResp.Executions, stResp.Executions...),
			NextPageToken: []byte(constructToken(string(ctResp.NextPageToken), string(stResp.NextPageToken))),
		}, nil
	} else if cloudTemporalIndex == 0 {
		resp, err := c.temporalClient.ListArchivedWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		snapTemporalToken := ""
		if snapTemporalIndex != -1 {
			snapTemporalToken = nextPageToken[snapTemporalIndex+len(snapTemporal):]
		}
		resp.NextPageToken = []byte(constructToken(string(resp.NextPageToken), snapTemporalToken))
		return resp, nil
	} else {
		resp, err := c.snapClient.ListArchivedWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		resp.NextPageToken = []byte(constructToken("", string(resp.NextPageToken)))
		return resp, nil
	}
}

func (c *compositeClient) ScanWorkflow(ctx context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
	nextPageToken := string(request.NextPageToken)

	cloudTemporalIndex := strings.Index(nextPageToken, cloudTemporal)
	snapTemporalIndex := strings.Index(nextPageToken, snapTemporal)

	if cloudTemporalIndex == -1 && snapTemporalIndex == -1 {
		ctResp, err := c.temporalClient.ScanWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		stResp, err := c.snapClient.ScanWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		return &workflowservice.ScanWorkflowExecutionsResponse{
			Executions:    append(ctResp.Executions, stResp.Executions...),
			NextPageToken: []byte(constructToken(string(ctResp.NextPageToken), string(stResp.NextPageToken))),
		}, nil
	} else if cloudTemporalIndex == 0 {
		resp, err := c.temporalClient.ScanWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		snapTemporalToken := ""
		if snapTemporalIndex != -1 {
			snapTemporalToken = nextPageToken[snapTemporalIndex+len(snapTemporal):]
		}
		resp.NextPageToken = []byte(constructToken(string(resp.NextPageToken), snapTemporalToken))
		return resp, nil
	} else {
		resp, err := c.snapClient.ScanWorkflow(ctx, request)
		if err != nil {
			return nil, err
		}
		resp.NextPageToken = []byte(constructToken("", string(resp.NextPageToken)))
		return resp, nil
	}
}

func (c *compositeClient) CountWorkflow(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	ctResp, err := c.temporalClient.CountWorkflow(ctx, request)
	if err != nil {
		return nil, err
	}

	stResp, err := c.snapClient.CountWorkflow(ctx, request)
	if err != nil {
		return nil, err
	}

	return &workflowservice.CountWorkflowExecutionsResponse{
		Count:  ctResp.GetCount() + stResp.GetCount(),
		Groups: append(ctResp.GetGroups(), stResp.GetGroups()...),
	}, nil
}

func (c *compositeClient) GetSearchAttributes(ctx context.Context) (*workflowservice.GetSearchAttributesResponse, error) {
	return c.temporalClient.GetSearchAttributes(ctx)
}

func (c *compositeClient) QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...interface{}) (converter.EncodedValue, error) {
	ctVal, err := c.temporalClient.QueryWorkflow(ctx, workflowID, runID, queryType, args...)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return nil, err
		}
	} else {
		return ctVal, nil
	}

	return c.snapClient.QueryWorkflow(ctx, workflowID, runID, queryType, args...)
}

func (c *compositeClient) QueryWorkflowWithOptions(ctx context.Context, request *client.QueryWorkflowWithOptionsRequest) (*client.QueryWorkflowWithOptionsResponse, error) {
	ctVal, err := c.temporalClient.QueryWorkflowWithOptions(ctx, request)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return nil, err
		}
	} else {
		return ctVal, nil
	}

	return c.snapClient.QueryWorkflowWithOptions(ctx, request)
}

func (c *compositeClient) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	ctResp, err := c.temporalClient.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return nil, err
		}
	} else {
		return ctResp, nil
	}

	return c.snapClient.DescribeWorkflowExecution(ctx, workflowID, runID)
}

func (c *compositeClient) DescribeTaskQueue(ctx context.Context, taskqueue string, taskqueueType enumspb.TaskQueueType) (*workflowservice.DescribeTaskQueueResponse, error) {
	ctResp, err := c.temporalClient.DescribeTaskQueue(ctx, taskqueue, taskqueueType)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return nil, err
		}
	} else {
		return ctResp, nil
	}

	return c.snapClient.DescribeTaskQueue(ctx, taskqueue, taskqueueType)
}

func (c *compositeClient) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	ctResp, err := c.temporalClient.ResetWorkflowExecution(ctx, request)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return nil, err
		}
	} else {
		return ctResp, nil
	}

	return c.snapClient.ResetWorkflowExecution(ctx, request)
}

func (c *compositeClient) UpdateWorkerBuildIdCompatibility(ctx context.Context, options *client.UpdateWorkerBuildIdCompatibilityOptions) error {
	// intentional, we do not use this
	return c.temporalClient.UpdateWorkerBuildIdCompatibility(ctx, options)
}

func (c *compositeClient) GetWorkerBuildIdCompatibility(ctx context.Context, options *client.GetWorkerBuildIdCompatibilityOptions) (*client.WorkerBuildIDVersionSets, error) {
	// intentional, we do not use this
	return c.temporalClient.GetWorkerBuildIdCompatibility(ctx, options)
}

func (c *compositeClient) GetWorkerTaskReachability(ctx context.Context, options *client.GetWorkerTaskReachabilityOptions) (*client.WorkerTaskReachability, error) {
	// intentional, we do not use this
	return c.temporalClient.GetWorkerTaskReachability(ctx, options)
}

func (c *compositeClient) CheckHealth(ctx context.Context, request *client.CheckHealthRequest) (*client.CheckHealthResponse, error) {
	// intentional, we do not use this
	return c.temporalClient.CheckHealth(ctx, request)
}

func (c *compositeClient) UpdateWorkflow(ctx context.Context, workflowID string, workflowRunID string, updateName string, args ...interface{}) (client.WorkflowUpdateHandle, error) {
	ctResp, err := c.temporalClient.UpdateWorkflow(ctx, workflowID, workflowRunID, updateName, args...)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return nil, err
		}
	} else {
		return ctResp, nil
	}

	return c.snapClient.UpdateWorkflow(ctx, workflowID, workflowRunID, updateName, args...)
}

func (c *compositeClient) UpdateWorkflowWithOptions(ctx context.Context, request *client.UpdateWorkflowWithOptionsRequest) (client.WorkflowUpdateHandle, error) {
	ctResp, err := c.temporalClient.UpdateWorkflowWithOptions(ctx, request)
	if err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return nil, err
		}
	} else {
		return ctResp, nil
	}

	return c.snapClient.UpdateWorkflowWithOptions(ctx, request)
}

func (c *compositeClient) GetWorkflowUpdateHandle(ref client.GetWorkflowUpdateHandleOptions) client.WorkflowUpdateHandle {
	return &compositeWorkflowUpdateHandle{
		temporalWorkflowUpdateHandle: c.temporalClient.GetWorkflowUpdateHandle(ref),
		snapWorkflowUpdateHandle:     c.snapClient.GetWorkflowUpdateHandle(ref),
	}
}

func (c *compositeClient) WorkflowService() workflowservice.WorkflowServiceClient {
	// intentional, we do not use this
	return c.temporalClient.WorkflowService()
}

func (c *compositeClient) OperatorService() operatorservice.OperatorServiceClient {
	// intentional, we do not use this
	return c.temporalClient.OperatorService()
}

func (c *compositeClient) ScheduleClient() client.ScheduleClient {
	// intentional, we do not use this
	return c.temporalClient.ScheduleClient()
}

func (c *compositeClient) Close() {
	c.temporalClient.Close()
	c.snapClient.Close()
}

func constructToken(cloudTemporalToken string, snapTemporalToken string) string {
	newNextPageToken := ""
	if len(cloudTemporalToken) > 0 {
		newNextPageToken = fmt.Sprintf("%s%s%s", newNextPageToken, cloudTemporal, cloudTemporalToken)
	}
	if len(snapTemporalToken) > 0 {
		newNextPageToken = fmt.Sprintf("%s%s%s", newNextPageToken, snapTemporal, snapTemporalToken)
	}
	return newNextPageToken
}
