package temporalhelpers

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/brevdev/dev-plane/pkg/collections"
	"github.com/brevdev/dev-plane/pkg/errors"
	"github.com/brevdev/dev-plane/pkg/logger"
	"github.com/stretchr/testify/mock"
	temporalenums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	temporallog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	temporalworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func NewWorkflowTestSuite(t *testing.T) *testsuite.WorkflowTestSuite {
	t.Helper()
	s := &testsuite.WorkflowTestSuite{}
	zl := zaptest.NewLogger(t, zaptest.Level(zap.InfoLevel),
		zaptest.WrapOptions(zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel)),
	)
	s.SetLogger(NewZapAdapter(zl))
	return s
}

type WorkflowRegisterer interface {
	GetTaskQueueName() string
	RegisterWorkflowWithOptions(w interface{}, options workflow.RegisterOptions)
	RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions)
}

type DummyWorkflowRegisterer struct{}

var _ WorkflowRegisterer = DummyWorkflowRegisterer{}

func (d DummyWorkflowRegisterer) GetTaskQueueName() string {
	return ""
}

func (d DummyWorkflowRegisterer) RegisterWorkflowWithOptions(_ interface{}, _ workflow.RegisterOptions) {
}

func (d DummyWorkflowRegisterer) RegisterActivityWithOptions(_ interface{}, _ activity.RegisterOptions) {
}

type TestWorkflowEnvironment struct {
	*testsuite.TestWorkflowEnvironment
}

var _ WorkflowRegisterer = TestWorkflowEnvironment{}

func (t TestWorkflowEnvironment) GetTaskQueueName() string {
	return ""
}

type MultiRegisterRegisterer struct {
	Primary WorkflowRegisterer
	Other   []WorkflowRegisterer
}

var _ WorkflowRegisterer = MultiRegisterRegisterer{}

// useful for migrating from one queue to the other
// put new worker/queue in primary and old ones in other
// newly created worfklow will be put in new queue
// old ones will continue to work until they are re-created
func NewMultiRegisterRegisterer(primary WorkflowRegisterer, registerers ...WorkflowRegisterer) MultiRegisterRegisterer {
	return MultiRegisterRegisterer{
		Primary: primary,
		Other:   registerers,
	}
}

func (m MultiRegisterRegisterer) GetTaskQueueName() string {
	return m.Primary.GetTaskQueueName()
}

func (m MultiRegisterRegisterer) RegisterWorkflowWithOptions(w interface{}, options workflow.RegisterOptions) {
	m.Primary.RegisterWorkflowWithOptions(w, options)
	for _, r := range m.Other {
		r.RegisterWorkflowWithOptions(w, options)
	}
}

func (m MultiRegisterRegisterer) RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions) {
	m.Primary.RegisterActivityWithOptions(a, options)
	for _, r := range m.Other {
		r.RegisterActivityWithOptions(a, options)
	}
}

// use this to achieve better contract testing
type TemporalClient interface {
	ExecuteWorkflow(ctx context.Context, options temporalclient.StartWorkflowOptions, workflow interface{}, args ...interface{}) (temporalclient.WorkflowRun, error)
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
	GetWorkflow(ctx context.Context, workflowID string, runID string) (temporalclient.WorkflowRun, error)
	QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...interface{}) (converter.EncodedValue, error)
	SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error
	CancelWorkflow(ctx context.Context, workflowID string, runID string) error
	TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string, details ...interface{}) error
	ScheduleClient() temporalclient.ScheduleClient
	ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error)
	SignalWithStartWorkflow(ctx context.Context, workflowID string, signalName string, signalArg interface{}, options temporalclient.StartWorkflowOptions, workflow interface{}, args ...interface{}) (temporalclient.WorkflowRun, error)
}

type DPTemporalClient struct {
	temporalclient.Client
}

func NewDPTemporalClient(client temporalclient.Client) DPTemporalClient {
	return DPTemporalClient{Client: client}
}

func (d DPTemporalClient) GetWorkflow(ctx context.Context, workflowID string, runID string) (temporalclient.WorkflowRun, error) {
	return d.Client.GetWorkflow(ctx, workflowID, runID), nil
}

var _ TemporalClient = DPTemporalClient{}

type TestTemporalClient struct {
	Env *testsuite.TestWorkflowEnvironment
}

var _ TemporalClient = TestTemporalClient{}

func (t TestTemporalClient) SignalWithStartWorkflow(_ context.Context, _ string, _ string, _ interface{}, _ temporalclient.StartWorkflowOptions, _ interface{}, _ ...interface{}) (temporalclient.WorkflowRun, error) {
	// TODO not implemented
	return nil, errors.Errorf("not implemented")
}

func (t TestTemporalClient) RegisterWorkflowWithOptions(w interface{}, options workflow.RegisterOptions) {
	t.Env.RegisterWorkflowWithOptions(w, options)
}

func (t TestTemporalClient) RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions) {
	t.Env.RegisterActivityWithOptions(a, options)
}

func (t TestTemporalClient) GetTaskQueueName() string {
	return ""
}

func (t TestTemporalClient) GetWorkflow(_ context.Context, workflowID string, runID string) (temporalclient.WorkflowRun, error) {
	return TestWorkflowRun{workflowID, runID, t.Env}, nil
}

func (t TestTemporalClient) DescribeWorkflowExecution(_ context.Context, _, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return nil, nil
}

func (t TestTemporalClient) ExecuteWorkflow(_ context.Context, _ temporalclient.StartWorkflowOptions, workflow interface{}, args ...interface{}) (temporalclient.WorkflowRun, error) {
	t.Env.ExecuteWorkflow(workflow, args...)
	return TestWorkflowRun{"1i", "1r", t.Env}, nil
}

func (t TestTemporalClient) QueryWorkflow(_ context.Context, _ string, _ string, queryType string, args ...interface{}) (converter.EncodedValue, error) {
	res, err := t.Env.QueryWorkflow(queryType, args...)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	return res, nil
}

func (t TestTemporalClient) CancelWorkflow(_ context.Context, _ string, _ string) error {
	t.Env.CancelWorkflow()
	return nil
}

func (t TestTemporalClient) TerminateWorkflow(_ context.Context, _ string, _ string, _ string, _ ...interface{}) error {
	return nil
}

func (t TestTemporalClient) ScheduleClient() temporalclient.ScheduleClient {
	return nil
}

func (t TestTemporalClient) ListWorkflow(_ context.Context, _ *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	return nil, nil
}

func (t TestTemporalClient) SignalWorkflow(_ context.Context, _ string, _ string, signalName string, arg interface{}) error {
	t.Env.SignalWorkflow(signalName, arg)
	return nil
}

type TestWorkflowRun struct {
	ID    string
	RunID string
	Env   *testsuite.TestWorkflowEnvironment
}

var _ temporalclient.WorkflowRun = TestWorkflowRun{}

func (t TestWorkflowRun) GetID() string {
	return t.ID
}

func (t TestWorkflowRun) GetRunID() string {
	return t.RunID
}

func (t TestWorkflowRun) Get(_ context.Context, res interface{}) error {
	err := t.Env.GetWorkflowError()
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	err = t.Env.GetWorkflowResult(res)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

func (t TestWorkflowRun) GetWithOptions(_ context.Context, res interface{}, _ temporalclient.WorkflowRunGetOptions) error {
	err := t.Env.GetWorkflowError()
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	err = t.Env.GetWorkflowResult(res)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

type GenericFuture[T any] struct {
	workflow.Future
}

func (g GenericFuture[T]) GetT(ctx workflow.Context) (T, error) {
	var t T
	err := g.Get(ctx, &t)
	if err != nil {
		return t, errors.WrapAndTrace(err)
	}
	return t, nil
}

type Activity[T any, R any] struct {
	// exec  func(context.Context, T) (R, error)
	// name  string
	// queue string
}

type Workflow[T any, R any] struct {
	exec                             func(workflow.Context, T) (R, error)
	name                             string
	queue                            string
	maxQueueActiviesPerSecond        float64
	maxConcurrentActivitiesPerWorker int
	singleActivityRef                interface{}
}

// this one differes from NewWorkflow by supporting reference to itself (useful for w.NewContinueAsNewError)
func NewExecWorkflow[T, R any](name string, exec func(Workflow[T, R], workflow.Context, T) (R, error)) Workflow[T, R] {
	w := Workflow[T, R]{name: name}
	innerExec := func(c workflow.Context, t T) (R, error) {
		return exec(w, c, t)
	}
	w.exec = innerExec
	return w
}

func NewWorkflow[T, R any](name string, exec func(workflow.Context, T) (R, error)) Workflow[T, R] {
	return Workflow[T, R]{name: name, exec: exec}
}

func (w Workflow[T, R]) WithQueue(queue string) Workflow[T, R] {
	w.queue = queue
	return w
}

// useful for clientside ratelimiting across all workers
func (w Workflow[T, R]) WithMaxQueueActivitiesPerSecond(max float64) Workflow[T, R] {
	w.maxQueueActiviesPerSecond = max
	return w
}

// this can be useful if you know how much resources your activities take and want to limit the number of activities
// this is not a way to make a semaphore
func (w Workflow[T, R]) WithMaxConcurrentActivitiesPerWorker(max int) Workflow[T, R] {
	w.maxConcurrentActivitiesPerWorker = max
	return w
}

func (w Workflow[T, R]) GetTaskQueueName() string {
	return w.queue
}

func (w Workflow[T, R]) GetName() string {
	return w.name
}

func (w Workflow[T, R]) NewContinueAsNewError(ctx workflow.Context, args T) error {
	if w.name == "" {
		return workflow.NewContinueAsNewError(ctx, w.exec, args)
	} else {
		return workflow.NewContinueAsNewError(ctx, w.name, args)
	}
}

func (w Workflow[T, R]) MakeWorker(workerFactory WorkerFactory, options temporalworker.Options) (TemporalWorker, error) {
	if w.queue == "" {
		return nil, errors.Errorf("queue must be set")
	}
	if w.maxQueueActiviesPerSecond > 0 {
		if options.TaskQueueActivitiesPerSecond != 0 {
			logger.L().Warn("task queue activities per second is already set, overwriting",
				zap.Float64("options.task_queue_activities_per_second", options.TaskQueueActivitiesPerSecond),
				zap.String("workflow", w.name),
				zap.String("queue", w.queue),
			)
		}
		options.TaskQueueActivitiesPerSecond = w.maxQueueActiviesPerSecond
	}
	if w.maxConcurrentActivitiesPerWorker > 0 {
		if options.MaxConcurrentActivityExecutionSize != 0 {
			logger.L().Warn("max concurrent activity execution size is already set, overwriting",
				zap.Int("options.max_concurrent_activity_execution_size", options.MaxConcurrentActivityExecutionSize),
				zap.String("workflow", w.name),
				zap.String("queue", w.queue),
			)
		}
		options.MaxConcurrentActivityExecutionSize = w.maxConcurrentActivitiesPerWorker
	}
	workflowWorker := workerFactory.MakeWorker(w.queue, options)
	if w.singleActivityRef != nil {
		workflowWorker = workflowWorker.WithMustHaveActivities(w.singleActivityRef)
	}
	err := w.Register(workflowWorker)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	return workflowWorker, nil
}

func MakeSingleActivityWorkflow[T any, R any](name string, activity func(context.Context, T) (R, error), activityOptions workflow.ActivityOptions) Workflow[T, R] {
	act := func(ctx workflow.Context, arg T) (R, error) {
		return ExecuteActivity(workflow.WithActivityOptions(ctx, activityOptions), activity, arg).GetT(ctx)
	}
	return Workflow[T, R]{exec: act, name: name, queue: activityOptions.TaskQueue, singleActivityRef: activity}
}

func ExecuteActivity[T any, R any](ctx workflow.Context, activity func(context.Context, T) (R, error), arg T) GenericFuture[R] {
	fut := workflow.ExecuteActivity(
		ctx,
		activity,
		arg,
	)
	return GenericFuture[R]{fut}
}

func ExecuteActivityTest[T any, R any](te *testsuite.TestActivityEnvironment, activity func(context.Context, T) (R, error), arg T) (R, error) {
	var r R
	val, err := te.ExecuteActivity(
		activity,
		arg,
	)
	if err != nil {
		return r, errors.WrapAndTrace(err)
	}
	err = val.Get(&r)
	if err != nil {
		return r, errors.WrapAndTrace(err)
	}
	return r, nil
}

type GenericWorkflowRun[T any] struct {
	temporalclient.WorkflowRun
}

func NewGenericWorkflowRun[T any](w temporalclient.WorkflowRun) GenericWorkflowRun[T] {
	return GenericWorkflowRun[T]{WorkflowRun: w}
}

func IsErrorNotFound(err error) bool {
	return errors.ErrorContainsAny(err, "no rows", "not found")
}

func IsErrorUnknownQueryType(err error) bool {
	return errors.ErrorContains(err, "unknown queryType")
}

func (g GenericWorkflowRun[T]) GetT(ctx context.Context) (T, error) {
	var t T
	err := g.Get(ctx, &t)
	if err != nil {
		return t, errors.WrapAndTrace(err)
	}
	return t, nil
}

func (g GenericWorkflowRun[T]) GetWithOptionsT(ctx context.Context, options temporalclient.WorkflowRunGetOptions) (T, error) {
	var t T
	err := g.GetWithOptions(ctx, &t, options)
	if err != nil {
		return t, errors.WrapAndTrace(err)
	}
	return t, nil
}

// will get exisiting workflow or start a new one id DNE
func ExecuteWorkflowWithIdentifierOrGet[T any, R any](ctx context.Context, client TemporalClient, workflowIdentifier WorkflowIdentifier, options temporalclient.StartWorkflowOptions, fun func(workflow.Context, T) (R, error), arg T) (GenericWorkflowRun[R], error) {
	if options.ID != "" {
		wfe, err := client.DescribeWorkflowExecution(ctx, options.ID, "")
		if err != nil && !IsErrorNotFound(err) {
			return GenericWorkflowRun[R]{}, errors.WrapAndTrace(err)
		}
		if wfe != nil {
			wf, err := client.GetWorkflow(ctx, options.ID, wfe.WorkflowExecutionInfo.Execution.RunId)
			if err != nil {
				return GenericWorkflowRun[R]{}, errors.WrapAndTrace(err)
			}
			return GenericWorkflowRun[R]{wf}, nil
		}
	}
	wf, err := ExecuteWorkflowWithIdentifier[T, R](ctx, client, workflowIdentifier, options, fun, arg)
	if err != nil {
		return GenericWorkflowRun[R]{}, errors.WrapAndTrace(err)
	}
	return wf, nil
}

func ExecuteWorkflowWithIdentifier[T any, R any](ctx context.Context, client TemporalClient, workflowIdentifier WorkflowIdentifier, options temporalclient.StartWorkflowOptions, _ func(workflow.Context, T) (R, error), arg T) (GenericWorkflowRun[R], error) {
	fut, err := client.ExecuteWorkflow(
		ctx,
		options,
		workflowIdentifier,
		arg,
	)
	if err != nil {
		return GenericWorkflowRun[R]{}, errors.WrapAndTrace(err)
	}
	return GenericWorkflowRun[R]{fut}, nil
}

// will get exisiting workflow or start a new one if DNE
// unsure what happens if workflow is completed, terminated, failed etc
func (w Workflow[T, R]) ExecuteWorkflowOrGet(ctx context.Context,
	client TemporalClient,
	options ExecuteWorkflowOrGetOptions,
	args T,
) (GenericWorkflowRun[R], error) {
	if options.ID != "" {
		wfe, err := client.DescribeWorkflowExecution(ctx, options.ID, "")
		if err != nil && !IsErrorNotFound(err) {
			return GenericWorkflowRun[R]{}, errors.WrapAndTrace(err)
		}
		if wfe != nil && options.shouldGet(wfe.WorkflowExecutionInfo.Status) {
			wf, err := client.GetWorkflow(ctx, options.ID, wfe.WorkflowExecutionInfo.Execution.RunId)
			if err != nil {
				return GenericWorkflowRun[R]{}, errors.WrapAndTrace(err)
			}
			return GenericWorkflowRun[R]{wf}, nil
		}
	}
	wf, err := w.ExecuteWorkflow(ctx, client, options.StartWorkflowOptions, args)
	if err != nil {
		return GenericWorkflowRun[R]{}, errors.WrapAndTrace(err)
	}
	return wf, nil
}

type ExecuteWorkflowOrGetOptions struct {
	temporalclient.StartWorkflowOptions
	GetOnStatus []temporalenums.WorkflowExecutionStatus
}

func (e ExecuteWorkflowOrGetOptions) shouldGet(status temporalenums.WorkflowExecutionStatus) bool {
	if e.GetOnStatus == nil {
		return collections.ListContains([]temporalenums.WorkflowExecutionStatus{
			temporalenums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			temporalenums.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			temporalenums.WORKFLOW_EXECUTION_STATUS_FAILED,
		}, status)
	} else {
		return collections.ListContains(e.GetOnStatus, status)
	}
}

// StartWorkflowOptions will get exisiting workflow or start a new one if DNE
func ExecuteWorkflowOrGet[T any, R any](ctx context.Context,
	client TemporalClient,
	options ExecuteWorkflowOrGetOptions,
	wrkflw func(workflow.Context, T) (R, error), arg T,
) (GenericWorkflowRun[R], error) {
	if options.ID != "" {
		wfe, err := client.DescribeWorkflowExecution(ctx, options.ID, "")
		if err != nil && !IsErrorNotFound(err) {
			return GenericWorkflowRun[R]{}, errors.WrapAndTrace(err)
		}
		if wfe != nil && options.shouldGet(wfe.WorkflowExecutionInfo.Status) {
			wf, err := client.GetWorkflow(ctx, options.ID, wfe.WorkflowExecutionInfo.Execution.RunId)
			if err != nil {
				return GenericWorkflowRun[R]{}, errors.WrapAndTrace(err)
			}
			return GenericWorkflowRun[R]{wf}, nil
		}
	}
	wf, err := ExecuteWorkflow[T, R](ctx, client, options.StartWorkflowOptions, wrkflw, arg)
	if err != nil {
		return GenericWorkflowRun[R]{}, errors.WrapAndTrace(err)
	}
	return wf, nil
}

func (w Workflow[T, R]) ExecuteWorkflow(ctx context.Context, client TemporalClient, options temporalclient.StartWorkflowOptions, args T) (GenericWorkflowRun[R], error) {
	if w.queue != "" {
		if options.TaskQueue != "" {
			logger.Ctx(ctx).Warn("task queue is already set, overwriting",
				zap.String("options.task_queue", options.TaskQueue),
				zap.String("workflow", w.name),
				zap.String("queue", w.queue),
			)
		}
		options.TaskQueue = w.queue
	}
	if w.name == "" {
		fut, err := client.ExecuteWorkflow(
			ctx,
			options,
			w.exec,
			args,
		)
		if err != nil {
			return GenericWorkflowRun[R]{}, errors.WrapAndTrace(err)
		}
		return GenericWorkflowRun[R]{fut}, nil
	} else {
		fut, err := client.ExecuteWorkflow(
			ctx,
			options,
			w.name,
			args,
		)
		if err != nil {
			return GenericWorkflowRun[R]{}, errors.WrapAndTrace(err)
		}
		return GenericWorkflowRun[R]{fut}, nil
	}
}

func (w Workflow[T, R]) Register(wr WorkflowRegisterer) error {
	if w.maxQueueActiviesPerSecond != 0 && w.queue == "" {
		// we do this because setting max queue activities applies to all activities
		// on worker/queue and its unclear when you set this at the workflow level
		// thus we restrict to max queue activities per second to be set only when queue is set
		return errors.Errorf("can not set max queue activities when queue is not set")
	}
	wr.RegisterWorkflowWithOptions(w.exec, workflow.RegisterOptions{Name: w.name})
	return nil
}

func ExecuteWorkflow[T any, R any](ctx context.Context, client TemporalClient, options temporalclient.StartWorkflowOptions, wrkflw func(workflow.Context, T) (R, error), arg T) (GenericWorkflowRun[R], error) {
	fut, err := client.ExecuteWorkflow(
		ctx,
		options,
		wrkflw,
		arg,
	)
	if err != nil {
		return GenericWorkflowRun[R]{}, errors.WrapAndTrace(err)
	}
	return GenericWorkflowRun[R]{fut}, nil
}

func MockOnActivity[T any, R any](te *testsuite.TestWorkflowEnvironment, toMockActivity func(context.Context, T) (R, error), mockActivity func(context.Context, T) (R, error)) {
	te.OnActivity(toMockActivity, mock.Anything, mock.Anything).Return(mockActivity)
}

const (
	AcquireLockSignalName = "acquire-lock-event"
	RequestLockSignalName = "request-lock-event"
)

type ContextKey string

var TemporalClientContextKey ContextKey = "TemporalClient"

type TemporalMutex struct {
	currentWorkflowID string
	lockNamespace     string
}

type TemporalMutexUnlockFunc func() error

func NewTemporalMutex(currentWorkflowID string, lockNamespace string) *TemporalMutex {
	return &TemporalMutex{
		currentWorkflowID: currentWorkflowID,
		lockNamespace:     lockNamespace,
	}
}

var MutexQueueName = "mutex"

func MakeTemporalMutexWorker(workerFactory WorkerFactory, temporalClient TemporalClient) (TemporalWorker, error) {
	options := temporalworker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), TemporalClientContextKey, temporalClient),
	}
	worker := workerFactory.MakeWorker(MutexQueueName, options)
	worker.RegisterWorkflowWithOptions(MutexWorkflow, workflow.RegisterOptions{})
	return worker, nil
}

// WARNING the worker that calls this must have the temporalClient in the context with ClientContextKey
func (m *TemporalMutex) Lock(ctx workflow.Context, resourceID string, unlockTimeout time.Duration) (TemporalMutexUnlockFunc, error) {
	activityCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	})

	var releaseLockChannelName string
	var execution workflow.Execution
	err := workflow.ExecuteLocalActivity(activityCtx, SignalWithStartMutexWorkflowActivity, m.lockNamespace, resourceID, m.currentWorkflowID, unlockTimeout).
		Get(ctx, &execution)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	workflow.GetSignalChannel(ctx, AcquireLockSignalName).
		Receive(ctx, &releaseLockChannelName)

	unlockFunc := func() error {
		return workflow.SignalExternalWorkflow(ctx, execution.ID, execution.RunID,
			releaseLockChannelName, "releaseLock").Get(ctx, nil)
	}
	return unlockFunc, nil
}

func generateUnlockChannelName(senderWorkflowID string) string {
	return fmt.Sprintf("unlock-event-%s", senderWorkflowID)
}

func MutexWorkflow(ctx workflow.Context, _ string, _ string, unlockTimeout time.Duration) error {
	currentWorkflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
	logger := workflow.GetLogger(ctx)
	logger.Info("started", "currentWorkflowID", currentWorkflowID)
	var ack string
	requestLockCh := workflow.GetSignalChannel(ctx, RequestLockSignalName)
	for {
		var senderWorkflowID string
		if !requestLockCh.ReceiveAsync(&senderWorkflowID) {
			logger.Info("MutexWorkflow receive no more signals")
			break
		}
		var releaseLockChannelName string
		_ = workflow.SideEffect(ctx, func(_ workflow.Context) interface{} {
			return generateUnlockChannelName(senderWorkflowID)
		}).Get(&releaseLockChannelName)

		err := workflow.SignalExternalWorkflow(ctx, senderWorkflowID, "", AcquireLockSignalName, releaseLockChannelName).
			Get(ctx, nil)
		if err != nil {
			logger.Error("SignalExternalWorklowError error", zap.Error(err))
			continue
		}

		selector := workflow.NewSelector(ctx)
		selector.AddFuture(workflow.NewTimer(ctx, unlockTimeout), func(_ workflow.Future) {
			logger.Info("MutexWorkflow exceeded unlockTimeout limit")
		})
		selector.AddReceive(workflow.GetSignalChannel(ctx, releaseLockChannelName), func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &ack)
			logger.Info("MutexWorkflow release signal received")
		})
		selector.Select(ctx)
	}
	return nil
}

func SignalWithStartMutexWorkflowActivity(ctx context.Context, namespace string, resourceID string, senderWorkflowID string, unlockTimeout time.Duration) (*workflow.Execution, error) {
	c, ok := ctx.Value(TemporalClientContextKey).(TemporalClient)
	if !ok {
		return nil, errors.Errorf("client not found in context")
	}
	workflowID := fmt.Sprintf("mutex_%s_%s", namespace, resourceID)
	workflowOptions := temporalclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: MutexQueueName,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	}
	wr, err := c.SignalWithStartWorkflow(ctx, workflowID, RequestLockSignalName, senderWorkflowID,
		workflowOptions, MutexWorkflow, namespace, resourceID, unlockTimeout)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	return &workflow.Execution{
		ID:    wr.GetID(),
		RunID: wr.GetRunID(),
	}, nil
}

// MockMutexLock stubs mutex.Lock call
func MockMutexLock(env *testsuite.TestWorkflowEnvironment, resourceID string, mockError error) {
	execution := &workflow.Execution{ID: "mockID", RunID: "mockRunID"}
	env.OnActivity(SignalWithStartMutexWorkflowActivity,
		mock.Anything, mock.Anything, resourceID, mock.Anything, mock.Anything).
		Return(execution, mockError)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(AcquireLockSignalName, "mockReleaseLockChannelName")
	}, time.Millisecond*0)
	if mockError == nil {
		env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, execution.RunID,
			mock.Anything, mock.Anything).Return(nil)
	}
}

type (
	WorkflowIdentifier string
	WorkflowClient     struct {
		TaskQueueName  string
		TemporalClient TemporalClient
		Registerer     WorkflowRegisterer
	}
)

var _ temporallog.Logger = logger.StandardLogger{}

type ScopedWorkflowScheduler struct {
	Client TemporalClient
	Scope  string
}

func NewGlobalWorkflowScheduler(client TemporalClient) ScopedWorkflowScheduler {
	return ScopedWorkflowScheduler{
		Client: client,
		Scope:  "global",
	}
}

func NewScopedWorkflowScheduler(
	client TemporalClient,
	scope string,
) ScopedWorkflowScheduler {
	return ScopedWorkflowScheduler{
		Client: client,
		Scope:  scope,
	}
}

type CreateScheduledWorkflowArgs interface {
	Untyped() CreateScheduledWorkflowArgsUntyped
}
type CreateScheduledWorkflowArgsUntyped struct {
	Name      string
	TaskQueue string
	Workflow  interface{}
	Spec      temporalclient.ScheduleSpec
	Args      interface{}
}

type DeleteScheduledWorkflowArgs interface {
	Untyped() DeleteScheduledWorkflowArgsUntyped
}
type DeleteScheduledWorkflowArgsUntyped struct {
	Name string
	Args interface{}
}

func (d DeleteScheduledWorkflowArgsUntyped) Validate() error {
	var err error
	if d.Name == "" {
		err = errors.Join(err, errors.Errorf("name is required"))
	}
	return errors.WrapAndTrace(err)
}

func (c CreateScheduledWorkflowArgsUntyped) Validate() error {
	var err error
	if c.Name == "" {
		err = errors.Join(err, errors.Errorf("name is required"))
	}
	if c.Workflow == nil {
		err = errors.Join(err, errors.Errorf("workflow is required"))
	}
	if c.TaskQueue == "" {
		err = errors.Join(err, errors.Errorf("task queue is required"))
	}
	if collections.IsEmpty(c.Spec) {
		err = errors.Join(err, errors.Errorf("schedule spec is required"))
	}
	return errors.WrapAndTrace(err)
}

type CreateScheduledWorkflowArgsWorkflow[T, R any] struct {
	Name      string
	Workflow  Workflow[T, R]
	Spec      temporalclient.ScheduleSpec
	TaskQueue string
	Args      T
}

func (c CreateScheduledWorkflowArgsWorkflow[T, R]) Untyped() CreateScheduledWorkflowArgsUntyped {
	taskQueue := c.TaskQueue
	if taskQueue == "" {
		taskQueue = c.Workflow.GetTaskQueueName()
	}
	return CreateScheduledWorkflowArgsUntyped{
		Name:      c.Name,
		Workflow:  c.Workflow.GetName(),
		Spec:      c.Spec,
		TaskQueue: taskQueue,
		Args:      c.Args,
	}
}

type DeleteScheduledWorkflowArgsWorkflow[T, R any] struct {
	Name string
	Args T
}

func (c DeleteScheduledWorkflowArgsWorkflow[T, R]) Untyped() DeleteScheduledWorkflowArgsUntyped {
	return DeleteScheduledWorkflowArgsUntyped{
		Name: c.Name,
		Args: c.Args,
	}
}

type CreateScheduledWorkflowArgsName struct {
	Name         string
	WorkflowName string
	TaskQueue    string
	Spec         temporalclient.ScheduleSpec
	Args         any
}

func (c CreateScheduledWorkflowArgsName) Untyped() CreateScheduledWorkflowArgsUntyped {
	return CreateScheduledWorkflowArgsUntyped{
		Name:      c.Name,
		TaskQueue: c.TaskQueue,
		Workflow:  c.WorkflowName,
		Spec:      c.Spec,
		Args:      c.Args,
	}
}

// Creates/updates schedules and deletes any schedules that are not in the list
func (g ScopedWorkflowScheduler) SyncSchedules(ctx context.Context, scheduledWorkflows []CreateScheduledWorkflowArgs) error {
	res, err := g.Client.ScheduleClient().List(ctx, temporalclient.ScheduleListOptions{})
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	scopedSchedules := []*temporalclient.ScheduleListEntry{}
	err = collections.Iterate(res.HasNext, res.Next, func(s *temporalclient.ScheduleListEntry) (bool, error) {
		if strings.HasPrefix(s.ID, g.Scope) {
			scopedSchedules = append(scopedSchedules, s)
		}
		return true, nil
	})
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	prevScheduledIDs := collections.Map(scopedSchedules, func(s *temporalclient.ScheduleListEntry) string {
		return s.ID
	})
	createdSched, err := collections.MapE(scheduledWorkflows, func(sw CreateScheduledWorkflowArgs) (temporalclient.ScheduleHandle, error) {
		sched, err1 := g.CreateOrUpdateScheduledWorkflow(ctx, sw)
		if err1 != nil {
			return nil, errors.WrapAndTrace(err1)
		}
		return sched, nil
	})
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	createdScheduledIDs := collections.Map(createdSched, func(s temporalclient.ScheduleHandle) string {
		return s.GetID()
	})
	toDeleteSchedules := collections.ListDiff(prevScheduledIDs, createdScheduledIDs)
	deletedSchedules, err := collections.MapE(toDeleteSchedules, func(id string) (string, error) {
		schedHan := g.Client.ScheduleClient().GetHandle(ctx, id)
		err = schedHan.Delete(ctx)
		if err != nil {
			return "", errors.WrapAndTrace(err)
		}
		return id, nil
	})
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	logger.Ctx(ctx).Info("deleted schedules", zap.Any("deleted_schedules", deletedSchedules))
	return nil
}

// Creates a new schedule if not exists or updates an existing schedule
func (g ScopedWorkflowScheduler) CreateOrUpdateScheduledWorkflow(
	ctx context.Context,
	args CreateScheduledWorkflowArgs,
) (temporalclient.ScheduleHandle, error) {
	uArgs := args.Untyped()
	err := uArgs.Validate()
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	options := g.makeScheduledWorkfowOptions(uArgs)
	sched := g.Client.ScheduleClient().GetHandle(ctx, options.ID)
	_, err = sched.Describe(ctx)
	if errors.ErrorContainsAny(err, "no rows in result set", "schedule not found", "current workflow not found") {
		var errc error
		sched, errc = g.Client.ScheduleClient().Create(ctx, options)
		if errc != nil {
			return nil, errors.WrapAndTrace(errc)
		}
		return sched, nil
	} else if err != nil {
		return nil, errors.Errorf("error getting schedule %s: %w", options.ID, err)
	}
	err = sched.Update(ctx, temporalclient.ScheduleUpdateOptions{
		DoUpdate: func(schedule temporalclient.ScheduleUpdateInput) (*temporalclient.ScheduleUpdate, error) {
			schedule.Description.Schedule.Action = options.Action
			schedule.Description.Schedule.Spec = &options.Spec
			return &temporalclient.ScheduleUpdate{
				Schedule: &schedule.Description.Schedule,
			}, nil
		},
	})
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	return sched, nil
}

// Pauses an existing scheduled workflow
func (g ScopedWorkflowScheduler) PauseScheduledWorkflow(
	ctx context.Context,
	scheduleID string,
) (temporalclient.ScheduleHandle, error) {
	sched := g.Client.ScheduleClient().GetHandle(ctx, scheduleID)
	desc, err := sched.Describe(ctx)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}

	if desc.Schedule.State.Paused {
		logger.Ctx(ctx).Info("schedule is already paused, no action taken", zap.String("scheduleID", scheduleID))
		return sched, nil
	}

	err = sched.Pause(ctx, temporalclient.SchedulePauseOptions{
		Note: "The Schedule has been paused.",
	})
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}

	logger.Ctx(ctx).Info("paused schedule with ID", zap.String("scheduleID", scheduleID))
	return sched, nil
}

// Unpauses an existing scheduled workflow
func (g ScopedWorkflowScheduler) UnpauseScheduledWorkflow(
	ctx context.Context,
	scheduleID string,
) (temporalclient.ScheduleHandle, error) {
	sched := g.Client.ScheduleClient().GetHandle(ctx, scheduleID)
	desc, err := sched.Describe(ctx)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}

	if !desc.Schedule.State.Paused {
		logger.Ctx(ctx).Info("schedule is not paused, no action taken", zap.String("scheduleID", scheduleID))
		return sched, nil
	}

	err = sched.Unpause(ctx, temporalclient.ScheduleUnpauseOptions{
		Note: "The Schedule has been unpaused.",
	})
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}

	logger.Ctx(ctx).Info("unpaused schedule with ID", zap.String("scheduleID", scheduleID))
	return sched, nil
}

// Deletes Scheduled Workflow If It Exists
func (g ScopedWorkflowScheduler) DeleteScheduledWorkflow(
	ctx context.Context,
	args DeleteScheduledWorkflowArgs,
) (temporalclient.ScheduleHandle, error) {
	uArgs := args.Untyped()
	err := uArgs.Validate()
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	scheduleID := fmt.Sprintf("%s-%s", g.Scope, uArgs.Name)
	sched := g.Client.ScheduleClient().GetHandle(ctx, scheduleID)
	_, err = sched.Describe(ctx)
	if err != nil {
		return nil, errors.Errorf("error getting schedule %s: %w", scheduleID, err)
	}
	err = sched.Delete(ctx)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	return sched, nil
}

func (g ScopedWorkflowScheduler) makeScheduledWorkfowOptions(
	args CreateScheduledWorkflowArgsUntyped,
) temporalclient.ScheduleOptions {
	scheduleID := fmt.Sprintf("%s-%s", g.Scope, args.Name)
	schedOptions := temporalclient.ScheduleOptions{
		ID:   scheduleID,
		Spec: args.Spec,
		Action: &temporalclient.ScheduleWorkflowAction{
			Workflow:  args.Workflow,
			TaskQueue: args.TaskQueue,
			Args:      []any{args.Args},
		},
	}
	return schedOptions
}

type TemporalWorkerManager struct {
	Name    string
	Workers []TemporalWorker
}

var _ collections.Runnable = (*TemporalWorkerManager)(nil)

func (m *TemporalWorkerManager) AddWorker(ws ...TemporalWorker) {
	for _, w := range ws {
		m.Workers = append(m.Workers, w)
	}
}

func (m TemporalWorkerManager) Run(ctx context.Context) error {
	logger.Ctx(ctx).Info("starting worker manager", zap.String("name", m.Name))
	groupedByQ := collections.GroupBy(m.Workers, func(w TemporalWorker) string {
		return w.GetTaskQueueName()
	})
	for q, workers := range groupedByQ {
		if len(workers) > 1 {
			return errors.Errorf("can not have multiple workers for queue %s", q)
		}
	}
	runnables := collections.Map(m.Workers, func(w TemporalWorker) collections.Runnable {
		return w
	})
	err := collections.RunAllWithShutdown(ctx, runnables, temporalworker.InterruptCh())
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

type TemporalWorker interface {
	collections.Runnable
	WorkflowRegisterer
	WithMustHaveActivities(acts ...interface{}) TemporalWorker
}

func (m TemporalWorkerManager) Shutdown(ctx context.Context) error {
	var allError error
	for _, w := range m.Workers {
		err := w.Shutdown(ctx)
		if err != nil {
			allError = errors.Join(allError, err)
		}
	}
	return errors.WrapAndTrace(allError)
}

type WorkerFactory interface {
	MakeWorker(taskQueueName string, options temporalworker.Options) TemporalWorker
}

type MultiClientWorkerFactory struct {
	Clients []temporalclient.Client
}

func NewMultiClientWorkerFactory(clients ...temporalclient.Client) MultiClientWorkerFactory {
	return MultiClientWorkerFactory{
		Clients: clients,
	}
}

var _ WorkerFactory = (*MultiClientWorkerFactory)(nil)

func (m MultiClientWorkerFactory) MakeWorker(taskQueueName string, options temporalworker.Options) TemporalWorker {
	var workers []TemporalWorker
	for _, c := range m.Clients {
		workers = append(workers, &TemporalRunnableWorker{
			TaskQueueName: taskQueueName,
			Worker:        temporalworker.New(c, taskQueueName, options),
		})
	}
	return NewMultiTemporalRunnableWorker(taskQueueName, workers...)
}

type MultiTemporalRunnableWorker struct {
	TaskQueueName string
	Workers       []TemporalWorker
}

var _ TemporalWorker = (*MultiTemporalRunnableWorker)(nil)

func NewMultiTemporalRunnableWorker(taskQueueName string, workers ...TemporalWorker) MultiTemporalRunnableWorker {
	return MultiTemporalRunnableWorker{
		TaskQueueName: taskQueueName,
		Workers:       workers,
	}
}

func (w MultiTemporalRunnableWorker) Name() string {
	return w.TaskQueueName
}

func (w MultiTemporalRunnableWorker) GetTaskQueueName() string {
	return w.TaskQueueName
}

func (w MultiTemporalRunnableWorker) WithMustHaveActivities(acts ...interface{}) TemporalWorker {
	for i, ww := range w.Workers {
		w.Workers[i] = ww.WithMustHaveActivities(acts...)
	}
	return w
}

func (w MultiTemporalRunnableWorker) RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions) {
	for _, ww := range w.Workers {
		ww.RegisterActivityWithOptions(a, options)
	}
}

func (w MultiTemporalRunnableWorker) RegisterWorkflowWithOptions(wr interface{}, options workflow.RegisterOptions) {
	for _, ww := range w.Workers {
		ww.RegisterWorkflowWithOptions(wr, options)
	}
}

func (w MultiTemporalRunnableWorker) Run(ctx context.Context) error {
	logger.Ctx(ctx).Info("starting worker", zap.String("name", w.Name()))
	runnables := collections.Map(w.Workers, func(w TemporalWorker) collections.Runnable {
		return w
	})
	_, err := collections.ParallelWorkerMapExitOnE(ctx, runnables, func(ctx context.Context, r collections.Runnable) (any, error) {
		errr := r.Run(ctx)
		if errr != nil {
			return nil, errors.WrapAndTrace(errr)
		}
		return nil, nil
	}, len(runnables))
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

func (w MultiTemporalRunnableWorker) Shutdown(ctx context.Context) error {
	var allError error
	for _, r := range w.Workers {
		err := r.Shutdown(ctx)
		if err != nil {
			allError = errors.Join(allError, err)
		}
	}
	return errors.WrapAndTrace(allError)
}

type TemporalRunnableWorker struct {
	TaskQueueName     string
	mustHaveActivites []interface{}
	temporalworker.Worker

	registeredActivities []interface{}
}

var (
	_ WorkflowRegisterer   = (*TemporalRunnableWorker)(nil)
	_ collections.Runnable = (*TemporalRunnableWorker)(nil)
)

func (w TemporalRunnableWorker) WithMustHaveActivities(acts ...interface{}) TemporalWorker {
	w.mustHaveActivites = acts
	return &w
}

func (w TemporalRunnableWorker) GetTaskQueueName() string {
	return w.TaskQueueName
}

func (w *TemporalRunnableWorker) RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions) {
	w.Worker.RegisterActivityWithOptions(a, options)
	w.registeredActivities = append(w.registeredActivities, a)
}

func (w TemporalRunnableWorker) Run(ctx context.Context) error {
	if len(w.mustHaveActivites) > 0 {
		var err error
		activitiesToStr := collections.Map(w.registeredActivities, collections.GetFunctionName)
		for _, a := range w.mustHaveActivites {
			activityStr := collections.GetFunctionName(a)
			if !collections.ListContains(activitiesToStr, activityStr) {
				err = errors.Join(err, errors.Errorf("must have activity %T", a))
			}
		}
		if err != nil {
			return errors.WrapAndTrace(err)
		}
	}
	logger.Ctx(ctx).Info("starting worker", zap.String("name", w.Name()))
	err := w.Worker.Start()
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	select {} // block forever
}

func (w TemporalRunnableWorker) Shutdown(_ context.Context) error {
	w.Worker.Stop()
	return nil
}

func (w TemporalRunnableWorker) Name() string {
	return w.TaskQueueName
}

type MultiClient struct {
	Primary                TemporalClient
	Other                  []TemporalClient
	ScheduleTemporalClient TemporalClient // client to run schedules on
}

// create new workflows on primary
// keep old workflows going (continue as new on old)
// read from all (start with primary)
// re-create workflows on primary

var _ TemporalClient = MultiClient{}

// primary clients will be used for creating new workflows on
func NewMultiClient(primary TemporalClient, clients ...TemporalClient) MultiClient {
	return MultiClient{
		Primary: primary,
		Other:   clients,
	}
}

func (m MultiClient) WithScheduleClient(client TemporalClient) MultiClient {
	m.ScheduleTemporalClient = client
	m.Other = append(m.Other, client)
	return m
}

func (m MultiClient) SignalWithStartWorkflow(ctx context.Context, workflowID string, signalName string, signalArg interface{}, options temporalclient.StartWorkflowOptions, wf interface{}, args ...interface{}) (temporalclient.WorkflowRun, error) {
	if len(m.Other) == 0 {
		res, err := m.Primary.SignalWithStartWorkflow(ctx, workflowID, signalName, signalArg, options, wf, args...)
		if err != nil {
			return nil, errors.WrapAndTrace(err)
		}
		return res, nil
	}
	cl, we, err := m.describeWorkflowExecution(ctx, options.ID, "")
	if IsErrorNotFound(err) {
		res, errr := m.Primary.SignalWithStartWorkflow(ctx, workflowID, signalName, signalArg, options, wf, args...)
		if errr != nil {
			return nil, errors.WrapAndTrace(errr)
		}
		return res, nil
	} else if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	err = cl.SignalWorkflow(ctx, we.WorkflowExecutionInfo.Execution.WorkflowId, we.WorkflowExecutionInfo.Execution.RunId, signalName, signalArg)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}

	wfe, err := cl.GetWorkflow(ctx, we.WorkflowExecutionInfo.Execution.GetWorkflowId(), we.WorkflowExecutionInfo.Execution.GetRunId())
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	return wfe, nil
}

func (m MultiClient) ExecuteWorkflow(ctx context.Context, //nolint:gocyclo,gocognit // TODO refactor
	options temporalclient.StartWorkflowOptions,
	workflow interface{},
	args ...interface{},
) (temporalclient.WorkflowRun, error) {
	if len(m.Other) == 0 {
		res, err := m.Primary.ExecuteWorkflow(ctx, options, workflow, args...)
		if err != nil {
			return nil, errors.WrapAndTrace(err)
		}
		return res, nil
	}
	if options.ID == "" {
		res, err := m.Primary.ExecuteWorkflow(ctx, options, workflow, args...)
		if err != nil {
			return nil, errors.WrapAndTrace(err)
		}
		return res, nil
	}
	// It is not possible for a new Workflow Execution to spawn with the same Workflow Id as another Open (open == running) Workflow Execution, regardless of the Workflow Id Reuse Policy.
	// In some cases, an attempt to spawn a Workflow Execution with a Workflow Id that is the same as the Id of a currently Open Workflow Execution results in a Workflow execution already started error.
	// A Workflow Id Reuse Policy applies only if a Closed Workflow Execution with the same Workflow Id exists within the Retention Period of the associated Namespace.
	// For example, if the Namespace's retention period is 30 days, a Workflow Id Reuse Policy can only compare the Workflow Id of the spawning Workflow Execution against the Closed Workflow Executions for the last 30 days.
	// https://docs.temporal.io/workflows#workflow-id
	cl, we, err := m.describeWorkflowExecution(ctx, options.ID, "")
	if err != nil && !IsErrorNotFound(err) {
		return nil, errors.WrapAndTrace(err)
	}
	if we != nil && we.GetWorkflowExecutionInfo().Status == temporalenums.WORKFLOW_EXECUTION_STATUS_RUNNING { // is open
		if options.WorkflowExecutionErrorWhenAlreadyStarted {
			return nil, serviceerror.NewWorkflowExecutionAlreadyStarted("workflow already started",
				we.WorkflowExecutionInfo.Execution.WorkflowId,
				we.WorkflowExecutionInfo.Execution.RunId,
			)
		}
		if options.WorkflowIDReusePolicy == temporalenums.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING {
			// Specifies that if a Workflow Execution with the same Workflow Id is already running, it should be terminated and a new Workflow Execution with the same Workflow Id should be started.
			// This policy allows for only one Workflow Execution with a specific Workflow Id to be running at any given time.
			err = cl.TerminateWorkflow(ctx, options.ID, we.WorkflowExecutionInfo.Execution.RunId, "new workflow started")
			if err != nil {
				return nil, errors.WrapAndTrace(err)
			}
		} else {
			wf, errw := cl.GetWorkflow(ctx, options.ID, we.WorkflowExecutionInfo.Execution.RunId)
			if errw != nil {
				return nil, errors.WrapAndTrace(errw)
			}
			return wf, nil
		}
	}

	// what do we do when closed
	if collections.ListContains([]temporalenums.WorkflowIdReusePolicy{ //nolint:gocritic // this if else is good
		temporalenums.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
		temporalenums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		temporalenums.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
	}, options.WorkflowIDReusePolicy) {
		// he Workflow Execution is allowed to exist regardless of the Closed status of a previous Workflow Execution with the same Workflow Id.
		// This is the default policy, if one is not specified. Use this when it is OK to have a Workflow Execution with the same Workflow Id as a previous, but now Closed, Workflow Execution.
		res, errr := m.Primary.ExecuteWorkflow(ctx, options, workflow, args...)
		if errr != nil {
			return nil, errors.WrapAndTrace(errr)
		}
		return res, nil
	} else if options.WorkflowIDReusePolicy == temporalenums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY {
		// The Workflow Execution is allowed to exist only if a previous Workflow Execution with the same Workflow Id does not have a Completed status.
		// Use this policy when there is a need to re-execute a Failed, Timed Out, Terminated or Canceled Workflow Execution and guarantee that the Completed Workflow Execution will not be re-executed.
		if we != nil && collections.ListContains([]temporalenums.WorkflowExecutionStatus{temporalenums.WORKFLOW_EXECUTION_STATUS_COMPLETED, temporalenums.WORKFLOW_EXECUTION_STATUS_RUNNING}, we.GetWorkflowExecutionInfo().Status) {
			// im unsure if we should error here or return workflow
			wf, errw := cl.GetWorkflow(ctx, options.ID, we.WorkflowExecutionInfo.Execution.RunId)
			if errw != nil {
				return nil, errors.WrapAndTrace(errw)
			}
			return wf, nil
		}
		res, errr := m.Primary.ExecuteWorkflow(ctx, options, workflow, args...)
		if errr != nil {
			return nil, errors.WrapAndTrace(errr)
		}
		return res, nil
	} else if options.WorkflowIDReusePolicy == temporalenums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE {
		// The Workflow Execution cannot exist if a previous Workflow Execution has the same Workflow Id, regardless of the Closed status.
		// Use this when there can only be one Workflow Execution per Workflow Id within a Namespace for the given retention period.
		if we != nil {
			wf, errw := cl.GetWorkflow(ctx, options.ID, we.WorkflowExecutionInfo.Execution.RunId)
			// im unsure if we should error here or return workflow
			if errw != nil {
				return nil, errors.WrapAndTrace(errw)
			}
			return wf, nil
		}
		res, errr := m.Primary.ExecuteWorkflow(ctx, options, workflow, args...)
		if errr != nil {
			return nil, errors.WrapAndTrace(errr)
		}
		return res, nil
	} else {
		return nil, errors.Errorf("unknown workflow id reuse policy %s", options.WorkflowIDReusePolicy)
	}
}

func (m MultiClient) describeWorkflowExecution(ctx context.Context, workflowID, runID string) (TemporalClient, *workflowservice.DescribeWorkflowExecutionResponse, error) {
	pdwer, err := m.Primary.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil && !IsErrorNotFound(err) {
		return nil, nil, errors.WrapAndTrace(err)
	}
	if pdwer != nil {
		return m.Primary, pdwer, nil
	}
	for _, c := range m.Other {
		dwer, err := c.DescribeWorkflowExecution(ctx, workflowID, runID)
		if err != nil && !IsErrorNotFound(err) {
			return nil, nil, errors.WrapAndTrace(err)
		}
		if dwer != nil {
			return c, dwer, nil
		}
	}
	return nil, nil, errors.Errorf("workflow not found")
}

func (m MultiClient) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if len(m.Other) == 0 {
		dwer, err := m.Primary.DescribeWorkflowExecution(ctx, workflowID, runID)
		if err != nil {
			return nil, errors.WrapAndTrace(err)
		}
		return dwer, nil
	}
	_, dwer, err := m.describeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	return dwer, nil
}

func (m MultiClient) GetWorkflow(ctx context.Context, workflowID string, runID string) (temporalclient.WorkflowRun, error) {
	if len(m.Other) == 0 {
		wf, err := m.Primary.GetWorkflow(ctx, workflowID, runID)
		if err != nil {
			return nil, errors.WrapAndTrace(err)
		}
		return wf, nil
	}
	cl, _, err := m.describeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	wf, err := cl.GetWorkflow(ctx, workflowID, runID)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	return wf, nil
}

func (m MultiClient) QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...interface{}) (converter.EncodedValue, error) {
	if len(m.Other) == 0 {
		qw, err := m.Primary.QueryWorkflow(ctx, workflowID, runID, queryType, args...)
		if err != nil {
			return nil, errors.WrapAndTrace(err)
		}
		return qw, nil
	}
	cl, _, err := m.describeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	qw, err := cl.QueryWorkflow(ctx, workflowID, runID, queryType, args...)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	return qw, nil
}

func (m MultiClient) SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error {
	if len(m.Other) == 0 {
		err := m.Primary.SignalWorkflow(ctx, workflowID, runID, signalName, arg)
		if err != nil {
			return errors.WrapAndTrace(err)
		}
		return nil
	}
	cl, _, err := m.describeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	err = cl.SignalWorkflow(ctx, workflowID, runID, signalName, arg)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

func (m MultiClient) CancelWorkflow(ctx context.Context, workflowID string, runID string) error {
	if len(m.Other) == 0 {
		err := m.Primary.CancelWorkflow(ctx, workflowID, runID)
		if err != nil {
			return errors.WrapAndTrace(err)
		}
		return nil
	}
	cl, _, err := m.describeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	err = cl.CancelWorkflow(ctx, workflowID, runID)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

func (m MultiClient) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string, details ...interface{}) error {
	if len(m.Other) == 0 {
		err := m.Primary.TerminateWorkflow(ctx, workflowID, runID, reason, details...)
		if err != nil {
			return errors.WrapAndTrace(err)
		}
		return nil
	}
	cl, _, err := m.describeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	err = cl.TerminateWorkflow(ctx, workflowID, runID, reason, details...)
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}

func (m MultiClient) ScheduleClient() temporalclient.ScheduleClient {
	if m.ScheduleTemporalClient != nil {
		return m.ScheduleTemporalClient.ScheduleClient()
	}
	scheduleClient := m.Primary.ScheduleClient()
	return scheduleClient
}

func (m MultiClient) ListWorkflow(ctx context.Context, lwer *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	if len(m.Other) == 0 {
		primRes, err := m.Primary.ListWorkflow(ctx, lwer)
		if err != nil {
			return nil, errors.WrapAndTrace(err)
		}
		return primRes, nil
	}
	if len(lwer.NextPageToken) != 0 {
		// TODO: support next page token
		// will need to implement a way where clients can keep paging and we exhaust all clients
		return nil, errors.Errorf("can not use next page token with multiple clients")
	}
	lwer.Namespace = ""
	primRes, err := m.Primary.ListWorkflow(ctx, lwer)
	if err != nil {
		return nil, errors.WrapAndTrace(err)
	}
	nextTokens := []string{
		string(primRes.NextPageToken),
	}
	executions := primRes.Executions
	for _, c := range m.Other {
		lwer.Namespace = ""
		otherRes, err := c.ListWorkflow(ctx, lwer)
		if err != nil {
			return nil, errors.WrapAndTrace(err)
		}
		nextTokens = append(nextTokens, string(otherRes.NextPageToken))
		executions = append(executions, otherRes.Executions...)
	}
	joinedTokens := strings.Join(nextTokens, ",")
	return &workflowservice.ListWorkflowExecutionsResponse{
		Executions:    executions,
		NextPageToken: []byte(joinedTokens),
	}, nil
}

func WaitForever(ctx workflow.Context) error {
	return errors.WrapAndTrace(workflow.Await(ctx, func() bool {
		return false
	}))
}

func NewNonRetryableError(err error) error {
	return temporal.NewNonRetryableApplicationError("", "", errors.Wrap(err, "no-temporal-retry"))
}

func CreateNamespace(ctx context.Context, client temporalclient.NamespaceClient, name string) error {
	err := client.Register(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: name,
	})
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}
