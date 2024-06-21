package temporalhelpers

import (
	"context"
	"testing"
	"time"

	"github.com/brevdev/dev-plane/pkg/collections"
	"github.com/brevdev/dev-plane/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	temporalenums "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type TestActivities struct{}

func (a *TestActivities) DoSomething() {
}

var testActivities *TestActivities

func Test_GetFunctionName(t *testing.T) {
	name := collections.GetFunctionName(testActivities.DoSomething)
	assert.Equal(t, "github.com/brevdev/dev-plane/internal/temporalhelpers.(*TestActivities).DoSomething-fm", name)

	name = collections.GetFunctionName("myName")
	assert.Equal(t, "myName", name)
}

func Test_ExecuteWorkflowOrGetOptions(t *testing.T) {
	opts := ExecuteWorkflowOrGetOptions{}
	assert.True(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_RUNNING))
	assert.True(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_COMPLETED))
	assert.True(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_FAILED))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_CANCELED))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_TERMINATED))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT))

	opts = ExecuteWorkflowOrGetOptions{
		GetOnStatus: []temporalenums.WorkflowExecutionStatus{},
	}
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_RUNNING))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_COMPLETED))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_FAILED))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_CANCELED))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_TERMINATED))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT))

	opts = ExecuteWorkflowOrGetOptions{
		GetOnStatus: []temporalenums.WorkflowExecutionStatus{
			temporalenums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			temporalenums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		},
	}
	assert.True(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_RUNNING))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_COMPLETED))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_FAILED))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_CANCELED))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_TERMINATED))
	assert.False(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW))
	assert.True(t, opts.shouldGet(temporalenums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT))
}

type UnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}

func (s *UnitTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()

	s.env.SetWorkerOptions(worker.Options{})
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), TemporalClientContextKey, s.env),
	})
}

func SampleWorkflowWithMutex(
	ctx workflow.Context,
	resourceID string,
) error {
	currentWorkflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
	logger := workflow.GetLogger(ctx)
	logger.Info("started", "currentWorkflowID", currentWorkflowID, "resourceID", resourceID)

	mutex := NewTemporalMutex(currentWorkflowID, "TestUseCase")
	unlockFunc, err := mutex.Lock(ctx, resourceID, 10*time.Minute)
	if err != nil {
		return err
	}
	logger.Info("resource locked")

	// emulate long running process
	logger.Info("critical operation started")
	_ = workflow.Sleep(ctx, 10*time.Second)
	logger.Info("critical operation finished")

	_ = unlockFunc()

	logger.Info("finished")
	return nil
}

var mockResourceID = "mockResourceID"

func (s *UnitTestSuite) Test_Workflow_Success() {
	env := s.NewTestWorkflowEnvironment()
	MockMutexLock(env, mockResourceID, nil)
	env.ExecuteWorkflow(SampleWorkflowWithMutex, mockResourceID)

	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
	env.AssertExpectations(s.T())
}

func (s *UnitTestSuite) Test_Workflow_Error() {
	env := s.NewTestWorkflowEnvironment()
	MockMutexLock(env, mockResourceID, errors.New("bad-error"))
	env.ExecuteWorkflow(SampleWorkflowWithMutex, mockResourceID)

	s.True(env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	var applicationErr *temporal.ApplicationError
	s.True(errors.As(err, &applicationErr))
	s.Contains(applicationErr.Error(), "bad-error")
	env.AssertExpectations(s.T())
}

func (s *UnitTestSuite) Test_MutexWorkflow_Success() {
	mockNamespace := "mockNamespace"
	mockUnlockTimeout := 10 * time.Minute
	mockSenderWorkflowID := "mockSenderWorkflowID"
	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(RequestLockSignalName, mockSenderWorkflowID)
	}, time.Millisecond*0)
	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow("unlock-event-mockSenderWorkflowID", "releaseLock")
	}, time.Millisecond*0)
	s.env.OnSignalExternalWorkflow(mock.Anything, mockSenderWorkflowID, "",
		AcquireLockSignalName, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(
		MutexWorkflow,
		mockNamespace,
		mockResourceID,
		mockUnlockTimeout,
	)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_MutexWorkflow_TimeoutSuccess() {
	mockNamespace := "mockNamespace"
	mockUnlockTimeout := 10 * time.Minute
	mockSenderWorkflowID := "mockSenderWorkflowID"
	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(RequestLockSignalName, mockSenderWorkflowID)
	}, time.Millisecond*0)
	s.env.OnSignalExternalWorkflow(mock.Anything, mockSenderWorkflowID, "",
		AcquireLockSignalName, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(
		MutexWorkflow,
		mockNamespace,
		mockResourceID,
		mockUnlockTimeout,
	)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}
