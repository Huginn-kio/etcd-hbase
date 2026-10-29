package trace

import (
	"context"
	"errors"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ClassifyError(err error) *Error {
	if err == nil {
		return nil
	}
	kind := "go-error"
	grpcCode := status.Code(err)
	switch {
	case errors.Is(err, context.Canceled):
		kind = "context-canceled"
		grpcCode = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		kind = "context-deadline"
		grpcCode = codes.DeadlineExceeded
	default:
		var codeProvider interface{ Code() codes.Code }
		if errors.As(err, &codeProvider) {
			kind = "etcd-client-error"
			grpcCode = codeProvider.Code()
		} else if _, ok := status.FromError(err); ok {
			kind = "grpc-status"
		}
	}
	return &Error{GRPCCode: grpcCode.String(), ConsumerKind: kind, SemanticKind: semanticKind(err)}
}

func validGRPCCode(value string) bool {
	for code := codes.OK; code <= codes.Unauthenticated; code++ {
		if code.String() == value && code != codes.OK {
			return true
		}
	}
	return false
}

// Preserve canonical error identities without copying descriptions that may contain secrets.
var canonicalErrors = []struct {
	kind  string
	value error
}{
	{"etcd/EmptyKey", rpctypes.Error(rpctypes.ErrGRPCEmptyKey)},
	{"etcd/KeyNotFound", rpctypes.Error(rpctypes.ErrGRPCKeyNotFound)},
	{"etcd/ValueProvided", rpctypes.Error(rpctypes.ErrGRPCValueProvided)},
	{"etcd/LeaseProvided", rpctypes.Error(rpctypes.ErrGRPCLeaseProvided)},
	{"etcd/TooManyOps", rpctypes.Error(rpctypes.ErrGRPCTooManyOps)},
	{"etcd/DuplicateKey", rpctypes.Error(rpctypes.ErrGRPCDuplicateKey)},
	{"etcd/Compacted", rpctypes.Error(rpctypes.ErrGRPCCompacted)},
	{"etcd/FutureRev", rpctypes.Error(rpctypes.ErrGRPCFutureRev)},
	{"etcd/NoSpace", rpctypes.Error(rpctypes.ErrGRPCNoSpace)},
	{"etcd/LeaseNotFound", rpctypes.Error(rpctypes.ErrGRPCLeaseNotFound)},
	{"etcd/LeaseExist", rpctypes.Error(rpctypes.ErrGRPCLeaseExist)},
	{"etcd/LeaseTTLTooLarge", rpctypes.Error(rpctypes.ErrGRPCLeaseTTLTooLarge)},
	{"etcd/WatchCanceled", rpctypes.Error(rpctypes.ErrGRPCWatchCanceled)},
	{"etcd/MemberExist", rpctypes.Error(rpctypes.ErrGRPCMemberExist)},
	{"etcd/PeerURLExist", rpctypes.Error(rpctypes.ErrGRPCPeerURLExist)},
	{"etcd/MemberNotEnoughStarted", rpctypes.Error(rpctypes.ErrGRPCMemberNotEnoughStarted)},
	{"etcd/MemberBadURLs", rpctypes.Error(rpctypes.ErrGRPCMemberBadURLs)},
	{"etcd/MemberNotFound", rpctypes.Error(rpctypes.ErrGRPCMemberNotFound)},
	{"etcd/MemberNotLearner", rpctypes.Error(rpctypes.ErrGRPCMemberNotLearner)},
	{"etcd/LearnerNotReady", rpctypes.Error(rpctypes.ErrGRPCLearnerNotReady)},
	{"etcd/TooManyLearners", rpctypes.Error(rpctypes.ErrGRPCTooManyLearners)},
	{"etcd/ClusterIdMismatch", rpctypes.Error(rpctypes.ErrGRPCClusterIdMismatch)},
	{"etcd/RequestTooLarge", rpctypes.Error(rpctypes.ErrGRPCRequestTooLarge)},
	{"etcd/RequestTooManyRequests", rpctypes.Error(rpctypes.ErrGRPCRequestTooManyRequests)},
	{"etcd/RootUserNotExist", rpctypes.Error(rpctypes.ErrGRPCRootUserNotExist)},
	{"etcd/RootRoleNotExist", rpctypes.Error(rpctypes.ErrGRPCRootRoleNotExist)},
	{"etcd/UserAlreadyExist", rpctypes.Error(rpctypes.ErrGRPCUserAlreadyExist)},
	{"etcd/UserEmpty", rpctypes.Error(rpctypes.ErrGRPCUserEmpty)},
	{"etcd/UserNotFound", rpctypes.Error(rpctypes.ErrGRPCUserNotFound)},
	{"etcd/RoleAlreadyExist", rpctypes.Error(rpctypes.ErrGRPCRoleAlreadyExist)},
	{"etcd/RoleNotFound", rpctypes.Error(rpctypes.ErrGRPCRoleNotFound)},
	{"etcd/RoleEmpty", rpctypes.Error(rpctypes.ErrGRPCRoleEmpty)},
	{"etcd/AuthFailed", rpctypes.Error(rpctypes.ErrGRPCAuthFailed)},
	{"etcd/PermissionNotGiven", rpctypes.Error(rpctypes.ErrGRPCPermissionNotGiven)},
	{"etcd/PermissionDenied", rpctypes.Error(rpctypes.ErrGRPCPermissionDenied)},
	{"etcd/RoleNotGranted", rpctypes.Error(rpctypes.ErrGRPCRoleNotGranted)},
	{"etcd/PermissionNotGranted", rpctypes.Error(rpctypes.ErrGRPCPermissionNotGranted)},
	{"etcd/AuthNotEnabled", rpctypes.Error(rpctypes.ErrGRPCAuthNotEnabled)},
	{"etcd/InvalidAuthToken", rpctypes.Error(rpctypes.ErrGRPCInvalidAuthToken)},
	{"etcd/InvalidAuthMgmt", rpctypes.Error(rpctypes.ErrGRPCInvalidAuthMgmt)},
	{"etcd/AuthOldRevision", rpctypes.Error(rpctypes.ErrGRPCAuthOldRevision)},
	{"etcd/NoLeader", rpctypes.Error(rpctypes.ErrGRPCNoLeader)},
	{"etcd/NotLeader", rpctypes.Error(rpctypes.ErrGRPCNotLeader)},
	{"etcd/LeaderChanged", rpctypes.Error(rpctypes.ErrGRPCLeaderChanged)},
	{"etcd/NotCapable", rpctypes.Error(rpctypes.ErrGRPCNotCapable)},
	{"etcd/Stopped", rpctypes.Error(rpctypes.ErrGRPCStopped)},
	{"etcd/Timeout", rpctypes.Error(rpctypes.ErrGRPCTimeout)},
	{"etcd/TimeoutDueToLeaderFail", rpctypes.Error(rpctypes.ErrGRPCTimeoutDueToLeaderFail)},
	{"etcd/TimeoutDueToConnectionLost", rpctypes.Error(rpctypes.ErrGRPCTimeoutDueToConnectionLost)},
	{"etcd/TimeoutWaitAppliedIndex", rpctypes.Error(rpctypes.ErrGRPCTimeoutWaitAppliedIndex)},
	{"etcd/Unhealthy", rpctypes.Error(rpctypes.ErrGRPCUnhealthy)},
	{"etcd/Corrupt", rpctypes.Error(rpctypes.ErrGRPCCorrupt)},
	{"etcd/NotSupportedForLearner", rpctypes.Error(rpctypes.ErrGPRCNotSupportedForLearner)},
	{"etcd/BadLeaderTransferee", rpctypes.Error(rpctypes.ErrGRPCBadLeaderTransferee)},
	{"etcd/ClusterVersionUnavailable", rpctypes.Error(rpctypes.ErrGRPCClusterVersionUnavailable)},
	{"etcd/WrongDowngradeVersionFormat", rpctypes.Error(rpctypes.ErrGRPCWrongDowngradeVersionFormat)},
	{"etcd/InvalidDowngradeTargetVersion", rpctypes.Error(rpctypes.ErrGRPCInvalidDowngradeTargetVersion)},
	{"etcd/DowngradeInProcess", rpctypes.Error(rpctypes.ErrGRPCDowngradeInProcess)},
	{"etcd/NoInflightDowngrade", rpctypes.Error(rpctypes.ErrGRPCNoInflightDowngrade)},
	{"etcd/Canceled", rpctypes.Error(rpctypes.ErrGRPCCanceled)},
	{"etcd/DeadlineExceeded", rpctypes.Error(rpctypes.ErrGRPCDeadlineExceeded)},
}

func semanticKind(err error) string {
	if errors.Is(err, context.Canceled) {
		return "context/canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context/deadline-exceeded"
	}
	grpcStatus, ok := originalStatus(err)
	if ok && grpcStatus.Code() == codes.Canceled && grpcStatus.Message() == context.Canceled.Error() {
		return "grpc/context-canceled"
	}
	if ok && grpcStatus.Code() == codes.DeadlineExceeded && grpcStatus.Message() == context.DeadlineExceeded.Error() {
		return "grpc/context-deadline-exceeded"
	}
	normalized := rpctypes.Error(err)
	if ok {
		normalized = rpctypes.Error(grpcStatus.Err())
	}
	for _, canonical := range canonicalErrors {
		if errors.Is(err, canonical.value) || errors.Is(normalized, canonical.value) {
			return canonical.kind
		}
	}
	return "unclassified"
}

func validSemanticKind(kind string) bool {
	switch kind {
	case "unclassified", "context/canceled", "context/deadline-exceeded", "grpc/context-canceled", "grpc/context-deadline-exceeded":
		return true
	}
	for _, canonical := range canonicalErrors {
		if canonical.kind == kind {
			return true
		}
	}
	return false
}

// grpc-go decorates a wrapped status with the outer error text. Walk a bounded
// chain to recover the original wire status for whitelist classification only.
func originalStatus(err error) (*status.Status, bool) {
	pending := []error{err}
	for inspected := 0; len(pending) > 0 && inspected < 64; inspected++ {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if current == nil {
			continue
		}
		if provider, ok := current.(interface{ GRPCStatus() *status.Status }); ok {
			value := provider.GRPCStatus()
			return value, value != nil
		}
		switch wrapper := current.(type) {
		case interface{ Unwrap() error }:
			pending = append(pending, wrapper.Unwrap())
		case interface{ Unwrap() []error }:
			children := wrapper.Unwrap()
			if len(children) > 64-len(pending) {
				return nil, false
			}
			for index := len(children) - 1; index >= 0; index-- {
				pending = append(pending, children[index])
			}
		}
	}
	return nil, false
}
