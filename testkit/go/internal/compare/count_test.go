package compare

import (
	"bytes"
	"testing"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

func TestLimitedRangeCountUsesPageLengthAcrossScopes(t *testing.T) {
	cases := []struct {
		name     string
		caseID   string
		limit    int64
		total    int64
		pageSize int
		more     bool
	}{
		{name: "truncated C03 page", caseID: "C03", limit: 2, total: 3, pageSize: 2, more: true},
		{name: "same request under another case", caseID: "range-with-limit", limit: 2, total: 3, pageSize: 2, more: true},
		{name: "single item page", caseID: "C03", limit: 1, total: 3, pageSize: 1, more: true},
		{name: "short final page", caseID: "C03", limit: 2, total: 1, pageSize: 1},
		{name: "empty page", caseID: "C03", limit: 2},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := rangeCountRequest(test.limit, false)
			native := rangeCountTrace(t, test.caseID, request, rangeCountResponse(test.total, test.pageSize, test.more), nil, "native-etcd")
			target := rangeCountTrace(t, test.caseID, request, rangeCountResponse(int64(test.pageSize), test.pageSize, test.more), nil, "target-hbase")
			assertCountComparison(t, native, target, false)
		})
	}
}

func TestLimitedRangeCountRejectsWrongTargetAndPreservesOtherFields(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*etcdserverpb.RangeResponse)
	}{
		{name: "matching native total is invalid target page count", mutate: func(response *etcdserverpb.RangeResponse) { response.Count = 3 }},
		{name: "target count below page length", mutate: func(response *etcdserverpb.RangeResponse) { response.Count = 1 }},
		{name: "negative count", mutate: func(response *etcdserverpb.RangeResponse) { response.Count = -1 }},
		{name: "More differs", mutate: func(response *etcdserverpb.RangeResponse) { response.More = false }},
		{name: "KV omitted", mutate: func(response *etcdserverpb.RangeResponse) { response.Kvs = response.Kvs[:1]; response.Count = 1 }},
		{name: "KV value differs", mutate: func(response *etcdserverpb.RangeResponse) { response.Kvs[0].Value = []byte("changed") }},
		{name: "KV order differs", mutate: func(response *etcdserverpb.RangeResponse) {
			response.Kvs[0], response.Kvs[1] = response.Kvs[1], response.Kvs[0]
		}},
		{name: "Header differs", mutate: func(response *etcdserverpb.RangeResponse) { response.Header.RaftTerm++ }},
		{name: "revision differs", mutate: func(response *etcdserverpb.RangeResponse) { response.Header.Revision++ }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := rangeCountRequest(2, false)
			native := rangeCountTrace(t, "C03", request, rangeCountResponse(3, 2, true), nil, "native-etcd")
			response := rangeCountResponse(2, 2, true)
			test.mutate(response)
			target := rangeCountTrace(t, "C03", request, response, nil, "target-hbase")
			assertCountComparison(t, native, target, true)
		})
	}
}

func TestLimitedRangeCountRejectsInvalidEmptyAndShortPageCounts(t *testing.T) {
	for _, test := range []struct {
		name        string
		nativeCount int64
		targetCount int64
		pageSize    int
	}{
		{name: "empty page cannot claim a match", targetCount: 1},
		{name: "short page cannot claim the limit", nativeCount: 1, targetCount: 2, pageSize: 1},
		{name: "native total cannot be below page length", nativeCount: 1, targetCount: 2, pageSize: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := rangeCountRequest(2, false)
			native := rangeCountTrace(t, "C03", request, rangeCountResponse(test.nativeCount, test.pageSize, false), nil, "native-etcd")
			target := rangeCountTrace(t, "C03", request, rangeCountResponse(test.targetCount, test.pageSize, false), nil, "target-hbase")
			assertCountComparison(t, native, target, true)
		})
	}
}

func TestLimitedRangeCountRejectsNonCanonicalAndOverflowCounts(t *testing.T) {
	for _, count := range []string{"02", "+2", "9223372036854775808"} {
		t.Run(count, func(t *testing.T) {
			request := rangeCountRequest(2, false)
			native := rangeCountTrace(t, "C03", request, rangeCountResponse(3, 2, true), nil, "native-etcd")
			target := rangeCountTrace(t, "C03", request, rangeCountResponse(2, 2, true), nil, "target-hbase")
			target[1].Response = replace(target[1].Response, `"count":"2"`, `"count":"`+count+`"`)
			assertCountComparison(t, native, target, true)
		})
	}
}

func TestCountAllowancesLeaveOtherRequestsAndSameScopesStrict(t *testing.T) {
	requests := []struct {
		name    string
		request *etcdserverpb.RangeRequest
	}{
		{name: "unlimited Range", request: &etcdserverpb.RangeRequest{Key: []byte("p/"), RangeEnd: []byte("p0")}},
		{name: "exact key", request: &etcdserverpb.RangeRequest{Key: []byte("p/"), Limit: 2}},
		{name: "other range CountOnly", request: rangeCountRequest(2, true)},
		{name: "exact CountOnly", request: &etcdserverpb.RangeRequest{Key: []byte("p/"), Limit: 1, CountOnly: true}},
	}
	for _, test := range requests {
		t.Run(test.name, func(t *testing.T) {
			native := rangeCountTrace(t, "C03", test.request, rangeCountResponse(3, 2, true), nil, "native-etcd")
			target := rangeCountTrace(t, "C03", test.request, rangeCountResponse(2, 2, true), nil, "target-hbase")
			assertCountComparison(t, native, target, true)
		})
	}
	for _, scope := range []string{"native-etcd", "target-hbase"} {
		t.Run(scope, func(t *testing.T) {
			request := rangeCountRequest(2, false)
			left := rangeCountTrace(t, "C03", request, rangeCountResponse(3, 2, true), nil, scope)
			right := rangeCountTrace(t, "C03", request, rangeCountResponse(2, 2, true), nil, scope)
			assertCountComparison(t, left, right, true)
		})
	}
}

func TestCountOnlyFailureObservationsCompareStrictlyAcrossScopes(t *testing.T) {
	request := rangeCountRequest(1, true)
	request.Revision = 100
	cases := []struct {
		name        string
		targetError error
		wantError   bool
	}{
		{name: "same FutureRev", targetError: rpctypes.ErrGRPCFutureRev},
		{name: "different error with same gRPC code", targetError: rpctypes.ErrGRPCCompacted, wantError: true},
		{name: "different gRPC code", targetError: rpctypes.ErrGRPCAuthFailed, wantError: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			native := rangeCountTrace(t, "C05", request, nil, rpctypes.ErrGRPCFutureRev, "native-etcd")
			target := rangeCountTrace(t, "C05", request, nil, test.targetError, "target-hbase")
			assertCountComparison(t, native, target, test.wantError)
		})
	}
	t.Run("different consumer identity", func(t *testing.T) {
		native := rangeCountTrace(t, "C05", request, nil, rpctypes.ErrGRPCFutureRev, "native-etcd")
		target := rangeCountTrace(t, "C05", request, nil, rpctypes.ErrGRPCFutureRev, "target-hbase")
		target[1].Error.ConsumerKind = "etcd-client-error"
		assertCountComparison(t, native, target, true)
	})
}

func TestFailedRangeObservationsRetainRequestAndResponseIdentity(t *testing.T) {
	for _, test := range []struct {
		name      string
		countOnly bool
	}{{name: "ordinary limited Range"}, {name: "range CountOnly", countOnly: true}} {
		t.Run(test.name, func(t *testing.T) {
			request := rangeCountRequest(1, test.countOnly)
			request.Revision = 100
			native := rangeCountTrace(t, "range-error", request, nil, rpctypes.ErrGRPCFutureRev, "native-etcd")
			target := rangeCountTrace(t, "range-error", request, nil, rpctypes.ErrGRPCFutureRev, "target-hbase")
			assertCountComparison(t, native, target, false)
			request.Revision++
			changedRequest := rangeCountTrace(t, "range-error", request, nil, rpctypes.ErrGRPCFutureRev, "target-hbase")
			assertCountComparison(t, native, changedRequest, true)
			request.Revision--
			changedResponse := rangeCountTrace(t, "range-error", request, rangeCountResponse(0, 0, false), rpctypes.ErrGRPCFutureRev, "target-hbase")
			assertCountComparison(t, native, changedResponse, true)
		})
	}
}

func TestCountOnlySuccessStillEnforcesExistenceBeforeEquality(t *testing.T) {
	for _, test := range []struct {
		name        string
		caseID      string
		nativeCount int64
		targetCount int64
		wantError   bool
	}{
		{name: "zero", caseID: "C05"},
		{name: "one", caseID: "C05", nativeCount: 1, targetCount: 1},
		{name: "multiple", caseID: "C05", nativeCount: 3, targetCount: 1},
		{name: "request semantics outside C05 label", caseID: "range-count-only", nativeCount: 3, targetCount: 1},
		{name: "equal total is not target existence", caseID: "C05", nativeCount: 3, targetCount: 3, wantError: true},
		{name: "lost existence", caseID: "C05", nativeCount: 3, wantError: true},
		{name: "invented existence", caseID: "C05", targetCount: 1, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := rangeCountRequest(1, true)
			native := rangeCountTrace(t, test.caseID, request, rangeCountResponse(test.nativeCount, 0, false), nil, "native-etcd")
			target := rangeCountTrace(t, test.caseID, request, rangeCountResponse(test.targetCount, 0, false), nil, "target-hbase")
			assertCountComparison(t, native, target, test.wantError)
		})
	}
}

func rangeCountRequest(limit int64, countOnly bool) *etcdserverpb.RangeRequest {
	return &etcdserverpb.RangeRequest{Key: []byte("p/"), RangeEnd: []byte("p0"), Limit: limit, CountOnly: countOnly}
}

func rangeCountResponse(count int64, pageSize int, more bool) *etcdserverpb.RangeResponse {
	response := &etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 101, MemberId: 202, Revision: 2, RaftTerm: 1},
		Count:  count,
		More:   more,
		Kvs:    make([]*mvccpb.KeyValue, pageSize),
	}
	for index := range response.Kvs {
		response.Kvs[index] = &mvccpb.KeyValue{
			Key:            []byte{'k', byte(index)},
			Value:          []byte{byte(index + 1)},
			CreateRevision: 2,
			ModRevision:    2,
			Version:        1,
		}
	}
	return response
}

func rangeCountTrace(t *testing.T, caseID string, request *etcdserverpb.RangeRequest, response *etcdserverpb.RangeResponse, callError error, scope string) []trace.Record {
	t.Helper()
	requestJSON, err := trace.SnapshotProto(request)
	if err != nil {
		t.Fatal(err)
	}
	responseJSON, err := trace.SnapshotNullableProto(response)
	if err != nil {
		t.Fatal(err)
	}
	baseline := baselineTrace(1, 101, 202, 7)[0]
	baseline.EvidenceScope = scope
	return []trace.Record{baseline, {
		Schema: trace.SchemaVersion, CaseID: caseID, Ordinal: 1, Method: "etcdserverpb.KV/Range",
		Request: requestJSON, Response: responseJSON, Error: trace.ClassifyError(callError),
		EvidenceScope: scope, ElapsedNS: "1",
	}}
}

func assertCountComparison(t *testing.T, left, right []trace.Record, wantError bool) {
	t.Helper()
	for _, direction := range []struct {
		name        string
		left, right []trace.Record
	}{{name: "forward", left: left, right: right}, {name: "reverse", left: right, right: left}} {
		t.Run(direction.name, func(t *testing.T) {
			beforeLeft, beforeRight := encodeTrace(t, direction.left), encodeTrace(t, direction.right)
			err := Compare(direction.left, direction.right)
			if wantError && err == nil {
				t.Error("invalid Count or other response difference was accepted")
			}
			if !wantError && err != nil {
				t.Errorf("equivalent Range observations were rejected: %v", err)
			}
			if !bytes.Equal(beforeLeft, encodeTrace(t, direction.left)) || !bytes.Equal(beforeRight, encodeTrace(t, direction.right)) {
				t.Error("comparison changed the caller's trace")
			}
		})
	}
}
