package history

import (
	"bytes"
	"errors"
	"sort"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// KVState is an immutable serial reference state. It is independent of the
// Java evaluator and is intentionally bounded by the caller's test dataset.
type KVState struct {
	revision int64
	values   map[string]*mvccpb.KeyValue
	history  map[int64]map[string]*mvccpb.KeyValue
}

func NewKVState(revision int64, values []*mvccpb.KeyValue) (*KVState, error) {
	if revision < 1 || len(values) > 4096 {
		return nil, errors.New("invalid initial KV state")
	}
	state := &KVState{revision: revision, values: make(map[string]*mvccpb.KeyValue, len(values)), history: make(map[int64]map[string]*mvccpb.KeyValue)}
	for _, value := range values {
		if value == nil || len(value.Key) == 0 || value.CreateRevision < 1 || value.ModRevision < value.CreateRevision || value.ModRevision > revision || value.Version < 1 || value.Lease != 0 {
			return nil, errors.New("invalid initial KV metadata")
		}
		key := string(value.Key)
		if _, exists := state.values[key]; exists {
			return nil, errors.New("duplicate initial key")
		}
		state.values[key] = proto.Clone(value).(*mvccpb.KeyValue)
	}
	state.remember()
	return state, nil
}

// KVModel checks the explicitly supported point operations. Other requests
// return inconclusive through Check, never a false pass.
type KVModel struct{}

func (model KVModel) CanOmit(state any, operation Observation) (bool, error) {
	switch operation.Code {
	case "Canceled", "DeadlineExceeded", "Unavailable", "Unknown", "Internal", "ResourceExhausted":
		// Even an omitted call must have a request the model understands;
		// otherwise an unsupported operation could incorrectly produce a pass.
		if _, _, err := model.Apply(state, operation); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, errors.New("failure semantics not yet modeled")
	}
}

func (KVModel) Apply(raw any, operation Observation) (any, bool, error) {
	state, ok := raw.(*KVState)
	if !ok || state == nil {
		return nil, false, errors.New("invalid KV state")
	}
	switch operation.Method {
	case "Put":
		return applyPut(state, operation)
	case "Range":
		return applyRange(state, operation)
	case "DeleteRange":
		return applyDelete(state, operation)
	case "Txn":
		return applyTxn(state, operation)
	default:
		return nil, false, errors.New("KV operation not yet modeled")
	}
}

func applyPut(state *KVState, operation Observation) (any, bool, error) {
	var request etcdserverpb.PutRequest
	if proto.Unmarshal(operation.Request, &request) != nil || !knownFields(&request) || len(request.Key) == 0 || request.Lease != 0 || request.IgnoreLease || request.IgnoreValue {
		return nil, false, errors.New("unsupported Put request")
	}
	previous := state.values[string(request.Key)]
	next := state.copy()
	if next.revision == int64(^uint64(0)>>1) {
		return nil, false, errors.New("revision overflow")
	}
	next.revision++
	value := &mvccpb.KeyValue{Key: append([]byte(nil), request.Key...), Value: append([]byte(nil), request.Value...),
		CreateRevision: next.revision, ModRevision: next.revision, Version: 1}
	if previous != nil {
		if previous.Version == int64(^uint64(0)>>1) {
			return nil, false, errors.New("version overflow")
		}
		value.CreateRevision, value.Version = previous.CreateRevision, previous.Version+1
	}
	next.values[string(request.Key)] = value
	next.remember()
	if operation.Code != "OK" {
		return next, true, nil
	}
	var response etcdserverpb.PutResponse
	if proto.Unmarshal(operation.Response, &response) != nil || !knownFields(&response) || response.Header == nil {
		return nil, false, errors.New("invalid Put response")
	}
	return next, response.Header.Revision == next.revision && equalPrevious(request.PrevKv, previous, response.PrevKv), nil
}

func applyRange(state *KVState, operation Observation) (any, bool, error) {
	var request etcdserverpb.RangeRequest
	if proto.Unmarshal(operation.Request, &request) != nil || !knownFields(&request) || !supportedRange(&request) {
		return nil, false, errors.New("unsupported Range request")
	}
	values, err := state.at(request.Revision)
	if err != nil {
		return nil, false, err
	}
	if operation.Code != "OK" {
		return state, true, nil
	}
	var response etcdserverpb.RangeResponse
	if proto.Unmarshal(operation.Response, &response) != nil || !knownFields(&response) || response.Header == nil {
		return nil, false, errors.New("invalid Range response")
	}
	return state, matchesRange(values, &request, &response, state.revision), nil
}

func supportedRange(request *etcdserverpb.RangeRequest) bool {
	return request != nil && len(request.Key) > 0 && request.Revision >= 0 &&
		request.SortTarget == etcdserverpb.RangeRequest_KEY && request.SortOrder >= etcdserverpb.RangeRequest_NONE &&
		request.SortOrder <= etcdserverpb.RangeRequest_DESCEND &&
		request.MinModRevision >= 0 && request.MaxModRevision >= 0 && request.MinCreateRevision >= 0 && request.MaxCreateRevision >= 0 &&
		(!request.CountOnly || len(request.RangeEnd) == 0 || request.Limit == 1)
}

func inRange(key string, start, end []byte) bool {
	if len(end) == 0 {
		return key == string(start)
	}
	if bytes.Compare([]byte(key), start) < 0 {
		return false
	}
	return len(end) == 1 && end[0] == 0 || bytes.Compare([]byte(key), end) < 0
}

func sortedRangeKeys(values map[string]*mvccpb.KeyValue, start, end []byte) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		if inRange(key, start, end) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func matchesRevisionFilter(value *mvccpb.KeyValue, request *etcdserverpb.RangeRequest) bool {
	return !(request.MinModRevision > 0 && value.ModRevision < request.MinModRevision ||
		request.MaxModRevision > 0 && value.ModRevision > request.MaxModRevision ||
		request.MinCreateRevision > 0 && value.CreateRevision < request.MinCreateRevision ||
		request.MaxCreateRevision > 0 && value.CreateRevision > request.MaxCreateRevision)
}

// matchesRange implements the approved M3 lightweight Count contract.
func matchesRange(values map[string]*mvccpb.KeyValue, request *etcdserverpb.RangeRequest, response *etcdserverpb.RangeResponse, revision int64) bool {
	if response == nil || response.Header == nil || response.Header.Revision != revision {
		return false
	}
	keys := sortedRangeKeys(values, request.Key, request.RangeEnd)
	if request.CountOnly {
		count := int64(0)
		if len(keys) > 0 {
			count = 1
		}
		return response.Count == count && len(response.Kvs) == 0 && !response.More
	}
	filtered, more := selectRangeRecords(values, keys, request)
	return matchesRangeResponse(filtered, more, request.KeysOnly, response)
}

func selectRangeRecords(values map[string]*mvccpb.KeyValue, keys []string, request *etcdserverpb.RangeRequest) ([]*mvccpb.KeyValue, bool) {
	if request.SortOrder == etcdserverpb.RangeRequest_DESCEND {
		for left, right := 0, len(keys)-1; left < right; left, right = left+1, right-1 {
			keys[left], keys[right] = keys[right], keys[left]
		}
	}
	filtered := make([]*mvccpb.KeyValue, 0, len(keys))
	for _, key := range keys {
		kv := values[key]
		if !matchesRevisionFilter(kv, request) {
			continue
		}
		filtered = append(filtered, kv)
	}
	more := request.Limit > 0 && int64(len(filtered)) > request.Limit
	if more {
		filtered = filtered[:int(request.Limit)]
	}
	return filtered, more
}

func matchesRangeResponse(filtered []*mvccpb.KeyValue, more bool, keysOnly bool, response *etcdserverpb.RangeResponse) bool {
	if response.Count != int64(len(filtered)) || response.More != more || len(response.Kvs) != len(filtered) {
		return false
	}
	for i, kv := range filtered {
		expected := kv
		if keysOnly {
			expected = proto.Clone(kv).(*mvccpb.KeyValue)
			expected.Value = nil
		}
		if !proto.Equal(expected, response.Kvs[i]) {
			return false
		}
	}
	return true
}

func applyDelete(state *KVState, operation Observation) (any, bool, error) {
	var request etcdserverpb.DeleteRangeRequest
	if proto.Unmarshal(operation.Request, &request) != nil || !knownFields(&request) || len(request.Key) == 0 {
		return nil, false, errors.New("unsupported DeleteRange request")
	}
	keys := sortedRangeKeys(state.values, request.Key, request.RangeEnd)
	next := state
	if len(keys) > 0 {
		next = state.copy()
		if next.revision == int64(^uint64(0)>>1) {
			return nil, false, errors.New("revision overflow")
		}
		next.revision++
		for _, key := range keys {
			delete(next.values, key)
		}
		next.remember()
	}
	if operation.Code != "OK" {
		return next, true, nil
	}
	var response etcdserverpb.DeleteRangeResponse
	if proto.Unmarshal(operation.Response, &response) != nil || !knownFields(&response) || response.Header == nil {
		return nil, false, errors.New("invalid DeleteRange response")
	}
	return next, matchesDeleteResponse(state, keys, &request, &response, next.revision), nil
}

func matchesDeleteResponse(state *KVState, keys []string, request *etcdserverpb.DeleteRangeRequest,
	response *etcdserverpb.DeleteRangeResponse, revision int64) bool {
	if response.Header.Revision != revision || response.Deleted != int64(len(keys)) {
		return false
	}
	if !request.PrevKv {
		return len(response.PrevKvs) == 0
	}
	if len(response.PrevKvs) != len(keys) {
		return false
	}
	for i, key := range keys {
		if !proto.Equal(state.values[key], response.PrevKvs[i]) {
			return false
		}
	}
	return true
}

func equalPrevious(requested bool, expected, actual *mvccpb.KeyValue) bool {
	if !requested || expected == nil {
		return actual == nil
	}
	return actual != nil && proto.Equal(expected, actual)
}

// Unknown wire fields may change semantics in a future protocol version.
// Discard only on a clone so the captured evidence remains unchanged.
func knownFields(message proto.Message) bool {
	copy := proto.Clone(message)
	proto.DiscardUnknown(copy)
	return proto.Size(copy) == proto.Size(message)
}

func (state *KVState) copy() *KVState {
	values := make(map[string]*mvccpb.KeyValue, len(state.values))
	for key, value := range state.values {
		values[key] = value
	}
	history := make(map[int64]map[string]*mvccpb.KeyValue, len(state.history))
	for revision, snapshot := range state.history {
		history[revision] = snapshot
	}
	return &KVState{revision: state.revision, values: values, history: history}
}

// Snapshots and values are immutable after publication; copies are shallow.
// The test driver bounds the number of operations and the initial dataset.
func (state *KVState) remember() {
	snapshot := make(map[string]*mvccpb.KeyValue, len(state.values))
	for key, value := range state.values {
		snapshot[key] = value
	}
	state.history[state.revision] = snapshot
}

func (state *KVState) at(revision int64) (map[string]*mvccpb.KeyValue, error) {
	if revision == 0 || revision == state.revision {
		return state.values, nil
	}
	snapshot, ok := state.history[revision]
	if !ok {
		return nil, errors.New("historical revision is outside modeled history")
	}
	return snapshot, nil
}
