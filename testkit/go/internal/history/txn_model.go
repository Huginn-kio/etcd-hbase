package history

import (
	"bytes"
	"errors"
	"math"
	"sort"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// applyTxn models a restricted nested-transaction subset, including supported
// ranges. Static validation includes both branches; unsupported shapes are inconclusive.
func applyTxn(base *KVState, observation Observation) (any, bool, error) {
	var request etcdserverpb.TxnRequest
	if proto.Unmarshal(observation.Request, &request) != nil || !knownFields(&request) {
		return nil, false, errors.New("invalid Txn request")
	}
	validator := txnValidator{budget: 64}
	if err := validator.validate(&request, 0, 0, 0); err != nil {
		return nil, false, err
	}
	var response *etcdserverpb.TxnResponse
	if observation.Code == "OK" {
		response = new(etcdserverpb.TxnResponse)
		if proto.Unmarshal(observation.Response, response) != nil || !knownFields(response) || response.Header == nil {
			return nil, false, errors.New("invalid Txn response")
		}
	}
	working := &txnWorking{base: base, state: base.copy()}
	matches, err := working.execute(&request, response, 0)
	if err != nil {
		return nil, false, err
	}
	if response != nil && response.Header.Revision != working.state.revision {
		matches = false
	}
	if working.modified {
		working.state.remember()
	}
	return working.state, matches, nil
}

type writeClaim struct {
	start   []byte
	end     []byte
	put     bool
	known   uint64
	choices uint64
}

// A conflicting pair is permitted only if some common ancestor chooses
// opposite branches, so that both operations cannot run in one transaction.
func claimWrite(writes *[]writeClaim, start, end []byte, put bool, known, choices uint64) bool {
	for _, old := range *writes {
		if old.known&known&(old.choices^choices) != 0 {
			continue
		}
		if put && old.put && bytes.Equal(start, old.start) ||
			put && !old.put && inRange(string(start), old.start, old.end) ||
			!put && old.put && inRange(string(old.start), start, end) {
			return false
		}
	}
	*writes = append(*writes, writeClaim{start: start, end: end, put: put, known: known, choices: choices})
	return true
}

// txnValidator owns the shared node budget and branch-aware write claims.
type txnValidator struct {
	budget   int
	nextNode uint
	writes   []writeClaim
}

func (validator *txnValidator) validate(txn *etcdserverpb.TxnRequest, depth int, known, choices uint64) error {
	if txn == nil || depth > 8 || validator.budget <= 0 || validator.nextNode >= 64 || (depth == 0 && len(txn.Compare)+len(txn.Success)+len(txn.Failure) == 0) {
		return errors.New("unsupported Txn shape")
	}
	branchBit := uint64(1) << validator.nextNode
	validator.nextNode++
	validator.budget--
	for _, compare := range txn.Compare {
		if validator.budget <= 0 || compare == nil || len(compare.Key) == 0 || compare.Result < etcdserverpb.Compare_EQUAL || compare.Result > etcdserverpb.Compare_NOT_EQUAL {
			return errors.New("unsupported Txn compare")
		}
		validator.budget--
		if err := validateCompareTarget(compare); err != nil {
			return err
		}
	}
	for index, branch := range [][]*etcdserverpb.RequestOp{txn.Success, txn.Failure} {
		branchKnown, branchChoices := known|branchBit, choices
		if index == 0 {
			branchChoices |= branchBit
		}
		if err := validator.validateBranch(branch, depth, branchKnown, branchChoices); err != nil {
			return err
		}
	}
	return nil
}

func (validator *txnValidator) validateBranch(branch []*etcdserverpb.RequestOp, depth int, known, choices uint64) error {
	for _, op := range branch {
		if op == nil || validator.budget <= 0 {
			return errors.New("unsupported Txn operation")
		}
		validator.budget--
		if err := validator.validateOperation(op, depth, known, choices); err != nil {
			return err
		}
	}
	return nil
}

func (validator *txnValidator) validateOperation(op *etcdserverpb.RequestOp, depth int, known, choices uint64) error {
	switch value := op.Request.(type) {
	case *etcdserverpb.RequestOp_RequestPut:
		p := value.RequestPut
		if !validator.validateAndClaimPut(p, known, choices) {
			return errors.New("unsupported Txn Put")
		}
	case *etcdserverpb.RequestOp_RequestDeleteRange:
		d := value.RequestDeleteRange
		if !validator.validateAndClaimDelete(d, known, choices) {
			return errors.New("unsupported Txn DeleteRange")
		}
	case *etcdserverpb.RequestOp_RequestRange:
		if !supportedRange(value.RequestRange) {
			return errors.New("unsupported Txn Range")
		}
	case *etcdserverpb.RequestOp_RequestTxn:
		return validator.validate(value.RequestTxn, depth+1, known, choices)
	default:
		return errors.New("unsupported Txn operation")
	}
	return nil
}

func (validator *txnValidator) validateAndClaimPut(request *etcdserverpb.PutRequest, known, choices uint64) bool {
	return request != nil && len(request.Key) != 0 && request.Lease == 0 && !request.IgnoreLease &&
		!request.IgnoreValue && claimWrite(&validator.writes, request.Key, nil, true, known, choices)
}

func (validator *txnValidator) validateAndClaimDelete(request *etcdserverpb.DeleteRangeRequest, known, choices uint64) bool {
	return request != nil && len(request.Key) != 0 &&
		!(len(request.RangeEnd) == 1 && request.RangeEnd[0] == 0) &&
		claimWrite(&validator.writes, request.Key, request.RangeEnd, false, known, choices)
}

func validateCompareTarget(c *etcdserverpb.Compare) error {
	switch c.Target {
	case etcdserverpb.Compare_VERSION:
		if _, ok := c.TargetUnion.(*etcdserverpb.Compare_Version); !ok {
			return errors.New("invalid compare target")
		}
	case etcdserverpb.Compare_CREATE:
		if _, ok := c.TargetUnion.(*etcdserverpb.Compare_CreateRevision); !ok {
			return errors.New("invalid compare target")
		}
	case etcdserverpb.Compare_MOD:
		if _, ok := c.TargetUnion.(*etcdserverpb.Compare_ModRevision); !ok {
			return errors.New("invalid compare target")
		}
	case etcdserverpb.Compare_VALUE:
		if _, ok := c.TargetUnion.(*etcdserverpb.Compare_Value); !ok {
			return errors.New("invalid compare target")
		}
	case etcdserverpb.Compare_LEASE:
		if _, ok := c.TargetUnion.(*etcdserverpb.Compare_Lease); !ok {
			return errors.New("invalid compare target")
		}
	default:
		return errors.New("unsupported compare target")
	}
	return nil
}

type txnWorking struct {
	base     *KVState
	state    *KVState
	modified bool
}

func (working *txnWorking) execute(request *etcdserverpb.TxnRequest, response *etcdserverpb.TxnResponse, depth int) (bool, error) {
	succeeded := true
	for _, compare := range request.Compare {
		if !compareRange(working.base.values, compare) {
			succeeded = false
			break
		}
	}
	branch := request.Failure
	if succeeded {
		branch = request.Success
	}
	matches := true
	if response != nil {
		if response.Header == nil || (depth > 0 && !proto.Equal(response.Header, &etcdserverpb.ResponseHeader{})) || response.Succeeded != succeeded || len(response.Responses) != len(branch) {
			return false, nil
		}
	}
	for index, op := range branch {
		var observed *etcdserverpb.ResponseOp
		if response != nil {
			observed = response.Responses[index]
			if observed == nil {
				return false, nil
			}
		}
		matched, err := working.applyOperation(op, observed, depth)
		if err != nil {
			return false, err
		}
		matches = matches && matched
	}
	return matches, nil
}

func (working *txnWorking) applyOperation(op *etcdserverpb.RequestOp, response *etcdserverpb.ResponseOp, depth int) (bool, error) {
	switch value := op.Request.(type) {
	case *etcdserverpb.RequestOp_RequestTxn:
		return working.applyNestedTxn(value, response, depth)
	case *etcdserverpb.RequestOp_RequestRange:
		return working.applyRangeOperation(value, response)
	case *etcdserverpb.RequestOp_RequestPut:
		return working.applyPutOperation(value, response)
	case *etcdserverpb.RequestOp_RequestDeleteRange:
		return working.applyDeleteOperation(value, response)
	default:
		return false, errors.New("unsupported Txn operation")
	}
}

func (working *txnWorking) applyNestedTxn(value *etcdserverpb.RequestOp_RequestTxn, response *etcdserverpb.ResponseOp, depth int) (bool, error) {
	var child *etcdserverpb.TxnResponse
	if response != nil {
		child = response.GetResponseTxn()
		if child == nil {
			return false, nil
		}
	}
	return working.execute(value.RequestTxn, child, depth+1)
}

func (working *txnWorking) applyRangeOperation(value *etcdserverpb.RequestOp_RequestRange, response *etcdserverpb.ResponseOp) (bool, error) {
	values := working.state.values
	if value.RequestRange.Revision > 0 {
		var err error
		values, err = working.base.at(value.RequestRange.Revision)
		if err != nil {
			return false, err
		}
	}
	if response == nil {
		return true, nil
	}
	return matchesRange(values, value.RequestRange, response.GetResponseRange(), working.state.revision), nil
}

func (working *txnWorking) applyPutOperation(value *etcdserverpb.RequestOp_RequestPut, response *etcdserverpb.ResponseOp) (bool, error) {
	p := value.RequestPut
	previous := working.state.values[string(p.Key)]
	if previous != nil && previous.Version == math.MaxInt64 {
		return false, errors.New("version overflow")
	}
	if err := working.markWrite(); err != nil {
		return false, err
	}
	kv := &mvccpb.KeyValue{Key: append([]byte(nil), p.Key...), Value: append([]byte(nil), p.Value...), CreateRevision: working.state.revision, ModRevision: working.state.revision, Version: 1}
	if previous != nil {
		kv.CreateRevision, kv.Version = previous.CreateRevision, previous.Version+1
	}
	working.state.values[string(p.Key)] = kv
	if response == nil {
		return true, nil
	}
	r := response.GetResponsePut()
	return r != nil && r.Header != nil && r.Header.Revision == working.state.revision && equalPrevious(p.PrevKv, previous, r.PrevKv), nil
}

func (working *txnWorking) applyDeleteOperation(value *etcdserverpb.RequestOp_RequestDeleteRange, response *etcdserverpb.ResponseOp) (bool, error) {
	d := value.RequestDeleteRange
	keys := make([]string, 0)
	for key := range working.state.values {
		if inRange(key, d.Key, d.RangeEnd) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	previous := make([]*mvccpb.KeyValue, 0, len(keys))
	for _, key := range keys {
		previous = append(previous, working.state.values[key])
	}
	if len(keys) > 0 {
		if err := working.markWrite(); err != nil {
			return false, err
		}
		for _, key := range keys {
			delete(working.state.values, key)
		}
	}
	if response == nil {
		return true, nil
	}
	r := response.GetResponseDeleteRange()
	if r == nil || r.Header == nil || r.Header.Revision != working.state.revision {
		return false, nil
	}
	if r.Deleted != int64(len(keys)) {
		return false, nil
	}
	if !d.PrevKv {
		return len(r.PrevKvs) == 0, nil
	}
	if len(r.PrevKvs) != len(previous) {
		return false, nil
	}
	for index, value := range previous {
		if !proto.Equal(value, r.PrevKvs[index]) {
			return false, nil
		}
	}
	return true, nil
}

func (working *txnWorking) markWrite() error {
	if !working.modified {
		if working.state.revision == math.MaxInt64 {
			return errors.New("revision overflow")
		}
		working.state.revision++
		working.modified = true
	}
	return nil
}

func compareRange(values map[string]*mvccpb.KeyValue, compare *etcdserverpb.Compare) bool {
	found := false
	for key, value := range values {
		if inRange(key, compare.Key, compare.RangeEnd) {
			found = true
			if !comparePoint(value, compare) {
				return false
			}
		}
	}
	return found || comparePoint(nil, compare)
}

func comparePoint(kv *mvccpb.KeyValue, c *etcdserverpb.Compare) bool {
	if kv == nil {
		if c.Target == etcdserverpb.Compare_VALUE {
			return false
		}
		kv = &mvccpb.KeyValue{}
	}
	var result int
	switch c.Target {
	case etcdserverpb.Compare_VALUE:
		result = bytes.Compare(kv.Value, c.GetValue())
	case etcdserverpb.Compare_VERSION:
		result = compareInt(kv.Version, c.GetVersion())
	case etcdserverpb.Compare_CREATE:
		result = compareInt(kv.CreateRevision, c.GetCreateRevision())
	case etcdserverpb.Compare_MOD:
		result = compareInt(kv.ModRevision, c.GetModRevision())
	case etcdserverpb.Compare_LEASE:
		result = compareInt(kv.Lease, c.GetLease())
	}
	switch c.Result {
	case etcdserverpb.Compare_EQUAL:
		return result == 0
	case etcdserverpb.Compare_NOT_EQUAL:
		return result != 0
	case etcdserverpb.Compare_GREATER:
		return result > 0
	case etcdserverpb.Compare_LESS:
		return result < 0
	}
	return false
}

func compareInt(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
