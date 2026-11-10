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

// applyTxn models a deliberately restricted point-transaction subset. Static
// validation includes both branches. Unsupported shapes are inconclusive.
func applyTxn(base *KVState, observation Observation) (any, bool, error) {
	var request etcdserverpb.TxnRequest
	if proto.Unmarshal(observation.Request, &request) != nil || !knownFields(&request) {
		return nil, false, errors.New("invalid Txn request")
	}
	budget := 64
	nextNode := uint(0)
	var writes []writeClaim
	if err := validateTxn(&request, 0, &budget, &nextNode, &writes, 0, 0); err != nil {
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

func validateTxn(txn *etcdserverpb.TxnRequest, depth int, budget *int, nextNode *uint, writes *[]writeClaim, known, choices uint64) error {
	if txn == nil || depth > 8 || *budget <= 0 || *nextNode >= 64 || (depth == 0 && len(txn.Compare)+len(txn.Success)+len(txn.Failure) == 0) {
		return errors.New("unsupported Txn shape")
	}
	branchBit := uint64(1) << *nextNode
	*nextNode++
	*budget--
	for _, c := range txn.Compare {
		if *budget <= 0 || c == nil || len(c.Key) == 0 || c.Result < etcdserverpb.Compare_EQUAL || c.Result > etcdserverpb.Compare_NOT_EQUAL {
			return errors.New("unsupported Txn compare")
		}
		*budget--
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
	}
	for index, branch := range [][]*etcdserverpb.RequestOp{txn.Success, txn.Failure} {
		branchKnown, branchChoices := known|branchBit, choices
		if index == 0 {
			branchChoices |= branchBit
		}
		for _, op := range branch {
			if op == nil || *budget <= 0 {
				return errors.New("unsupported Txn operation")
			}
			*budget--
			switch value := op.Request.(type) {
			case *etcdserverpb.RequestOp_RequestPut:
				p := value.RequestPut
				if p == nil || len(p.Key) == 0 || p.Lease != 0 || p.IgnoreLease || p.IgnoreValue || !claimWrite(writes, p.Key, nil, true, branchKnown, branchChoices) {
					return errors.New("unsupported Txn Put")
				}
			case *etcdserverpb.RequestOp_RequestDeleteRange:
				d := value.RequestDeleteRange
				if d == nil || len(d.Key) == 0 || len(d.RangeEnd) == 1 && d.RangeEnd[0] == 0 || !claimWrite(writes, d.Key, d.RangeEnd, false, branchKnown, branchChoices) {
					return errors.New("unsupported Txn DeleteRange")
				}
			case *etcdserverpb.RequestOp_RequestRange:
				r := value.RequestRange
				if !supportedRange(r) {
					return errors.New("unsupported Txn Range")
				}
			case *etcdserverpb.RequestOp_RequestTxn:
				if err := validateTxn(value.RequestTxn, depth+1, budget, nextNode, writes, branchKnown, branchChoices); err != nil {
					return err
				}
			default:
				return errors.New("unsupported Txn operation")
			}
		}
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
		var child *etcdserverpb.TxnResponse
		if response != nil {
			child = response.GetResponseTxn()
			if child == nil {
				return false, nil
			}
		}
		return working.execute(value.RequestTxn, child, depth+1)
	case *etcdserverpb.RequestOp_RequestRange:
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
	case *etcdserverpb.RequestOp_RequestPut:
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
	case *etcdserverpb.RequestOp_RequestDeleteRange:
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
	default:
		return false, errors.New("unsupported Txn operation")
	}
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
