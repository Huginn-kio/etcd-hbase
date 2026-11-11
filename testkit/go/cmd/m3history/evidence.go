package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/history"
	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

const (
	evidenceSchema   = "etcd-hbase-j28-point-txn-history/v1"
	evidenceSchedule = "fixed-mixed-concurrent-workload"
	evidenceSeed     = "not-applicable-fixed-workload"
	evidenceScope    = "modeled-kv-and-revision; cluster-member-and-raft-headers-not-checked"
)

type historyEvidence struct {
	Schema           string                `json:"schema"`
	Schedule         string                `json:"schedule"`
	Seed             string                `json:"seed"`
	ModelScope       string                `json:"model_scope"`
	RestartMode      string                `json:"restart_mode"`
	BaseRevision     int64                 `json:"base_revision"`
	SuccessfulWrites int                   `json:"successful_writes"`
	Observations     []history.Observation `json:"observations"`
	Result           history.SearchResult  `json:"result"`
}

// checkEvidence recomputes the independent model verdict; a stored verdict is
// not trusted. The fixture metadata and per-mode provenance are checked by the
// calling report validator.
func checkEvidence(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > 4<<20 {
		return errors.New("invalid local history file")
	}
	file, err := os.Open(path)
	if err != nil {
		return errors.New("history file could not be read")
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 4<<20+1))
	decoder.DisallowUnknownFields()
	var evidence historyEvidence
	if decoder.Decode(&evidence) != nil {
		return errors.New("invalid history encoding")
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return errors.New("trailing history data")
	}
	if evidence.Schema != evidenceSchema || evidence.Schedule != evidenceSchedule || evidence.Seed != evidenceSeed || evidence.ModelScope != evidenceScope || evidence.BaseRevision < 1 || evidence.SuccessfulWrites < 1 || evidence.SuccessfulWrites > 2 {
		return errors.New("invalid history identity")
	}
	count := 10
	if evidence.RestartMode != "none" {
		if !unpublishedMode(evidence.RestartMode) && evidence.RestartMode != "cas-ack-lost" && evidence.RestartMode != "partial-materialization" && evidence.RestartMode != "old-owner-resume" && evidence.RestartMode != "cancel-after-cas" && evidence.RestartMode != "after-commit" {
			return errors.New("invalid history fault mode")
		}
		count += 2
		if evidence.RestartMode == "old-owner-resume" {
			count += 2
		}
	}
	if len(evidence.Observations) != count || !validFixtureOperations(evidence) {
		return errors.New("incomplete history fixture")
	}
	initial, err := history.NewKVState(evidence.BaseRevision, nil)
	if err != nil {
		return errors.New("invalid history baseline")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := history.Check(ctx, evidence.Observations, history.KVModel{}, initial, 10000)
	if err != nil || result.Verdict != history.Linearizable || !reflect.DeepEqual(result, evidence.Result) {
		return errors.New("saved history did not reproduce its model result")
	}
	return nil
}

func validFixtureOperations(e historyEvidence) bool {
	if !validInitialOperations(e) {
		return false
	}
	faults, lostKey, faultEnd, valid := findFaultOperation(e)
	if !valid {
		return false
	}
	if e.RestartMode == "none" {
		return faults == 0
	}
	return faults == 1 && validRecovery(e, lostKey, faultEnd)
}

func validInitialOperations(e historyEvidence) bool {
	first, second := e.Observations[0], e.Observations[1]
	if first.Worker == second.Worker || first.Worker < 1 || first.Worker > 2 || second.Worker < 1 || second.Worker > 2 {
		return false
	}
	if !(first.Method == "Put" && second.Method == "Txn" || first.Method == "Txn" && second.Method == "Put") {
		return false
	}
	success := 0
	for _, operation := range []history.Observation{first, second} {
		if operation.Code == "OK" {
			success++
		} else if operation.Code != "ResourceExhausted" {
			return false
		}
	}
	if success != e.SuccessfulWrites {
		return false
	}
	return true
}

func findFaultOperation(e historyEvidence) (int, []byte, uint64, bool) {
	faults := 0
	var lostKey []byte
	var faultEnd uint64
	for _, operation := range e.Observations {
		if operation.Method != "Txn" {
			continue
		}
		var request etcdserverpb.TxnRequest
		if proto.Unmarshal(operation.Request, &request) != nil || len(request.Success) != 2 {
			continue
		}
		first := request.Success[0].GetRequestPut()
		second := request.Success[1].GetRequestPut()
		if !isLostResponsePair(first, second) {
			continue
		}
		faults++
		lostKey = first.Key
		faultEnd = operation.End
		expected := expectedFaultCode(e.RestartMode)
		if operation.Code != expected {
			return 0, nil, 0, false
		}
	}
	return faults, lostKey, faultEnd, true
}

func isLostResponsePair(first, second *etcdserverpb.PutRequest) bool {
	if first == nil || second == nil {
		return false
	}
	return bytes.HasSuffix(first.Key, []byte("/lost-response")) &&
		bytes.Equal(second.Key, append(append([]byte(nil), first.Key...), []byte("-2")...)) &&
		string(first.Value) == "first" && string(second.Value) == "second"
}

func expectedFaultCode(mode string) string {
	switch mode {
	case "old-owner-resume":
		return "DeadlineExceeded"
	case "cancel-after-cas":
		return "Canceled"
	default:
		return "Unavailable"
	}
}

func validRecovery(e historyEvidence, lostKey []byte, faultEnd uint64) bool {
	foundRecovery := false
	for _, operation := range e.Observations {
		if operation.Method != "Range" || operation.Code != "OK" {
			continue
		}
		var request etcdserverpb.RangeRequest
		var response etcdserverpb.RangeResponse
		if proto.Unmarshal(operation.Request, &request) != nil || proto.Unmarshal(operation.Response, &response) != nil || operation.Begin <= faultEnd || !bytes.Equal(request.Key, lostKey) || !bytes.Equal(request.RangeEnd, append(append([]byte(nil), lostKey...), 0xff)) {
			continue
		}
		if unpublishedMode(e.RestartMode) {
			foundRecovery = len(response.Kvs) == 0
		} else {
			lostKey2 := append(append([]byte(nil), lostKey...), []byte("-2")...)
			foundRecovery = len(response.Kvs) == 2 && bytes.Equal(response.Kvs[0].Key, lostKey) && string(response.Kvs[0].Value) == "first" && bytes.Equal(response.Kvs[1].Key, lostKey2) && string(response.Kvs[1].Value) == "second"
		}
		break
	}
	return foundRecovery && (e.RestartMode != "old-owner-resume" || validNewOwnerWrite(e.Observations, lostKey))
}

func validNewOwnerWrite(operations []history.Observation, key []byte) bool {
	var writeEnd uint64
	for _, operation := range operations {
		if operation.Method == "Put" && operation.Code == "OK" {
			var request etcdserverpb.PutRequest
			if proto.Unmarshal(operation.Request, &request) == nil && bytes.Equal(request.Key, key) && string(request.Value) == "new-owner" && request.PrevKv {
				writeEnd = operation.End
			}
		}
		if writeEnd == 0 || operation.Begin <= writeEnd || operation.Method != "Range" || operation.Code != "OK" {
			continue
		}
		var request etcdserverpb.RangeRequest
		var response etcdserverpb.RangeResponse
		if proto.Unmarshal(operation.Request, &request) == nil && proto.Unmarshal(operation.Response, &response) == nil && bytes.Equal(request.Key, key) && len(request.RangeEnd) == 0 && len(response.Kvs) == 1 && string(response.Kvs[0].Value) == "new-owner" {
			return true
		}
	}
	return false
}
