package compare

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
)

var m4LeaseKeepAliveCases = []struct {
	id     string
	method string
}{
	{"native-baseline", "etcdserverpb.Lease/LeaseLeases"},
	{"keepalive-grant-a", "etcdserverpb.Lease/LeaseGrant"},
	{"keepalive-grant-b", "etcdserverpb.Lease/LeaseGrant"},
	{"keepalive-grant-managed-a", "etcdserverpb.Lease/LeaseGrant"},
	{"keepalive-grant-managed-b", "etcdserverpb.Lease/LeaseGrant"},
	{"keepalive-range-before", "etcdserverpb.KV/Range"},
	{"keepalive-stream-a", "etcdserverpb.Lease/LeaseKeepAlive"},
	{"keepalive-stream-b", "etcdserverpb.Lease/LeaseKeepAlive"},
	{"keepalive-stream-missing", "etcdserverpb.Lease/LeaseKeepAlive"},
	{"keepalive-stream-a-repeat", "etcdserverpb.Lease/LeaseKeepAlive"},
	{"keepalive-ttl-a", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"keepalive-ttl-b", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"keepalive-managed-a-first", "etcdserverpb.Lease/LeaseKeepAlive"},
	{"keepalive-managed-a-second", "etcdserverpb.Lease/LeaseKeepAlive"},
	{"keepalive-managed-b-first", "etcdserverpb.Lease/LeaseKeepAlive"},
	{"keepalive-managed-b-second", "etcdserverpb.Lease/LeaseKeepAlive"},
	{"keepalive-range-after", "etcdserverpb.KV/Range"},
}

func validateM4LeaseKeepAliveTrace(records []trace.Record) error {
	if len(records) != len(m4LeaseKeepAliveCases) {
		return fmt.Errorf("M4 KeepAlive trace has %d records, expected %d",
			len(records), len(m4LeaseKeepAliveCases))
	}
	requests := make([]map[string]any, len(records))
	responses := make([]map[string]any, len(records))
	for index, expected := range m4LeaseKeepAliveCases {
		record := records[index]
		if record.CaseID != expected.id || record.Method != expected.method || record.Error != nil {
			return fmt.Errorf("M4 KeepAlive record %d is not the successful %s step", index+1, expected.id)
		}
		if index == 9 {
			if record.StreamTerminal == nil || record.StreamTerminal.Kind != "eof" {
				return errors.New("raw KeepAlive stream does not record a clean EOF")
			}
		} else if record.StreamTerminal != nil {
			return fmt.Errorf("M4 KeepAlive step %s has an unexpected stream terminal", expected.id)
		}
		request, err := m4SnapshotFields(record.Request)
		if err != nil {
			return fmt.Errorf("decode %s request: %w", expected.id, err)
		}
		response, err := m4SnapshotFields(record.Response)
		if err != nil {
			return fmt.Errorf("decode %s response: %w", expected.id, err)
		}
		requests[index], responses[index] = request, response
	}

	if responseInt64(responses[0], "header.revision") < 1 || len(responseList(responses[0], "leases")) != 0 {
		return errors.New("KeepAlive scenario does not start from an empty Lease set")
	}
	leaseA, grantedA, err := grantObservation(requests[1], responses[1], nil, false)
	if err != nil || leaseA <= 0 || grantedA != 30 {
		return errors.New("first raw KeepAlive Lease grant is invalid")
	}
	leaseB, grantedB, err := grantObservation(requests[2], responses[2], nil, false)
	if err != nil || leaseB <= 0 || leaseB == leaseA || grantedB != 30 {
		return errors.New("second raw KeepAlive Lease grant is invalid")
	}
	managedA, managedGrantedA, err := grantObservation(requests[3], responses[3], nil, false)
	if err != nil || managedA <= 0 || managedA == leaseA || managedA == leaseB || managedGrantedA != 12 {
		return errors.New("first managed KeepAlive Lease grant is invalid")
	}
	managedB, managedGrantedB, err := grantObservation(requests[4], responses[4], nil, false)
	if err != nil || managedB <= 0 || managedB == leaseA || managedB == leaseB || managedB == managedA ||
		managedGrantedB != 12 {
		return errors.New("second managed KeepAlive Lease grant is invalid")
	}
	revision, err := rangeRevision(responses[5])
	if err != nil {
		return fmt.Errorf("read baseline KV revision: %w", err)
	}
	if err := validateStreamResponse(requests[6], responses[6], leaseA, 30, revision); err != nil {
		return fmt.Errorf("first KeepAlive request: %w", err)
	}
	if err := validateStreamResponse(requests[7], responses[7], leaseB, 30, revision); err != nil {
		return fmt.Errorf("second KeepAlive request: %w", err)
	}
	missingID, err := requestInt64(requests[8], "id")
	if err != nil || missingID == 0 || missingID == leaseA || missingID == leaseB ||
		missingID == managedA || missingID == managedB ||
		responseInt64(responses[8], "id") != missingID || responseInt64(responses[8], "ttl") != 0 ||
		responseInt64(responses[8], "header.revision") != revision {
		return errors.New("unknown KeepAlive ID did not return an ordered TTL=0 response")
	}
	if err := validateStreamResponse(requests[9], responses[9], leaseA, 30, revision); err != nil {
		return fmt.Errorf("repeated KeepAlive request: %w", err)
	}
	for index, observation := range []struct {
		response map[string]any
		id       int64
		granted  int64
	}{{responses[10], leaseA, grantedA}, {responses[11], leaseB, grantedB}} {
		if responseInt64(observation.response, "id") != observation.id ||
			responseInt64(observation.response, "ttl") <= 0 ||
			responseInt64(observation.response, "ttl") > observation.granted ||
			responseInt64(observation.response, "grantedTTL") != observation.granted ||
			len(responseList(observation.response, "keys")) != 0 ||
			responseInt64(observation.response, "header.revision") != revision {
			return fmt.Errorf("TimeToLive observation %d differs from its active Lease", index+1)
		}
	}
	if err := validateManagedObservation(records, requests, responses, 12, managedA,
		"keepalive-managed-a-first", "keepalive-managed-a-second"); err != nil {
		return err
	}
	if err := validateManagedObservation(records, requests, responses, 12, managedB,
		"keepalive-managed-b-first", "keepalive-managed-b-second"); err != nil {
		return err
	}
	afterRevision, err := rangeRevision(responses[16])
	if err != nil || afterRevision != revision {
		return errors.New("pure KeepAlive changed the KV revision")
	}
	return nil
}

func validateStreamResponse(request, response map[string]any, expectedID, grantedTTL, revision int64) error {
	requestID, err := requestInt64(request, "id")
	if err != nil || requestID != expectedID || responseInt64(response, "id") != expectedID ||
		responseInt64(response, "ttl") != grantedTTL ||
		responseInt64(response, "header.revision") != revision {
		return errors.New("ID, granted TTL, or response revision differs from the active Lease")
	}
	return nil
}

func validateManagedObservation(records []trace.Record, requests, responses []map[string]any,
	grantedTTL, leaseID int64, firstID, secondID string) error {
	first, second := -1, -1
	for index, expected := range m4LeaseKeepAliveCases {
		if expected.id == firstID {
			first = index
		}
		if expected.id == secondID {
			second = index
		}
	}
	if first < 0 || second < 0 || requests[first] == nil || requests[second] == nil ||
		requestInt64Unchecked(requests[first], "id") != leaseID ||
		requestInt64Unchecked(requests[second], "id") != leaseID ||
		responseInt64(responses[first], "id") != leaseID || responseInt64(responses[second], "id") != leaseID ||
		responseInt64(responses[first], "ttl") != grantedTTL ||
		responseInt64(responses[second], "ttl") != grantedTTL {
		return fmt.Errorf("Go TTL/3 KeepAlive responses for Lease %d are invalid", leaseID)
	}
	firstElapsed, err := strconv.ParseInt(records[first].ElapsedNS, 10, 64)
	if err != nil || firstElapsed < 0 {
		return fmt.Errorf("Go KeepAlive first response time for Lease %d is invalid", leaseID)
	}
	secondElapsed, err := strconv.ParseInt(records[second].ElapsedNS, 10, 64)
	if err != nil || secondElapsed <= firstElapsed {
		return fmt.Errorf("Go KeepAlive response order for Lease %d is invalid", leaseID)
	}
	interval := time.Duration(secondElapsed - firstElapsed)
	expected := time.Duration(grantedTTL) * time.Second / 3
	if interval < expected/2 || interval > expected*2 {
		return fmt.Errorf("Go KeepAlive interval for Lease %d was %s, expected approximately %s",
			leaseID, interval, expected)
	}
	return nil
}

func rangeRevision(response map[string]any) (int64, error) {
	if response == nil || len(responseList(response, "kvs")) != 0 {
		return 0, errors.New("revision probe is malformed or unexpectedly non-empty")
	}
	revision := responseInt64(response, "header.revision")
	if revision < 1 {
		return 0, errors.New("revision probe has no positive header revision")
	}
	return revision, nil
}
