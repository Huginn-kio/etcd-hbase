package compare

import (
	"errors"
	"fmt"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
)

var m4LeaseRevokeCases = []struct {
	id     string
	method string
}{
	{"native-baseline", "etcdserverpb.Lease/LeaseLeases"},
	{"revoke-grant-bound", "etcdserverpb.Lease/LeaseGrant"},
	{"revoke-put-bound-key-a", "etcdserverpb.KV/Put"},
	{"revoke-put-bound-key-b", "etcdserverpb.KV/Put"},
	{"revoke-range-before-a", "etcdserverpb.KV/Range"},
	{"revoke-range-before-b", "etcdserverpb.KV/Range"},
	{"revoke-bound", "etcdserverpb.Lease/LeaseRevoke"},
	{"revoke-range-after-a", "etcdserverpb.KV/Range"},
	{"revoke-range-after-b", "etcdserverpb.KV/Range"},
	{"revoke-ttl-after", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"revoke-missing", "etcdserverpb.Lease/LeaseRevoke"},
	{"revoke-grant-empty", "etcdserverpb.Lease/LeaseGrant"},
	{"revoke-empty", "etcdserverpb.Lease/LeaseRevoke"},
	{"revoke-empty-ttl", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"expiry-grant", "etcdserverpb.Lease/LeaseGrant"},
	{"expiry-put-bound-key-a", "etcdserverpb.KV/Put"},
	{"expiry-put-bound-key-b", "etcdserverpb.KV/Put"},
	{"expiry-range-before-a", "etcdserverpb.KV/Range"},
	{"expiry-range-before-b", "etcdserverpb.KV/Range"},
	{"expiry-ttl-after", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"expiry-range-after-a", "etcdserverpb.KV/Range"},
	{"expiry-range-after-b", "etcdserverpb.KV/Range"},
}

func validateM4LeaseRevokeTrace(records []trace.Record) error {
	if len(records) != len(m4LeaseRevokeCases) {
		return fmt.Errorf("M4 Revoke trace has %d records, expected %d", len(records), len(m4LeaseRevokeCases))
	}
	requests := make([]map[string]any, len(records))
	responses := make([]map[string]any, len(records))
	for index, expected := range m4LeaseRevokeCases {
		record := records[index]
		if record.CaseID != expected.id || record.Method != expected.method || record.StreamTerminal != nil {
			return fmt.Errorf("M4 Revoke record %d is not the pinned %s step", index+1, expected.id)
		}
		request, err := m4SnapshotFields(record.Request)
		if err != nil {
			return fmt.Errorf("decode %s request: %w", expected.id, err)
		}
		requests[index] = request
		if expected.id == "revoke-missing" {
			if record.Error == nil || record.Error.GRPCCode != "NotFound" || responsePresent(record.Response) {
				return errors.New("unknown Lease Revoke must return NotFound without a response")
			}
			continue
		}
		if record.Error != nil || !responsePresent(record.Response) {
			return fmt.Errorf("M4 Revoke step %s failed or omitted its response", expected.id)
		}
		response, err := m4SnapshotFields(record.Response)
		if err != nil {
			return fmt.Errorf("decode %s response: %w", expected.id, err)
		}
		requests[index], responses[index] = request, response
	}

	baseRevision := responseInt64(responses[0], "header.revision")
	if baseRevision < 1 || len(responseList(responses[0], "leases")) != 0 {
		return errors.New("J43 scenario must start from an empty Lease set and a valid revision")
	}
	boundID, boundTTL, err := grantObservation(requests[1], responses[1], nil, false)
	if err != nil || boundID <= 0 || boundTTL != 60 {
		return errors.New("bound Lease grant is invalid")
	}
	for offset, putIndex := range []int{2, 3} {
		if requestInt64Unchecked(requests[putIndex], "lease") != boundID ||
			responseInt64(responses[putIndex], "header.revision") != baseRevision+1+int64(offset) {
			return errors.New("bound-key Put did not advance one KV revision")
		}
	}
	if err := requireRangeLease(responses[4], boundID, true, baseRevision+2); err != nil {
		return fmt.Errorf("pre-Revoke range a: %w", err)
	}
	if err := requireRangeLease(responses[5], boundID, true, baseRevision+2); err != nil {
		return fmt.Errorf("pre-Revoke range b: %w", err)
	}
	if requestInt64Unchecked(requests[6], "id") != boundID ||
		responseInt64(responses[6], "header.revision") != baseRevision+3 {
		return errors.New("nonempty Revoke did not delete its key in one KV revision")
	}
	if err := requireRangeLease(responses[7], boundID, false, baseRevision+3); err != nil {
		return fmt.Errorf("post-Revoke range a: %w", err)
	}
	if err := requireRangeLease(responses[8], boundID, false, baseRevision+3); err != nil {
		return fmt.Errorf("post-Revoke range b: %w", err)
	}
	if requestInt64Unchecked(requests[9], "id") != boundID ||
		responseInt64(responses[9], "ttl") != -1 || responseInt64(responses[9], "grantedTTL") != 0 ||
		responseInt64(responses[9], "header.revision") != baseRevision+3 {
		return errors.New("revoked Lease did not use the native missing-TTL projection")
	}
	if requestInt64Unchecked(requests[10], "id") != boundID {
		return errors.New("unknown Revoke did not repeat the revoked Lease ID")
	}
	emptyID, emptyTTL, err := grantObservation(requests[11], responses[11], nil, false)
	if err != nil || emptyID <= 0 || emptyID == boundID || emptyTTL != 60 ||
		responseInt64(responses[11], "header.revision") != baseRevision+3 {
		return errors.New("empty Lease grant changed the KV revision or reused an ID")
	}
	if requestInt64Unchecked(requests[12], "id") != emptyID ||
		responseInt64(responses[12], "header.revision") != baseRevision+3 ||
		requestInt64Unchecked(requests[13], "id") != emptyID ||
		responseInt64(responses[13], "ttl") != -1 ||
		responseInt64(responses[13], "header.revision") != baseRevision+3 {
		return errors.New("empty Lease Revoke changed the KV revision or remained active")
	}
	expiryID, expiryTTL, err := grantObservation(requests[14], responses[14], nil, false)
	if err != nil || expiryID <= 0 || expiryID == boundID || expiryID == emptyID || expiryTTL != 5 ||
		responseInt64(responses[14], "header.revision") != baseRevision+3 {
		return errors.New("automatic-expiry Lease grant is invalid")
	}
	for offset, putIndex := range []int{15, 16} {
		if requestInt64Unchecked(requests[putIndex], "lease") != expiryID ||
			responseInt64(responses[putIndex], "header.revision") != baseRevision+4+int64(offset) {
			return errors.New("expiry-bound key Put did not advance one KV revision")
		}
	}
	if err := requireRangeLease(responses[17], expiryID, true, baseRevision+5); err != nil {
		return fmt.Errorf("pre-expiry range a: %w", err)
	}
	if err := requireRangeLease(responses[18], expiryID, true, baseRevision+5); err != nil {
		return fmt.Errorf("pre-expiry range b: %w", err)
	}
	if requestInt64Unchecked(requests[19], "id") != expiryID ||
		responseInt64(responses[19], "ttl") != -1 || responseInt64(responses[19], "grantedTTL") != 0 ||
		responseInt64(responses[19], "header.revision") != baseRevision+6 {
		return errors.New("automatic expiry did not durably revoke the Lease with one KV revision")
	}
	if err := requireRangeLease(responses[20], expiryID, false, baseRevision+6); err != nil {
		return fmt.Errorf("post-expiry range a: %w", err)
	}
	if err := requireRangeLease(responses[21], expiryID, false, baseRevision+6); err != nil {
		return fmt.Errorf("post-expiry range b: %w", err)
	}
	return nil
}

func requireRangeLease(response map[string]any, leaseID int64, present bool, revision int64) error {
	if responseInt64(response, "header.revision") != revision {
		return errors.New("range revision differs from the committed Lease operation")
	}
	kvs := responseList(response, "kvs")
	if present {
		if len(kvs) != 1 {
			return errors.New("expected one Lease-bound KV")
		}
		kv, ok := kvs[0].(map[string]any)
		if !ok {
			return errors.New("range returned a malformed KV")
		}
		fields, err := m4NestedFields(kv, "fields")
		if err != nil || responseInt64(fields, "lease") != leaseID {
			return errors.New("range returned the wrong Lease binding")
		}
		return nil
	}
	if len(kvs) != 0 {
		return errors.New("revoked Lease key remained visible")
	}
	return nil
}

func responsePresent(raw []byte) bool {
	return len(raw) > 0 && string(raw) != "null"
}
