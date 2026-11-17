package scenario

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"

	"sigs.k8s.io/yaml"
)

var expectedM4LeaseSteps = []Step{
	{CaseID: "L05", ID: "native-baseline"},
	{CaseID: "L01", ID: "auto-grant"},
	{CaseID: "L01", ID: "auto-ttl"},
	{CaseID: "L01", ID: "explicit-grant"},
	{CaseID: "L01", ID: "explicit-ttl"},
	{CaseID: "L01", ID: "duplicate-id"},
	{CaseID: "L04", ID: "missing-ttl"},
	{CaseID: "L01", ID: "negative-id"},
	{CaseID: "L05", ID: "leases-list"},
	{CaseID: "L01", ID: "oversized-ttl"},
	{CaseID: "L04", ID: "zero-id-ttl"},
}

var expectedM4LeaseBindingSteps = []Step{
	{CaseID: "L05", ID: "binding-baseline"},
	{CaseID: "L01", ID: "binding-grant-a"},
	{CaseID: "L01", ID: "binding-grant-b"},
	{CaseID: "T01", ID: "binding-register-then"},
	{CaseID: "L04", ID: "binding-ttl-a-initial"},
	{CaseID: "L04", ID: "binding-ttl-b-initial"},
	{CaseID: "P01", ID: "binding-ignore-flags"},
	{CaseID: "L04", ID: "binding-ttl-a-preserved"},
	{CaseID: "P01", ID: "binding-rebind-a-to-b"},
	{CaseID: "L04", ID: "binding-ttl-a-after-rebind"},
	{CaseID: "L04", ID: "binding-ttl-b-after-rebind"},
	{CaseID: "T01", ID: "binding-register-else"},
	{CaseID: "L04", ID: "binding-ttl-b-after-else"},
	{CaseID: "T01", ID: "binding-register-nested"},
	{CaseID: "L04", ID: "binding-ttl-a-after-nested"},
	{CaseID: "L04", ID: "binding-ttl-b-after-nested"},
	{CaseID: "P01", ID: "binding-unbind"},
	{CaseID: "L04", ID: "binding-ttl-a-after-unbind"},
	{CaseID: "D01", ID: "binding-delete"},
	{CaseID: "L04", ID: "binding-ttl-b-after-delete"},
	{CaseID: "P01", ID: "binding-recreate-unleased"},
	{CaseID: "L04", ID: "binding-ttl-b-after-recreate"},
	{CaseID: "D01", ID: "binding-delete-range"},
	{CaseID: "L04", ID: "binding-ttl-a-after-delete-range"},
	{CaseID: "L04", ID: "binding-ttl-b-after-delete-range"},
	{CaseID: "R01", ID: "binding-role-prefix-after-delete-range"},
	{CaseID: "R01", ID: "binding-version-after-delete-range"},
	{CaseID: "P01", ID: "binding-survivor-put"},
	{CaseID: "L04", ID: "binding-ttl-b-after-survivor-put"},
	{CaseID: "T01", ID: "binding-invalid-lease-atomicity"},
	{CaseID: "R01", ID: "binding-atomicity-range-first"},
	{CaseID: "R01", ID: "binding-atomicity-range-second"},
	{CaseID: "L04", ID: "binding-ttl-a-after-failed-txn"},
	{CaseID: "P01", ID: "binding-invalid-lease-put"},
	{CaseID: "R01", ID: "binding-atomicity-range-invalid-put"},
	{CaseID: "T01", ID: "binding-invalid-lease-unselected-branch"},
	{CaseID: "R01", ID: "binding-unselected-then-absent"},
	{CaseID: "R01", ID: "binding-unselected-else-visible"},
	{CaseID: "L04", ID: "binding-ttl-b-after-unselected-branch"},
}

var expectedM4LeaseKeepAliveSteps = []Step{
	{CaseID: "L05", ID: "native-baseline"},
	{CaseID: "L01", ID: "keepalive-grant-a"},
	{CaseID: "L01", ID: "keepalive-grant-b"},
	{CaseID: "L01", ID: "keepalive-grant-managed-a"},
	{CaseID: "L01", ID: "keepalive-grant-managed-b"},
	{CaseID: "R01", ID: "keepalive-range-before"},
	{CaseID: "L02", ID: "keepalive-stream-a"},
	{CaseID: "L02", ID: "keepalive-stream-b"},
	{CaseID: "L02", ID: "keepalive-stream-missing"},
	{CaseID: "L02", ID: "keepalive-stream-a-repeat"},
	{CaseID: "L04", ID: "keepalive-ttl-a"},
	{CaseID: "L04", ID: "keepalive-ttl-b"},
	{CaseID: "L02", ID: "keepalive-managed-a-first"},
	{CaseID: "L02", ID: "keepalive-managed-a-second"},
	{CaseID: "L02", ID: "keepalive-managed-b-first"},
	{CaseID: "L02", ID: "keepalive-managed-b-second"},
	{CaseID: "R01", ID: "keepalive-range-after"},
}

var expectedM4LeaseRevokeSteps = []Step{
	{CaseID: "L05", ID: "native-baseline"},
	{CaseID: "L01", ID: "revoke-grant-bound"},
	{CaseID: "P01", ID: "revoke-put-bound-key-a"},
	{CaseID: "P01", ID: "revoke-put-bound-key-b"},
	{CaseID: "R01", ID: "revoke-range-before-a"},
	{CaseID: "R01", ID: "revoke-range-before-b"},
	{CaseID: "L03", ID: "revoke-bound"},
	{CaseID: "R01", ID: "revoke-range-after-a"},
	{CaseID: "R01", ID: "revoke-range-after-b"},
	{CaseID: "L04", ID: "revoke-ttl-after"},
	{CaseID: "L03", ID: "revoke-missing"},
	{CaseID: "L01", ID: "revoke-grant-empty"},
	{CaseID: "L03", ID: "revoke-empty"},
	{CaseID: "L04", ID: "revoke-empty-ttl"},
	{CaseID: "L01", ID: "expiry-grant"},
	{CaseID: "P01", ID: "expiry-put-bound-key-a"},
	{CaseID: "P01", ID: "expiry-put-bound-key-b"},
	{CaseID: "R01", ID: "expiry-range-before-a"},
	{CaseID: "R01", ID: "expiry-range-before-b"},
	{CaseID: "L04", ID: "expiry-ttl-after"},
	{CaseID: "R01", ID: "expiry-range-after-a"},
	{CaseID: "R01", ID: "expiry-range-after-b"},
}

var expectedM4WatchStartSteps = []Step{
	{CaseID: "P01", ID: "watch-start-history-put"},
	{CaseID: "C19", ID: "initial-minus-two-created"},
	{CaseID: "C19", ID: "initial-minus-two-compacted"},
	{CaseID: "C19", ID: "initial-minus-two-duplicate-id"},
	{CaseID: "C19", ID: "initial-minus-two-cancel-id"},
	{CaseID: "C19", ID: "initial-minus-two-reused-created"},
	{CaseID: "C19", ID: "initial-minus-two-reused-cancelled"},
	{CaseID: "C19", ID: "initial-minus-one-created"},
	{CaseID: "C19", ID: "initial-minus-one-cancelled"},
	{CaseID: "C19", ID: "initial-zero-created"},
	{CaseID: "C19", ID: "initial-zero-cancelled"},
	{CaseID: "C19", ID: "initial-watch-id-minus-one-created"},
	{CaseID: "C19", ID: "initial-watch-id-minus-one-cancelled"},
	{CaseID: "C19", ID: "initial-future-created"},
	{CaseID: "C19", ID: "initial-future-cancelled"},
	{CaseID: "C19", ID: "watch-negative-live-created"},
	{CaseID: "C19", ID: "watch-negative-live-progress"},
	{CaseID: "P01", ID: "watch-negative-live-put"},
	{CaseID: "C19", ID: "watch-negative-live-event"},
	{CaseID: "C19", ID: "watch-negative-live-cancelled"},
	{CaseID: "P01", ID: "watch-compact-fixture-put"},
	{CaseID: "R01", ID: "watch-compact-fixture-range"},
	{CaseID: "C14", ID: "watch-compact-fixture-compact"},
	{CaseID: "C19", ID: "compacted-minus-two-created"},
	{CaseID: "C19", ID: "compacted-minus-two-compacted"},
	{CaseID: "C19", ID: "compacted-minus-two-duplicate-id"},
	{CaseID: "C19", ID: "compacted-minus-two-cancel-id"},
	{CaseID: "C19", ID: "compacted-minus-two-reused-created"},
	{CaseID: "C19", ID: "compacted-minus-two-reused-cancelled"},
	{CaseID: "C19", ID: "compacted-minus-one-created"},
	{CaseID: "C19", ID: "compacted-minus-one-compacted"},
	{CaseID: "C19", ID: "compacted-minus-one-duplicate-id"},
	{CaseID: "C19", ID: "compacted-minus-one-cancel-id"},
	{CaseID: "C19", ID: "compacted-minus-one-reused-created"},
	{CaseID: "C19", ID: "compacted-minus-one-reused-cancelled"},
	{CaseID: "C19", ID: "compacted-zero-created"},
	{CaseID: "C19", ID: "compacted-zero-cancelled"},
	{CaseID: "C19", ID: "compacted-watch-id-minus-one-created"},
	{CaseID: "C19", ID: "compacted-watch-id-minus-one-cancelled"},
}

// LoadM4LeaseSuite accepts only the pinned J40 public lease-query scenario.
func LoadM4LeaseSuite(path string) (Suite, string, error) {
	return loadM4Suite(path, "m4-lease-v1", expectedM4LeaseSteps)
}

// LoadM4LeaseBindingSuite accepts only the pinned J41 lease-binding scenario.
func LoadM4LeaseBindingSuite(path string) (Suite, string, error) {
	return loadM4Suite(path, "m4-lease-bindings-v1", expectedM4LeaseBindingSteps)
}

// LoadM4LeaseKeepAliveSuite accepts only the pinned J42 bidi-stream scenario.
func LoadM4LeaseKeepAliveSuite(path string) (Suite, string, error) {
	return loadM4Suite(path, "m4-lease-keepalive-v1", expectedM4LeaseKeepAliveSteps)
}

// LoadM4LeaseRevokeSuite accepts only the pinned J43 revoke and automatic-expiry scenario.
func LoadM4LeaseRevokeSuite(path string) (Suite, string, error) {
	return loadM4Suite(path, "m4-lease-revoke-v1", expectedM4LeaseRevokeSteps)
}

// LoadM4WatchStartSuite accepts only the pinned J45 raw-proto Watch start matrix.
func LoadM4WatchStartSuite(path string) (Suite, string, error) {
	return loadM4Suite(path, "m4-watch-starts-v1", expectedM4WatchStartSteps)
}

func loadM4Suite(path, expectedID string, expectedSteps []Step) (Suite, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return Suite{}, "", errors.New("M4 scenario manifest cannot be opened")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 65_537))
	if err != nil || len(data) == 0 || len(data) > 65_536 {
		return Suite{}, "", errors.New("M4 scenario manifest is empty or oversized")
	}
	var suite Suite
	if err := yaml.UnmarshalStrict(data, &suite); err != nil {
		return Suite{}, "", errors.New("M4 scenario manifest is invalid")
	}
	if suite.SchemaVersion != 1 || suite.ID != expectedID || suite.Protocol != protocolVersion ||
		len(suite.Steps) != len(expectedSteps) {
		return Suite{}, "", errors.New("M4 scenario identity or coverage is not pinned")
	}
	for index, expected := range expectedSteps {
		if suite.Steps[index] != expected {
			return Suite{}, "", errors.New("M4 scenario coverage differs from its pinned steps")
		}
	}
	digest := sha256.Sum256(data)
	return suite, hex.EncodeToString(digest[:]), nil
}
