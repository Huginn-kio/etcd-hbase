package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunRejectsEvidenceScopesOutsideTheGoDriver(t *testing.T) {
	for _, scope := range []string{"native-etcd", "target-hbase"} {
		t.Run(scope, func(t *testing.T) {
			err := run([]string{"--scope", scope}, &bytes.Buffer{}, &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), "scope must be transport or transport-fixture") {
				t.Fatalf("scope %q was not rejected: %v", scope, err)
			}
		})
	}
}
