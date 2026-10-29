package wire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc"
)

func TestExternalInventoryMatchesPinnedGeneratedGoServices(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "cases", "m0-wire.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Methods []struct {
			Method   string `json:"method"`
			Client   bool   `json:"client_streaming"`
			Server   bool   `json:"server_streaming"`
			Revision string `json:"source_revision"`
		} `json:"additional_methods"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	defer server.Stop()
	// Registration descriptors come from the locked v3.5.23 server module.
	// No RPC is served and no fake successful implementation is installed.
	v3electionpb.RegisterElectionServer(server, nil)
	v3lockpb.RegisterLockServer(server, nil)
	expected := make(map[string]grpc.MethodInfo)
	for service, info := range server.GetServiceInfo() {
		for _, method := range info.Methods {
			expected[service+"/"+method.Name] = method
		}
	}
	if len(expected) != 7 || len(manifest.Methods) != len(expected) {
		t.Fatal("external method inventory is incomplete")
	}
	for _, entry := range manifest.Methods {
		method, ok := expected[entry.Method]
		if !ok || entry.Client != method.IsClientStream || entry.Server != method.IsServerStream || entry.Revision != "1cf4bc40111fd95d1e14a90a09a146357b15eb13" {
			t.Fatalf("external inventory differs from pinned source for %s", entry.Method)
		}
		delete(expected, entry.Method)
	}
	if len(expected) != 0 {
		t.Fatal("duplicate external inventory entry")
	}
}
