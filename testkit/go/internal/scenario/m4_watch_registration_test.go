//go:build m4watchintegration

package scenario

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	m4MilvusDefaultSessionTTL = 15
	m4MilvusKeepAliveInterval = 5 * time.Second
	m4MilvusRoleCount         = 3
)

type m4MilvusSessionValue struct {
	ServerID   int64             `json:"ServerID,omitempty"`
	ServerName string            `json:"ServerName,omitempty"`
	Address    string            `json:"Address,omitempty"`
	Exclusive  bool              `json:"Exclusive,omitempty"`
	Version    string            `json:"Version,omitempty"`
	LeaseID    *clientv3.LeaseID `json:"LeaseID,omitempty"`
}

type m4MilvusServiceList struct {
	values map[string]m4MilvusSessionValue
}

func sameM4MilvusSessionValue(left, right m4MilvusSessionValue) bool {
	if left.ServerID != right.ServerID || left.ServerName != right.ServerName ||
		left.Address != right.Address || left.Exclusive != right.Exclusive ||
		left.Version != right.Version {
		return false
	}
	if left.LeaseID == nil || right.LeaseID == nil {
		return left.LeaseID == nil && right.LeaseID == nil
	}
	return *left.LeaseID == *right.LeaseID
}

func (serviceList *m4MilvusServiceList) apply(event *clientv3.Event) error {
	if event == nil || event.Kv == nil {
		return fmt.Errorf("Milvus service event has no key-value")
	}
	_, key := path.Split(string(event.Kv.Key))
	switch event.Type {
	case mvccpb.PUT:
		var session m4MilvusSessionValue
		if err := json.Unmarshal(event.Kv.Value, &session); err != nil {
			return fmt.Errorf("decode Milvus service PUT for %q: %w", key, err)
		}
		serviceList.values[key] = session
	case mvccpb.DELETE:
		if event.PrevKv == nil {
			return fmt.Errorf("Milvus service DELETE for %q has no PrevKV", key)
		}
		var previous m4MilvusSessionValue
		if err := json.Unmarshal(event.PrevKv.Value, &previous); err != nil {
			return fmt.Errorf("decode Milvus service DELETE PrevKV for %q: %w", key, err)
		}
		if current, exists := serviceList.values[key]; !exists || !sameM4MilvusSessionValue(current, previous) {
			return fmt.Errorf("Milvus service DELETE for %q differs from the listed value", key)
		}
		delete(serviceList.values, key)
	default:
		return fmt.Errorf("unsupported Milvus service event type %s", event.Type)
	}
	return nil
}

func TestM4WatchJavaServiceRegistrationDiscoveryLifecycle(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv(m4WatchJavaEndpointEnv))
	if endpoint == "" {
		t.Skipf("set %s to run the Java registration/discovery integration test", m4WatchJavaEndpointEnv)
	}
	prefixBase := strings.TrimSpace(os.Getenv(m4WatchJavaPrefixEnv))
	if prefixBase == "" {
		t.Fatalf("%s is required when %s is set", m4WatchJavaPrefixEnv, m4WatchJavaEndpointEnv)
	}

	testContext, cancelTest := context.WithTimeout(context.Background(), m4WatchJavaTestTimeout)
	defer cancelTest()
	client := newM4WatchJavaClient(t, endpoint)
	prefix := newM4WatchJavaTestPrefix(t, prefixBase)
	registerM4WatchJavaCleanup(t, client, prefix)
	leaseClient := etcdserverpb.NewLeaseClient(client.ActiveConnection())
	kvClient := etcdserverpb.NewKVClient(client.ActiveConnection())
	watchRoot := prefix + "session/"
	versionKey := []byte(watchRoot + "version")
	roleNames := []string{"querycoord", "datacoord", "rootcoord"}
	roleKeys := make([][]byte, 0, m4MilvusRoleCount)
	for _, roleName := range roleNames {
		roleKeys = append(roleKeys, []byte(watchRoot+roleName))
	}
	completeKey := []byte(watchRoot + "querycoord-7001")

	leaseA := grantM4RegistrationLease(t, testContext, client, m4MilvusDefaultSessionTTL)
	leaseB := grantM4RegistrationLease(t, testContext, client, 60)
	defer revokeM4RegistrationLease(t, leaseClient, leaseA.ID)
	defer revokeM4RegistrationLease(t, leaseClient, leaseB.ID)
	if leaseA.TTL != m4MilvusDefaultSessionTTL {
		t.Fatalf("Milvus default session TTL changed: requested=%d granted=%d",
			m4MilvusDefaultSessionTTL, leaseA.TTL)
	}

	initialValues := registerM4MilvusCoordinator(t, testContext, client, leaseA.ID,
		completeKey, roleKeys, versionKey)
	consumers := make(map[string]*m4MilvusServiceList, len(roleNames))
	watchContext, cancelWatch := context.WithCancel(testContext)
	defer func() {
		cancelWatch()
		if err := client.Watcher.Close(); err != nil {
			t.Errorf("close and drain Milvus registration Watches before Lease cleanup: %v", err)
		}
	}()
	watchChannels := make(map[string]clientv3.WatchChan, len(roleNames))
	for index, roleName := range roleNames {
		rolePrefix := roleKeys[index]
		listed, err := client.Get(testContext, string(rolePrefix),
			clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if err != nil {
			t.Fatalf("list Milvus %s sessions: %v", roleName, err)
		}
		if listed.Header == nil || listed.Header.Revision != initialValues.revision {
			t.Fatalf("Milvus %s list revision differs from registration: response=%v registration=%d",
				roleName, listed.Header, initialValues.revision)
		}
		values := make(map[string]m4MilvusSessionValue, len(listed.Kvs))
		for _, keyValue := range listed.Kvs {
			if keyValue.CreateRevision != initialValues.revision ||
				keyValue.ModRevision != initialValues.revision || keyValue.Lease != int64(leaseA.ID) {
				t.Fatalf("Milvus %s registration was not committed atomically at revision %d: %v",
					roleName, initialValues.revision, keyValue)
			}
			_, mapKey := path.Split(string(keyValue.Key))
			var session m4MilvusSessionValue
			if err := json.Unmarshal(keyValue.Value, &session); err != nil {
				t.Fatalf("decode listed Milvus %s session %q: %v", roleName, mapKey, err)
			}
			values[mapKey] = session
		}
		if index == 0 && (len(values) != 2 || values["querycoord-7001"].ServerID != 7001 ||
			values["querycoord"].ServerName != "querycoord") {
			t.Fatalf("querycoord service list did not contain both complete and role keys: %v", values)
		}
		if index > 0 && (len(values) != 1 || values[roleName].ServerName != roleName) {
			t.Fatalf("%s service list did not contain its registered role key: %v", roleName, values)
		}
		consumers[roleName] = &m4MilvusServiceList{values: values}
		watchChannels[roleName] = client.Watcher.Watch(watchContext, string(rolePrefix),
			clientv3.WithPrefix(), clientv3.WithPrevKV(),
			clientv3.WithRev(listed.Header.Revision+1), clientv3.WithCreatedNotify())
		awaitM4MilvusWatchCreated(t, testContext, watchChannels[roleName], roleName)
	}

	version, err := client.Get(testContext, string(versionKey))
	if err != nil || version.Count != 1 || version.Kvs[0].Lease != 0 ||
		!bytes.Equal(version.Kvs[0].Value, []byte("2.6.18")) {
		t.Fatalf("coordinator version must be present without a Lease: response=%v error=%v",
			version, err)
	}
	versionResult, err := kvClient.Range(testContext, &etcdserverpb.RangeRequest{Key: versionKey})
	if err != nil || versionResult == nil || versionResult.Header == nil ||
		versionResult.Header.Revision < initialValues.revision {
		t.Fatalf("read coordinator version after registration: response=%v error=%v", versionResult, err)
	}

	rootRoleKey := roleKeys[2]
	reboundValue := marshalM4MilvusSession(t, "rootcoord", 8002, leaseB.ID)
	putContext, cancelPut := context.WithTimeout(testContext, m4WatchJavaRPCTimeout)
	rebound, err := client.Put(putContext, string(rootRoleKey), string(reboundValue),
		clientv3.WithLease(leaseB.ID))
	cancelPut()
	if err != nil || rebound == nil || rebound.Header == nil ||
		rebound.Header.Revision <= initialValues.revision {
		t.Fatalf("rebind rootcoord session to lease B: response=%v error=%v", rebound, err)
	}
	rebindResponse := receiveM4MilvusWatch(t, testContext, watchChannels["rootcoord"], "rootcoord rebind")
	if rebindResponse.Header.Revision != rebound.Header.Revision ||
		len(rebindResponse.Events) != 1 || rebindResponse.Events[0].Type != mvccpb.PUT ||
		!bytes.Equal(rebindResponse.Events[0].Kv.Key, rootRoleKey) ||
		rebindResponse.Events[0].PrevKv == nil ||
		!bytes.Equal(rebindResponse.Events[0].PrevKv.Value, initialValues.values[string(rootRoleKey)]) ||
		rebindResponse.Events[0].Kv.Lease != int64(leaseB.ID) {
		t.Fatalf("rootcoord consumer received an invalid rebind event: %v", rebindResponse)
	}
	if err := consumers["rootcoord"].apply(rebindResponse.Events[0]); err != nil {
		t.Fatalf("apply rootcoord rebind to service list: %v", err)
	}

	if err := verifyM4MilvusRegistrationRace(t, testContext, client, leaseA.ID, leaseB.ID, watchRoot); err != nil {
		t.Fatal(err)
	}

	keepAliveContext, cancelKeepAlive := context.WithCancel(testContext)
	defer cancelKeepAlive()
	keepAliveResponses, err := client.KeepAlive(keepAliveContext, leaseA.ID)
	if err != nil {
		t.Fatalf("start Milvus TTL/3 Lease keepalive: %v", err)
	}
	keepAliveCount := 0
	keepAliveDeadline := time.NewTimer(2*m4MilvusKeepAliveInterval + 2*time.Second)
	for keepAliveCount < 2 {
		select {
		case response, open := <-keepAliveResponses:
			if !open || response == nil || response.TTL <= 0 || response.ID != leaseA.ID {
				cancelKeepAlive()
				t.Fatalf("Milvus Lease keepalive did not return a live response: %v", response)
			}
			keepAliveCount++
		case <-keepAliveDeadline.C:
			cancelKeepAlive()
			t.Fatalf("Milvus TTL/3 keepalive produced only %d responses", keepAliveCount)
		case <-testContext.Done():
			cancelKeepAlive()
			t.Fatalf("Milvus TTL/3 keepalive timed out: %v", testContext.Err())
		}
	}
	if !keepAliveDeadline.Stop() {
		select {
		case <-keepAliveDeadline.C:
		default:
		}
	}
	cancelKeepAlive()

	deletionRevision := awaitM4MilvusLeaseExpiry(t, testContext, watchChannels, consumers)
	if deletionRevision <= rebound.Header.Revision {
		t.Fatalf("Lease expiry revision %d did not follow the rebind revision %d",
			deletionRevision, rebound.Header.Revision)
	}
	if len(consumers["querycoord"].values) != 0 || len(consumers["datacoord"].values) != 0 {
		t.Fatalf("expired querycoord/datacoord registrations remain in the consumer lists: query=%v data=%v",
			consumers["querycoord"].values, consumers["datacoord"].values)
	}
	if len(consumers["rootcoord"].values) != 1 ||
		consumers["rootcoord"].values["rootcoord"].LeaseID == nil ||
		*consumers["rootcoord"].values["rootcoord"].LeaseID != leaseB.ID {
		t.Fatalf("lease A expiry removed or changed the role rebound to lease B: %v",
			consumers["rootcoord"].values)
	}
	version, err = client.Get(testContext, string(versionKey))
	if err != nil || version.Count != 1 || version.Kvs[0].Lease != 0 ||
		!bytes.Equal(version.Kvs[0].Value, []byte("2.6.18")) {
		t.Fatalf("lease expiry changed the unleased Milvus version key: response=%v error=%v",
			version, err)
	}
}

type m4MilvusRegistrationResult struct {
	revision int64
	values   map[string][]byte
}

func registerM4MilvusCoordinator(t *testing.T, ctx context.Context, client *clientv3.Client,
	leaseID clientv3.LeaseID, completeKey []byte, roleKeys [][]byte, versionKey []byte,
) m4MilvusRegistrationResult {
	t.Helper()
	values := map[string][]byte{
		string(completeKey): marshalM4MilvusSession(t, "querycoord", 7001, leaseID),
	}
	for index, key := range roleKeys {
		values[string(key)] = marshalM4MilvusSession(t,
			[]string{"querycoord", "datacoord", "rootcoord"}[index], 7001, leaseID)
	}
	comparisons := make([]clientv3.Cmp, 0, len(values)+1)
	operations := make([]clientv3.Op, 0, len(values)+1)
	for key, value := range values {
		comparisons = append(comparisons, clientv3.Compare(clientv3.Version(key), "=", 0))
		operations = append(operations, clientv3.OpPut(key, string(value), clientv3.WithLease(leaseID)))
	}
	comparisons = append(comparisons, clientv3.Compare(clientv3.Version(string(versionKey)), "=", 0))
	operations = append(operations, clientv3.OpPut(string(versionKey), "2.6.18"))
	response, err := client.Txn(ctx).If(comparisons...).Then(operations...).Commit()
	if err != nil || response == nil || !response.Succeeded || response.Header == nil {
		t.Fatalf("register Milvus coordinator roles in one transaction: response=%v error=%v", response, err)
	}
	return m4MilvusRegistrationResult{revision: response.Header.Revision, values: values}
}

func verifyM4MilvusRegistrationRace(t *testing.T, ctx context.Context, client *clientv3.Client,
	leaseA, leaseB clientv3.LeaseID, watchRoot string,
) error {
	t.Helper()
	raceRoot := watchRoot + "race/"
	completeKey := raceRoot + "querycoord-7002"
	roleKeys := []string{raceRoot + "querycoord", raceRoot + "datacoord", raceRoot + "rootcoord"}
	versionKey := raceRoot + "version"
	blockedValue := marshalM4MilvusSession(t, "datacoord", 9002, leaseB)
	if _, err := client.Put(ctx, roleKeys[1], string(blockedValue), clientv3.WithLease(leaseB)); err != nil {
		return fmt.Errorf("seed the competing Milvus registration: %w", err)
	}
	comparisons := make([]clientv3.Cmp, 0, len(roleKeys)+2)
	operations := make([]clientv3.Op, 0, len(roleKeys)+2)
	keys := append(append([]string{completeKey}, roleKeys...), versionKey)
	for _, key := range keys {
		comparisons = append(comparisons, clientv3.Compare(clientv3.Version(key), "=", 0))
		value := []byte("registration-candidate")
		if key == versionKey {
			operations = append(operations, clientv3.OpPut(key, "2.6.18"))
		} else {
			operations = append(operations, clientv3.OpPut(key, string(value), clientv3.WithLease(leaseA)))
		}
	}
	response, err := client.Txn(ctx).If(comparisons...).Then(operations...).Commit()
	if err != nil || response == nil || response.Succeeded || response.Header == nil {
		return fmt.Errorf("competing Milvus registration should lose atomically: response=%v error=%v",
			response, err)
	}
	for _, key := range keys {
		result, rangeErr := client.Get(ctx, key)
		if rangeErr != nil {
			return fmt.Errorf("read role after competing registration for %q: %w", key, rangeErr)
		}
		if key == roleKeys[1] {
			if result.Count != 1 || !bytes.Equal(result.Kvs[0].Value, blockedValue) ||
				result.Kvs[0].Lease != int64(leaseB) {
				return fmt.Errorf("competing service role changed after losing its registration: %q", key)
			}
			continue
		}
		if result.Count != 0 {
			return fmt.Errorf("failed registration partially published role or version key %q", key)
		}
	}
	return nil
}

func awaitM4MilvusWatchCreated(t *testing.T, ctx context.Context,
	watchChannel clientv3.WatchChan, role string,
) {
	t.Helper()
	for {
		response := receiveM4MilvusWatch(t, ctx, watchChannel, role+" Created")
		if response.Created {
			return
		}
		if response.Canceled || response.Err() != nil {
			t.Fatalf("Milvus %s Watch did not become ready: %v", role, response)
		}
	}
}

func receiveM4MilvusWatch(t *testing.T, ctx context.Context, watchChannel clientv3.WatchChan,
	label string,
) clientv3.WatchResponse {
	t.Helper()
	select {
	case response, open := <-watchChannel:
		if !open {
			t.Fatalf("Milvus %s Watch closed before the expected event", label)
		}
		if response.Err() != nil {
			t.Fatalf("Milvus %s Watch returned an error: %v", label, response.Err())
		}
		return response
	case <-ctx.Done():
		t.Fatalf("timed out waiting for Milvus %s Watch: %v", label, ctx.Err())
		return clientv3.WatchResponse{}
	}
}

func awaitM4MilvusLeaseExpiry(t *testing.T, ctx context.Context,
	watchChannels map[string]clientv3.WatchChan, consumers map[string]*m4MilvusServiceList,
) int64 {
	t.Helper()
	queryResponse := receiveM4MilvusWatch(t, ctx, watchChannels["querycoord"],
		"querycoord session expiry")
	if queryResponse.Header.Revision <= 0 || len(queryResponse.Events) != 2 {
		t.Fatalf("querycoord expiry must atomically remove complete and role keys: %v", queryResponse)
	}
	deletionRevision := queryResponse.Header.Revision
	for _, event := range queryResponse.Events {
		if event.Type != mvccpb.DELETE || event.Kv == nil || event.PrevKv == nil ||
			event.Kv.ModRevision != deletionRevision {
			t.Fatalf("querycoord expiry event is missing atomic revision/PrevKV: %v", event)
		}
		if err := consumers["querycoord"].apply(event); err != nil {
			t.Fatalf("apply querycoord expiry to local service list: %v", err)
		}
	}
	datacoordResponse := receiveM4MilvusWatch(t, ctx, watchChannels["datacoord"],
		"datacoord session expiry")
	if datacoordResponse.Header.Revision != deletionRevision ||
		len(datacoordResponse.Events) != 1 || datacoordResponse.Events[0].Type != mvccpb.DELETE ||
		datacoordResponse.Events[0].PrevKv == nil || datacoordResponse.Events[0].Kv.ModRevision != deletionRevision {
		t.Fatalf("datacoord expiry did not share the atomic lease deletion revision: %v", datacoordResponse)
	}
	if err := consumers["datacoord"].apply(datacoordResponse.Events[0]); err != nil {
		t.Fatalf("apply datacoord expiry to local service list: %v", err)
	}
	return deletionRevision
}

func grantM4RegistrationLease(t *testing.T, ctx context.Context, client *clientv3.Client,
	ttl int64,
) *clientv3.LeaseGrantResponse {
	t.Helper()
	grantContext, cancelGrant := context.WithTimeout(ctx, m4WatchJavaRPCTimeout)
	defer cancelGrant()
	response, err := client.Grant(grantContext, ttl)
	if err != nil || response == nil || response.ID == 0 || response.TTL <= 0 {
		t.Fatalf("grant Milvus registration Lease with TTL %d: response=%v error=%v", ttl, response, err)
	}
	return response
}

func revokeM4RegistrationLease(t *testing.T, lease etcdserverpb.LeaseClient,
	leaseID clientv3.LeaseID,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), m4WatchJavaRPCTimeout)
	defer cancel()
	if _, err := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: int64(leaseID)}); err != nil {
		if status.Code(err) != codes.NotFound {
			t.Errorf("revoke temporary Milvus registration Lease %d: %v", leaseID, err)
		}
	}
}

func marshalM4MilvusSession(t *testing.T, role string, serverID int64,
	leaseID clientv3.LeaseID,
) []byte {
	t.Helper()
	value, err := json.Marshal(m4MilvusSessionValue{
		ServerID: serverID, ServerName: role, Address: "127.0.0.1:19530",
		Exclusive: false, Version: "2.6.18", LeaseID: &leaseID,
	})
	if err != nil {
		t.Fatalf("marshal isolated Milvus session fixture: %v", err)
	}
	return value
}
