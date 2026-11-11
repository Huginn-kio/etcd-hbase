// Package history records actual KV RPC invocations for bounded linearizability checks.
package history

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Observation contains protobuf wire snapshots, not error descriptions that may contain secrets.
// A non-OK call has an unknown outcome until an independent model proves otherwise.
type Observation struct {
	ID       uint64
	Worker   uint32
	Method   string
	Begin    uint64
	End      uint64
	Request  []byte
	Response []byte
	Code     string
}

// Recorder owns a bounded serialized history. The byte limit is not a bound on
// gRPC decoding or temporary protobuf allocations. Its logical clock orders
// events in one process; descheduling around an RPC can widen an interval.
type Recorder struct {
	mutex        sync.Mutex
	maxCalls     int
	maxBytes     int
	usedBytes    int
	clock        uint64
	active       int
	preparing    int
	failed       bool
	observations []*Observation
}

func NewRecorder(maxCalls, maxBytes int) (*Recorder, error) {
	if maxCalls < 1 || maxCalls > 1024 || maxBytes < 1 || maxBytes > 256<<20 {
		return nil, errors.New("invalid history bounds")
	}
	return &Recorder{maxCalls: maxCalls, maxBytes: maxBytes}, nil
}

// Invoke forwards one real KV RPC attempt. Its interval starts after request
// snapshot preparation; callers must separately record any higher-level retries.
// Request and response bytes are test evidence and must stay in controlled storage.
// If the response cannot be recorded, the RPC result is preserved and Snapshot fails.
func (recorder *Recorder) Invoke(ctx context.Context, client etcdserverpb.KVClient, request proto.Message) (proto.Message, error) {
	return recorder.InvokeWorker(ctx, client, 0, request)
}

// InvokeWorker associates a caller-provided worker ID with the actual attempt.
// Worker zero denotes the driver; it carries no credentials or business data.
func (recorder *Recorder) InvokeWorker(ctx context.Context, client etcdserverpb.KVClient, worker uint32, request proto.Message) (proto.Message, error) {
	if ctx == nil || client == nil || request == nil {
		return nil, errors.New("history invocation requires context, client and request")
	}
	method, err := methodName(request)
	if err != nil {
		return nil, err
	}
	size := proto.Size(request)
	if err := recorder.reserveRequest(size); err != nil {
		return nil, err
	}
	requestBytes, err := proto.Marshal(request)
	if err != nil || len(requestBytes) != size {
		recorder.abortRequest()
		return nil, errors.New("history request could not be encoded")
	}
	copyRequest := proto.Clone(request)
	record := recorder.begin(method, worker, requestBytes)
	response, callErr := invoke(ctx, client, copyRequest)
	recorder.finish(record, response, callErr)
	return response, callErr
}

func methodName(request proto.Message) (string, error) {
	switch value := request.(type) {
	case *etcdserverpb.RangeRequest:
		if value != nil {
			return "Range", nil
		}
	case *etcdserverpb.PutRequest:
		if value != nil {
			return "Put", nil
		}
	case *etcdserverpb.DeleteRangeRequest:
		if value != nil {
			return "DeleteRange", nil
		}
	case *etcdserverpb.TxnRequest:
		if value != nil {
			return "Txn", nil
		}
	}
	return "", errors.New("unsupported history request")
}

func invoke(ctx context.Context, client etcdserverpb.KVClient, request proto.Message) (proto.Message, error) {
	switch request := request.(type) {
	case *etcdserverpb.RangeRequest:
		return client.Range(ctx, request)
	case *etcdserverpb.PutRequest:
		return client.Put(ctx, request)
	case *etcdserverpb.DeleteRangeRequest:
		return client.DeleteRange(ctx, request)
	case *etcdserverpb.TxnRequest:
		return client.Txn(ctx, request)
	default:
		return nil, errors.New("unsupported history request")
	}
}

func (recorder *Recorder) reserveRequest(size int) error {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	if recorder.failed || size < 0 || len(recorder.observations)+recorder.preparing >= recorder.maxCalls || size > recorder.maxBytes-recorder.usedBytes {
		recorder.failed = true
		return errors.New("history capacity exhausted")
	}
	recorder.preparing++
	recorder.usedBytes += size
	return nil
}

func (recorder *Recorder) begin(method string, worker uint32, request []byte) *Observation {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	recorder.preparing--
	recorder.clock++
	record := &Observation{ID: uint64(len(recorder.observations) + 1), Worker: worker, Method: method, Begin: recorder.clock, Request: request}
	recorder.observations = append(recorder.observations, record)
	recorder.active++
	return record
}

func (recorder *Recorder) finish(record *Observation, response proto.Message, callErr error) {
	code := status.Code(callErr)
	if errors.Is(callErr, context.Canceled) {
		code = codes.Canceled
	} else if errors.Is(callErr, context.DeadlineExceeded) {
		code = codes.DeadlineExceeded
	}
	recorder.mutex.Lock()
	recorder.clock++
	record.End = recorder.clock
	record.Code = code.String()
	recorder.mutex.Unlock()

	var encoded []byte
	var encodeErr error
	if callErr == nil && response != nil && !(reflect.ValueOf(response).Kind() == reflect.Ptr && reflect.ValueOf(response).IsNil()) {
		if proto.Size(response) > recorder.maxBytes {
			encodeErr = errors.New("history response exceeds byte limit")
		} else {
			encoded, encodeErr = proto.Marshal(response)
		}
	} else if callErr == nil {
		encodeErr = errors.New("successful RPC has no response")
	}
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	recorder.active--
	if encodeErr != nil || len(encoded) > recorder.maxBytes-recorder.usedBytes {
		recorder.failed = true
		return
	}
	record.Response = encoded
	recorder.usedBytes += len(encoded)
}

func (recorder *Recorder) abortRequest() {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	recorder.preparing--
	recorder.failed = true
}

// Snapshot fails closed for incomplete or truncated histories and copies owned bytes.
func (recorder *Recorder) Snapshot() ([]Observation, error) {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	if recorder.failed || recorder.active != 0 || recorder.preparing != 0 || len(recorder.observations) == 0 {
		return nil, errors.New("history is incomplete or exceeded its bounds")
	}
	result := make([]Observation, len(recorder.observations))
	for index, record := range recorder.observations {
		result[index] = *record
		result[index].Request = append([]byte(nil), record.Request...)
		result[index].Response = append([]byte(nil), record.Response...)
	}
	return result, nil
}
