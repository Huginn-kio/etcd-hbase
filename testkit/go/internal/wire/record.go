package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"strconv"
	"time"
)

func expectDeadline(err error) error {
	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("got gRPC code %s, want caller deadline", errorCode(err))
	}
	classified := trace.ClassifyError(err)
	if classified == nil || classified.GRPCCode != codes.DeadlineExceeded.String() {
		return fmt.Errorf("got trace error %v, want gRPC code %s", classified, codes.DeadlineExceeded)
	}
	return nil
}

func expectError(err error, expectedCode codes.Code, sentinel error) error {
	if err == nil {
		return fmt.Errorf("expected gRPC code %s, got success", expectedCode)
	}
	if code := errorCode(err); code != expectedCode {
		return fmt.Errorf("got gRPC code %s, want %s", code, expectedCode)
	}
	if sentinel != nil && !errors.Is(err, sentinel) {
		return fmt.Errorf("error %T does not match client sentinel %v", err, sentinel)
	}
	return nil
}

func errorCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	var codeProvider interface{ Code() codes.Code }
	if errors.As(err, &codeProvider) {
		return codeProvider.Code()
	}
	return status.Code(err)
}

func grpcTerminal(err error) trace.StreamTerminal {
	return trace.StreamTerminal{Kind: "grpc-status", GRPCCode: errorCode(err).String()}
}

type legacyProtoMessage interface {
	Reset()
	String() string
	ProtoMessage()
}

func writeUnaryRecord(
	output io.Writer,
	caseID string,
	scope string,
	request legacyProtoMessage,
	err error,
	elapsed time.Duration,
) error {
	return writeRecord(output, caseID, scope, "etcdserverpb.KV/Range", request, err, nil, elapsed)
}

func writeStreamRecord(
	output io.Writer,
	caseID string,
	scope string,
	method string,
	request legacyProtoMessage,
	err error,
	terminal trace.StreamTerminal,
	elapsed time.Duration,
) error {
	return writeRecord(output, caseID, scope, method, request, err, &terminal, elapsed)
}

func writeRecord(
	output io.Writer,
	caseID string,
	scope string,
	method string,
	request legacyProtoMessage,
	err error,
	terminal *trace.StreamTerminal,
	elapsed time.Duration,
) error {
	requestJSON, snapshotErr := trace.SnapshotProto(request)
	if snapshotErr != nil {
		return errors.New("request snapshot failed")
	}
	record := trace.Record{
		Schema:         trace.SchemaVersion,
		CaseID:         caseID,
		Ordinal:        1,
		Method:         method,
		Request:        requestJSON,
		Response:       json.RawMessage("null"),
		Error:          trace.ClassifyError(err),
		StreamTerminal: terminal,
		EvidenceScope:  scope,
		ElapsedNS:      strconv.FormatInt(max(0, elapsed.Nanoseconds()), 10),
	}
	if err := trace.WriteJSONL(output, record); err != nil {
		return errors.New("wire trace record could not be written")
	}
	return nil
}
