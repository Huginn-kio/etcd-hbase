package trace

import (
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
)

const SchemaVersion = "etcd-hbase-trace/v1"

const (
	maxRecordBytes  = 64 << 20
	maxTraceBytes   = 256 << 20
	maxTraceRecords = 10_000
)

// Record is one request/response observation. Protobuf snapshots are built
// from generated field tags before JSON encoding, preserving bytes, 64-bit
// values, nested messages, and selected oneof branches.
type Record struct {
	Schema         string          `json:"schema"`
	CaseID         string          `json:"case_id"`
	Ordinal        uint64          `json:"ordinal"`
	Method         string          `json:"method"`
	Request        json.RawMessage `json:"request"`
	Response       json.RawMessage `json:"response"`
	Error          *Error          `json:"error"`
	StreamTerminal *StreamTerminal `json:"stream_terminal"`
	EvidenceScope  string          `json:"evidence_scope"`
	ElapsedNS      string          `json:"elapsed_ns"`
}

// Error stores stable classifications only. Error descriptions may contain
// endpoints, credentials, or request data, so they are never copied to traces.
type Error struct {
	GRPCCode     string `json:"grpc_code"`
	ConsumerKind string `json:"consumer_kind"`
	SemanticKind string `json:"semantic_kind"`
}

type StreamTerminal struct {
	Kind     string `json:"kind"`
	GRPCCode string `json:"grpc_code,omitempty"`
}

func (record Record) Validate() error {
	if err := record.ValidateMetadata(); err != nil {
		return err
	}

	authMethod := strings.HasPrefix(record.Method, "etcdserverpb.Auth/")
	if err := validateObject(record.Request, "request", false, authMethod); err != nil {
		return err
	}
	if err := validateObject(record.Response, "response", true, authMethod); err != nil {
		return err
	}
	if bytes.Equal(bytes.TrimSpace(record.Response), []byte("null")) && record.Error == nil && record.StreamTerminal == nil {
		return errors.New("completed observation requires a response, error, or stream terminal")
	}
	if record.Error != nil {
		if !validSemanticKind(record.Error.SemanticKind) {
			return errors.New("invalid semantic error kind")
		}
		if !validGRPCCode(record.Error.GRPCCode) {
			return errors.New("invalid grpc_code")
		}
		switch record.Error.ConsumerKind {
		case "grpc-status", "etcd-client-error", "context-canceled", "context-deadline", "go-error":
		default:
			return errors.New("invalid consumer_kind")
		}
	}
	if err := validateTerminal(record.StreamTerminal); err != nil {
		return err
	}
	if record.StreamTerminal != nil && record.StreamTerminal.GRPCCode != "" && record.Error != nil && record.StreamTerminal.GRPCCode != record.Error.GRPCCode {
		return errors.New("terminal and error codes disagree")
	}

	return nil
}

// ValidateMetadata validates routing and evidence labels before an RPC exists.
func (record Record) ValidateMetadata() error {
	if record.Schema != SchemaVersion {
		return errors.New("unsupported trace schema")
	}
	if record.CaseID == "" || len(record.CaseID) > 128 || !safeIdentifier(record.CaseID) {
		return errors.New("invalid case_id")
	}
	if record.Ordinal == 0 || record.Ordinal > 1<<53-1 {
		return errors.New("ordinal must be a positive safe JSON integer")
	}
	if len(record.Method) > 256 || !methodPattern.MatchString(record.Method) {
		return errors.New("invalid method path")
	}
	switch record.EvidenceScope {
	case "transport", "transport-fixture", "native-etcd", "target-hbase":
	default:
		return errors.New("invalid evidence_scope")
	}
	elapsed, err := strconv.ParseUint(record.ElapsedNS, 10, 64)
	if err != nil || strconv.FormatUint(elapsed, 10) != record.ElapsedNS {
		return errors.New("elapsed_ns must be a canonical non-negative decimal string")
	}
	return nil
}

var methodPattern = regexp.MustCompile(`^[A-Za-z0-9_.]+/[A-Za-z0-9_]+$`)

func safeIdentifier(value string) bool {
	if value == "" || !((value[0] >= 'a' && value[0] <= 'z') || (value[0] >= 'A' && value[0] <= 'Z') || (value[0] >= '0' && value[0] <= '9')) {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func validateTerminal(terminal *StreamTerminal) error {
	if terminal == nil {
		return nil
	}
	switch terminal.Kind {
	case "eof", "client-close":
		if terminal.GRPCCode != "" {
			return errors.New("a clean terminal must not claim a failure status")
		}
	case "grpc-status":
		if !validGRPCCode(terminal.GRPCCode) {
			return errors.New("invalid stream terminal grpc_code")
		}
	case "context-canceled":
		if terminal.GRPCCode != "" && terminal.GRPCCode != "Canceled" {
			return errors.New("invalid cancellation terminal")
		}
	case "context-deadline":
		if terminal.GRPCCode != "" && terminal.GRPCCode != "DeadlineExceeded" {
			return errors.New("invalid deadline terminal")
		}
	default:
		return errors.New("invalid stream terminal kind")
	}
	return nil
}
