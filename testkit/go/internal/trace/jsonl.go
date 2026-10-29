package trace

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

func WriteJSONL(writer io.Writer, record Record) error {
	return writeJSONL(writer, record, maxRecordBytes)
}

func writeJSONL(writer io.Writer, record Record, maxBytes int) error {
	if writer == nil {
		return errors.New("trace writer is required")
	}
	if err := record.Validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode trace record: %w", err)
	}
	if len(encoded) > maxBytes {
		return fmt.Errorf("encoded trace record exceeds maximum %d-byte line", maxBytes)
	}
	line := append(encoded, '\n')
	written, err := writer.Write(line)
	if err != nil {
		return fmt.Errorf("write trace record: %w", err)
	}
	if written != len(line) {
		return io.ErrShortWrite
	}
	return nil
}

func ReadJSONL(reader io.Reader) ([]Record, error) {
	return readJSONL(reader, maxTraceBytes, maxTraceRecords, maxRecordBytes)
}

func readJSONL(reader io.Reader, maxBytes int64, maxRecords, maxLineBytes int) ([]Record, error) {
	if reader == nil || maxBytes < 1 || maxRecords < 1 || maxLineBytes < 1 {
		return nil, errors.New("trace input and positive budgets are required")
	}
	limitedReader := &io.LimitedReader{R: reader, N: maxBytes + 1}
	scanner := bufio.NewScanner(limitedReader)
	scanner.Buffer(make([]byte, min(64*1024, maxLineBytes+2)), maxLineBytes+2)
	scanner.Split(terminatedLine)
	var records []Record
	lastOrdinal := make(map[string]uint64)
	line := 0
	for scanner.Scan() {
		if limitedReader.N == 0 {
			return nil, errors.New("trace exceeds maximum total size")
		}
		if len(records) >= maxRecords {
			return nil, errors.New("trace exceeds maximum record count")
		}
		line++
		payload := bytes.TrimSpace(scanner.Bytes())
		if len(payload) == 0 {
			return nil, fmt.Errorf("trace line %d is empty", line)
		}
		if len(payload) > maxLineBytes {
			return nil, errors.New("trace record exceeds maximum line size")
		}
		record, err := decodeRecord(payload)
		if err != nil {
			return nil, fmt.Errorf("trace line %d is invalid: %w", line, err)
		}
		if previous, ok := lastOrdinal[record.CaseID]; ok && record.Ordinal <= previous {
			return nil, fmt.Errorf("trace line %d ordinal %d is not after previous ordinal %d", line, record.Ordinal, previous)
		}
		lastOrdinal[record.CaseID] = record.Ordinal
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read trace line %d: %w", line+1, err)
	}
	if limitedReader.N == 0 {
		return nil, errors.New("trace exceeds maximum total size")
	}
	if len(records) == 0 {
		return nil, errors.New("trace is empty")
	}
	return records, nil
}

func terminatedLine(data []byte, atEOF bool) (int, []byte, error) {
	if atEOF && len(data) > 0 && !bytes.ContainsRune(data, '\n') {
		return 0, nil, errors.New("trace record is missing its newline terminator")
	}
	return bufio.ScanLines(data, atEOF)
}

var requiredRecordFields = []string{"schema", "case_id", "ordinal", "method", "request", "response", "error", "stream_terminal", "evidence_scope", "elapsed_ns"}

func decodeRecord(payload []byte) (Record, error) {
	var record Record
	keys, err := inspectJSON(payload)
	if err != nil {
		return record, err
	}
	if len(keys) != len(requiredRecordFields) {
		return record, errors.New("trace record envelope is incomplete or has unknown fields")
	}
	for _, field := range requiredRecordFields {
		if _, ok := keys[field]; !ok {
			return record, errors.New("trace record field is missing")
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return record, errors.New("invalid trace record JSON")
	}
	if err := record.Validate(); err != nil {
		return record, err
	}
	return record, nil
}
