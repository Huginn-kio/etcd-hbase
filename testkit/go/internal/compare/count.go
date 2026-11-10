package compare

import (
	"reflect"
	"strconv"
)

type countPolicy string

const (
	strictCount    countPolicy = ""
	pageCount      countPolicy = "page"
	existenceCount countPolicy = "existence"
)

func equalWithCountAllowance(left, right map[string]any, leftScope string, policy countPolicy) bool {
	if !reflect.DeepEqual(withoutResponse(left), withoutResponse(right)) {
		return false
	}
	leftResponse, okLeft := left["response"].(map[string]any)
	rightResponse, okRight := right["response"].(map[string]any)
	if !okLeft || !okRight {
		return false
	}
	leftFields, okLeft := leftResponse["fields"].(map[string]any)
	rightFields, okRight := rightResponse["fields"].(map[string]any)
	if !okLeft || !okRight {
		return false
	}
	nativeFields, targetFields := leftFields, rightFields
	if leftScope == "target-hbase" {
		nativeFields, targetFields = rightFields, leftFields
	}
	if !validTargetCount(nativeFields, targetFields, policy) {
		return false
	}
	return reflect.DeepEqual(responseWithoutCount(leftResponse, leftFields), responseWithoutCount(rightResponse, rightFields))
}

func validTargetCount(nativeFields, targetFields map[string]any, policy countPolicy) bool {
	nativeCount, nativeOK := responseCount(nativeFields)
	targetCount, targetOK := responseCount(targetFields)
	if !nativeOK || !targetOK {
		return false
	}
	switch policy {
	case pageCount:
		kvs, ok := targetFields["kvs"].([]any)
		pageSize := int64(len(kvs))
		return ok && nativeCount >= pageSize && targetCount == pageSize
	case existenceCount:
		if nativeCount == 0 {
			return targetCount == 0
		}
		return targetCount == 1
	default:
		return false
	}
}

func responseCount(fields map[string]any) (int64, bool) {
	value, ok := fields["count"].(string)
	if !ok {
		return 0, false
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	return parsed, err == nil && parsed >= 0 && strconv.FormatInt(parsed, 10) == value
}

func responseWithoutCount(response, fields map[string]any) map[string]any {
	copy := cloneMap(response)
	fieldCopy := cloneMap(fields)
	delete(fieldCopy, "count")
	copy["fields"] = fieldCopy
	return copy
}

func withoutResponse(record map[string]any) map[string]any {
	copy := cloneMap(record)
	delete(copy, "response")
	return copy
}

func cloneMap(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, child := range value {
		result[key] = child
	}
	return result
}

// Case IDs describe coverage. The allowance follows the actual Range request.
func countPolicyForRequest(record map[string]any) countPolicy {
	if record["method"] != "etcdserverpb.KV/Range" {
		return strictCount
	}
	request, ok := record["request"].(map[string]any)
	if !ok {
		return strictCount
	}
	fields, ok := request["fields"].(map[string]any)
	if !ok {
		return strictCount
	}
	return countPolicyForRange(fields)
}

func countPolicyForRange(fields map[string]any) countPolicy {
	if end, ok := fields["rangeEnd"].(string); !ok || end == "" {
		return strictCount
	}
	encodedLimit, ok := fields["limit"].(string)
	if !ok {
		return strictCount
	}
	limit, err := strconv.ParseInt(encodedLimit, 10, 64)
	if err != nil || limit <= 0 || strconv.FormatInt(limit, 10) != encodedLimit {
		return strictCount
	}
	countOnly, ok := fields["countOnly"].(bool)
	if !ok {
		return strictCount
	}
	if !countOnly {
		return pageCount
	}
	if limit == 1 {
		return existenceCount
	}
	return strictCount
}

// Apply a Count allowance only to the selected, matching flat Range operation.
// Nested operations and all other response fields remain strict.
type countPair struct{ left, right map[string]any }

type selectedTxn struct {
	operations, leftOps, rightOps []any
}

func equalFlatTxnWithCountAllowance(left, right map[string]any, leftScope string) (bool, bool) {
	selected, ok := selectFlatTxn(left, right)
	if !ok {
		return false, false
	}
	matches, handled, valid := matchCountAllowances(selected, leftScope)
	if !valid {
		return handled, false
	}
	if len(matches) == 0 {
		return false, false
	}
	for _, match := range matches {
		delete(match.left, "count")
		delete(match.right, "count")
	}
	return true, reflect.DeepEqual(left, right)
}

func selectFlatTxn(left, right map[string]any) (selectedTxn, bool) {
	if left["method"] != "etcdserverpb.KV/Txn" || !reflect.DeepEqual(withoutResponse(left), withoutResponse(right)) {
		return selectedTxn{}, false
	}
	request, ok := left["request"].(map[string]any)
	if !ok {
		return selectedTxn{}, false
	}
	requestFields, ok := request["fields"].(map[string]any)
	if !ok {
		return selectedTxn{}, false
	}
	leftResponse, ok := left["response"].(map[string]any)
	if !ok {
		return selectedTxn{}, false
	}
	rightResponse, ok := right["response"].(map[string]any)
	if !ok {
		return selectedTxn{}, false
	}
	leftFields, leftOK := leftResponse["fields"].(map[string]any)
	rightFields, rightOK := rightResponse["fields"].(map[string]any)
	if !leftOK || !rightOK {
		return selectedTxn{}, false
	}
	succeeded, ok := leftFields["succeeded"].(bool)
	if !ok || rightFields["succeeded"] != succeeded {
		return selectedTxn{}, false
	}
	branch := "failure"
	if succeeded {
		branch = "success"
	}
	operations, ok := requestFields[branch].([]any)
	if !ok {
		return selectedTxn{}, false
	}
	leftOps, leftOK := leftFields["responses"].([]any)
	rightOps, rightOK := rightFields["responses"].([]any)
	if !leftOK || !rightOK || len(operations) != len(leftOps) || len(operations) != len(rightOps) {
		return selectedTxn{}, false
	}
	return selectedTxn{operations, leftOps, rightOps}, true
}

// Validate every pair before modifying either response.
func matchCountAllowances(selected selectedTxn, leftScope string) ([]countPair, bool, bool) {
	var matches []countPair
	for index, raw := range selected.operations {
		op, ok := raw.(map[string]any)
		if !ok {
			return nil, false, false
		}
		fields, ok := op["fields"].(map[string]any)
		if !ok {
			return nil, false, false
		}
		rangeRequest, ok := fields["requestRange"].(map[string]any)
		if !ok {
			continue
		}
		rangeFields, ok := rangeRequest["fields"].(map[string]any)
		if !ok {
			return nil, false, false
		}
		policy := countPolicyForRange(rangeFields)
		if policy == strictCount {
			continue
		}
		if !hasTxnOneof(op, "request", "request_range") {
			return nil, true, false
		}
		leftRange, leftOK := txnRangeResponse(selected.leftOps[index])
		rightRange, rightOK := txnRangeResponse(selected.rightOps[index])
		if !leftOK || !rightOK {
			return nil, true, false
		}
		native, target := leftRange, rightRange
		if leftScope == "target-hbase" {
			native, target = rightRange, leftRange
		}
		if !validTargetCount(native, target, policy) {
			return nil, true, false
		}
		matches = append(matches, countPair{leftRange, rightRange})
	}
	return matches, len(matches) > 0, true
}

func txnRangeResponse(value any) (map[string]any, bool) {
	op, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}
	if !hasTxnOneof(op, "response", "response_range") {
		return nil, false
	}
	fields, ok := op["fields"].(map[string]any)
	if !ok {
		return nil, false
	}
	rangeResponse, ok := fields["responseRange"].(map[string]any)
	if !ok {
		return nil, false
	}
	result, ok := rangeResponse["fields"].(map[string]any)
	return result, ok
}

func hasTxnOneof(value map[string]any, group, expected string) bool {
	oneofs, ok := value["oneofs"].(map[string]any)
	return ok && oneofs[group] == expected
}
