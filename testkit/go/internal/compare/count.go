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

// Apply Count allowances only to matching selected Range positions, including nested branches.
// Validate the entire path before changing either normalized trace.
type countPair struct {
	left, right map[string]any
	policy      countPolicy
}

type selectedTxn struct {
	request, leftResponse, rightResponse map[string]any
}

func equalTxnWithCountAllowance(left, right map[string]any, leftScope string) (bool, bool) {
	selected, ok := selectTxn(left, right)
	if !ok {
		return false, false
	}
	var matches []countPair
	valid, rejected := collectTxnCountMatches(selected.request, selected.leftResponse, selected.rightResponse, 0, &matches)
	if !valid {
		return rejected, false
	}
	if len(matches) == 0 {
		return false, false
	}
	if !validCountMatches(matches, leftScope) {
		return true, false
	}
	for _, match := range matches {
		delete(match.left, "count")
		delete(match.right, "count")
	}
	return true, reflect.DeepEqual(left, right)
}

func selectTxn(left, right map[string]any) (selectedTxn, bool) {
	if left["method"] != "etcdserverpb.KV/Txn" || !reflect.DeepEqual(withoutResponse(left), withoutResponse(right)) {
		return selectedTxn{}, false
	}
	request, requestOK := left["request"].(map[string]any)
	leftResponse, leftOK := left["response"].(map[string]any)
	rightResponse, rightOK := right["response"].(map[string]any)
	if !requestOK || !leftOK || !rightOK {
		return selectedTxn{}, false
	}
	return selectedTxn{request, leftResponse, rightResponse}, true
}

func validCountMatches(matches []countPair, leftScope string) bool {
	for _, match := range matches {
		native, target := match.left, match.right
		if leftScope == "target-hbase" {
			native, target = match.right, match.left
		}
		if !validTargetCount(native, target, match.policy) {
			return false
		}
	}
	return true
}

// The second result identifies malformed selected oneofs that must not fall back to
// strict equality: matching malformed traces are not valid Count evidence.
func collectTxnCountMatches(request, leftResponse, rightResponse map[string]any,
	depth int, matches *[]countPair) (bool, bool) {
	if depth > 16 {
		return false, false
	}
	requestFields, requestOK := request["fields"].(map[string]any)
	leftFields, leftOK := leftResponse["fields"].(map[string]any)
	rightFields, rightOK := rightResponse["fields"].(map[string]any)
	if !requestOK || !leftOK || !rightOK {
		return false, false
	}
	succeeded, ok := leftFields["succeeded"].(bool)
	if !ok || rightFields["succeeded"] != succeeded {
		return false, false
	}
	branch := "failure"
	if succeeded {
		branch = "success"
	}
	operations, ok := requestFields[branch].([]any)
	if !ok {
		return false, false
	}
	leftOps, leftOK := leftFields["responses"].([]any)
	rightOps, rightOK := rightFields["responses"].([]any)
	if !leftOK || !rightOK || len(operations) != len(leftOps) || len(operations) != len(rightOps) {
		return false, false
	}
	for index, raw := range operations {
		op, ok := raw.(map[string]any)
		if !ok {
			return false, false
		}
		fields, ok := op["fields"].(map[string]any)
		if !ok {
			return false, false
		}
		if nested, exists := fields["requestTxn"]; exists {
			child, ok := nested.(map[string]any)
			leftChild, leftOK := txnResponse(leftOps[index])
			rightChild, rightOK := txnResponse(rightOps[index])
			if !ok || !hasTxnOneof(op, "request", "request_txn") || !leftOK || !rightOK {
				return false, true
			}
			if valid, rejected := collectTxnCountMatches(child, leftChild, rightChild, depth+1, matches); !valid {
				return false, rejected
			}
			continue
		}
		rangeRequest, ok := fields["requestRange"].(map[string]any)
		if !ok {
			continue
		}
		rangeFields, ok := rangeRequest["fields"].(map[string]any)
		if !ok {
			return false, false
		}
		if !hasTxnOneof(op, "request", "request_range") {
			return false, true
		}
		leftRange, leftOK := txnRangeResponse(leftOps[index])
		rightRange, rightOK := txnRangeResponse(rightOps[index])
		if !leftOK || !rightOK {
			return false, true
		}
		policy := countPolicyForRange(rangeFields)
		if policy != strictCount {
			*matches = append(*matches, countPair{leftRange, rightRange, policy})
		}
	}
	return true, false
}

func txnResponse(value any) (map[string]any, bool) {
	op, ok := value.(map[string]any)
	if !ok || !hasTxnOneof(op, "response", "response_txn") {
		return nil, false
	}
	fields, ok := op["fields"].(map[string]any)
	if !ok {
		return nil, false
	}
	response, ok := fields["responseTxn"].(map[string]any)
	return response, ok
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
