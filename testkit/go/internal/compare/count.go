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
