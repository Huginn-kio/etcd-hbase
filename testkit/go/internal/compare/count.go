package compare

import (
	"reflect"
	"strconv"
)

func equalWithCountAllowance(left, right map[string]any, leftScope, rightScope string) bool {
	if leftScope == rightScope {
		return false
	}
	if !reflect.DeepEqual(withoutResponse(left), withoutResponse(right)) {
		return false
	}
	if !existenceCountRequest(left) {
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
	leftCount, ok := leftFields["count"].(string)
	if !ok || !canonicalCount(leftCount) {
		return false
	}
	rightCount, ok := rightFields["count"].(string)
	if !ok || !canonicalCount(rightCount) {
		return false
	}
	nativeCount, targetCount := leftCount, rightCount
	if leftScope == "target-hbase" {
		nativeCount, targetCount = rightCount, leftCount
	}
	want := "1"
	if nativeCount == "0" {
		want = "0"
	}
	if targetCount != want {
		return false
	}
	leftCopy := cloneMap(leftResponse)
	rightCopy := cloneMap(rightResponse)
	leftFieldCopy := cloneMap(leftFields)
	rightFieldCopy := cloneMap(rightFields)
	delete(leftFieldCopy, "count")
	delete(rightFieldCopy, "count")
	leftCopy["fields"] = leftFieldCopy
	rightCopy["fields"] = rightFieldCopy
	return reflect.DeepEqual(leftCopy, rightCopy)
}

func canonicalCount(value string) bool {
	parsed, err := strconv.ParseInt(value, 10, 64)
	return err == nil && parsed >= 0 && strconv.FormatInt(parsed, 10) == value
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

func existenceCountRequest(left map[string]any) bool {
	caseID, _ := left["case_id"].(string)
	method, _ := left["method"].(string)
	if caseID != "C05" || method != "etcdserverpb.KV/Range" {
		return false
	}
	request, ok := left["request"].(map[string]any)
	if !ok {
		return false
	}
	requestFields, ok := request["fields"].(map[string]any)
	if !ok || requestFields["countOnly"] != true || requestFields["limit"] != "1" {
		return false
	}
	if end, ok := requestFields["rangeEnd"].(string); !ok || end == "" {
		return false
	}
	return true
}
