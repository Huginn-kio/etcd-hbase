package compare

import (
	"errors"
	"strconv"
)

// Only the top-level Put response is registered in J30. Do not suppress term
// fields in arbitrary nested messages or change the original M0 comparison.
func projectM3PutTerm(left, right map[string]any, leftScope string, bothSucceeded bool) error {
	if left["method"] != "etcdserverpb.KV/Put" || right["method"] != "etcdserverpb.KV/Put" {
		return errors.New("method is not registered for the J30 policy")
	}
	if !bothSucceeded {
		return nil // Error observations still compare strictly.
	}
	native, target := left, right
	if leftScope == "target-hbase" {
		native, target = right, left
	}
	nativeResponse, ok := native["response"].(map[string]any)
	if !ok {
		return errors.New("native response is missing")
	}
	targetResponse, ok := target["response"].(map[string]any)
	if !ok {
		return errors.New("target response is missing")
	}
	nativeHeader, err := nestedFields(nativeResponse, "fields", "header", "fields")
	if err != nil {
		return errors.New("native response header is missing")
	}
	targetHeader, err := nestedFields(targetResponse, "fields", "header", "fields")
	if err != nil {
		return errors.New("target response header is missing")
	}
	nativeTerm, ok := nativeHeader["raftTerm"].(string)
	parsed, parseErr := strconv.ParseUint(nativeTerm, 10, 64)
	if !ok || parseErr != nil || parsed == 0 || strconv.FormatUint(parsed, 10) != nativeTerm ||
		targetHeader["raftTerm"] != "0" {
		return errors.New("invalid native or target Raft term")
	}
	targetHeader["raftTerm"] = nativeTerm
	return nil
}
