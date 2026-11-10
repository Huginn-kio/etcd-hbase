package compare

import (
	"errors"
	"strconv"
)

// Only registered top-level methods have a term projection. Never suppress
// child headers, arbitrary methods, or change the original M0 comparison.
func projectM3Term(left, right map[string]any, leftScope string, bothSucceeded bool, policy tracePolicy) error {
	method := left["method"]
	if right["method"] != method ||
		(method != "etcdserverpb.KV/Put" && !(policy == m3TxnPolicy && method == "etcdserverpb.KV/Txn")) {
		return errors.New("method is not registered for the M3 policy")
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
