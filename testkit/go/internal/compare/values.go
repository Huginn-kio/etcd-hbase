package compare

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func (normalizer *traceNormalizer) value(value any, method string) (any, error) {
	switch typed := value.(type) {
	case map[string]any:
		return normalizer.object(typed, method)
	case []any:
		return normalizer.array(typed, method)
	default:
		return value, nil
	}
}

func (normalizer *traceNormalizer) object(fields map[string]any, method string) (map[string]any, error) {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make(map[string]any, len(fields))
	for _, key := range keys {
		converted, err := normalizer.field(key, fields[key], method)
		if err != nil {
			return nil, err
		}
		result[key] = converted
	}
	return result, nil
}

func (normalizer *traceNormalizer) field(key string, value any, method string) (any, error) {
	if isRevisionField(key) {
		return normalizer.revision(value)
	}
	if domain := identityDomain(method, key); domain != "" {
		return normalizer.identity(value, domain)
	}
	return normalizer.value(value, method)
}

func (normalizer *traceNormalizer) revision(value any) (string, error) {
	revision, err := parseRevision(value)
	if err != nil {
		return "", fmt.Errorf("invalid revision field: %w", err)
	}
	if revision > 0 {
		return "relative:" + strconv.FormatInt(revision-normalizer.baseRevision, 10), nil
	}
	return strconv.FormatInt(revision, 10), nil
}

func (normalizer *traceNormalizer) identity(value any, domain string) (string, error) {
	id, err := parseID(value, domain)
	if err != nil {
		return "", fmt.Errorf("invalid identity field: %w", err)
	}
	if id == "0" {
		return id, nil
	}
	if canonical, ok := normalizer.identities[domain][id]; ok {
		return canonical, nil
	}
	normalizer.identitySequences[domain]++
	canonical := fmt.Sprintf("%s:%d", domain, normalizer.identitySequences[domain])
	normalizer.identities[domain][id] = canonical
	return canonical, nil
}

func (normalizer *traceNormalizer) array(values []any, method string) ([]any, error) {
	result := make([]any, len(values))
	for index, child := range values {
		converted, err := normalizer.value(child, method)
		if err != nil {
			return nil, err
		}
		result[index] = converted
	}
	return result, nil
}

func isRevisionField(key string) bool {
	switch strings.ToLower(strings.ReplaceAll(key, "_", "")) {
	case "revision", "createrevision", "modrevision", "compactrevision", "startrevision":
		return true
	default:
		return false
	}
}

func parseRevision(value any) (int64, error) {
	text, ok := value.(string)
	if !ok {
		return 0, errors.New("revision is not encoded as a decimal string")
	}
	parsed, err := strconv.ParseInt(text, 10, 64)
	if err != nil || strconv.FormatInt(parsed, 10) != text {
		return 0, errors.New("revision is not a canonical int64")
	}
	return parsed, nil
}

func identityDomain(method, key string) string {
	switch strings.ToLower(key) {
	case "clusterid":
		return "cluster"
	case "memberid", "leader":
		return "member"
	case "lease":
		return "lease"
	case "id":
		if strings.HasPrefix(method, "etcdserverpb.Lease/") {
			return "lease"
		}
	}
	return ""
}

func parseID(value any, domain string) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", errors.New("ID is not encoded as a decimal string")
	}
	if domain == "lease" {
		parsed, err := strconv.ParseInt(text, 10, 64)
		if err != nil || strconv.FormatInt(parsed, 10) != text {
			return "", errors.New("lease ID is not a canonical int64")
		}
		return text, nil
	}
	parsed, err := strconv.ParseUint(text, 10, 64)
	if err != nil || strconv.FormatUint(parsed, 10) != text {
		return "", errors.New("ID is not a canonical uint64")
	}
	return text, nil
}
