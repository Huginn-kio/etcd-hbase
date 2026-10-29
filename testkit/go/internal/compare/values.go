package compare

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func normalizeValue(value any, method string, baseRevision int64, ids map[string]map[string]string, sequences map[string]uint64) (any, error) {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		result := make(map[string]any, len(typed))
		for _, key := range keys {
			child := typed[key]
			if isRevisionField(key) {
				revision, err := parseRevision(child)
				if err != nil {
					return nil, fmt.Errorf("invalid revision field: %w", err)
				}
				if revision > 0 {
					revision -= baseRevision
					if revision < 0 {
						return nil, fmt.Errorf("revision field %q precedes scenario baseline", key)
					}
				}
				if child != "0" && revision >= 0 {
					result[key] = "relative:" + strconv.FormatInt(revision, 10)
				} else {
					result[key] = strconv.FormatInt(revision, 10)
				}
				continue
			}
			if domain := identityDomain(method, key); domain != "" {
				id, err := parseID(child, domain)
				if err != nil {
					return nil, fmt.Errorf("invalid identity field: %w", err)
				}
				if id == "0" {
					result[key] = id
					continue
				}
				canonical, ok := ids[domain][id]
				if !ok {
					sequences[domain]++
					canonical = fmt.Sprintf("%s:%d", domain, sequences[domain])
					ids[domain][id] = canonical
				}
				result[key] = canonical
				continue
			}
			normalized, err := normalizeValue(child, method, baseRevision, ids, sequences)
			if err != nil {
				return nil, err
			}
			result[key] = normalized
		}
		return result, nil
	case []any:
		result := make([]any, len(typed))
		for index, child := range typed {
			normalized, err := normalizeValue(child, method, baseRevision, ids, sequences)
			if err != nil {
				return nil, err
			}
			result[index] = normalized
		}
		return result, nil
	default:
		return value, nil
	}
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
