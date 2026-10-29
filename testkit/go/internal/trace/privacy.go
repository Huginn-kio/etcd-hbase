package trace

import (
	"encoding/json"
	"strconv"
	"strings"
)

func isSensitiveName(name string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(name, "_", ""), "-", ""))
	for _, marker := range []string{"password", "token", "authorization", "credential", "secret", "privatekey", "username"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func containsName(name string) bool {
	normalized := strings.ToLower(name)
	switch normalized {
	case "name", "names", "user", "users", "role", "roles":
		return true
	default:
		return strings.HasSuffix(normalized, "_name") || strings.HasSuffix(normalized, "_names")
	}
}

func findSecretKey(value any, authMethod bool) string {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "_", ""), "-", ""))
			lowerKey := strings.ToLower(key)
			if strings.HasSuffix(lowerKey, "_present") || strings.HasSuffix(key, "Present") {
				if _, ok := child.(bool); !ok {
					return key
				}
			} else if strings.HasSuffix(lowerKey, "_count") || (len(key) > len("count") && strings.HasSuffix(key, "Count")) {
				if number, ok := child.(json.Number); !ok || !isNonNegativeInteger(number) {
					return key
				}
			} else if isSensitiveName(normalized) || (authMethod && containsName(key)) {
				return key
			}
			if nested := findSecretKey(child, authMethod); nested != "" {
				return nested
			}
		}
	case []any:
		for _, child := range typed {
			if nested := findSecretKey(child, authMethod); nested != "" {
				return nested
			}
		}
	}
	return ""
}

func isNonNegativeInteger(number json.Number) bool {
	parsed, err := strconv.ParseUint(number.String(), 10, 64)
	return err == nil && strconv.FormatUint(parsed, 10) == number.String()
}
