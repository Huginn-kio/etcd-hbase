package trace

import "errors"

// encoding/json replaces isolated UTF-16 surrogates with U+FFFD. Reject them
// before decoding so damaged inputs cannot collapse into the same observation.
func validateUnicodeEscapes(raw []byte) error {
	for index := 0; index < len(raw); {
		if raw[index] != '\\' {
			index++
			continue
		}
		if index+1 >= len(raw) {
			return errors.New("incomplete JSON escape")
		}
		if raw[index+1] != 'u' {
			index += 2
			continue
		}
		unit, ok := hexUnit(raw[index+2:])
		if !ok {
			return errors.New("invalid JSON Unicode escape")
		}
		index += 6
		switch {
		case unit >= 0xd800 && unit <= 0xdbff:
			if index+6 > len(raw) || raw[index] != '\\' || raw[index+1] != 'u' {
				return errors.New("unpaired JSON Unicode surrogate")
			}
			low, ok := hexUnit(raw[index+2:])
			if !ok || low < 0xdc00 || low > 0xdfff {
				return errors.New("invalid JSON Unicode surrogate pair")
			}
			index += 6
		case unit >= 0xdc00 && unit <= 0xdfff:
			return errors.New("unpaired JSON Unicode surrogate")
		}
	}
	return nil
}

func hexUnit(raw []byte) (uint16, bool) {
	if len(raw) < 4 {
		return 0, false
	}
	var unit uint16
	for _, character := range raw[:4] {
		unit *= 16
		switch {
		case character >= '0' && character <= '9':
			unit += uint16(character - '0')
		case character >= 'a' && character <= 'f':
			unit += uint16(character - 'a' + 10)
		case character >= 'A' && character <= 'F':
			unit += uint16(character - 'A' + 10)
		default:
			return 0, false
		}
	}
	return unit, true
}
