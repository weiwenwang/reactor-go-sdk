package reactor

import (
	"fmt"
	"os"
	"regexp"
	"time"
)

const (
	DATE_FORMAT = "2006-01-02 15:04:05.000"
	KEY_PATTERN = "^[a-zA-Z#][A-Za-z0-9_]{0,49}$"
	VALUE_MAX   = 2048
)

var keyPattern, _ = regexp.Compile(KEY_PATTERN)

func mergeProperties(target, source map[string]interface{}) {
	for k, v := range source {
		target[k] = v
	}
}

func extractTime(p map[string]interface{}) string {
	if t, ok := p["#time"]; ok {
		delete(p, "#time")

		v, ok := t.(time.Time)
		if !ok {
			fmt.Fprintln(os.Stderr, "Invalid data type for #time")
			return time.Now().Format(DATE_FORMAT)
		}
		return v.Format(DATE_FORMAT)
	}

	return time.Now().Format(DATE_FORMAT)
}

func extractUUID(p map[string]interface{}) string {
	if t, ok := p["#uuid"]; ok {
		delete(p, "#uuid")
		v, ok := t.(string)
		if !ok {
			fmt.Fprintln(os.Stderr, "Invalid data type for #uuid")
		}
		return v
	}
	return ""
}

func extractIp(p map[string]interface{}) string {
	if t, ok := p["#ip"]; ok {
		delete(p, "#ip")

		v, ok := t.(string)
		if !ok {
			fmt.Fprintln(os.Stderr, "Invalid data type for #ip")
			return ""
		}
		return v
	}

	return ""
}

func isNotNumber(v interface{}) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
	case float32, float64:
	default:
		return true
	}
	return false
}


func checkPattern(name []byte) bool {
	return keyPattern.Match(name)
}
