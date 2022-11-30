package utils

import "strings"

func ContainString(str1 string, str2 string, ignore bool) bool {
	if ignore {
		return strings.Contains(strings.ToUpper(str1), strings.ToUpper(str2))
	} else {
		return strings.Contains(str1, str2)
	}
}
