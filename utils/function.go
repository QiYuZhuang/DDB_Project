package utils

import "strings"

const MaxUint = ^uint(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1)
const MinInt = -MaxInt - 1

func GetMidStr(s string, sep1 string, sep2 string) string {
	//
	c1 := strings.Index(s, sep1) + 1
	c2 := strings.LastIndex(s, sep2)
	s = string([]byte(s)[c1:c2])
	return s
}

//  使用第三变量交换a,b值
func Swap(a *int, b *int) {
	tem := *a
	*a = *b
	*b = tem
}

//  使用第三变量交换a,b值
func SwapStr(a *string, b *string) {
	tem := *a
	*a = *b
	*b = tem
}

func FindStr(src []string, target string) int {
	index := -1
	for i, str_ := range src {
		if target == str_ {
			index = i
			break
		}
	}
	return index
}
