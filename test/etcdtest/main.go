package main

import (
	"fmt"
	"project/etcd"
	"reflect"
	"strconv"
	"strings"
)

func main() {
	etcd.Connect_etcd()
	//listtostring()
}

func listtostring() {
	a := []int{1, 2, 3}
	var b []string
	for _, i := range a {
		b = append(b, strconv.Itoa(i))
	}
	c := strings.Join(b, ",")
	c += ","
	c = c[:len(c)-1]
	fmt.Println(c)
	fmt.Println(len(c))

	cc := strings.Split(c, ",")
	for i := 0; i < len(cc); i++ {
		fmt.Println(reflect.TypeOf(cc[i]))
	}
}
