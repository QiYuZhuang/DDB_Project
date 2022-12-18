package main

import (
	"log"
	"project/utils"
)

func main() {
	str := "Hello 蓝影闪电"
	strGZIPEn, _ := utils.GZIPEn(str)
	//log.Println(unsafe.Sizeof(strGZIPEn))
	log.Println(strGZIPEn) //加密
	strGZIPDe, _ := utils.GZIPDe(strGZIPEn)
	//log.Println(unsafe.Sizeof(str))
	log.Println(string(strGZIPDe)) //解密
}
