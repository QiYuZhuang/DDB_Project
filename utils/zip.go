package utils

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

//GZIPEn gzip加密
func GZIPEn(str string) ([]byte, error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write([]byte(str)); err != nil {
		return b.Bytes(), err
	}
	if err := gz.Flush(); err != nil {
		return b.Bytes(), err
	}
	if err := gz.Close(); err != nil {
		return b.Bytes(), err
	}
	return b.Bytes(), nil
}

//GZIPDe gzip解密
func GZIPDe(in []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		var out []byte
		return out, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}
