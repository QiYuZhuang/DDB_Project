package meta

import "strconv"

type Publish struct {
	Id     int32
	Name   string
	Nation string
}

func (p *Publish) ToString() string {
	var s string = ""
	s = s + strconv.Itoa(int(p.Id)) + "|" + p.Name + "|" + p.Nation + "\n"
	return s
}
