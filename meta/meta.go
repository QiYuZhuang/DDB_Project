package meta

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"project/utils"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

/******************* partition json *************************/
// test, this meta should be in etcd
type PartitionStrategy int

const (
	// HORIZONTAL
	Horizontal PartitionStrategy = iota
	Vertical
	StrategyNum
	NoStrategy
)

// type I interface {
// 	GetPartAttributes() string
// 	GetPartPredicates() string
// }

type Partitions struct {
	Partitions []Partition `json:"patitions"`
}

func (p Partitions) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.Partitions)
}

type Partition struct {
	TableName  string            `json:"table_name"`
	SiteInfos  []SiteInfo        `json:"site_info"`
	FragType   PartitionStrategy `json:"fragmentation_type"`
	HFragInfos []HFragInfo       `json:"horizontal_fragmentation"`
	VFragInfos []VFragInfo       `json:"vertical_fragmentation"`
}

func (p Partition) MarshalJSON() ([]byte, error) {
	if p.FragType == Horizontal {
		return json.Marshal(map[string]interface{}{
			"TableName":  p.TableName,
			"FragType":   "Horizontal",
			"SiteInfos":  p.SiteInfos,
			"HFragInfos": p.HFragInfos,
		})
	} else {
		return json.Marshal(map[string]interface{}{
			"TableName":  p.TableName,
			"FragType":   "Vertical",
			"SiteInfos":  p.SiteInfos,
			"HFragInfos": p.VFragInfos,
		})
	}
}

type SiteInfo struct {
	SiteName string `json:"site_name"`
	FragName string `json:"frag_name"`
	IP       string `json:"ip"`
	Port     string `json:"port"`
}

func (s SiteInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		// "SiteName": s.SiteName,
		"FragName": s.FragName,
		"IP":       s.IP,
		"Port":     s.Port,
	})
}

type HFragInfo struct {
	FragName   string           `json:"frag_name"`
	Conditions []ConditionRange `json:"range"`
}

func (h HFragInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"Conditions": h.Conditions,
		"FragName":   h.FragName,
	})
}

// func (h HFragInfo) GetPartAttributes() string {

// }

// func (h HFragInfo) GetPartPredicates() string {

// }

type ConditionRange struct {
	ColName          string `json:"col_name"`
	GreaterEqualThan string `json:"get"`
	LessThan         string `json:"lt"`
	Equal            string `json:"eq"`
}

func (c ConditionRange) MarshalJSON() ([]byte, error) {
	var val string
	if len(c.GreaterEqualThan) > 0 {
		val += c.ColName + " >= " + c.GreaterEqualThan + " "
	}
	if len(c.LessThan) > 0 {
		val += c.ColName + " < " + c.LessThan + " "
	}
	if len(c.Equal) > 0 {
		val += c.ColName + " = " + c.Equal + " "
	}
	return json.Marshal(map[string]interface{}{
		"Contition": val,
	})
}

type VFragInfo struct {
	FragName   string   `json:"frag_name"`
	ColumnName []string `json:"col_names"`
}

func (v VFragInfo) MarshalJSON() ([]byte, error) {
	fmt.Println(v)
	return json.Marshal(map[string]interface{}{
		"FragName": v.FragName,
		"Columns":  v.ColumnName,
	})
}

// func (h VFragInfo) GetPartAttributes() string {

// }

// func (h VFragInfo) GetPartPredicates() string {

// }

/************************************************************/

/********************** tableMeta json **********************/
// test, this meta should be in etcd
type TableMetas struct {
	TableMetas []TableMeta `json:"tables"`
}

type TableMeta struct {
	TableName string   `json:"table_name"`
	Columns   []Column `json:"columns"`
	IsTemp    bool     `json:"temp_table"`
}

func (t TableMeta) FindColumnByName(col_name string) (Column, error) {
	var col Column
	for _, c := range t.Columns {
		if strings.EqualFold(c.ColumnName, col_name) {
			return c, nil
		}
	}
	return col, errors.New("cannot find column, name is" + col_name)
}

func (t TableMeta) EqualTableMeta(other TableMeta) bool {
	if len(t.Columns) != len(other.Columns) {
		return false
	}

	for i := 0; i < len(t.Columns); i++ {
		if !t.Columns[i].EqualColumn(other.Columns[i]) {
			return false
		}
	}
	return true
}

func (t *TableMeta) EraseColumnByName(table_name string, column_name string) {
	for i := 0; i < len(t.Columns); i++ {
		if strings.EqualFold(t.Columns[i].ColumnName, table_name+"$"+column_name) {
			t.Columns = append(t.Columns[:i], t.Columns[i+1:]...)
			return
		}
	}
}

func (t *TableMeta) EraseColumnByIdx(idx int) {
	t.Columns = append(t.Columns[:idx], t.Columns[idx+1:]...)
}

type FieldType int

const (
	Int32 FieldType = iota
	Varchar
	FieldTypeNum
)

func FieldType2String(field_type FieldType) (string, error) {
	var str string
	var err error
	err = nil
	switch field_type {
	case Int32:
		str = "int"
	case Varchar:
		str = "varchar(255)"
	default:
		str = ""
		err_msg := "do not support this type, FieldType is " + strconv.Itoa(int(field_type))
		err = errors.New(err_msg)
	}

	return str, err
}

func String2Field(str string) (FieldType, error) {
	var field_type FieldType
	var err error
	err = nil
	if utils.ContainString(str, "int", true) {
		field_type = Int32
	} else if utils.ContainString(str, "char", true) {
		field_type = Varchar
	} else {
		field_type = FieldTypeNum
		err_msg := "do not support this type, FieldType is " + strconv.Itoa(int(field_type))
		err = errors.New(err_msg)
	}

	return field_type, err
}

type Column struct {
	ColumnName string    `json:"col_name"`
	Type       FieldType `json:"type"`
}

func (c Column) EqualColumn(other Column) bool {
	if strings.EqualFold(c.ColumnName, other.ColumnName) && c.Type == other.Type {
		return true
	} else {
		return false
	}
}

type SqlRouter struct {
	File_path string
	Sql       string
	Site_ip   string
	Site_port string
}

type Peer struct {
	Id   int16
	Ip   string
	Port string
}

type Context struct {
	Messages        *chan Message
	TableMetas      TableMetas
	TablePartitions Partitions
	Peers           []Peer
	IP              string
	Port            string
	DB              *sql.DB
	Logger          *logrus.Logger
	IsDebugLocal    bool
}

type TempResult struct {
	Filename   string
	Table_meta TableMeta
}

type TempResultList []TempResult

func (e TempResultList) Len() int {
	return len(e)
}

func (e TempResultList) Less(i, j int) bool {
	return e[i].Table_meta.TableName > e[j].Table_meta.TableName
}

func (e TempResultList) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}
