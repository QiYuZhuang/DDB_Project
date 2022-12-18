package meta

import (
	"database/sql"
	"errors"
	"project/utils"
	"strconv"

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

type Partition struct {
	TableName  string            `json:"table_name"`
	SiteInfos  []SiteInfo        `json:"site_info"`
	FragType   PartitionStrategy `json:"fragmentation_type"`
	HFragInfos []HFragInfo       `json:"horizontal_fragmentation"`
	VFragInfos []VFragInfo       `json:"vertical_fragmentation"`
}

type SiteInfo struct {
	SiteName string `json:"site_name"`
	FragName string `json:"frag_name"`
	IP       string `json:"ip"`
	Port     string `json:"port"`
}

type HFragInfo struct {
	FragName   string           `json:"frag_name"`
	Conditions []ConditionRange `json:"range"`
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

type VFragInfo struct {
	FragName   string   `json:"frag_name"`
	ColumnName []string `json:"col_names"`
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

type SqlRouter struct {
	File_path string
	Sql       string
	Site_ip   string
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
