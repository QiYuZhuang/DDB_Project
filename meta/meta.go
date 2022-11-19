package meta

/******************* partition json *************************/
// test, this meta should be in etcd
type Partitions struct {
	Partitions []Partition `json:"patitions"`
}
type Partition struct {
	TableName  string      `json:"table_name"`
	SiteInfos  []SiteInfo  `json:"site_info"`
	FragType   string      `json:"fragmentation_type"`
	HFragInfos []HFragInfo `json:"horizontal_fragmentation"`
	VFragInfos []VFragInfo `json:"vertical_fragmentation"`
}

type SiteInfo struct {
	Name string `json:"frag_name"`
	IP   string `json:"ip"`
}
type HFragInfo struct {
	FragName   string           `json:"frag_name"`
	Conditions []ConditionRange `json:"range"`
}

type ConditionRange struct {
	ColName          string `json:"col_name"`
	GreaterEqualThan string `json:"get"`
	LessThan         string `json:"lt"`
	Equal            string `json:"eq"`
}

type VFragInfo struct {
	SiteName   string   `json:"frag_name"`
	ColumnName []string `json:"col_names"`
}

/************************************************************/

/******************* tableMeta json *************************/
// test, this meta should be in etcd
type TableMetas struct {
	TableMetas []TableMeta `json:"tables"`
}
type TableMeta struct {
	TableName string   `json:"table_name"`
	Columns   []Column `json:"columns"`
}
type Column struct {
	ColumnName string `json:"col_name"`
	Type       string `json:"type"`
}

type SqlRouter struct {
	Sql     string
	Site_ip string
}

type Peer struct {
	Id   int16
	Ip   string
	Port string
}

type Context struct {
	TableMetas      TableMetas
	TablePartitions Partitions
	Peers           []Peer
	IP              string
	IsDebugLocal    bool
}
