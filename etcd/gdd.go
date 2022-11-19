package etcd

import (
	"context"
	"errors"
	"fmt"
	"project/meta"
	"strconv"
	"strings"
	"time"
	"unsafe"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var Cli *clientv3.Client

const (
	kvRequesttimeout   = time.Second * 10
	kvslowRequeusttime = time.Second * 1
	dialtimeout        = time.Second * 15
)

func Connect_etcd() {
	config := clientv3.Config{
		Endpoints:   []string{"10.77.110.145:2279", "10.77.110.146:2279", "10.77.110.148:2279"},
		DialTimeout: dialtimeout,
	}
	var err error
	if Cli, err = clientv3.New(config); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("connect etcd succussfully!")

	//defer Cli.Close()

	//a test for put and get
	/*
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = Cli.Put(ctx, "name", "cwh")
		if err != nil {
			fmt.Printf("put to etcd failed,err:%#v", err)
			fmt.Println()
		} else {
			fmt.Println("put data successfully")
		}
		cancel()

		// get 取数据
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		resp, err := Cli.Get(ctx, "name")
		cancel()
		if err != nil {
			fmt.Printf("get from etcd failed, err:%v\n", err)
			return
		}
		for _, ev := range resp.Kvs {
			fmt.Printf("%s:%s\n", ev.Key, ev.Value)
		}

		// del 取数据
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		delResponse, err := Cli.Delete(ctx, "name")
		cancel()
		if err != nil {
			fmt.Printf("del from etcd failed, err:%v\n", err)
			return
		}
		fmt.Println("delete count: ", delResponse.Deleted)
	*/
}

func PutKey(key string, value string) {
	var err error
	var resp *clientv3.PutResponse
	resp, err = Cli.Put(context.Background(), key, value)
	if err != nil {
		fmt.Printf("failed to put key:%v\n", err)
		return
	}
	fmt.Printf("putkey successfully:%v\n", resp)
}

func GetKey(key string) ([]byte, error) {
	var err error
	var res *clientv3.GetResponse
	res, err = Cli.Get(context.Background(), key)
	if err != nil {
		fmt.Printf("get key failed :%v\n", err)
		return nil, err
	}
	n := len(res.Kvs)
	if n == 0 {
		return nil, nil
	}
	fmt.Printf("key %v value is:=%+v\n", key, res)
	return res.Kvs[0].Value, nil
}

func DeleteVal(key string) error {
	var err error
	var res *clientv3.DeleteResponse
	res, err = Cli.Delete(context.Background(), key)
	if err != nil {
		fmt.Printf("delete key:%v failed:%v\n", key, err)
		return err
	}
	fmt.Printf("delete successfully:%+v\n", res)
	return nil
}

func DeletevalwithPrefix(key string) error {
	ctx, _ := context.WithTimeout(context.Background(), kvRequesttimeout)
	_, err := Cli.Delete(ctx, key, clientv3.WithPrefix())
	if err != nil {
		fmt.Println("delete key with prefix error")
		return err
	}
	return nil
}

func GetTables() ([]string, error) {
	tablestr, err := GetKey("/tables")
	if err != nil {
		fmt.Println("get table failed")
		return nil, err
	}
	temp := strings.Split(bytetoString(tablestr), ",")
	return temp, nil
}

func SaveTabletoEtcd(table meta.TableMeta) error {
	exist_tables, err := GetTables()
	if err != nil {
		fmt.Println("save table meta to etcd failed")
		return err
	}
	for i := 0; i < len(exist_tables); i++ {
		if exist_tables[i] == table.TableName {
			err = errors.New("table exist! save table meta to etcd failed")
			return err
		}
	}

	k1 := "/tables"
	v1 := strings.Join(exist_tables, ",")
	if len(v1) == 0 {
		v1 += table.TableName
	} else {
		v1 += "," + table.TableName
	}

	//存放tables信息-->table
	PutKey(k1, v1)

	k2 := "/tables/" + table.TableName
	v2 := ""

	for i := 0; i < len(table.Columns); i++ {
		v2 += table.Columns[i].ColumnName
		v2 += ","
	}
	v2 = v2[:len(v2)-1]
	//存放tables/table信息--table/columns
	PutKey(k2, v2)

	for i := 0; i < len(table.Columns); i++ {
		col := table.Columns[i]

		k3 := k2 + "/" + col.ColumnName
		v3 := col.Type

		//通过/tables/table/columnname得到type
		PutKey(k3, v3)
	}
	return nil
}

func SaveFragmenttoEtcd(partition meta.Partition) error {
	mode := partition.FragType
	switch mode {
	case "HORIZONTAL":
		k1 := "/patitions/" + partition.TableName
		v1 := "HORIZONTAL"

		//写入基本分片信息 name:fragmenttype
		PutKey(k1, v1)

		k2 := k1 + "/HORIZONTAL"
		v2 := ""
		for i := 0; i < len(partition.HFragInfos); i++ {
			v2 += partition.HFragInfos[i].FragName
			v2 += ","
		}
		v2 = v2[:len(v2)-1]
		//写入分片/表名/HORIZONTAL:中所有FragName ，用于访问col_name,以及siteinfo中的ip
		PutKey(k2, v2)

		for i := 0; i < len(partition.HFragInfos); i++ {
			k3 := k2 + "/" + partition.HFragInfos[i].FragName
			v3 := ""

			for j := 0; j < len(partition.HFragInfos[i].Conditions); j++ {
				v3 += partition.HFragInfos[i].Conditions[j].ColName
				v3 += ","
			}
			v3 = v3[:len(v3)-1]
			//写入分片/表名/HORIZONTAL/fragname中所有的 col_name，用于访问 condition
			PutKey(k3, v3)

			for j := 0; j < len(partition.HFragInfos[i].Conditions); j++ {
				k4 := k3 + "/" + partition.HFragInfos[i].Conditions[j].ColName
				k41 := k4 + "/get"
				k42 := k4 + "/lt"
				k43 := k4 + "/eq"

				v41 := partition.HFragInfos[i].Conditions[j].GreaterEqualThan
				v42 := partition.HFragInfos[i].Conditions[j].LessThan
				v43 := partition.HFragInfos[i].Conditions[j].Equal

				//写入condition的基本信息
				PutKey(k41, v41)
				PutKey(k42, v42)
				PutKey(k43, v43)
			}
		}

		for i := 0; i < len(partition.SiteInfos); i++ {
			k5 := k1 + "/" + partition.SiteInfos[i].Name + "/ip"
			v5 := partition.SiteInfos[i].IP

			PutKey(k5, v5)
		}

	case "VERTICAL":
		k1 := "/patitions/" + partition.TableName
		v1 := "VERTICAL"

		//写入基本分片信息 name:fragmenttype
		PutKey(k1, v1)

		k2 := k1 + "VERTICAL"
		v2 := ""
		for i := 0; i < len(partition.VFragInfos); i++ {
			v2 += partition.VFragInfos[i].SiteName
			v2 += ","
		}
		v2 = v2[:len(v2)-1]
		//写入分片/表名/VERTICAL：所有sitename
		PutKey(k2, v2)

		for i := 0; i < len(partition.VFragInfos); i++ {
			k3 := k2 + "/" + partition.VFragInfos[i].SiteName
			k4 := k1 + "/" + partition.SiteInfos[i].Name + "/ip"

			v3 := ""
			for j := 0; j < len(partition.VFragInfos[i].ColumnName); j++ {
				v3 += partition.VFragInfos[i].ColumnName[j]
				v3 += ","
			}
			v3 = v3[:len(v3)-1]
			v4 := partition.SiteInfos[i].IP

			PutKey(k3, v3)
			PutKey(k4, v4)
		}
	default:
		err := errors.New("wrong fragment type!")
		return err
	}
	return nil
}

func SavePeertoEtcd(peer meta.Peer) error {
	key := "/peer/" + strconv.Itoa(int(peer.Id))
	key1 := key + "/" + "ip"
	key2 := key + "/" + "port"

	val1 := peer.Ip
	val2 := peer.Port

	PutKey(key1, val1)
	PutKey(key2, val2)

	return nil
}

func GetFragmentType(tablename string) (string, error) {
	key := "/patitions/" + tablename
	mode, err := GetKey(key)
	if err != nil {
		fmt.Println("get fragment type error")
		return "", nil
	}
	return bytetoString(mode), err
}

func getTableColumns(tablename string) ([]meta.Column, error) {
	var res []meta.Column
	key := "/tables/" + tablename
	columns, err := GetKey(key)

	if err != nil {
		fmt.Println("get table columns failed")
		return nil, err
	}

	temp := strings.Split(bytetoString(columns), ",")
	for i := 0; i < len(temp); i++ {
		columnname := temp[i]
		colunmtype, err := getTableColumnType(tablename, columnname)
		if err != nil {
			fmt.Println("get table columns failed")
			return nil, err
		}
		c := meta.Column{
			ColumnName: columnname,
			Type:       colunmtype,
		}
		res = append(res, c)
	}
	return res, nil
}

func getTableColumnType(tablename string, columnname string) (string, error) {
	key := "/tables/" + tablename + "/" + columnname
	columntype, err := GetKey(key)
	if err != nil {
		fmt.Println("get table column type failed")
		return "", err
	}
	return bytetoString(columntype), err
}

func getTableMetas() (meta.TableMetas, error) {
	tablenames, err := GetTables()
	var res meta.TableMetas
	if err != nil {
		fmt.Println("get table metas failed")
		return res, err
	}
	for i := 0; i < len(tablenames); i++ {
		tablename := tablenames[i]

		tablemeta, err := getTableMeta(tablename)
		if err != nil {
			fmt.Println("get table metas failed")
			return res, err
		}
		res.TableMetas = append(res.TableMetas, tablemeta)
	}
	return res, nil
}

func getTableMeta(tablename string) (meta.TableMeta, error) {
	columns, err := getTableColumns(tablename)
	if err != nil {
		fmt.Println("get table meta failed")
		var res meta.TableMeta
		return res, err
	}
	res := meta.TableMeta{
		TableName: tablename,
		Columns:   columns,
	}
	return res, nil
}

func GetFragmentSite(tablename string) ([]meta.SiteInfo, error) {
	frag_type, err := GetFragmentType(tablename)
	if err != nil {
		fmt.Println("get fragment site error")
		var s []meta.SiteInfo
		return s, err
	}

	key := "/patitions/" + tablename + "/" + frag_type
	sitenamesbyte, err := GetKey(key)
	if err != nil {
		fmt.Println("get fragment site error")
		var s []meta.SiteInfo
		return s, err
	}
	sitenames := bytetoString(sitenamesbyte)
	sitenameslist := strings.Split(sitenames, ",")

	var res []meta.SiteInfo
	for i := 0; i < len(sitenameslist); i++ {
		sitename := sitenameslist[i]
		key1 := "/patitions/" + tablename + "/" + sitename + "/ip"
		ipget, err := GetKey(key1)
		if err != nil {
			fmt.Println("get fragment site error")
			var s []meta.SiteInfo
			return s, err
		}

		siteinfo := meta.SiteInfo{
			Name: sitename,
			IP:   bytetoString(ipget),
		}
		res = append(res, siteinfo)
	}

	return res, nil
}

func GetHFragInfos(tablename string) ([]meta.HFragInfo, error) {
	key := "/patitions/" + tablename + "/HORIZONTAL"
	fragnames, err := GetKey(key)

	var res []meta.HFragInfo
	if err != nil {
		fmt.Println("get HORIZONTAL fragment infos errer")
		return res, err
	}

	fragnameslist := strings.Split(bytetoString(fragnames), ",")

	for i := 0; i < len(fragnameslist); i++ {
		frag_name := fragnameslist[i]
		Hfraginfo, err := GetHFragInfo(tablename, frag_name)

		if err != nil {
			fmt.Println("get HORIZONTAL fragment infos errer")
			return res, err
		}

		res = append(res, Hfraginfo)
	}
	return res, nil
}

func GetHFragInfo(tablename string, frag_name string) (meta.HFragInfo, error) {
	key := "/patitions/" + tablename + "/HORIZONTAL/" + frag_name
	var res meta.HFragInfo
	ConditionColNames, err := GetKey(key)
	if err != nil {
		fmt.Println("get HORIZONTAL fragment info errer")
		return res, err
	}

	var conditions []meta.ConditionRange
	ConditionColNamesList := strings.Split(bytetoString(ConditionColNames), ",")
	for i := 0; i < len(ConditionColNamesList); i++ {
		col_name := ConditionColNamesList[i]

		condition, err := GetCounditionRange(tablename, frag_name, col_name)

		if err != nil {
			fmt.Println("get HORIZONTAL fragment info errer")
			return res, err
		}
		conditions = append(conditions, condition)
	}

	hfraginfo := meta.HFragInfo{
		FragName:   frag_name,
		Conditions: conditions,
	}
	return hfraginfo, nil
}

func GetCounditionRange(tablename string, frag_name string, col_name string) (meta.ConditionRange, error) {
	key := "/patitions/" + tablename + "/HORIZONTAL/" + frag_name + "/" + col_name
	key1 := key + "/get"
	key2 := key + "/lt"
	key3 := key + "/eq"
	var res meta.ConditionRange
	get, err1 := GetKey(key1)
	if err1 != nil {
		fmt.Println("get condition range errer")
		return res, err1
	}
	lt, err2 := GetKey(key2)
	if err2 != nil {
		fmt.Println("get condition range errer")
		return res, err2
	}
	eq, err3 := GetKey(key3)
	if err3 != nil {
		fmt.Println("get condition range errer")
		return res, err3
	}
	conditionrange := meta.ConditionRange{
		ColName:          col_name,
		GreaterEqualThan: bytetoString(get),
		LessThan:         bytetoString(lt),
		Equal:            bytetoString(eq),
	}
	return conditionrange, nil
}

func GetVFragInfos(tablename string) ([]meta.VFragInfo, error) {
	key := "/patitions/" + tablename + "/VERTICAL"
	sitenames, err := GetKey(key)

	var res []meta.VFragInfo

	if err != nil {
		fmt.Println("get VERTICAL fragment infos error")
		return res, err
	}

	sitenameslist := strings.Split(string(sitenames), ",")
	for i := 0; i < len(sitenameslist); i++ {
		sitename := sitenameslist[i]

		vfragment, err := GetVFragInfo(tablename, sitename)
		if err != nil {
			fmt.Println("get VERTICAL fragment infos error")
			return res, err
		}
		res = append(res, vfragment)
	}
	return res, nil
}

func GetVFragInfo(tablename string, sitename string) (meta.VFragInfo, error) {
	key := "/patitions/" + tablename + "/VERTICAL/" + sitename
	columns, err := GetKey(key)

	var res meta.VFragInfo

	if err != nil {
		fmt.Println("get VERTICAL fragment info error")
		return res, err
	}
	columnlist := strings.Split(bytetoString(columns), ",")
	vfrag := meta.VFragInfo{
		SiteName:   sitename,
		ColumnName: columnlist,
	}
	return vfrag, nil
}

func GetPartitions(tablenames []string) (meta.Partitions, error) {
	var res meta.Partitions
	for i := 0; i < len(tablenames); i++ {
		tablename := tablenames[i]

		partition, err := GetPartition(tablename)
		if err != nil {
			fmt.Println("get partitions error")
			return res, err
		}

		res.Partitions = append(res.Partitions, partition)
	}
	return res, nil
}

func GetPartition(tablename string) (meta.Partition, error) {
	var res meta.Partition
	siteinfos, err := GetFragmentSite(tablename)
	if err != nil {
		fmt.Println("get partition error")
		return res, err
	}
	fragmentype, err := GetFragmentType(tablename)
	if err != nil {
		fmt.Println("get partition error")
		return res, err
	}
	if fragmentype == "HORIZONTAL" {
		hfraginfo, err := GetHFragInfos(tablename)
		if err != nil {
			fmt.Println("get partition error")
			return res, err
		}
		var vfraginfo []meta.VFragInfo

		partition := meta.Partition{
			TableName:  tablename,
			SiteInfos:  siteinfos,
			FragType:   fragmentype,
			HFragInfos: hfraginfo,
			VFragInfos: vfraginfo,
		}

		return partition, nil

	} else {
		vfraginfo, err := GetVFragInfos(tablename)
		if err != nil {
			fmt.Println("get partition error")
			return res, err
		}
		var hfraginfo []meta.HFragInfo

		partition := meta.Partition{
			TableName:  tablename,
			SiteInfos:  siteinfos,
			FragType:   fragmentype,
			HFragInfos: hfraginfo,
			VFragInfos: vfraginfo,
		}

		return partition, nil
	}
}

func DropTablefromEtcd(tablename string) error {
	exist_tables, err := GetTables()
	if err != nil {
		fmt.Println("drop table from etcd failed")
		return err
	}
	k1 := "/tables"
	v1 := ""
	for i := 0; i < len(exist_tables); i++ {
		if exist_tables[i] != tablename {
			v1 += exist_tables[i]
			v1 += ","
		}
	}
	v1 = v1[:len(v1)-1]
	//存放tables信息-->table  把k1:v1的中tablename的信息删除
	PutKey(k1, v1)

	k2 := "/tables/" + tablename
	err = DeletevalwithPrefix(k2)
	if err != nil {
		fmt.Println("drop table meta from etcd error")
		return err
	}
	return nil
}

func bytetoString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func RefreshContext(ctx *meta.Context) error {
	var err error

	table_names, err := GetTables()
	if err != nil {
		return err
	}
	ctx.TableMetas, err = getTableMetas()
	if err != nil {
		return err
	}
	ctx.TablePartitions, err = GetPartitions(table_names)
	if err != nil {
		return err
	}
	return err
}
