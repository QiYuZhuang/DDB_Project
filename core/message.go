package core

import (
	"errors"
	"os/user"
	"path/filepath"
	"project/meta"
	"project/mysql"
	"project/utils"
	"strconv"
	"strings"
)

func MessageHandler(m meta.Message, c *Coordinator) error {
	l := c.Context.Logger
	l.Infoln("src: " + m.Src + " dst: " + m.Dst)

	var err error
	err = nil
	switch m.Type {
	case meta.QueryRequest:
		err = QueryRequestHandler(m, c)
	case meta.QueryResponse:
		err = QueryResponseHandler(m, c)
	case meta.HeartBeat:
		err = HeartBeatHandler(m, c)
	case meta.HeartBeatAck:
		err = HeartBeatAckHandler(m, c)
	case meta.DataLoadRequest:
		err = DataLoadRequestHandler(m, c)
	case meta.DataLoadResponse:
		err = DataLoadResponseHandler(m, c)
	default:
		l.Errorln("Unsupport message type, message", m)
	}
	return err
}

func chownScpRemove(username string, src_path string, dst_path string, dst_ip string) error {
	if err := utils.Chown(username, src_path, false); err != nil {
		return err
	}

	if err := utils.ScpFile(username, dst_path, src_path, dst_ip, false); err != nil {
		return err
	}

	if err := utils.RmFile(src_path, false); err != nil {
		return err
	}

	return nil
}

func QueryRequestHandler(m meta.Message, c *Coordinator) error {
	l := c.Context.Logger
	l.Infoln("query is ", m.Query)
	db := c.Context.DB
	err := db.Ping()
	if err != nil {
		l.Errorln("database ping failed")
	}

	u, _ := user.Current()

	resp := meta.NewMessage(meta.QueryResponse, m.Dst, m.Src, m.TxnId)
	resp.SetQueryId(m.QueryId)
	if utils.ContainString(m.Query, "SELECT", true) {
		rows, err := db.Query(m.Query)
		if err != nil {
			l.Errorln("exec failed", err)
		}

		row_cnt := mysql.ParseRows(rows)
		// resp.SetQueryResult(result)
		resp.SetRowCnt(row_cnt)

		// select into tmp file
		tmp_filename := "INTER_TMP_" + strconv.FormatInt(int64(m.TxnId), 10) + "_" + strconv.FormatInt(int64(m.QueryId), 10) + ".csv"
		tmp_path := "/tmp/data/" + tmp_filename
		tmp_sql := utils.GenerateSelectIntoFileSql(strings.Trim(m.Query, ";"), tmp_path, "|", "")
		_, err = db.Exec(tmp_sql)
		if err != nil {
			l.Errorln("select into file failed. err: ", err.Error())
		}

		dst_path := "/home/" + u.Username
		err = chownScpRemove(u.Username, tmp_path, dst_path, m.Src)
		if err != nil {
			l.Errorln("chown - scp - rm failed, error is", err.Error())
		}

		resp.SetFilepath(dst_path)
		resp.SetFilename(tmp_filename)
	} else {
		res, err := db.Exec(m.Query)
		if err != nil {
			l.Errorln("exec failed", err)
		}
		resp.SetResult(res)
	}

	if err != nil {
		resp.SetError(true)
		c.Messages <- *resp
		// AsyncFlushMessage(resp)
		return errors.New("query request handler failed")
	}
	// resp.SetResult(res)
	resp.SetError(false)
	c.Messages <- *resp
	return nil
}

func QueryResponseHandler(m meta.Message, c *Coordinator) error {
	// move result to specific variable, according to txn_id and query_id
	// trigger a check (if remote queries in the executor are already, join the results and go to next)
	l := c.Context.Logger
	txn, ok := c.ActiveTransactions[uint64(m.TxnId)]
	if ok {
		query_id := m.QueryId
		if int(query_id) >= len(txn.Participants) {
			l.Errorln("not a vaild sub query result")
			return errors.New("invaild arguments")
		}

		txn.Responses[query_id] = true
		txn.EffectRows[query_id] = m.RowCnt
		if len(m.Filepath) != 0 || len(m.Filename) != 0 {
			txn.TmpResultInFile[query_id] = m.Filename
		}

		return nil
	} else {
		l.Errorln("can not find active transcation, id is ", txn.TxnId)
	}
	return errors.New("query response handler failed")
}

func HeartBeatHandler(m meta.Message, c *Coordinator) error {
	// no-use
	return nil
}

func HeartBeatAckHandler(m meta.Message, c *Coordinator) error {
	// no-use
	return nil
}

func DataLoadRequestHandler(m meta.Message, c *Coordinator) error {
	// no-use
	l := c.Context.Logger
	l.Infoln("data load query is ", m.Query)
	db := c.Context.DB
	err := db.Ping()
	if err != nil {
		l.Errorln("database ping failed")
		// TODO: try to reconnect
	}

	u, _ := user.Current()

	resp := meta.NewMessage(meta.QueryResponse, m.Dst, m.Src, m.TxnId)
	resp.SetQueryId(m.QueryId)

	_, filename := filepath.Split(m.Filepath)
	src_path := "/home/" + u.Username + "/" + filename
	if err = utils.MvFile(src_path, m.Filepath, false); err != nil {
		l.Errorln("mv file failed, error is ", err.Error())
		return dataLoadRequestErrorHandler(err, resp, c)
	}
	if err = utils.Chown("mysql", m.Filepath, false); err != nil {
		l.Errorln("chown failed, error is ", err.Error())
		return dataLoadRequestErrorHandler(err, resp, c)
	}

	res, err := db.Exec(m.Query)
	if err != nil {
		l.Errorln("exec failed", err)
		return dataLoadRequestErrorHandler(err, resp, c)
	}

	err = utils.RmFile(m.Filepath, false)
	if err != nil {
		l.Errorln("delete failed, error is ", err.Error())
		return dataLoadRequestErrorHandler(err, resp, c)
	}

	resp.SetResult(res)

	if err != nil {
		return dataLoadRequestErrorHandler(err, resp, c)
	}

	resp.SetError(false)
	c.Messages <- *resp

	return nil
}

func dataLoadRequestErrorHandler(err error, resp *meta.Message, c *Coordinator) error {
	resp.SetError(true)
	c.Messages <- *resp
	return err
}

func DataLoadResponseHandler(m meta.Message, c *Coordinator) error {
	// no-use
	return nil
}
