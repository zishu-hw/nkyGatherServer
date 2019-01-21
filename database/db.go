package database

import (
	"database/sql"
	"time"
	"zylog"
)

// Node 节点的标示相关信息
type Node struct {
	ID    int
	EnvID int
}

// NodeComm 节点和通信信息
type NodeComm struct {
	Node
	CommAddr int
	CommCoil int
}

// NodeData 节点和采集数据
type NodeData struct {
	Node
	Data float64
}

// Relay 继电器相关信息
type Relay struct {
	ID      int
	CoilNum int
}

// RelayComm 继电器和通信信息
type RelayComm struct {
	Relay
	CommAddr int
}

// RelayStatus 继电器和继电器状态
type RelayStatus struct {
	Relay
	Status []byte
}

var mDb *sql.DB

// Open 连接数据库
func Open() error {
	var err error
	mDb, err = sql.Open("mysql", "root:zy0802@/db_test?charset=utf8")
	return err
}

// Close 断开数据库
func Close() {
	mDb.Close()
}

// ReadNodeComm 读取节点通信数据
func ReadNodeComm() ([]NodeComm, error) {
	rows, err := mDb.Query("select f_nodeID,f_commAddr,f_commCoil,f_envID from tb_nodeInfo order by f_nodeID")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// 初始化节点id
	nodes := make([]NodeComm, 0)
	var nodeID, commAddr, commCoil, envID int
	for rows.Next() {
		err = rows.Scan(&nodeID, &commAddr, &commCoil, &envID)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, NodeComm{Node: Node{ID: nodeID, EnvID: envID}, CommAddr: commAddr, CommCoil: commCoil})
	}
	return nodes, err
}

// ReadLogTablePeriod 读取写入历史记录表的周期时间
func ReadLogTablePeriod() (t time.Duration, err error) {
	rows, err := mDb.Query("select f_timeH,f_timeM,f_timeS from tb_logPeriod")
	if err != nil {
		return
	}
	defer rows.Close()

	var h, m, s int
	if rows.Next() {
		err = rows.Scan(&h, &m, &s)
	}
	t = time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second
	return
}

// WriteValueRealtime 写入实时记录表
func WriteValueRealtime(ndata NodeData) error {
	stmt, err := mDb.Prepare("update tb_valueRealTime set f_value=?,f_dateTime=? where f_nodeID=? and f_envID=?")
	if err != nil {
		return err
	}
	_, err = stmt.Exec(ndata.Data, time.Now().Format("2006-01-02 15:04:05"), ndata.ID, ndata.EnvID)
	return err
}

// WriteValueLog 写入历史表
func WriteValueLog() error {
	_, err := mDb.Exec(`insert into tb_valueLog (f_nodeID,f_envID,f_value,f_dateTime) 
	select f_nodeID,f_envID,f_value,f_dateTime from tb_valueRealTime`)
	return err
}

// ReadRelayComm 读取继电器通信数据
func ReadRelayComm() ([]RelayComm, error) {
	rows, err := mDb.Query("select f_relayID,f_coilNum,f_commAddr from tb_relay")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	relays := make([]RelayComm, 0)
	var relayID, coilNum, commAddr int
	for rows.Next() {
		err = rows.Scan(&relayID, &coilNum, &commAddr)
		if err != nil {
			return nil, err
		}
		relays = append(relays, RelayComm{Relay{relayID, coilNum}, commAddr})
	}
	return relays, err
}

// WriteRelayWstatus 写入继电器的写入状态，期待被装换
func WriteRelayWstatus(relay RelayStatus) {
	writeRelayStatus("f_coilWriteStatus", &relay)
}

// WriteRelayRstatus 写入继电器的读入状态，继电器实际状态
func WriteRelayRstatus(relay RelayStatus) {
	writeRelayStatus("f_coilReadStatus", &relay)
}

func writeRelayStatus(field string, relay *RelayStatus) {
	if relay.CoilNum != len(relay.Status) {
		zylog.Log.Error(relay.ID, " 继电器的线圈个数和实际数据不符合")
		return
	}
	for i := 0; i < relay.CoilNum; i++ {
		updateStr := "update tb_relayInfo set " + field + "=? where f_relayID=? and f_coilID=?;"
		stmt, err := mDb.Prepare(updateStr)
		checkErrPrint(err)
		_, err = stmt.Exec(relay.Status[i], relay.ID, i)
		checkErrPrint(err)
	}
}

// ReadRelayRstatus 读取数据库中继电器的写入状态
func ReadRelayRstatus() ([]RelayStatus, error) {
	return readRelayStatus("f_coilReadStatus")
}

// ReadRelayWstatus 读取数据库中继电器的写入状态
func ReadRelayWstatus() ([]RelayStatus, error) {
	return readRelayStatus("f_coilWriteStatus")
}

func readRelayStatus(field string) ([]RelayStatus, error) {
	relayRows, err := mDb.Query("select f_relayID,f_coilNum,f_commAddr from tb_relay")
	if err != nil {
		return nil, err
	}
	defer relayRows.Close()
	relays := make([]RelayStatus, 0)
	var relayID, coilNum, commAddr int
	for relayRows.Next() {
		err = relayRows.Scan(&relayID, &coilNum, &commAddr)
		if err != nil {
			return nil, err
		}
		relay := RelayStatus{Relay: Relay{ID: relayID, CoilNum: coilNum}, Status: make([]byte, coilNum)}
		queryStr := "select f_coilID," + field + " from tb_relayInfo where f_relayID=?"
		coilRows, err := mDb.Query(queryStr, relayID)
		var coilID, coilStatus int
		for coilRows.Next() {
			err = coilRows.Scan(&coilID, &coilStatus)
			if err != nil {
				return nil, err
			}
			relay.Status[coilID] = byte(coilStatus)
		}
		relays = append(relays, relay)
		coilRows.Close() // 关闭连接
	}
	return relays, err
}

// EqualRelayStatus 判断两个继电器状态是否相等
func EqualRelayStatus(x, y *RelayStatus) bool {
	if x.ID != y.ID || x.CoilNum != y.CoilNum {
		return false
	}
	for i := range x.Status {
		if x.Status[i] != y.Status[i] {
			return false
		}
	}
	return true
}

func checkErrPrint(err error) {
	if err != nil {
		zylog.Log.Error(err)
	}
}

func checkErrFatal(err error) {
	if err != nil {
		zylog.Log.Fatal(err)
	}
}
