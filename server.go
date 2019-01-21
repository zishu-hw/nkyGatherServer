package main

import (
	"fmt"
	md "nkyServer/communication"
	db "nkyServer/database"
	"time"
	"zylog"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	fmt.Println("this is nkyServer")
	var err error

	// 初始化数据库
	err = db.Open()
	checkErrFatal(err)
	defer db.Close()
	nodes, err := db.ReadNodeComm()
	checkErrFatal(err)
	relays, err := db.ReadRelayComm()
	checkErrFatal(err)
	// fmt.Println(relays)

	// 初始化modbus设备
	err = md.LoadConfig("./communication/config.json")
	checkErrFatal(err)
	err = md.OpenModbus()
	checkErrFatal(err)
	defer md.CloseModbus()
	err = md.InitNodes(nodes)
	checkErrFatal(err)
	err = md.InitRelays(relays)
	checkErrFatal(err)

	// 定时保存历史数据
	writeLogPeriod, err := db.ReadLogTablePeriod()
	checkErrFatal(err)
	timerWriteLogPeriod := time.NewTimer(writeLogPeriod)
	defer timerWriteLogPeriod.Stop()
	go writeValueLog(timerWriteLogPeriod)

	// 实时读取节点数据
	monitorPeriod := time.NewTicker(10 * time.Second)
	defer monitorPeriod.Stop()
	go readNodeData(monitorPeriod.C)

	// 操作继电器
	relayPeriod := time.NewTicker(1 * time.Second)
	defer relayPeriod.Stop()
	go wrRelayStatus(relayPeriod.C)

	// 写入实时记录表
	go writeValueRealTime()

	count := 0
	for {
		time.Sleep(10 * time.Second)
		count++
		fmt.Printf("%d0s\n", count)
	}
}

// 读取节点数据
func readNodeData(c <-chan time.Time) {
	for {
		select {
		case <-c:
			md.ReadNodeData()
		}
	}
}

// 将modbus读取的数据写入实时记录表
func writeValueRealTime() {
	for node := range md.CHNodeData {
		// fmt.Println("WriteValueRealtime")
		err := db.WriteValueRealtime(node)
		checkErrFatal(err)
	}
}

// 将实时记录表写入历史表
func writeValueLog(t *time.Timer) {
	for {
		select {
		case <-t.C:
			// fmt.Println("backup...")
			err := db.WriteValueLog()
			checkErrFatal(err)
			writeLogPeriod, err := db.ReadLogTablePeriod()
			checkErrFatal(err)
			t.Reset(writeLogPeriod)
		}
	}
}

// 读写继电器状态
func wrRelayStatus(c <-chan time.Time) {
	for {
		select {
		case <-c:
			rWstatus, err := db.ReadRelayWstatus()
			if err != nil {
				zylog.Log.Error(err)
				continue
			}
			rRstatus, err := db.ReadRelayRstatus()
			if err != nil {
				zylog.Log.Error(err)
				continue
			}
			for i := range rWstatus {
				if !db.EqualRelayStatus(&rWstatus[i], &rRstatus[i]) {
					fmt.Println(rWstatus[i].ID, rWstatus[i].CoilNum)
					err = md.WriteRelayStatus(&rWstatus[i])
					if err != nil {
						zylog.Log.Error("w, relayID:", rWstatus[i].ID, err)
						continue
					}
					rRelay, err := md.ReadRelaysStatus(rWstatus[i].ID)
					if err != nil {
						zylog.Log.Error("r, relayID:", rWstatus[i].ID, err)
						continue
					}
					db.WriteRelayRstatus(rRelay)
				}
			}
		}
	}
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
