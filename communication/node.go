// Package communication 通信相关
// node.go 节点相关操作
package communication

import (
	"fmt"
	"log"
	db "nkyServer/database"

	"github.com/goburrow/modbus"
)

// NodeClient 节点通信信息和modbus客户端
type NodeClient struct {
	db.NodeComm
	Client modbus.Client
}

var mdNodeClients []NodeClient

// CHNodeData modbus读取到的数据将会写入这个通道
var CHNodeData chan db.NodeData

// InitNodes 初始化通信节点
func InitNodes(nodes []db.NodeComm) error {
	mdNodeClients = mdNodeClients[:0]
	for _, dbnode := range nodes {
		var nodeTmp NodeClient
		nodeTmp.NodeComm = dbnode
		mdHandler.SlaveId = byte(nodeTmp.CommAddr)
		nodeTmp.Client = modbus.NewClient(mdHandler)
		mdNodeClients = append(mdNodeClients, nodeTmp)
	}
	CHNodeData = make(chan db.NodeData, len(nodes)) // 开辟两倍的节点数缓冲
	return nil
}

// ReadNodeData 定时执行，读取每一个节点后会写入ChNodeData
// 在其他线程中读取ChNodeData即可
func ReadNodeData() {
	fmt.Println("in ReadNodeData")
	for _, nodeClient := range mdNodeClients {
		mdMu.Lock()
		log.Println("addr:", nodeClient.CommAddr, "coil:", nodeClient.CommCoil)
		mdHandler.SlaveId = byte(nodeClient.CommAddr)
		var rData []byte
		var err error
		fmt.Println("in for:")
		for i := 0; i < mdConf.MdConfig.RetryNum; i++ {
			if rData, err = nodeClient.Client.ReadInputRegisters(uint16(nodeClient.CommCoil), 1); err == nil {
				fmt.Println("err:", err)
				break
			}
			fmt.Println("err:", err)
		}
		fmt.Println("out for:")
		mdMu.Unlock()
		if err != nil {
			continue
		}
		result := float64(rData[0])*256 + float64(rData[1])
		if nodeClient.CommCoil < 2 {
			result /= 10.0
		}
		// fmt.Println("rData:", rData, result)
		tmpNodeData := db.NodeData{Node: db.Node{ID: nodeClient.ID, EnvID: nodeClient.EnvID}, Data: result}
		if err != nil {
			log.Println(err)
		}
		CHNodeData <- tmpNodeData
	}
	fmt.Println("out ReadNodeData")
}
