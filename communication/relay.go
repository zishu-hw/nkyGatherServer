// Package communication 通信相关
// relay.go 继电器相关操作
package communication

import (
	"fmt"
	db "nkyServer/database"

	"github.com/goburrow/modbus"
)

// RelayClient 继电器通信信息和modbus客户端
type RelayClient struct {
	db.RelayComm
	Client modbus.Client
}

var mdRelayClients []RelayClient

// CHRelayStatus modbus读取到的继电器状态将会写入这个通道
var CHRelayStatus chan db.RelayStatus

// InitRelays 初始化继电器
func InitRelays(relays []db.RelayComm) error {
	mdRelayClients = mdRelayClients[:0]
	for _, dbRelay := range relays {
		var relayTmp RelayClient
		relayTmp.ID = dbRelay.ID
		relayTmp.CoilNum = dbRelay.CoilNum
		relayTmp.CommAddr = dbRelay.CommAddr
		relayTmp.Client = modbus.NewClient(mdHandler)
		mdRelayClients = append(mdRelayClients, relayTmp)
	}
	CHRelayStatus = make(chan db.RelayStatus, len(relays)) // 开辟两倍的节点数缓冲
	return nil
}

// WriteRelayStatus 写继电器状态
func WriteRelayStatus(relay *db.RelayStatus) error {
	fmt.Println("in WriteRelayStatus")
	var err error
	for _, relayClient := range mdRelayClients {
		if relay.ID == relayClient.ID {
			mdMu.Lock()
			mdHandler.SlaveId = byte(relayClient.CommAddr)
			// var rData []byte
			for i := 0; i < mdConf.MdConfig.RetryNum; i++ {
				_, err = relayClient.Client.WriteMultipleCoils(0,
					uint16(relay.CoilNum), relayStatusArrToByte(relay.Status))
				if err == nil {
					break
				}
			}
			mdMu.Unlock()
			if err != nil {
				continue
			}
			// rArr := relayStatusByteToArr(rData, relayClient.CoilNum)
			// tmpRelayStatu := db.RelayStatus{Relay: db.Relay{ID: relayClient.ID, CoilNum: relayClient.CoilNum}, Status: result}
		}
	}
	fmt.Println("out WriteRelayStatus")
	return err
}

// ReadRelaysStatus 读继电器状态
func ReadRelaysStatus(relayid int) (db.RelayStatus, error) {
	var result db.RelayStatus
	var err error
	mdMu.Lock()
	defer mdMu.Unlock()
	for _, relayClient := range mdRelayClients {
		if relayid == relayClient.ID {
			mdHandler.SlaveId = byte(relayClient.CommAddr)
			var rData []byte
			for i := 0; i < mdConf.MdConfig.RetryNum; i++ {
				rData, err = relayClient.Client.ReadCoils(0, uint16(relayClient.CoilNum))
				if err == nil {
					break
				}
			}
			if err != nil {
				continue
			}
			rArr := relayStatusByteToArr(rData, relayClient.CoilNum)
			result = db.RelayStatus{Relay: db.Relay{ID: relayClient.ID, CoilNum: relayClient.CoilNum}, Status: rArr}
		}
	}
	return result, err
}

func relayStatusByteToArr(rdata []byte, coilnum int) []byte {
	result := make([]byte, coilnum)
	for i := 0; i < coilnum; i++ {
		result[i] = (rdata[i/8] >> uint8(i%8)) & 0x01
	}
	return result
}

func relayStatusArrToByte(rdata []byte) []byte {
	result := make([]byte, (len(rdata)-1)/8+1)
	for i, v := range rdata {
		result[i/8] |= (v << uint8(i%8))
	}
	return result
}
