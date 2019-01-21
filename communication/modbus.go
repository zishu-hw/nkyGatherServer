// Package communication 通信相关
// modbus.go modbus相关操作
package communication

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/goburrow/modbus"
)

// ModbusConfig modbus相关配置
type ModbusConfig struct {
	Port     string `json:"port"`
	BaudRate int    `json:"baudRate"`
	DataBits int    `json:"dataBits"`
	Parity   string `json:"parity"`
	StopBits int    `json:"stopBits"`
	TimeOut  int    `json:"timeOut"`
	RetryNum int    `json:"retryNum"`
	// SlaveID  int    `json:"slaveId"`
}

// Config 配置
type Config struct {
	MdConfig ModbusConfig `json:"modbus"`
}

var mdConf *Config
var mdHandler *modbus.RTUClientHandler
var mdMu sync.Mutex

func init() {
	fmt.Println("modbus init...")
}

// LoadConfig 加载配置文件
func LoadConfig(filepath string) error {
	if mdConf == nil {
		mdConf = new(Config)
	}
	bytes, err := ioutil.ReadFile(filepath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bytes, mdConf)
	return err
}

// OpenModbus 打开modbus
func OpenModbus() error {
	mdHandler = modbus.NewRTUClientHandler(mdConf.MdConfig.Port)
	mdHandler.BaudRate = mdConf.MdConfig.BaudRate
	mdHandler.DataBits = mdConf.MdConfig.DataBits
	mdHandler.Parity = mdConf.MdConfig.Parity
	mdHandler.StopBits = mdConf.MdConfig.StopBits
	mdHandler.Timeout = time.Duration(mdConf.MdConfig.TimeOut) * time.Second
	// mdHandler.SlaveId = 1

	err := mdHandler.Connect()
	if err != nil {
		fmt.Println("connect:", err)
		return err
	}
	return nil
}

// CloseModbus 关闭modbus
func CloseModbus() {
	mdHandler.Close()
}

func checkErrPrint(err error) {
	if err != nil {
		log.Println(err)
	}
}

func checkErrPainc(err error) {
	if err != nil {
		log.Panicln(err)
	}
}
