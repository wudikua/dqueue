package replication

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/wudikua/dqueue/db"
	"github.com/wudikua/dqueue/fs"
	"github.com/wudikua/dqueue/global"
	"github.com/wudikua/dqueue/idx"
	"github.com/xuyu/goredis"
	"log"
)

// 同步从节点
type DQueueReplication struct {
	replicationChannel chan []byte
	master             *goredis.Redis
	queues             map[string]*fs.DQueueFs
}

func NewDQueueReplication(addr string) (*DQueueReplication, error) {
	master, err := goredis.Dial(&goredis.DialConfig{Address: addr})
	if err != nil {
		return nil, err
	}
	return &DQueueReplication{
		replicationChannel: make(chan []byte, 1024),
		master:             master,
		queues:             make(map[string]*fs.DQueueFs),
	}, nil
}

// 获取所有队列
func (this *DQueueReplication) Greet() ([]string, error) {
	queues := make([]string, 1)
	reply, _ := this.master.ExecuteCommand("GREET", "", "")
	bs, err := reply.BytesValue()
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(bs, &queues); err != nil {
		return nil, err
	}

	return queues, err
}

// 订阅一个队列数据的变更
func (this *DQueueReplication) SyncDQueue(queue string) error {
	quit := make(chan bool)
	var dbs *db.DQueueDB
	var index *idx.DQueueIndex
	// 初始化数据
	sub, err := this.master.PubSub()
	defer sub.Close()
	if err := sub.Subscribe(queue); err != nil {
		quit <- true
		return err
	}
	for {
		list, err := sub.Receive()
		if err != nil {
			quit <- true
			break
		}
		if list[0] == "message" {
			arr := []byte(list[2])
			switch arr[0] {
			case global.OP_NEW:
				// 创建新的DB
				dbNo := int(binary.BigEndian.Uint32(arr[1:]))
				log.Println("new ", dbNo)
				dbs = db.NewInstance(fmt.Sprintf("%s/dqueue_%d.db", list[1], dbNo), dbNo)
			case global.OP_DB_APPEND:
				// 向已经创建的DB顺序写
				log.Println("append", arr[1:])
				stream := dbs.GetWriteStream()
				_, err := stream.Write(arr)
				if err != nil {
					log.Println(err)
				}
				stream.Flush()
			case global.OP_IDX_READ_WRITE_LEN:
				readIndex := int(binary.BigEndian.Uint32(arr[1:]))
				writeIndex := int(binary.BigEndian.Uint32(arr[5:]))
				length := int(binary.BigEndian.Uint32(arr[9:]))
				if index == nil {
					// 创建索引文件
					index = idx.NewInstance(list[1] + "/dqueue.idx")
				}
				index.SetReadIndex(readIndex)
				index.SetWriteIndex(writeIndex)
				index.SetLength(length)
			case global.OP_IDX_READ:
				// 主库同步读队列的进度
				readIndex := int(binary.BigEndian.Uint32(arr[1:5]))
				length := int(binary.BigEndian.Uint32(arr[5:9]))
				// 主库同步写队列的进度
				if index == nil {
					// 创建索引文件
					index = idx.NewInstance(list[1] + "/dqueue.idx")
				}
				index.SetWriteIndex(readIndex)
				index.SetLength(length)
			case global.OP_IDX_WRITE:
				writeIndex := int(binary.BigEndian.Uint32(arr[1:5]))
				length := int(binary.BigEndian.Uint32(arr[5:9]))
				// 主库同步写队列的进度
				if index == nil {
					// 创建索引文件
					index = idx.NewInstance(list[1] + "/dqueue.idx")
				}
				index.SetWriteIndex(writeIndex)
				index.SetLength(length)
			case global.OP_CHANGE_READNO:
				// 主库读换页
				dbNo := int(binary.BigEndian.Uint32(arr[1:]))
				if index == nil {
					// 创建索引文件
					index = idx.NewInstance(list[1] + "/dqueue.idx")
				}
				index.SetReadNo(dbNo)
				// log.Println("change read", dbNo)
			case global.OP_CHANGE_WRITENO:
				// 主库写换页
				dbNo := int(binary.BigEndian.Uint32(arr[1:]))
				if index == nil {
					// 创建索引文件
					index = idx.NewInstance(list[1] + "/dqueue.idx")
				}
				index.SetWriteNo(dbNo)
				// log.Println("change write", dbNo)
			case global.OP_HEARTBEAT:
				// 主库发过来的心跳代表自己还活着
			default:
				// 未知的操作数
				log.Println("unknown", arr[1:])
			}
		}
	}
	<-quit
	log.Println("subscribe exit")
	return err
}

// block sync from master
func (this *DQueueReplication) Sync() {
	for {
		select {
		case b := <-this.replicationChannel:
			log.Println(b)
		}
	}
}
