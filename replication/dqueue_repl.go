package replication

import (
	"encoding/json"
	"github.com/wudikua/dqueue/fs"
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
func (this *DQueueReplication) SyncDQueueDB(queue string) error {
	quit := make(chan bool)
	// 初始化数据
	this.queues[queue] = fs.NewInstance(queue)
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
			this.queues[queue].Push([]byte(list[2]))
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
