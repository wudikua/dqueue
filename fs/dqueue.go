package fs

import (
	"encoding/binary"
	"fmt"
	"github.com/wudikua/dqueue/db"
	"github.com/wudikua/dqueue/global"
	"github.com/wudikua/dqueue/idx"
	"os"
	"sync"
	"time"
)

type DQueueFs struct {
	dbName    string
	path      string
	dbs       map[int]*db.DQueueDB
	idx       *idx.DQueueIndex
	rlock     sync.Mutex
	wlock     sync.Mutex
	syncEvent chan bool
}

func NewInstance(path string) *DQueueFs {
	// 创建队列目录
	if _, err := os.Stat(path); err != nil {
		if err := os.Mkdir(path, 0777); err != nil {
			return nil
		}
	}

	instance := &DQueueFs{
		dbName:    "dqueue",
		path:      path,
		dbs:       make(map[int]*db.DQueueDB, 1),
		syncEvent: make(chan bool),
	}

	// 载入索引文件
	idx := idx.NewInstance(path + "/dqueue.idx")
	if idx == nil {
		return nil
	}
	instance.idx = idx
	// 载入数据文件
	dbBegin := idx.GetReadNo()
	dbEnd := idx.GetWriteNo()
	for i := dbBegin; i <= dbEnd; i++ {
		dbs := db.NewInstance(fmt.Sprintf("%s/dqueue_%d.db", path, i), i)
		if dbs == nil {
			return nil
		}
		instance.dbs[i] = dbs
	}

	instance.dbs[dbBegin].SetReadPos(idx.GetReadIndex())
	instance.dbs[dbEnd].SetWritePos(idx.GetWriteIndex())
	return instance
}

func (this *DQueueFs) Push(bs []byte) (int, error) {
	this.wlock.Lock()
	defer this.wlock.Unlock()
	dbs := this.dbs[this.idx.GetWriteNo()]
push:
	err := dbs.Write(bs)
	if err != nil {
		if err.Error() == db.EFULL {
			// 当前db写满了,创建新的db
			dbNo := this.idx.GetWriteNo()
			dbs = db.NewInstance(fmt.Sprintf("%s/dqueue_%d.db", this.path, dbNo+1), dbNo+1)
			this.idx.SetWriteNo(dbNo + 1)
			this.idx.SetWriteIndex(0)
			this.dbs[dbNo+1] = dbs
			goto push
		}
		return this.idx.GetLength(), err
	}
	// 写索引文件
	writePos := dbs.GetWritePos()
	this.idx.SetWriteIndex(writePos)
	this.idx.IncLength()
	length := this.idx.GetLength()
	// 触发同步
	select {
	case this.syncEvent <- true:
	default:
	}
	return length, nil
}

func (this *DQueueFs) Pop() (int, []byte, error) {
	this.rlock.Lock()
	defer this.rlock.Unlock()
	dbs := this.dbs[this.idx.GetReadNo()]
pop:
	bs, err := dbs.Read()
	if err != nil {
		if err.Error() == db.ENEW {
			// 读完了,判断是否还有下一个db
			if this.idx.GetReadNo() < this.idx.GetWriteNo() {
				dbNo := this.idx.GetReadNo()
				if this.dbs[this.idx.GetReadNo()+1] != nil {
					dbs = this.dbs[this.idx.GetReadNo()+1]
				} else {
					dbs = db.NewInstance(fmt.Sprintf("%s/dqueue_%d.db", this.path, dbNo+1), dbNo+1)
					this.dbs[dbNo+1] = dbs
				}
				this.idx.SetReadNo(dbNo + 1)
				this.idx.SetReadIndex(0)
				goto pop
			}
		}
		return this.idx.GetLength(), bs, err
	}
	// 写索引文件
	readPos := dbs.GetReadPos()
	this.idx.SetReadIndex(readPos)
	this.idx.DecLength()
	length := this.idx.GetLength()
	// 触发同步
	select {
	case this.syncEvent <- true:
	default:
	}
	return length, bs, err
}

func (this *DQueueFs) SyncDB(queue string, output chan interface{}) *db.DQueueDB {
	for {
		dbBegin := this.idx.GetReadNo()
		dbEnd := this.idx.GetWriteNo()
		// 从第一个需要读的db开始同步,一直同步到当前在写的db
		for i := dbBegin; i <= dbEnd; i++ {
			output <- []byte{
				global.OP_NEW,
				byte(i >> 24),
				byte(i >> 16),
				byte(i >> 8),
				byte(i),
			}
			// 判断是不是当前在写的文件
			if i < dbEnd {
				// 不是的话创建对象，使用readAll
				dbold := db.NewInstance(fmt.Sprintf("%s/dqueue_%d.db", this.path, i), i)
				dbold.SetWritePos(db.MAX_FILE_LIMIT)
				dbold.ReadAll(output)
			} else {
				// 每1s同步消费进度
				go this.SyncIdx(queue, output)
				// 使用当前对象
				this.dbs[i].ReadAll(output)
			}
			// 修改dbEnd
			dbEnd = this.idx.GetWriteNo()
		}
	}

}

func (this *DQueueFs) SyncIdx(queue string, output chan interface{}) {
	preWriteNo := -1
	preReadNo := -1
	preRead := -1
	preWrite := -1
	bs := make([]byte, 13)
	for {
		// 阻塞等等入队或者出队事件的触发
		select {
		case <-time.After(time.Second * 10):
			// 10秒强制检查同步
			goto check_send
		case <-this.syncEvent:
		}
	check_send:
		length := this.idx.GetLength()
		readNo := this.idx.GetReadNo()
		writeNo := this.idx.GetWriteNo()
		readIndex := this.idx.GetReadIndex()
		writeIndex := this.idx.GetWriteIndex()
		if preRead != readIndex || preWrite != writeIndex {
			// 同步消费写入的offset
			bs[0] = global.OP_IDX_READ_WRITE_LEN
			binary.BigEndian.PutUint32(bs[1:], uint32(readIndex))
			binary.BigEndian.PutUint32(bs[5:], uint32(writeIndex))
			binary.BigEndian.PutUint32(bs[9:], uint32(length))
			output <- bs
			preRead = readIndex
			preWrite = writeIndex
		}

		if preWriteNo != writeNo {
			// 写队列换页
			output <- []byte{
				global.OP_CHANGE_WRITENO,
				byte(writeNo >> 24),
				byte(writeNo >> 16),
				byte(writeNo >> 8),
				byte(writeNo + 1),
			}
			preWriteNo = writeNo
		}
		if preReadNo != readNo {
			// 读队列换页
			output <- []byte{
				global.OP_CHANGE_READNO,
				byte(readNo >> 24),
				byte(readNo >> 16),
				byte(readNo >> 8),
				byte(readNo + 1),
			}
			preReadNo = readNo
		}

	}
}

func (this *DQueueFs) Stats() map[string]interface{} {
	stats := make(map[string]interface{})
	stats["idx"] = this.idx.Stats()
	for k, v := range this.dbs {
		stats[string(k)] = v.Stats()
	}
	return stats
}
