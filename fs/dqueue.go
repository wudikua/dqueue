package fs

import (
	"fmt"
	"github.com/wudikua/dqueue/db"
	"github.com/wudikua/dqueue/global"
	"github.com/wudikua/dqueue/idx"
	"os"
	"sync"
)

type DQueueFs struct {
	dbName string
	path   string
	dbs    map[int]*db.DQueueDB
	idx    *idx.DQueueIndex
	rlock  sync.Mutex
	wlock  sync.Mutex
	slave  bool
}

func NewInstance(path string) *DQueueFs {
	// 创建队列目录
	if _, err := os.Stat(path); err != nil {
		if err := os.Mkdir(path, 0777); err != nil {
			return nil
		}
	}

	instance := &DQueueFs{
		dbName: "dqueue",
		path:   path,
		dbs:    make(map[int]*db.DQueueDB, 1),
		slave:  false,
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
	this.idx.SetWriteIndex(dbs.GetWritePos())
	this.idx.IncLength()
	return this.idx.GetLength(), nil
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
	this.idx.SetReadIndex(dbs.GetReadPos())
	this.idx.DecLength()
	return this.idx.GetLength(), bs, err
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
				// 使用当前对象
				this.dbs[i].ReadAll(output)
			}
			// 修改dbEnd
			dbEnd = this.idx.GetWriteNo()
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
