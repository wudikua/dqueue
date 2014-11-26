package db

import (
	"bufio"
	"encoding/binary"
	"errors"
	"github.com/wudikua/dqueue/global"
	"io"
	"log"
	"os"
	"path"
)

const MAX_FILE_LIMIT = 1024 * 1024

const (
	ENEW   = "0"
	EEMPTY = "1"
	EFULL  = "2"
	EAGAIN = "3"
	ESYNC  = "4"
)

type DQueueDB struct {
	fpw       *os.File
	fpr       *os.File
	fis       *bufio.Writer
	fos       *bufio.Reader
	w, r      int
	dbNo      int
	file      string
	syncEvent chan string
	syncBlock bool
}

func NewInstance(file string, dbNo int) *DQueueDB {
	var instance *DQueueDB
	// 创建目录
	if _, err := os.Stat(path.Dir(file)); err != nil {
		if err := os.Mkdir(path.Dir(file), 0777); err != nil {
			return nil
		}
	}
	// 判断数据文件是否存在
	if _, err := os.Stat(file); err == nil {
		// 存在
		fpw, err := os.OpenFile(file, os.O_RDWR, 0666)
		if err != nil {
			log.Println(err)
			return nil
		}
		fpr, err := os.OpenFile(file, os.O_RDWR, 0666)
		if err != nil {
			log.Println(err)
			return nil
		}
		fis := bufio.NewWriter(fpw)
		fos := bufio.NewReader(fpr)
		instance = &DQueueDB{
			dbNo: dbNo,
			fpw:  fpw,
			fpr:  fpr,
			fis:  fis,
			fos:  fos,
			file: file,
		}
	} else {
		// 不存在 创建数据文件
		fpw, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0660)
		if err != nil {
			log.Println(err)
			return nil
		}
		fpr, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0660)
		if err != nil {
			log.Println(err)
			return nil
		}
		fis := bufio.NewWriter(fpw)
		fos := bufio.NewReader(fpr)
		instance = &DQueueDB{
			dbNo: dbNo,
			fpw:  fpw,
			fpr:  fpr,
			fis:  fis,
			fos:  fos,
			file: file,
			w:    0,
			r:    0,
		}
	}
	return instance
}

func (this *DQueueDB) SetWritePos(w int) {
	this.fpw.Seek(int64(w), 0)
	this.w = w
}

func (this *DQueueDB) SetReadPos(r int) {
	this.fpr.Seek(int64(r), 0)
	this.r = r
}

func (this *DQueueDB) GetWritePos() int {
	return this.w
}

func (this *DQueueDB) GetReadPos() int {
	return this.r
}

func (this *DQueueDB) GetWriteStream() *bufio.Writer {
	return this.fis
}

func (this *DQueueDB) writeInt32(i int) (int, error) {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(i))
	n, err := this.fis.Write(bs)
	this.fis.Flush()
	this.w += n
	return n, err
}

func (this *DQueueDB) readInt32() (int, error) {
	bs := make([]byte, 4)
	n, err := io.ReadFull(this.fos, bs)

	if err != nil {
		return n, err
	}
	this.r += n
	return int(binary.BigEndian.Uint32(bs)), nil
}

func (this *DQueueDB) Write(b []byte) error {
	// this.w是定位在了最后一个写入超过LIMIT的末尾
	if this.w >= MAX_FILE_LIMIT {
		return errors.New(EFULL)
	}
	// 写下一个的位置 当前位置 + 4个字节 + 数据长度
	next := this.w + 4 + len(b)
	// 写下一个数据的起始位置
	this.writeInt32(next)
	// 顺序写数据
	n, err := this.fis.Write(b)
	if err != nil {
		return err
	}
	// 为了消费不延迟，每次写都刷磁盘，也可以改成每10ms刷磁盘等
	this.fis.Flush()
	this.w += n
	if this.syncBlock {
		this.syncEvent <- ESYNC
	}
	return nil
}

func (this *DQueueDB) Read() ([]byte, error) {
	if this.r == this.w {
		if this.w >= MAX_FILE_LIMIT {
			return nil, errors.New(ENEW)
		}
		return nil, errors.New(EEMPTY)
	}
	// 当前位置
	cur := this.r
	// 读下一个数的位置
	next, _ := this.readInt32()
	// 计算数据的长度
	length := next - cur - 4
	bs := make([]byte, length)
	// 顺序读数据
	n, err := io.ReadFull(this.fos, bs)
	if err != nil {
		return nil, err
	}
	this.r += n
	return bs, nil
}

func (this *DQueueDB) ReadAll(output chan interface{}) error {
	fpr, _ := os.OpenFile(this.file, os.O_RDWR, 0666)
	fos := bufio.NewReader(fpr)
	rpos := 0
	this.syncEvent = make(chan string)
	for {
	retry:
		cur := rpos
		if cur == this.w {
			if this.w >= MAX_FILE_LIMIT {
				return nil
			}
			this.syncBlock = true
			// 阻塞等待下一次的PUSH
			select {
			case e := <-this.syncEvent:
				if e == ESYNC {
					this.syncBlock = false
					goto retry
				}
			}
		}
		// 读数据长度
		var bs [4]byte
		n, err := io.ReadFull(fos, bs)
		if err != nil {
			return err
		}
		// 增加读的位置
		rpos += n
		// 转换成长度
		next := int(binary.BigEndian.Uint32(bs))
		length := next - cur - 4
		//
		var bs2 [length + 5]byte
		bs2[0] = byte(global.OP_DB_APPEND)
		copy(bs2[1:], bs)
		n, err = io.ReadFull(fos, bs2[5:])
		if err != nil {
			return err
		}
		log.Println(bs2)
		output <- bs2
		rpos += n
	}
}

func (this *DQueueDB) Stats() map[string]interface{} {
	stats := make(map[string]interface{}, 3)
	stats["dbNo"] = this.dbNo
	stats["w"] = this.w
	stats["r"] = this.r
	return stats

}
