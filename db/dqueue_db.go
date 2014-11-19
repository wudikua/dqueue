package db

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const MAX_FILE_LIMIT = 1024 * 1024

const (
	ENEW   string = "0"
	EEMPTY string = "1"
	EFULL  string = "2"
	EAGAIN string = "3"
)

type DQueueDB struct {
	fpw  *os.File
	fpr  *os.File
	fis  *bufio.Writer
	fos  *bufio.Reader
	w, r int
	dbNo int
}

func NewInstance(file string, dbNo int) *DQueueDB {
	var instance *DQueueDB
	// 判断数据文件是否存在
	if _, err := os.Stat(file); err == nil {
		// 存在
		fpw, _ := os.OpenFile(file, os.O_RDWR, 0666)
		fpr, _ := os.OpenFile(file, os.O_RDWR, 0666)
		fis := bufio.NewWriter(fpw)
		fos := bufio.NewReader(fpr)
		instance = &DQueueDB{
			dbNo: dbNo,
			fpw:  fpw,
			fpr:  fpr,
			fis:  fis,
			fos:  fos,
		}
	} else {
		// 不存在 创建数据文件
		fpw, _ := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0660)
		fpr, _ := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0660)
		fis := bufio.NewWriter(fpw)
		fos := bufio.NewReader(fpr)
		instance = &DQueueDB{
			dbNo: dbNo,
			fpw:  fpw,
			fpr:  fpr,
			fis:  fis,
			fos:  fos,
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
	n, err := io.ReadFull(this.fos.Read, bs)

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
	n, err := io.ReadFull(this.fos.Read, bs)
	if err != nil {
		return nil, err
	}
	this.r += n
	return bs, nil
}

func (this *DQueueDB) Stats() map[string]interface{} {
	stats := make(map[string]interface{}, 3)
	stats["dbNo"] = this.dbNo
	stats["w"] = this.w
	stats["r"] = this.r
	return stats

}
