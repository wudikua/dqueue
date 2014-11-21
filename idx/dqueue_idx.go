package idx

import (
	"encoding/binary"
	"errors"
	"log"
	"os"
)

// dqueue 6个字节
var MAGIC = []byte{100, 113, 117, 101, 117, 101}
var MAGIC_LEN = 6

type DQueueIndex struct {
	readNo     int
	readIndex  int
	writeNo    int
	writeIndex int
	length     int
	file       string
	fp         *os.File
	w, r       int
}

func NewInstance(file string) *DQueueIndex {
	var instance *DQueueIndex
	// 判断索引文件是否存在
	if _, err := os.Stat(file); err == nil {
		// 存在
		fp, _ := os.OpenFile(file, os.O_RDWR, 0666)
		instance = &DQueueIndex{
			file: file,
			fp:   fp,
			w:    0,
			r:    0,
		}

		instance.initIndexInfoFromFile()

	} else {
		// 不存在 创建索引文件
		fp, _ := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0660)
		instance = &DQueueIndex{
			readNo:     1,
			readIndex:  0,
			writeNo:    1,
			writeIndex: 0,
			length:     0,
			file:       file,
			fp:         fp,
			w:          0,
			r:          0,
		}
		// 写6个字节是dqueue
		instance.writeMagic()
		// 写4个字节读到第几个库了
		instance.writeInt32(instance.readNo)
		// // 写4个字节读到数据库的位置
		instance.writeInt32(instance.readIndex)
		// // 写4个字节写到第几个库了
		instance.writeInt32(instance.writeNo)
		// // 写4个字节写到数据的库位置
		instance.writeInt32(instance.writeIndex)
	}

	return instance
}

func (this *DQueueIndex) initIndexInfoFromFile() error {
	this.w = 0
	this.r = 0
	if _, err := this.readMagic(); err != nil {
		log.Fatalln(err)
		return nil
	}

	this.readNo, _ = this.readInt32()
	this.readIndex, _ = this.readInt32()
	this.writeNo, _ = this.readInt32()
	this.writeIndex, _ = this.readInt32()
	this.length, _ = this.readInt32()
	return nil
}

func (this *DQueueIndex) readMagic() (int, error) {
	b := make([]byte, MAGIC_LEN)
	n, err := this.fp.ReadAt(b, int64(this.r))
	this.r += n
	if err != nil {
		return n, err
	}
	if string(b) != string(MAGIC) {
		return n, errors.New("Not a Index File")
	}
	return n, nil
}

func (this *DQueueIndex) writeMagic() (int, error) {
	n, err := this.fp.WriteAt(MAGIC, int64(this.w))
	this.w += n

	return n, err
}

func (this *DQueueIndex) readInt32() (int, error) {
	bs := make([]byte, 4)
	n, err := this.fp.ReadAt(bs, int64(this.r))
	if err != nil {
		return n, err
	}
	this.r += n
	return int(binary.BigEndian.Uint32(bs)), nil
}

func (this *DQueueIndex) writeInt32(i int) (int, error) {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(i))
	n, err := this.fp.WriteAt(bs, int64(this.w))
	this.w += n
	return n, err
}

func (this *DQueueIndex) SetReadNo(i int) (int, error) {
	this.readNo = i
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(i))
	return this.fp.WriteAt(bs, 6)
}

func (this *DQueueIndex) GetReadNo() int {
	return this.readNo
}

func (this *DQueueIndex) SetReadIndex(i int) (int, error) {
	this.readIndex = i
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(i))
	return this.fp.WriteAt(bs, 10)
}

func (this *DQueueIndex) GetReadIndex() int {
	return this.readIndex
}

func (this *DQueueIndex) SetWriteNo(i int) (int, error) {
	this.writeNo = i
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(i))
	return this.fp.WriteAt(bs, 14)
}

func (this *DQueueIndex) GetWriteNo() int {
	return this.writeNo
}

func (this *DQueueIndex) SetWriteIndex(i int) (int, error) {
	this.writeIndex = i
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(i))
	return this.fp.WriteAt(bs, 18)
}

func (this *DQueueIndex) GetWriteIndex() int {
	return this.writeIndex
}

func (this *DQueueIndex) IncLength() (int, error) {
	this.length = this.length + 1
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(this.length))
	return this.fp.WriteAt(bs, 22)
}

func (this *DQueueIndex) DecLength() (int, error) {
	this.length = this.length - 1
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(this.length))
	return this.fp.WriteAt(bs, 22)
}

func (this *DQueueIndex) GetLength() int {
	return this.length
}

func (this *DQueueIndex) Stats() map[string]interface{} {
	stats := make(map[string]interface{}, 4)
	stats["readNo"] = this.readNo
	stats["readIndex"] = this.readIndex
	stats["writeNo"] = this.writeNo
	stats["writeIndex"] = this.writeIndex
	stats["length"] = this.length
	return stats
}
