package db

import (
	"fmt"
	"os"
	"testing"
)

func Test_NewInstance(t *testing.T) {
	os.Remove("dqueue_0.db")
	db := NewInstance("dqueue_0.db", 0)
	if db == nil {
		t.Fail()
	}

	db = NewInstance("dqueue_0.db", 0)
	if db == nil {
		t.Fail()
	}
}

func Test_writeReadInt32WithClose(t *testing.T) {
	os.Remove("dqueue_0.db")
	db := NewInstance("dqueue_0.db", 0)
	if db == nil {
		t.Fail()
	}
	db.writeInt32(128)

	db = NewInstance("dqueue_0.db", 0)
	if db == nil {
		t.Fail()
	}
	i, err := db.readInt32()
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if i != 128 {
		t.Log("read:", i)
		t.Log("w:", db.w)
		t.Log("r:", db.r)
		t.Fail()
	}
}

func Test_writeReadInt32NoClose(t *testing.T) {
	os.Remove("dqueue_0.db")
	db := NewInstance("dqueue_0.db", 0)
	if db == nil {
		t.Fail()
	}
	db.writeInt32(128)
	i, err := db.readInt32()
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if i != 128 {
		t.Log("read:", i)
		t.Log("w:", db.w)
		t.Log("r:", db.r)
		t.Fail()
	}
}

func Benchmark_writeReadInt32(b *testing.B) {
	os.Remove("dqueue_0.db")
	db := NewInstance("dqueue_0.db", 0)
	if db == nil {
		b.Fail()
	}
	for i := 0; i < b.N; i++ {
		db.writeInt32(i)
		r, err := db.readInt32()
		if err != nil || r != i {
			b.Log(err)
			b.Fail()
		}
	}
}

func Test_WriteRead(t *testing.T) {
	os.Remove("dqueue_0.db")
	db := NewInstance("dqueue_0.db", 0)
	if db == nil {
		t.Fail()
	}
	db.Write([]byte("test"))
	bs, err := db.Read()
	if err != nil {
		t.Fail()
	}
	for k, v := range []byte("test") {
		if v != bs[k] {
			t.Fail()
		}
	}
}

func Benchmark_WriteRead(b *testing.B) {
	os.Remove("dqueue_0.db")
	db := NewInstance("dqueue_0.db", 0)
	if db == nil {
		b.Fail()
	}
	for i := 0; i < b.N; i++ {
		db.Write([]byte(fmt.Sprintf("%d", i)))
		bs, err := db.Read()
		if err != nil {
			b.Fail()
		}
		for k, v := range []byte(fmt.Sprintf("%d", i)) {
			if v != bs[k] {
				b.Fail()
			}
		}
	}
}

func Test_SetWriteRead(t *testing.T) {
	os.Remove("dqueue_0.db")
	db := NewInstance("dqueue_0.db", 0)
	if db == nil {
		t.Fail()
	}
	db.Write([]byte("abc"))
	db.Write([]byte("def"))
	pos := db.GetWritePos()
	db = NewInstance("dqueue_0.db", 0)
	db.SetWritePos(pos)
	bs, err := db.Read()
	if err != nil {
		t.Fail()
	}
	for k, v := range []byte("abc") {
		if v != bs[k] {
			t.Fail()
		}
	}
	bs, err = db.Read()
	if err != nil {
		t.Fail()
	}
	for k, v := range []byte("def") {
		if v != bs[k] {
			t.Fail()
		}
	}
}

func Test_SetWriteRead2(t *testing.T) {
	os.Remove("dqueue_0.db")
	db := NewInstance("dqueue_0.db", 0)
	if db == nil {
		t.Fail()
	}
	db.Write([]byte("abc"))
	db.Read()
	db.Write([]byte("def"))
	pos := db.GetWritePos()
	rpos := db.GetReadPos()
	db = NewInstance("dqueue_0.db", 0)
	db.SetWritePos(pos)
	db.SetReadPos(rpos)
	bs, err := db.Read()
	if err != nil {
		t.Fail()
	}
	for k, v := range []byte("def") {
		if v != bs[k] {
			t.Fail()
		}
	}
}

func Test_ReadUntilEmpty(t *testing.T) {
	os.Remove("dqueue_0.db")
	db := NewInstance("dqueue_0.db", 0)
	if db == nil {
		t.Fail()
	}
	db.Write([]byte("1000"))
	bs, err := db.Read()
	if err != nil {
		t.Fail()
	}
	for k, v := range []byte("1000") {
		if v != bs[k] {
			t.Fail()
		}
	}

	_, err = db.Read()
	if err.Error() != EEMPTY {
		t.Fail()
	}
}

func Test_WriteEmpty(t *testing.T) {
	os.Remove("dqueue_0.db")
	db := NewInstance("dqueue_0.db", 0)
	if db == nil {
		t.Fail()
	}
	bs := make([]byte, 1020)
	for i := 0; i < len(bs); i++ {
		bs[i] = 1
	}
	for i := 0; i < 1024; i++ {
		err := db.Write(bs)
		if err != nil {
			t.Log(err)
			t.Fail()
		}
	}
	err := db.Write(bs)
	if err == nil {
		t.Fail()
	}
}
