package idx

import (
	"os"
	"testing"
)

func Test_NewInstance(t *testing.T) {
	os.Remove("dqueue.idx")
	idx := NewInstance("dqueue.idx")
	if idx == nil {
		t.Fail()
	}
	if idx.readNo == 0 {
		t.Fail()
	}

	idx = NewInstance("dqueue.idx")
	if idx == nil {
		t.Fail()
	}
	if idx.readNo == 0 {
		t.Fail()
	}
}

func Test_setReadNo(t *testing.T) {
	os.Remove("dqueue.idx")
	idx := NewInstance("dqueue.idx")
	if idx == nil {
		t.Fail()
	}
	idx.SetReadNo(100)
	if idx.readNo != 100 {
		t.Fail()
	}
	idx = NewInstance("dqueue.idx")
	if idx == nil {
		t.Fail()
	}
	if idx.readNo != 100 {
		t.Fail()
	}
}

func Test_setReadIndex(t *testing.T) {
	os.Remove("dqueue.idx")
	idx := NewInstance("dqueue.idx")
	if idx == nil {
		t.Fail()
	}
	idx.SetReadIndex(100)
	if idx.readIndex != 100 {
		t.Fail()
	}
	idx = NewInstance("dqueue.idx")
	if idx == nil {
		t.Fail()
	}
	if idx.readIndex != 100 {
		t.Fail()
	}
}

func Test_setWriteNo(t *testing.T) {
	os.Remove("dqueue.idx")
	idx := NewInstance("dqueue.idx")
	if idx == nil {
		t.Fail()
	}
	idx.SetWriteNo(100)
	if idx.writeNo != 100 {
		t.Fail()
	}
	idx = NewInstance("dqueue.idx")
	if idx == nil {
		t.Fail()
	}
	if idx.writeNo != 100 {
		t.Fail()
	}
}

func Test_setWriteIndex(t *testing.T) {
	os.Remove("dqueue.idx")
	idx := NewInstance("dqueue.idx")
	if idx == nil {
		t.Fail()
	}
	idx.SetWriteIndex(100)
	if idx.writeIndex != 100 {
		t.Fail()
	}
	idx = NewInstance("dqueue.idx")
	if idx == nil {
		t.Fail()
	}
	if idx.writeIndex != 100 {
		t.Fail()
	}
}

func Benchmark_NewInstance(b *testing.B) {
	os.Remove("dqueue.idx")
	for i := 0; i < b.N; i++ {
		idx := NewInstance("dqueue.idx")
		if idx == nil {
			b.Fail()
		}
	}
}

func Benchmark_readMagic(b *testing.B) {
	os.Remove("dqueue.idx")
	idx := NewInstance("dqueue.idx")
	if idx == nil {
		b.Fail()
	}
	for i := 0; i < b.N; i++ {
		idx.readMagic()
	}
}

func Benchmark_writeMagic(b *testing.B) {
	os.Remove("dqueue.idx")
	idx := NewInstance("dqueue.idx")
	if idx == nil {
		b.Fail()
	}
	for i := 0; i < b.N; i++ {
		idx.writeMagic()
	}
}

func Benchmark_setWriteIndex(b *testing.B) {
	os.Remove("dqueue.idx")
	idx := NewInstance("dqueue.idx")
	if idx == nil {
		b.Fail()
	}
	for i := 0; i < b.N; i++ {
		idx.SetWriteIndex(100)
	}
}

func Benchmark_initIndexInfoFromFile(b *testing.B) {
	os.Remove("dqueue.idx")
	idx := NewInstance("dqueue.idx")
	if idx == nil || idx.readNo != 1 {
		b.Fail()
	}
	idx = NewInstance("dqueue.idx")
	if idx == nil || idx.readNo != 1 {
		b.Fail()
	}
	for i := 0; i < b.N; i++ {
		idx.initIndexInfoFromFile()
	}
}
