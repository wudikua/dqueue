package fs

import (
	"fmt"
	"os"
	"testing"
	// "time"
)

func Test_NewInstance(t *testing.T) {
	fs := NewInstance("test")
	if fs == nil {
		t.Fail()
	}
}

func Test_Push(t *testing.T) {
	fs := NewInstance("test")
	if fs == nil {
		t.Fail()
	}
	if _, err := fs.Push([]byte("abc")); err != nil {
		t.Fail()
	}
}

func Test_PushAndPop(t *testing.T) {
	os.Remove("test/dqueue.idx")
	os.Remove("test/dqueue_1.db")
	fs := NewInstance("test")
	if fs == nil {
		t.Fail()
	}
	if _, err := fs.Push([]byte("abc")); err != nil {
		t.Fail()
	}
	_, bs, err := fs.Pop()
	if err != nil {
		t.Fail()
	}
	for k, v := range []byte("abc") {
		if v != bs[k] {
			t.Fail()
		}
	}
}

func Test_PopEmpty(t *testing.T) {
	os.Remove("test/dqueue.idx")
	os.Remove("test/dqueue_1.db")
	fs := NewInstance("test")
	if fs == nil {
		t.Fail()
	}
	if _, err := fs.Push([]byte("abc")); err != nil {
		t.Fail()
	}
	_, bs, err := fs.Pop()
	if string(bs) != "abc" {
		t.Log(string(bs))
		t.Fail()
	}
	_, _, err = fs.Pop()
	if err == nil {
		t.Fail()
	}
}

func Test_PushSlice(t *testing.T) {
	os.Remove("test/dqueue.idx")
	os.Remove("test/dqueue_1.db")
	fs := NewInstance("test")
	if fs == nil {
		t.Fail()
	}
	for i := 0; i < 1; i++ {
		if _, err := fs.Push([]byte("abc")); err != nil {
			t.Fail()
		}
		_, bs, err := fs.Pop()
		if err != nil {
			t.Fail()
		}
		t.Log(string(bs))
	}
}

func Benchmark_PushAndPop(b *testing.B) {
	os.Remove("test/dqueue.idx")
	os.Remove("test/dqueue_1.db")
	fs := NewInstance("test")
	if fs == nil {
		b.Fail()
	}
	for i := 0; i < b.N; i++ {
		_, err := fs.Push([]byte(fmt.Sprintf("%d", i)))
		if err != nil {
			b.Fail()
		}

		_, bs, err := fs.Pop()
		if err != nil {
			b.Log(err)
			b.Fail()
		}
		for k, v := range []byte(fmt.Sprintf("%d", i)) {
			if v != bs[k] {
				b.Log(string(bs))
				b.Fail()
			}
		}
	}
}

func Benchmark_Push(b *testing.B) {
	os.Remove("test/dqueue.idx")
	os.Remove("test/dqueue_1.db")
	fs := NewInstance("test")
	if fs == nil {
		b.Fail()
	}
	bs := make([]byte, 1024)
	for i, _ := range bs {
		bs[i] = 'a'
	}
	for i := 0; i < b.N; i++ {
		_, err := fs.Push(bs)
		if err != nil {
			b.Fail()
		}
	}
}
