package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
)

var rpos = 0

func readInt32(r *bufio.Reader) (int, error) {
	bs := make([]byte, 4)
	n, err := r.Read(bs)

	if err != nil {
		return n, err
	}
	if n < 4 {
		return n, err
	}
	rpos += n
	return int(binary.BigEndian.Uint32(bs)), nil
}

func main() {
	var file string

	flag.StringVar(&file, "f", "db file", "db file")
	flag.Parse()

	fpr, _ := os.OpenFile(file, os.O_RDWR, 0666)
	fos := bufio.NewReader(fpr)
	for {
		cur := rpos
		next, err := readInt32(fos)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("header length", next)
		length := next - cur - 4
		fmt.Println("data length", length)
		bs := make([]byte, length)
		n, err := io.ReadFull(fos, bs)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(string(bs))
		rpos += n
		fmt.Println()
	}
}
