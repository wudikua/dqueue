package main

import (
	"github.com/wudikua/dqueue/proxy"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	proxy.ListenAndServeRedis()
}
