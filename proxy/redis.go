package proxy

import (
	"encoding/json"
	"flag"
	"fmt"
	redis "github.com/docker/go-redis-server"
	"github.com/julienschmidt/httprouter"
	"github.com/wudikua/dqueue/fs"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
)

type DQueueHandler struct {
	queues map[string]*fs.DQueueFs
}

var handler *DQueueHandler

func (h *DQueueHandler) RPOP(key string) ([]byte, error) {
	if h.queues[key] == nil {
		h.queues[key] = fs.NewInstance(key)
	}
	_, v, _ := h.queues[key].Pop()
	return v, nil
}

func (h *DQueueHandler) RPUSH(key string, value []byte) (int, error) {
	if h.queues[key] == nil {
		h.queues[key] = fs.NewInstance(key)
	}
	return h.queues[key].Push(value)
}

func ListenAndServeRedis() {
	var host string
	var port int
	flag.StringVar(&host, "h", "127.0.0.1", "host")
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	flag.IntVar(&port, "p", 9008, "port")
	flag.Parse()

	// 启动redis server
	handler = &DQueueHandler{
		queues: make(map[string]*fs.DQueueFs, 1),
	}
	server, _ := redis.NewServer(redis.DefaultConfig().Proto("tcp").Host(host).Port(port).Handler(handler))

	// 处理信号量
	go sigHandler()

	// 服务状态信息
	router := httprouter.New()
	router.GET("/status", Status)
	go http.ListenAndServe(":8080", router)

	// 性能分析
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
	}

	go http.ListenAndServe(":8081", nil)

	server.ListenAndServe()
}

func Destory() {
	pprof.StopCPUProfile()
}

func sigHandler() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGHUP, os.Interrupt)
	for {
		select {
		case sig := <-ch:
			Destory()
			fmt.Println(sig)
			os.Exit(0)
			break
		}
	}
}

func Status(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	status := make(map[string]interface{})
	for queueName, queue := range handler.queues {
		status[queueName] = queue.Stats()
	}
	b, _ := json.Marshal(status)

	w.Write(b)
}
