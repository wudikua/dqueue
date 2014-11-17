package proxy

import (
	"encoding/json"
	"flag"
	"fmt"
	"fs"
	redis "github.com/docker/go-redis-server"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"os"
	"os/signal"
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
	v, _ := h.queues[key].Pop()
	return v, nil
}

func (h *DQueueHandler) RPUSH(key string, value []byte) (int, error) {
	if h.queues[key] == nil {
		h.queues[key] = fs.NewInstance(key)
	}
	h.queues[key].Push(value)
	return 1, nil
}

func ListenAndServeRedis() {
	var host string
	var port int
	flag.StringVar(&host, "h", "127.0.0.1", "host")
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

	server.ListenAndServe()
}

func Destory() {

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
