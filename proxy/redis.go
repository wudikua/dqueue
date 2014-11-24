package proxy

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/wudikua/dqueue/fs"
	redis "github.com/wudikua/go-redis-server"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
)

type DQueueHandler struct {
	queues map[string]*fs.DQueueFs
	sub    map[string][]*redis.ChannelWriter
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
	length, err := h.queues[key].Push(value)
	// 同步到从库
	v, exists := h.sub[key]
	if exists {
		for _, c := range v {
			select {
			case c.Channel <- []interface{}{
				"message",
				key,
				value,
			}:
			default:
			}
		}
	}

	return length, err
}

func (h *DQueueHandler) GREET() ([]byte, error) {
	status := make([]string, len(h.queues))
	i := 0
	for queueName, _ := range handler.queues {
		status[i] = queueName
		i++
	}
	b, err := json.Marshal(status)
	return b, err
}

func (h *DQueueHandler) SUBSCRIBE(channels ...[]byte) (*redis.MultiChannelWriter, error) {
	ret := &redis.MultiChannelWriter{
		Chans: make([]*redis.ChannelWriter, 0, len(channels)),
	}
	// 订阅多个channels
	for _, key := range channels {
		cw := &redis.ChannelWriter{
			FirstReply: []interface{}{
				"subscribe",
				key,
				1,
			},
			Channel: make(chan []interface{}),
		}
		if h.sub[string(key)] == nil {
			// 当前channel一个订阅者
			h.sub[string(key)] = []*redis.ChannelWriter{cw}
		} else {
			// 当前channel多个订阅者
			h.sub[string(key)] = append(h.sub[string(key)], cw)
		}
		ret.Chans = append(ret.Chans, cw)
	}
	return ret, nil
}

func (h *DQueueHandler) PUBLISH(key string, value []byte) (int, error) {
	v, exists := h.sub[key]
	if !exists {
		return 0, nil
	}
	i := 0
	for _, c := range v {
		select {
		case c.Channel <- []interface{}{
			"message",
			key,
			value,
		}:
			i++
		default:
		}
	}
	return i, nil
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
		sub:    make(map[string][]*redis.ChannelWriter, 1),
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
