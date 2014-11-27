## 简介

* 之前写的taskbuffering的aofutil是个简易的写磁盘的工具，在这个基础上重新写了dqueue(disk based queue)，dqueue是一个基于顺序写文件的队列，包装了一层redis协议
* 通过一个索引文件，和多个数据文件来组织数据，数据文件按照1MB分成N个文件，自动删除已经消费的队列数据文件
* 从库会订阅主库的变更，主库异步来发布自己变更，但是不保证从库和主库的一致，也就是不需要从库的ack。数据文件的同步，因为本身就是顺序写，所以和binlog基本没区别，直接发送到从库，但是写入和消费的进度没法这么同步，因为这个是覆盖写，没法做到异步发送，同步发送又会阻塞本身，可以也就是必须保存变更的一个历史，如果写LOG感觉有点冗余，写BUFFER当从库延迟的时候还是会出问题。kafka是通过zookeeper来保存写入与消费的进度，没这个问题。为了简单，dqueue使用每隔一段时间同步一次消费进度，这样最坏的情况丢失一段时间的读写进度，写进度丢失会覆盖写，读进度丢失会重复读

## 使用

### 下载源码
* 确定已经设置了GOPATH
* go get github.com/wudikua/dqueue

### 下载依赖
* go get github.com/julienschmidt/httprouter 一个http服务
* go get github.com/wudikua/go-redis-server 一个redis代理

### 启动
* go run src/main.go

##测试

```
$data = array(
        'host'=>'prism001.m6',
        'port'=>'1807',
        'op'=>'RPUSH',
        'key'=>'new-log',
        'val'=>'999999999'
);
$redis = new Redis();
$redis->connect('127.0.0.1', 9008);
$re = $redis->rPush("buffering-redis", json_encode($data));
var_dump($re);
$re = $redis->rPop("buffering-redis");
var_dump($re);
```

## 对于队列写入性能测试
* cd src/fs 
* rm -rf test/ && go test -bench=".*"

```
mengjundeMacBook-Pro:fs mengjun$ rm -rf test/ && go test -bench=".*"
PASS
Benchmark_PushAndPop      500000	      6646 ns/op
ok  	fs	3.410s
```

## 可用性
* 通过主库的异步写从库来做replication保证可用，还没完善，计划是生成快照，然后增量同步

## TODO
* 更多的错误处理以及日志
* 队列长度管理 done
* 定时清理消费完的数据文件
* PUB SUB支持
* 集群和可用性 
* 优化写性能,flush的策略问题

