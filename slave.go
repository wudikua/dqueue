package main

import (
	"github.com/wudikua/dqueue/replication"
)

func main() {
	instance, _ := replication.NewDQueueReplication(":9008")
	instance.SyncDQueue("redis-buffering")
}
