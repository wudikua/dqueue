package replication

import (
	"testing"
)

// func Test_NewDQueueReplication(t *testing.T) {
// 	instance, err := NewDQueueReplication(":9008")
// 	if err != nil || instance == nil {
// 		t.Fail()
// 	}
// }

// func Test_Greet(t *testing.T) {
// 	instance, err := NewDQueueReplication(":9008")
// 	if err != nil {
// 		t.Fail()
// 	}
// 	queues, err := instance.Greet()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	t.Log(queues)
// }

func Test_SyncDQueue(t *testing.T) {
	instance, err := NewDQueueReplication(":9008")
	if err != nil {
		t.Fail()
	}
	instance.SyncDQueue("redis-buffering")
}
