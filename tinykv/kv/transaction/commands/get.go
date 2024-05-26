package commands

import (
	"encoding/hex"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Get struct {
	ReadOnly
	CommandBase
	request *kvrpcpb.GetRequest
}

func NewGet(request *kvrpcpb.GetRequest) Get {
	return Get{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.Version,
		},
		request: request,
	}
}

func (g *Get) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	key := g.request.Key
	log.Debug("read key", zap.Uint64("start_ts", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))
	response := new(kvrpcpb.GetResponse)

	// panic("kv get is not implemented yet")
	// YOUR CODE HERE (lab2).
	// Check for locks and their visibilities.
	// Hint: Check the interfaces provided by `mvcc.RoTxn`.
	lock,err:=txn.GetLock(key)
	if err != nil {
		return response,nil,err
	}
	// if lock.IsLockedFor(key,txn.StartTS,response) {
	// 	return response,nil,nil
	// }
	if lock !=nil && lock.Ts<txn.StartTS {
		response.NotFound = true
		response.Error = &kvrpcpb.KeyError{Locked: lock.Info(key)}
		return response,nil,nil
	}
	// YOUR CODE HERE (lab2).
	// Search writes for a committed value, set results in the response.
	// Hint: Check the interfaces provided by `mvcc.RoTxn`.
	value,err:=txn.GetValue(key)
	if err != nil {
		return nil,nil,err
	}
	response.Value=value
	if value == nil {
		response.NotFound = true
	}
	log.Info("Get read")
	return response, nil, nil
}
