package commands

import (
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Commit struct {
	CommandBase
	request *kvrpcpb.CommitRequest
}

func NewCommit(request *kvrpcpb.CommitRequest) Commit {
	return Commit{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

func (c *Commit) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	commitTs := c.request.CommitVersion
	// YOUR CODE HERE (lab2).
	// Check if the commitTs is invalid, the commitTs must be greater than the transaction startTs. If not
	// report unexpected error.
	// panic("PrepareWrites is not implemented for commit command")
	response := new(kvrpcpb.CommitResponse)
	if commitTs<=txn.StartTS {
		return  response,errors.New("unexpected error")
	}
	
	// Commit each key.
	for _, k := range c.request.Keys {
		resp, e := commitKey(k, commitTs, txn, response)
		if resp != nil || e != nil {
			return response, e
		}
	}

	return response, nil
}

func commitKey(key []byte, commitTs uint64, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}

	// If there is no correspond lock for this transaction.
	// panic("commitKey is not implemented yet")
	log.Debug("commitKey", zap.Uint64("startTS", txn.StartTS),
		zap.Uint64("commitTs", commitTs),
		zap.String("key", hex.EncodeToString(key)))
	if lock == nil || lock.Ts != txn.StartTS {
		// YOUR CODE HERE (lab2).
		// Key is locked by a different transaction, or there is no lock on the key. It's needed to
		// check the commit/rollback record for this key, if nothing is found report lock not found
		// error. Also the commit request could be stale that it's already committed or rolled back.
		// log.Info("here")
		// if lock !=nil {
		// 	log.Info(fmt.Sprintf("lock.kind %d",lock.Kind))
		// }
		// if lock != nil && (lock.Kind == mvcc.WriteKindPut || lock.Kind==mvcc.WriteKindRollback) {
		// 	log.Info(fmt.Sprintf("lock.kind %d",lock.Kind))
		// 	respValue := reflect.ValueOf(response)
		// 	keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("request is stale %v", key)}
		// 	reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
		// 	return response,nil
		// }
		if lock == nil {
			write,ts,err := txn.CurrentWrite(key)
			log.Info(fmt.Sprintf("write ts : %d",ts))
			if err != nil {
				return nil,err
			}
			if write != nil && write.StartTS == txn.StartTS && (write.Kind == mvcc.WriteKindPut || write.Kind == mvcc.WriteKindDelete){
				log.Info("has written before")
				return nil,nil
			}
			if write != nil && write.Kind == mvcc.WriteKindRollback {
				log.Info("rollbacked")
				// return nil,nil
			}
			respValue := reflect.ValueOf(response)
			keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("request is stale %v", key)}
			reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
			return response,nil
		}
		log.Info("lock not found for key")
		respValue := reflect.ValueOf(response)
		keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("lock not found for key %v", key)}
		reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
		return response, nil
	}
	log.Info(fmt.Sprintf("commit succeed ts:%d ttl:%d primary%d txn_st:%d commit_ts:%d",lock.Ts,lock.Ttl,lock.Primary,txn.StartTS,commitTs))
	// Commit a Write object to the DB
	write := mvcc.Write{StartTS: txn.StartTS, Kind: lock.Kind}
	txn.PutWrite(key, commitTs, &write)
	// Unlock the key
	txn.DeleteLock(key)

	return nil, nil
}

func (c *Commit) WillWrite() [][]byte {
	return c.request.Keys
}
