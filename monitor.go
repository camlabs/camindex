package indexer

import (
	"encoding/binary"
	"time"

	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/camlabs/camgo/rpc"
	"github.com/syndtr/goleveldb/leveldb"
)

var key = []byte("key")

// Monitor indexer server
type Monitor struct {
	slf4go.Logger
	client       *rpc.Client
	etl          *ETL
	pullDuration time.Duration
	db           *leveldb.DB
}

// NewMonitor .
func NewMonitor(conf *config.Config) (*Monitor, error) {
	client := rpc.NewClient(conf.GetString("indexer.camrpc", "http://localhost:16332"))

	etl, err := newETL(conf)

	if err != nil {
		return nil, err
	}

	db, err := leveldb.OpenFile(conf.GetString("indexer.localdb", "./cursor"), nil)

	if err != nil {
		return nil, err
	}

	startindexer := uint64(conf.GetInt64("indexer.start", 0))

	monitor := &Monitor{
		Logger:       slf4go.Get("cam-monitor"),
		client:       client,
		etl:          etl,
		pullDuration: time.Second * conf.GetDuration("indexer.pull", 4),
		db:           db,
	}

	if monitor.getCursor() < startindexer {
		if err := monitor.setCursor(startindexer); err != nil {
			return nil, err
		}

	}

	return monitor, nil
}

// Run .
func (monitor *Monitor) Run() {
	ticker := time.NewTicker(monitor.pullDuration)

	for range ticker.C {
		monitor.DebugF("fetch geth last block number ...")
		blocks, err := monitor.client.GetBlockCount()
		monitor.DebugF("fetch geth last block number -- success, %d", blocks)
		if err != nil {
			monitor.ErrorF("fetch geth blocks err, %s", err)
		}

		for monitor.getCursor() < uint64(blocks) {
			if err := monitor.fetchBlock(); err != nil {
				break
			}
		}
	}
}

func (monitor *Monitor) fetchBlock() error {

	blockNumber := monitor.getCursor()

	monitor.DebugF("fetch block(%d) ...", blockNumber)

	block, err := monitor.client.GetBlockByIndex(int64(blockNumber))

	if err != nil {
		monitor.ErrorF("fetch geth block(%d) err, %s", blockNumber, err)
		return err
	}

	monitor.DebugF("fetch block(%d) -- success", blockNumber)

	monitor.DebugF("etl handle block(%d) ...", blockNumber)

	if err := monitor.etl.Handle(block); err != nil {
		monitor.ErrorF("etl handle geth block(%d) err, %s", blockNumber, err)
		return err
	}

	monitor.DebugF("etl handle block(%d) -- success", blockNumber)

	if err := monitor.setCursor(blockNumber + 1); err != nil {
		monitor.ErrorF("monitor set cursor(%d) err, %s", blockNumber, err)
		return err
	}

	return nil
}

func (monitor *Monitor) getCursor() uint64 {
	buff, err := monitor.db.Get(key, nil)

	if err != nil {
		monitor.ErrorF("get Monitor local cursor error :%s", err)
		return 0
	}

	if buff == nil {
		monitor.ErrorF("get Monitor local cursor error : cursor not exists")
		return 0
	}

	return binary.BigEndian.Uint64(buff)
}

func (monitor *Monitor) setCursor(cursor uint64) error {
	buff := make([]byte, 8)
	binary.BigEndian.PutUint64(buff, cursor)

	return monitor.db.Put(key, buff, nil)
}
