package indexer

import (
	// "os"

	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"math/rand"

	"bytes"
	"net/http"

	"github.com/btcsuite/btcutil/base58"
	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"

	// "github.com/camlabs/gomq"
	// gomqkafka "github.com/camlabs/gomq-kafka"
	"github.com/camlabs/camdb"
	"github.com/camlabs/camgo/rpc"

	"github.com/Jeffail/gabs"
)

// ETL .
type ETL struct {
	slf4go.Logger
	conf   *config.Config
	engine *xorm.Engine
	// mq     gomq.Producer // mq producer
	// topic  string
	client *rpc.Client
}

func newETL(conf *config.Config) (*ETL, error) {

	username := conf.GetString("order.camdb.username", "xxx")
	password := conf.GetString("order.camdb.password", "xxx")
	port := conf.GetString("order.camdb.port", "6543")
	host := conf.GetString("order.camdb.host", "localhost")
	scheme := conf.GetString("order.camdb.schema", "postgres")

	// engine, err := xorm.NewEngine(
	// 	"postgres",
	// 	fmt.Sprintf(
	// 		"user=%v password=%v host=%v dbname=%v port=%v sslmode=disable",
	// 		username, password, host, scheme, port,
	// 	),
	// )

	engine, err := xorm.NewEngine(
		"mysql",
		fmt.Sprintf(
			"%v:%v@(%v:%v)/%v?charset=utf8",
			username, password, host, port, scheme,
		),
	)

	// engine.ShowSQL(true)

	// fmt.Println(fmt.Sprintf(
	// 	"%v:%v@(%v:%v)/%v?charset=utf8",
	// 	username, password, host, port,scheme ,
	// ))

	//ping mysql数据库
	// err2 := engine.Ping()
	// if err2 != nil {
	// 	return nil,err2
	// }

	if err != nil {
		return nil, err
	}

	engine.SetConnMaxLifetime(time.Second * 60)
	engine.SetMaxIdleConns(20)
	engine.SetMaxOpenConns(20)

	// mq, err := gomqkafka.NewAliyunProducer(conf)

	// if err != nil {
	// 	return nil, err
	// }

	return &ETL{
		Logger: slf4go.Get("cam-indexer-etl"),
		conf:   conf,
		engine: engine,
		// mq:     mq,
		// topic:  conf.GetString("aliyun.kafka.topic", "xxxxx"),
		client: rpc.NewClient(conf.GetString("order.camrpc", "http://localhost:16332")),
	}, nil
}

// Handle handle eth block
func (etl *ETL) Handle(block *rpc.Block) error {

	etl.DebugF("block %d tx %d", block.Index, len(block.Transactions))

	for _, tx := range block.Transactions {
		etl.DebugF("tx %s vin %d vout %d claims %d", tx.ID, len(tx.Vin), len(tx.Vout), len(tx.Claims))
	}

	if err := etl.insertUTXOs(block); err != nil {
		// etl.DebugF("insert utxos error %s",err.Error())
		errMsg := err.Error()
		//如果是索引引发的错误则忽略
		if strings.Contains(errMsg, "t_x_n") {
			etl.DebugF("Duplicate entry for key error, ignore ")
		} else {
			return err
		}

	}

	if err := etl.spentUTXOs(block); err != nil {
		return err
	}

	if err := etl.claimUTXOs(block); err != nil {
		return err
	}

	if err := etl.insertTx(block); err != nil {
		errMsg := err.Error()
		//如果是索引引发的错误则忽略
		if strings.Contains(errMsg, "t_x_from_to_asset") {
			etl.DebugF("Duplicate entry for key error, ignore ")
		} else {
			return err
		}
	}

	if err := etl.insertBlock(block); err != nil {
		errMsg := err.Error()
		//如果是索引引发的错误则忽略
		if strings.Contains(errMsg, "block_index") {
			etl.DebugF("Duplicate entry for key error, ignore ")
		} else if strings.Contains(errMsg, "PRIMARY") {
			etl.DebugF("Duplicate entry for key error, ignore ")
		} else {
			return err
		}
	}

	// if block.Index > 196{
	// 	os.Exit(1)
	// }

	//添加推送
	for _, tx := range block.Transactions {
		// if err := etl.mq.Produce(etl.topic, []byte(tx.ID), tx.ID); err != nil {
		// 	etl.ErrorF("mq insert tx %s err :%s", tx.ID, err)
		// 	return err
		// }

		txid := tx.ID

		err := etl.confirm(txid)
		if err != nil {
			// println(err.Error())
			errMsg := err.Error()
			//如果是索引引发的错误则忽略
			if strings.Contains(errMsg, "t_x_from_to_asset") {
				etl.DebugF("Duplicate entry for key error, ignore ")
			} else {
				return err
			}
		}

		etl.DebugF("tx %s event send", tx.ID)
	}

	return nil
}

func (etl *ETL) insertBlock(block *rpc.Block) (err error) {
	sysfee := float64(0)
	netfee := float64(0)

	for _, tx := range block.Transactions {
		fee, err := strconv.ParseFloat(tx.SysFee, 8)

		if err != nil {
			etl.ErrorF("parse tx(%s) sysfee(%s) err, %s", tx.ID, tx.SysFee, err)
			continue
		}

		sysfee += fee

		fee, err = strconv.ParseFloat(tx.NetFee, 8)

		if err != nil {
			etl.ErrorF("parse tx(%s) netfee(%s) err, %s", tx.ID, tx.NetFee, err)
			continue
		}

		netfee += fee
	}

	_, err = etl.engine.Insert(&camdb.Block{
		Block:      block.Index,
		SysFee:     sysfee,
		NetFee:     netfee,
		CreateTime: block.Time,
	})

	return err
}

func sysFeeToString(f float64) string {
	data := fmt.Sprintf("%.1f", f)

	data = data[0 : len(data)-2]

	return data
}

func netFeeToString(f float64) string {
	data := fmt.Sprintf("%.9f", f)

	data = data[0 : len(data)-1]

	return data
}

func (etl *ETL) insertTx(block *rpc.Block) (err error) {
	utxos := make([]*camdb.Tx, 0)

	for _, tx := range block.Transactions {

		from := ""

		if len(tx.Vin) > 0 {
			rawtx, err := etl.client.GetRawTransaction(tx.Vin[0].TransactionID)

			if err != nil {
				etl.ErrorF("get tx %s vin error %s", tx.ID, err)
				return err
			}

			from = rawtx.Vout[tx.Vin[0].Vout].Address
		}

		if tx.Type == "InvocationTransaction" {
			log, err := etl.client.ApplicationLog(tx.ID)

			if err != nil {
				etl.ErrorF("get application %s log error %s", tx.ID, err)
				continue
			}

			if strings.Contains(log.State, "FAULT") {
				goto NEXT
			}

			for _, notification := range log.Notifications {
				contract := notification.Contract

				//如果是字符串，说明不是cac20资产，cac20资产的结果state是对象
				_, ok := notification.State.Value.(string)

				if ok {
					continue
				}

				data, err := json.Marshal(notification.State.Value)

				if err != nil {
					etl.ErrorF("decode nep5 err, %s", err)
					continue
				}

				var values []*rpc.ValueN

				err = json.Unmarshal(data, &values)

				if err != nil {
					etl.ErrorF("decode nep5 value err , %s", err)
					continue
				}

				//必须有4项参数，transfer from to value
				if len(values) != 4 {
					continue
				}

				//调用函数 transfer
				if values[0].Value != "7472616e73666572" {
					continue
				}

				//from
				fromBytes, err := hex.DecodeString(values[1].Value)

				if err != nil {
					etl.ErrorF("decode nep5 from address %s error, %s", values[1].Value, err)
					continue
				}

				//确认地址是23版本
				from := base58.CheckEncode(fromBytes, 0x17)

				//to 地址解析
				toBytes, err := hex.DecodeString(values[2].Value)

				if err != nil {
					etl.ErrorF("decode nep5 to address %s error, %s", values[2].Value, err)
					continue
				}

				to := base58.CheckEncode(toBytes, 0x17)

				//转了多少钱
				var value string

				if values[3].Type == "ByteArray" {
					valueBytes, err := hex.DecodeString(values[3].Value)

					if err != nil {
						etl.ErrorF("decode nep5 transfer value %s error, %s", values[3].Value, err)
						continue
					}

					valueBytes = reverseBytes(valueBytes)

					value = fmt.Sprintf("%d", new(big.Int).SetBytes(valueBytes))

				} else {
					value = values[3].Value
				}

				utxos = append(utxos, &camdb.Tx{
					TX:         tx.ID,
					Block:      uint64(block.Index),
					From:       from,
					To:         to,
					Asset:      contract,
					Value:      value,
					CreateTime: block.Time,
				})
			}

		}

	NEXT:

		for _, vout := range tx.Vout {

			if len(tx.Claims) > 0 {
				from = vout.Address
			}

			// utxos = append(utxos, &camdb.Tx{
			// 	TX:         tx.ID,
			// 	Block:      uint64(block.Index),
			// 	From:       from,
			// 	To:         vout.Address,
			// 	Asset:      vout.Asset,
			// 	Value:      vout.Value,
			// 	CreateTime: block.Time,
			// })
			_, err := etl.engine.Insert(&camdb.Tx{
				TX:         tx.ID,
				TxIndex:    vout.N,
				Block:      uint64(block.Index),
				From:       from,
				To:         vout.Address,
				Asset:      vout.Asset,
				Value:      vout.Value,
				CreateTime: block.Time,
			})
			if err != nil {
				println(tx.ID + "===from:" + from + "===to:" + vout.Address + err.Error())
			}

			// if len(utxos) >= 100 {
			// if err := etl.batchInsertTx(utxos); err != nil {
			// 	return err
			// }

			// for _, utxo := range utxos {
			// 	etl.DebugF("create tx %s from %s to %s", utxo.TX, utxo.From, utxo.To)
			// }

			// utxos = make([]*camdb.Tx, 0)
			// }
		}
	}

	// if len(utxos) > 0 {
	// 	if err := etl.batchInsertTx(utxos); err != nil {
	// 		return err
	// 	}

	// 	for _, utxo := range utxos {
	// 		etl.DebugF("create tx %s from %s to %s", utxo.TX, utxo.From, utxo.To)
	// 	}
	// }

	return nil
}

func (etl *ETL) batchInsertTx(rows []*camdb.Tx) (err error) {
	session := etl.engine.NewSession()

	session.Begin()

	defer func() {
		if err != nil {
			session.Rollback()
		} else {
			session.Commit()
		}
	}()

	_, err = etl.engine.Insert(&rows)

	return
}

func reverseBytes(s []byte) []byte {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}

	return s
}

func (etl *ETL) spentUTXOs(block *rpc.Block) (err error) {
	utxos := make([]*camdb.UTXO, 0)

	for _, tx := range block.Transactions {

		// spentTime := time.Unix(block.Time, 0)

		for _, vin := range tx.Vin {
			utxos = append(utxos, &camdb.UTXO{
				TX:         vin.TransactionID,
				N:          vin.Vout,
				SpentTime:  block.Time,
				SpentBlock: block.Index,
			})

			if len(utxos) >= 100 {
				if err := etl.updateUTXOs(utxos, "spent_block", "spent_time"); err != nil {
					return err
				}

				for _, utxo := range utxos {
					etl.DebugF("spent utxo %s %d", utxo.TX, utxo.N)
				}

				utxos = make([]*camdb.UTXO, 0)
			}
		}
	}

	if len(utxos) > 0 {
		if err := etl.updateUTXOs(utxos, "spent_block", "spent_time"); err != nil {
			return err
		}

		for _, utxo := range utxos {
			etl.DebugF("spent utxo %s %d", utxo.TX, utxo.N)
		}
	}

	return
}

func (etl *ETL) claimUTXOs(block *rpc.Block) (err error) {
	utxos := make([]*camdb.UTXO, 0)

	for _, tx := range block.Transactions {

		for _, claim := range tx.Claims {
			utxos = append(utxos, &camdb.UTXO{
				TX:      claim.TransactionID,
				N:       claim.Vout,
				Claimed: true,
			})

			if len(utxos) >= 100 {
				if err := etl.updateUTXOs(utxos, "claimed"); err != nil {
					return err
				}

				for _, utxo := range utxos {
					etl.DebugF("claim utxo %s %s", utxo.TX, utxo.N)
				}

				utxos = make([]*camdb.UTXO, 0)
			}
		}
	}

	if len(utxos) > 0 {
		if err := etl.updateUTXOs(utxos, "claimed"); err != nil {
			return err
		}

		for _, utxo := range utxos {
			etl.DebugF("claim utxo %s %s", utxo.TX, utxo.N)
		}
	}

	return
}

func (etl *ETL) updateUTXOs(utxos []*camdb.UTXO, cols ...string) (err error) {
	session := etl.engine.NewSession()

	session.Begin()

	defer func() {
		if err != nil {
			session.Rollback()
		} else {
			session.Commit()
		}
	}()

	for _, utxo := range utxos {
		_, err = session.Where("t_x = ? and n = ?", utxo.TX, utxo.N).Cols(cols...).Update(utxo)

		if err != nil {
			return
		}
	}

	return nil
}

func (etl *ETL) insertUTXOs(block *rpc.Block) error {

	etl.DebugF("start insert utxos")

	utxos := make([]*camdb.UTXO, 0)

	for _, tx := range block.Transactions {

		for _, vout := range tx.Vout {
			utxos = append(utxos, &camdb.UTXO{
				TX:          tx.ID,
				N:           vout.N,
				Address:     vout.Address,
				CreateBlock: block.Index,
				SpentBlock:  -1,
				Asset:       vout.Asset,
				Value:       vout.Value,
				CreateTime:  block.Time,
				SpentTime:   0,
				Claimed:     false,
			})
			fmt.Printf("%v==%v==%v", tx.ID, vout.N, vout.Address)

			//交易数量大于100，则进行批处理
			if len(utxos) >= 100 {
				if err := etl.batchInsert(utxos); err != nil {
					return err
				}

				for _, utxo := range utxos {
					etl.DebugF("create utxo %s %d", utxo.TX, utxo.N)
				}

				utxos = make([]*camdb.UTXO, 0)
			}
		}
	}

	//剩余100以内的再次进行批处理
	if len(utxos) > 0 {
		if err := etl.batchInsert(utxos); err != nil {
			return err
		}

		for _, utxo := range utxos {
			etl.DebugF("create utxo %s %d", utxo.TX, utxo.N)
		}
	}

	etl.DebugF("finish insert utxos")

	return nil
}

func (etl *ETL) batchInsert(rows []*camdb.UTXO) (err error) {
	session := etl.engine.NewSession()

	session.Begin()

	defer func() {
		if err != nil {
			session.Rollback()
		} else {
			session.Commit()
		}
	}()

	_, err = etl.engine.Insert(&rows)

	return
}

func (etl *ETL) confirm(txid string) error {
	// watcher.DebugF("handle tx %s", txid)

	//查找交易是否存在
	neoTxs := make([]*camdb.Tx, 0)

	err := etl.engine.Where("t_x = ?", txid).Find(&neoTxs)

	etl.DebugF("find tx %s", txid)
	//查询错误则返回错误
	//重新读取区块，并填入交易
	if err != nil {
		return err
	}

	//不存在记录则返回，不处理
	if len(neoTxs) == 0 {
		etl.WarnF("handle tx %s -- not found", txid)
		return nil
	}

	//存在记录则操作订单
	order := new(camdb.Order)

	order.ConfirmTime = neoTxs[0].CreateTime
	order.Block = int64(neoTxs[0].Block)
	order.Status = 2

	//尝试确认交易
	updated, err := etl.engine.Where("t_x = ?", txid).Cols("confirm_time", "block", "status").Update(order)

	//错误则返回错误
	if err != nil {

		etl.DebugF("update error %s", err.Error())

		return err
	}

	//更新成功，则不继续处理，还是继续添加，因为还有另外获取零钱的交易需要添加
	if updated != 0 {

		etl.DebugF("updated orders(%d) for tx %s", updated, txid)

		//通知web后台
		go etl.pushToWeb(txid)

		// return nil
	} else {
		etl.DebugF("update order but updated 0")
	}

	etl.DebugF("add order %s", txid)
	//准备添加订单，一般是收款方需要添加记录
	// var orders []*camdb.Order
	wallet := new(camdb.Wallet)

	//根据交易内容添加订单
	for _, tx := range neoTxs {

		etl.DebugF("tx.From %s and tx.To = %s", tx.From, tx.To)
		count, err := etl.engine.Where(`address = ? or address = ?`, tx.From, tx.To).Count(wallet)

		if err != nil {

			return err
		}

		etl.DebugF("find count %d %s", count, txid)

		if count > 0 {

			order := new(camdb.Order)

			order.Asset = tx.Asset
			order.From = tx.From
			order.To = tx.To
			order.TX = tx.TX
			order.TxIndex = tx.TxIndex
			order.Value = tx.Value
			order.CreateTime = tx.CreateTime
			order.ConfirmTime = tx.CreateTime
			order.Block = int64(tx.Block)
			//区块中的交易转订单，则为成功
			order.Status = 2
			// orders = append(orders, order)
			_, err = etl.engine.InsertOne(order)

			if err == nil {
				//之前没有更新通知，也没有添加交易通知
				if updated == 0 {
					//每个交易只通知一次
					updated = 1
					//如果没有更新，则
					go etl.pushToWeb(tx.TX)
				}
			}
		}
	}

	return nil
}

//向web后台推送消息
func (etl *ETL) pushToWeb(txid string) {

	//等待3s再推送
	time.Sleep(3 * time.Second)

	// webaddr string,token string,
	webaddr := etl.conf.GetString("order.webaddr", "http://127.0.0.1:5010")

	token := etl.conf.GetString("order.token", "123456789")

	etl.DebugF("push to web %v,txid : %v", webaddr, txid)

	var pushMsg *PushTxMessage = &PushTxMessage{}

	pushMsg.Action = "nodePushMessage"
	pushMsg.AppId = "1536040633"
	pushMsg.Format = "json"
	pushMsg.Method = "avoid"
	pushMsg.Module = "Tool"
	pushMsg.SignMethod = "md5"
	pushMsg.Nonce = string(rand.Intn(100000000))
	pushMsg.TransactionID = txid
	pushMsg.WalletTypeName = "cam"
	pushMsg.Handle = string(rand.Intn(100000000))
	pushMsg.Sign = md5Convert(pushMsg.Nonce + token + pushMsg.Handle)

	body, err := json.Marshal(pushMsg)

	if err != nil {
		return
	}

	if resp, err := http.Post(webaddr,
		"application/json",
		bytes.NewBuffer(body)); err == nil {
		jsonParsed, _ := gabs.ParseJSONBuffer(resp.Body)

		println(pushMsg.TransactionID + "推送成功:" + jsonParsed.String())

	} else {
		println(pushMsg.TransactionID + "推送失败:" + err.Error())
	}
}

func md5Convert(msg string) string {
	h := md5.New()
	h.Write([]byte(msg))
	cipherStr := h.Sum(nil)

	reStr := hex.EncodeToString(cipherStr)

	return reStr
}

type PushTxMessage struct {
	Action         string `json:"action"`
	AppId          string `json:"app_id"`
	Format         string `json:"format"`
	Method         string `json:"method"`
	Module         string `json:"module"`
	SignMethod     string `json:"sign_method"`
	Nonce          string `json:"nonce"`
	TransactionID  string `json:"t_x"`
	WalletTypeName string `json:"wallet_type_name"`
	Handle         string `json:"handle"`
	Sign           string `json:"sign"`
}

//测试数据库插入
func (etl *ETL) testInsert() (err error) {
	//创建行
	utxos := make([]*camdb.UTXO, 0)

	timestamp := time.Now().Unix()
	//添加行
	utxos = append(utxos, &camdb.UTXO{
		TX:          "123",
		N:           3,
		Address:     "werewrewr",
		CreateBlock: 155,
		SpentBlock:  -1,
		Asset:       "sdfdsfdsfdsf",
		Value:       "test",
		CreateTime:  timestamp,
		SpentTime:   0,
		Claimed:     false,
	})

	//加入数据库
	session := etl.engine.NewSession()

	session.Begin()

	defer func() {
		if err != nil {
			session.Rollback()
		} else {
			session.Commit()
		}
	}()

	_, err = etl.engine.Insert(&utxos)

	return
}
