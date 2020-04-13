package main

import (
	"flag"

	// "github.com/dynamicgo/aliyunlog"
	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/camlabs/camindex"
	_ "github.com/lib/pq"
)

var logger = slf4go.Get("cam-indexer")
var configpath = flag.String("conf", "config.json", "cam indexer config file path")

func main() {

	logger.Debug("Version 1.0.0")

	flag.Parse()
	
	neocnf, err := config.NewFromFile(*configpath)

	if err != nil {
		logger.ErrorF("load cam config err , %s", err)
		return
	}		

	//交易监控
	monitor, err := indexer.NewMonitor(neocnf)

	if err != nil {
		logger.ErrorF("create cam monitor err , %s", err)
		return
	}

	//监控开始
	monitor.Run()

}
