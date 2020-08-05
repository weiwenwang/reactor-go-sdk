### 安装
go get github.com/weiwenwang/reactor-sdk-go

### example

```goland

func DealErrChan(err_chan <-chan string) {
	count := 0
	for {
		err_msg, ok := <-err_chan
		if ok {
			fmt.Println("err_msg: ", err_msg)
		}
		count++
		if count > 20 {
			count = 0
			time.Sleep(5)
		}
	}
}

func main() {
	// 区别正式环境和测试环境就是通过域名
	conf := reactor.Conf{
		App_id:     "1109861474",
		Secret_id:  "955e76838210f9427acc6a4bc88b1a22",
		Batch_size: 10,
		Test_url:   "https://reactor-test.xingye.work",
		Prod_url:   "https://reactor.xingye.work",
	}

	consumer, err_chan, err := reactor.NewBatchConsumerWithBatchSize(&conf, reactor.ENV_TEST)
	if err != nil {
		log.Println(err.Error())
	}
	// 开一个routine处理错误
	go DealErrChan(err_chan)
	reactor.New(consumer)

	r, err := reactor.GetReactor()
	if err != nil {
		log.Println(err.Error())
	}

	for i := 0; i < 1000; i++ {
		time.Sleep(10 * time.Millisecond)
		var err error
		// 如果您不赋值DataTime字段,sdk会自动去当前时间
		err = r.EventAdd(&reactor.Event{UserId: "123", EventId: "wang_test", DataTime: time.Now().Format("2006-01-02 15:04:05")})
		if err != nil {
			log.Println("event add:", err.Error())
		}

		// 库存事件, 有几个特殊事件定义了常量
		err = r.EventAdd(&reactor.Event{UserId: "123", EventId: reactor.INVENTORY})
		if err != nil {
			log.Println("event add:", err.Error())
		}

		err = r.LoginAdd(&reactor.Login{UserId: "123", ChannelId: "channel1", IsNew: "1"})
		if err != nil {
			log.Println("login add:", err.Error())
		}

		err = r.BattleAdd(&reactor.Battle{UserId: "123", BattleId: "123"})
		if err != nil {
			log.Println("battle add:", err.Error())
		}
	}
	r.Flush() // 这个是把当前的数据全部都发送

	r.Close() // close里面会调用一次flush, 您可以不需要单独调用flush
}


```