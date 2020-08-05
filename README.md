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
		Secret_id:  "955e76838210f9427acc6a4bcxxxxxx",
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

	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Millisecond)
		var err error
		// eg: 库存事件, 如果您不赋值DataTime字段,sdk会自动去当前时间
		inventory := reactor.InventoryEvent{UserId: "100", CoinCount: 1, DiamondCount: 2,
			LiquanCount: 3, JipaiqiCount: 4, KandipaiCount: 5, CansaiquanCount: 6, OtherProp1: 7, OtherProp2: 8, Params: "par"}
		err = r.EventAdd(inventory)
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

	r.Close() // close里面会调用一次flush, 您可以不需要单独调用flush, 这个要在整个进程退出的时候执行, 确保不再有数据发送的情况
}


```