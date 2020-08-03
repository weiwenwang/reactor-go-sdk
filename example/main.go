package main

import (
	"fmt"
	"github.com/weiwenwang/reactor-go-sdk/reactor"
	"time"
)

func main() {
	// 区别正式环境和测试环境就是通过域名
	conf := reactor.Conf{App_id: "1109861474", Secret_id: "955e76838210f9427acc6a4bc88b1a22",
		Batch_size: 10,
		Test_url:   "https://reactor-test.xingye.work", Prod_url: "https://reactor.xingye.work",
		Err_chan: make(chan string, 1000),
	}
	go func() {
		for {
			a, ok := <-conf.Err_chan
			if ok {
				fmt.Println(a)
			}
		}
	}()
	consumer, err := reactor.NewBatchConsumerWithBatchSize(&conf, reactor.ENV_TEST)
	if err != nil {
		panic(err.Error())
	}
	reactor.New(consumer)

	r, _ := reactor.GetReactor()
	for i := 0; i < 10000; i++ {
		time.Sleep(1 * time.Second)
		err := r.EventAdd(&reactor.Event{UserId: "123", EventId: "wang_test"})
		if err != nil {
			panic(err.Error())
		}

		err2 := r.LoginAdd(&reactor.Login{UserId: "123", ChannelId: "channel1", IsNew: "1"})
		if err2 != nil {
			panic(err2.Error())
		}
		err3 := r.BattleAdd(&reactor.Battle{UserId: "123", BattleId: "123"})
		if err3 != nil {
			panic(err3.Error())
		}
	}
	r.Flush() // 这个是把当前的数据全部都发送
	r.Close() //

}
