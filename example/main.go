package main

import (
	"github.com/weiwenwang/reactor-go-sdk/reactor"
	"time"
)

func main() {
	consumer, _ := reactor.NewBatchConsumerWithBatchSize("1109861474", "955e76838210f9427acc6a4bc88b1a22", 10, "https://reactor-test.xingye.work/")
	r := reactor.New(consumer)

	r.EventAdd(&reactor.Event{UserId: "123", EventId: "wang_test"})

	r.LoginAdd(&reactor.Login{UserId: "123"})

	r.BattleAdd(&reactor.Battle{UserId: "123"})
	time.Sleep(10 * time.Second)
	defer r.Close()
}
