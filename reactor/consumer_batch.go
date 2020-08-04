// BatchConsumer 实现了批量同步的向接收端传送数据的功能
package reactor

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const (
	DEFAULT_TIME_OUT             = 3000  // 默认超时时长 3 秒
	MAX_BATCH_SIZE               = 30    // 最大批量发送条数
	BATCH_CHANNEL_SIZE           = 2000  // 数据缓冲区大小, 超过此值时会阻塞
	BATCH_CHANNEL_THRESHOLD_SIZE = 1000  // 超过这个阈值, 内存的数据就丢弃掉, 防止内存过大影响主业务
	DEFAULT_COMPRESS             = false //默认关闭压缩gzip
	TIKER_TIME                   = 10
	ERR_CHANNEL_SIZE             = 2000 // 数据缓冲区大小, 超过此值时会阻塞
	ERR_CHANNEL_THRESHOLD_SIZE   = 1000 // 超过这个阈值, 内存的数据就丢弃掉, 防止内存过大影响主业务
	Concurrent_send              = 5
)
const (
	ENV_TEST ENV = 0
	ENV_PROD ENV = 1
)

type ENV int32

type Conf struct {
	Test_url   string // sdk是公开的, 这个地址还是开发者自己输入比较好
	Prod_url   string
	App_id     string
	Secret_id  string
	Batch_size int // 每一次请求带过去多少条数据
}

type BatchConsumer struct {
	serverUrl   string            // 接收端地址
	appId       string            // 项目 APP ID
	secretId    string            // 项目 SecretId
	timeout     time.Duration     // 网络请求超时时间, 单位毫秒
	compress    bool              //是否数据压缩
	event_ch    chan *Event_Plus  // 数据传输信道
	login_ch    chan *Login_Plus  // 数据传输信道
	battle_ch   chan *Battle_Plus // 数据传输信道
	is_flush    chan struct{}
	wg          sync.WaitGroup
	err_chan    chan string
	msg_chan    chan *Msg // 发送消息队列
	re_msg_chan chan *Msg // 再次尝试的队列
}

type Msg struct {
	Content []byte
	Path    string
	Count   int
}

func NewBatchConsumerWithBatchSize(conf *Conf, env ENV) (Consumer, chan string, error) {
	var url string
	if env == ENV_PROD {
		url = conf.Prod_url
	} else {
		url = conf.Test_url
	}
	if url[len(url)-1:] != "/" {
		url = url + "/"
	}

	return initBatchConsumer(url, conf, DEFAULT_TIME_OUT, DEFAULT_COMPRESS)
}

func initBatchConsumer(serverUrl string, conf *Conf, timeout int, compress bool) (Consumer, chan string, error) {
	u, err := url.Parse(serverUrl)
	if err != nil {
		return nil, nil, err
	}

	if conf.Batch_size > MAX_BATCH_SIZE {
		conf.Batch_size = MAX_BATCH_SIZE
	}
	err_chan := make(chan string, 1000)
	c := &BatchConsumer{
		serverUrl:   u.String(),
		appId:       conf.App_id,
		secretId:    conf.Secret_id,
		timeout:     time.Duration(timeout) * time.Millisecond,
		compress:    compress,
		is_flush:    make(chan struct{}, 10),
		event_ch:    make(chan *Event_Plus, BATCH_CHANNEL_SIZE),
		login_ch:    make(chan *Login_Plus, BATCH_CHANNEL_SIZE),
		battle_ch:   make(chan *Battle_Plus, BATCH_CHANNEL_SIZE),
		err_chan:    err_chan,
		msg_chan:    make(chan *Msg, BATCH_CHANNEL_SIZE),
		re_msg_chan: make(chan *Msg, BATCH_CHANNEL_SIZE),
	}

	c.wg.Add(3)
	go c.SendMsg()
	go c.ReSendMsg()
	go DealMsg(c, conf.Batch_size)
	return c, err_chan, nil
}

func (c *BatchConsumer) PreSendMsg(msg *Msg) {
	if len(c.msg_chan) > ERR_CHANNEL_THRESHOLD_SIZE {
		count := 0
		for i := 0; i < len(c.msg_chan)-ERR_CHANNEL_THRESHOLD_SIZE; i++ {
			count++
			<-c.msg_chan
		}
		c.err_chan <- "丢掉" + strconv.Itoa(count)
	}
	c.msg_chan <- msg
}

func (c *BatchConsumer) SendMsg() {
	defer func() {
		fmt.Println("event deal done sendmsg")
		c.wg.Done()
	}()
	mp := make(map[string][]int)
	mp["statistic/event"] = make([]int, 2)
	mp["statistic/login"] = make([]int, 2)
	mp["statistic/battle"] = make([]int, 2)
	ch := make(chan struct{}, 10)
	for i := 0; i < Concurrent_send; i++ {
		go func(i int) {
			for {
				rec, ok := <-c.msg_chan
				if ok == true {
					err := c.send(rec.Path, string(rec.Content))
					if rec.Count != 10 {
						fmt.Println("rec.Count:", rec.Count)
					}
					mp[rec.Path][0]++
					mp[rec.Path][1] = mp[rec.Path][1] + rec.Count
					if mp[rec.Path][0]%100 == 0 {
						c.err_chan <- "发送了" + rec.Path + " " + strconv.Itoa(mp[rec.Path][0])
					}
					if err != nil {
						c.re_msg_chan <- rec
					}
				} else {
					fmt.Println(i, "------")
					break
				}
			}
			fmt.Println(i, "======")
			ch <- struct{}{}
		}(i)
	}
	for i := 0; i < Concurrent_send; i++ {
		<-ch
	}
	close(c.re_msg_chan)
	fmt.Println("mp:", mp)
}

func (c *BatchConsumer) ReSendMsg() {
	defer func() {
		fmt.Println("event deal done resendmsg")
		c.wg.Done()
	}()
	ch := make(chan struct{}, 10)
	for i := 0; i < Concurrent_send; i++ {
		go func() {
			for {
				rec, ok := <-c.re_msg_chan
				if ok == true {
					err := c.send(rec.Path, string(rec.Content))
					if err != nil {
						c.err_chan <- err.Error() + "_content: " + string(rec.Content)
					}
				} else {
					break
				}
			}
			ch <- struct{}{}
		}()
	}
	for i := 0; i < Concurrent_send; i++ {
		<-ch
		fmt.Println("-k")
	}
}

func (c *BatchConsumer) ReceiveErrMsg(msg string) {
	// 超过这个值就丢弃
	if len(c.err_chan) > ERR_CHANNEL_THRESHOLD_SIZE {
		for i := 0; i < len(c.err_chan)-ERR_CHANNEL_THRESHOLD_SIZE; i++ {
			<-c.err_chan
		}
	}
	c.err_chan <- msg
}

func DealMsg(c *BatchConsumer, batchSize int) {
	defer func() {
		fmt.Println("event deal done dealmsg")
		c.wg.Done()
	}()
	buffer_event := make([]*Event_Plus, 0, batchSize+10)
	buffer_login := make([]*Login_Plus, 0, batchSize+10)
	buffer_battle := make([]*Battle_Plus, 0, batchSize+10)
	wg := sync.WaitGroup{}
	wg.Add(3)
	go DealEvent(c, buffer_event, c.event_ch, batchSize, "statistic/event", &wg)
	go DealLogin(c, buffer_login, c.login_ch, batchSize, "statistic/login", &wg)
	go DealBattle(c, buffer_battle, c.battle_ch, batchSize, "statistic/battle", &wg)
	wg.Wait()
	close(c.msg_chan)
	fmt.Println("over")
}

func DealEvent(c *BatchConsumer, buf []*Event_Plus, ch chan *Event_Plus, batchSize int, path string, wg *sync.WaitGroup) {
	defer wg.Done()
	tiker := time.NewTicker(time.Second * time.Duration(TIKER_TIME))
Loop:
	for {
		flush_event := false // 是否发送
		select {
		case rec, ok := <-ch:
			// 关闭了
			if ok == false {
				fmt.Println("event 已经关闭了")
				flush_event = true
				break Loop
			}
			buf = append(buf, rec)

		case <-tiker.C:
			flush_event = true

		case <-c.is_flush:
			fmt.Println("flush")
			flush_event = true
		}

		// buf满了, 或者flush_event = true 都要发送
		if len(buf) >= batchSize || flush_event {
			if len(buf) > 0 { // flush_event = true 但是又没有数据的时候, 不发送
				jdata, err := json.Marshal(buf)
				if err != nil {
					c.err_chan <- "json encode login msg error " + err.Error()
				} else {
					msg := Msg{
						Content: jdata,
						Path:    path,
						Count:   len(buf),
					}
					c.PreSendMsg(&msg)
					buf = buf[:0]
				}
			}
			if flush_event == true {
				flush_event = false
			}
		}
	}
	if len(buf) > 0 { // flush_event = true 但是又没有数据的时候, 不发送
		jdata, err := json.Marshal(buf)
		if err != nil {
			c.err_chan <- "json encode login msg error " + err.Error()
		} else {
			msg := Msg{
				Content: jdata,
				Path:    "statistic/event",
			}
			c.PreSendMsg(&msg)
			buf = buf[:0]
		}
	}
}

func DealLogin(c *BatchConsumer, buf []*Login_Plus, ch chan *Login_Plus, batchSize int, path string, wg *sync.WaitGroup) {
	defer wg.Done()
	tiker := time.NewTicker(time.Second * time.Duration(TIKER_TIME))
Loop:
	for {
		flush_event := false // 是否发送
		select {
		case rec, ok := <-ch:
			// 关闭了
			if ok == false {
				fmt.Println("login 已经关闭了")
				flush_event = true
				break Loop
			}
			buf = append(buf, rec)

		case <-tiker.C:
			flush_event = true

		case <-c.is_flush:
			fmt.Println("flush")
			flush_event = true
		}

		// buf满了, 或者flush_event = true 都要发送
		if len(buf) >= batchSize || flush_event {
			if len(buf) > 0 { // flush_event = true 但是又没有数据的时候, 不发送
				jdata, err := json.Marshal(buf)
				if err != nil {
					c.err_chan <- "json encode login msg error " + err.Error()
				} else {
					msg := Msg{
						Content: jdata,
						Path:    path,
						Count:   len(buf),
					}
					c.PreSendMsg(&msg)
					buf = buf[:0]
				}
			}
			if flush_event == true {
				flush_event = false
			}
		}
	}
	if len(buf) > 0 { // flush_event = true 但是又没有数据的时候, 不发送
		jdata, err := json.Marshal(buf)
		if err != nil {
			c.err_chan <- "json encode login msg error " + err.Error()
		} else {
			msg := Msg{
				Content: jdata,
				Path:    path,
			}
			c.PreSendMsg(&msg)
			buf = buf[:0]
		}
	}
}

func DealBattle(c *BatchConsumer, buf []*Battle_Plus, ch chan *Battle_Plus, batchSize int, path string, wg *sync.WaitGroup) {
	defer wg.Done()
	tiker := time.NewTicker(time.Second * time.Duration(TIKER_TIME))
Loop:
	for {
		flush_event := false // 是否发送
		select {
		case rec, ok := <-ch:
			// 关闭了
			if ok == false {
				fmt.Println("battle 已经关闭了")
				flush_event = true
				break Loop
			}
			buf = append(buf, rec)

		case <-tiker.C:
			flush_event = true

		case <-c.is_flush:
			fmt.Println("flush")
			flush_event = true
		}

		// buf满了, 或者flush_event = true 都要发送
		if len(buf) >= batchSize || flush_event {
			if len(buf) > 0 { // flush_event = true 但是又没有数据的时候, 不发送
				jdata, err := json.Marshal(buf)
				if err != nil {
					c.err_chan <- "json encode login msg error " + err.Error()
				} else {
					msg := Msg{
						Content: jdata,
						Path:    path,
						Count:   len(buf),
					}
					c.PreSendMsg(&msg)
					buf = buf[:0]
				}
			}
			if flush_event == true {
				flush_event = false
			}
		}
	}
	if len(buf) > 0 { // flush_* = true 但是又没有数据的时候, 不发送
		jdata, err := json.Marshal(buf)
		if err != nil {
			c.err_chan <- "json encode login msg error " + err.Error()
		} else {
			msg := Msg{
				Content: jdata,
				Path:    path,
			}
			c.PreSendMsg(&msg)
			buf = buf[:0]
		}
	}
}

func (c *BatchConsumer) AddEvent(d *Event) error {
	c.event_ch <- &Event_Plus{*d, c.appId, Md5V(c.secretId + "_" + c.appId + "_" + d.DataTime)}
	return nil
}

func (c *BatchConsumer) AddLogin(d *Login) error {
	c.login_ch <- &Login_Plus{*d, c.appId, Md5V(c.secretId + "_" + c.appId + "_" + d.DataTime)}
	return nil
}

func (c *BatchConsumer) AddBattle(d *Battle) error {
	c.battle_ch <- &Battle_Plus{*d, c.appId, Md5V(c.secretId + "_" + c.appId + "_" + d.DataTime)}
	return nil
}

func (c *BatchConsumer) Flush() error {
	c.is_flush <- struct{}{}
	c.is_flush <- struct{}{}
	c.is_flush <- struct{}{}
	return nil
}

func (c *BatchConsumer) Close() error {
	fmt.Println("close()")
	close(c.event_ch)
	close(c.login_ch)
	close(c.battle_ch)
	c.wg.Wait()
	return nil
}

func (c *BatchConsumer) send(path string, data string) error {
	var encodedData string
	var err error
	var compressType = "gzip"
	if c.compress {
		encodedData, err = encodeData(data)
	} else {
		encodedData = data
		compressType = "none"
	}
	if err != nil {
		return err
	}
	postData := bytes.NewBufferString(encodedData)
	var resp *http.Response

	req, _ := http.NewRequest("POST", c.serverUrl+path, postData)
	req.Header.Set("user-agent", "reactor-go-sdk")
	req.Header.Set("version", "1.1.0")
	req.Header.Set("compress", compressType)
	client := &http.Client{Timeout: c.timeout}
	resp, err = client.Do(req)

	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		var result struct {
			Code int
		}
		err = json.Unmarshal(body, &result)
		if err != nil {
			return err
		}
		if result.Code != 0 {
			fmt.Println(string(body))
			return errors.New(fmt.Sprintf("send to receiver failed with return code: %d", result.Code))
		}
	} else {
		return errors.New(fmt.Sprintf("Unexpected Status Code: %d", resp.StatusCode))
	}
	return nil
}

// Gzip 压缩
func encodeData(data string) (string, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)

	_, err := gw.Write([]byte(data))
	if err != nil {
		gw.Close()
		return "", err
	}
	gw.Close()

	return string(buf.Bytes()), nil
}

func Md5V(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}
