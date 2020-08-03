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
	"sync"
	"time"
)

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
}

func (c *BatchConsumer) PreSendMsg(msg *Msg) {
	if len(c.msg_chan) > ERR_CHANNEL_THRESHOLD_SIZE {
		for i := 0; i < len(c.msg_chan)-ERR_CHANNEL_THRESHOLD_SIZE; i++ {
			<-c.msg_chan
		}
	}
	c.msg_chan <- msg
}

func (c *BatchConsumer) SendMsg() {
	for {
		rec, ok := <-c.msg_chan
		if ok {
			err := c.send(rec.Path, string(rec.Content))
			if err != nil {
				c.re_msg_chan <- rec
			}
		}
	}
}

func (c *BatchConsumer) ReSendMsg() {
	// 超过这个值就丢弃
	for {
		rec, ok := <-c.re_msg_chan
		if ok {
			err := c.send(rec.Path, string(rec.Content))
			if err != nil {
				c.err_chan <- err.Error() + "_content: " + string(rec.Content)
			}
		}
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

const (
	DEFAULT_TIME_OUT             = 3000  // 默认超时时长 3 秒
	MAX_BATCH_SIZE               = 30    // 最大批量发送条数
	BATCH_CHANNEL_SIZE           = 2000  // 数据缓冲区大小, 超过此值时会阻塞
	BATCH_CHANNEL_THRESHOLD_SIZE = 1000  // 超过这个阈值, 内存的数据就丢弃掉, 防止内存过大影响主业务
	DEFAULT_COMPRESS             = false //默认关闭压缩gzip
	TIKER_TIME                   = 10
	ERR_CHANNEL_SIZE             = 2000 // 数据缓冲区大小, 超过此值时会阻塞
	ERR_CHANNEL_THRESHOLD_SIZE   = 1000 // 超过这个阈值, 内存的数据就丢弃掉, 防止内存过大影响主业务
)

type Conf struct {
	Test_url   string // sdk是公开的, 这个地址还是开发者自己输入比较好
	Prod_url   string
	App_id     string
	Secret_id  string
	Batch_size int // 每一次请求带过去多少条数据
}

type ENV int32

const (
	ENV_TEST ENV = 0
	ENV_PROD ENV = 1
)

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

	//event_ch    chan *Event_Plus  // 数据传输信道
	//login_ch    chan *Login_Plus  // 数据传输信道
	//battle_ch   chan *Battle_Plus // 数据传输信道
	//is_flush    chan struct{}
	//wg          sync.WaitGroup
	//err_chan    chan string
	//msg_chan    chan *Msg // 发送消息队列
	//re_msg_chan chan *Msg // 再次尝试的队列

	c.wg.Add(3)

	go c.SendMsg()
	go c.ReSendMsg()
	go DealMsg(c, conf.Batch_size)
	return c, err_chan, nil
}

func DealEvent(c *BatchConsumer, batchSize int, path string) {
	defer func() {
		fmt.Println("event deal done")
		c.wg.Done()
	}()
	buffer := make([]*Event_Plus, 0, batchSize)
	tiker := time.NewTicker(time.Second * time.Duration(TIKER_TIME))
	is_stop := false // 是否停止
	for {
		flush := false // 是否发送
		select {
		case rec, ok := <-c.event_ch:
			c.ReceiveErrMsg("Event get on msg")
			// 关闭了
			if !ok {
				flush = true
				is_stop = true
				break
			}
			buffer = append(buffer, rec)
			if len(buffer) > batchSize {
				flush = true
			}
			// 超过阈值的时候, 丢掉老数据
			if len(buffer) > BATCH_CHANNEL_THRESHOLD_SIZE {
				for i := 0; i < len(buffer)-BATCH_CHANNEL_THRESHOLD_SIZE; i++ {
					c.ReceiveErrMsg(fmt.Sprintf("超过阈值, 丢掉了[%d]条数据", len(buffer)-BATCH_CHANNEL_THRESHOLD_SIZE))
					<-c.event_ch
				}
			}
		case <-tiker.C:
			flush = true

		case <-c.is_flush:
			flush = true
		}
		// 上传数据到服务端, 不会重试
		if flush {
			if len(buffer) > 0 { // 有数据才发送
				jdata, err := json.Marshal(buffer)
				if err == nil {
					err = c.send(path, string(jdata))
					if err != nil {
						fmt.Println(err)
					}
				}
				buffer = buffer[:0]
				flush = false
			}
		}
		if is_stop == true {
			break
		}
	}
}

func DealMsg(c *BatchConsumer, batchSize int) {
	defer func() {
		fmt.Println("event deal done")
		c.wg.Done()
	}()
	buffer_event := make([]*Event_Plus, 0, batchSize)
	buffer_login := make([]*Login_Plus, 0, batchSize)
	buffer_battle := make([]*Battle_Plus, 0, batchSize)
	tiker := time.NewTicker(time.Second * time.Duration(TIKER_TIME))
	is_stop := false // 是否停止
	for {
		flush_event := false  // 是否发送
		flush_login := false  // 是否发送
		flush_battle := false // 是否发送
		select {
		case rec, ok := <-c.event_ch:
			c.ReceiveErrMsg("Event get on msg")
			// 关闭了
			if !ok {
				flush_event = true
				is_stop = true
				break
			}
			buffer_event = append(buffer_event, rec)
		case rec, ok := <-c.login_ch:
			c.ReceiveErrMsg("login get on msg")
			// 关闭了
			if !ok {
				flush_login = true
				is_stop = true
				break
			}
			buffer_login = append(buffer_login, rec)

		case rec, ok := <-c.battle_ch:
			c.ReceiveErrMsg("battle get on msg")
			// 关闭了
			if !ok {
				flush_battle = true
				is_stop = true
				break
			}
			buffer_battle = append(buffer_battle, rec)

		case <-tiker.C:
			flush_event = true
			flush_login = true
			flush_battle = true

		case <-c.is_flush:
			flush_event = true
			flush_login = true
			flush_battle = true
		}

		// buf满了, 或者flush_event = true 都要发送
		if len(buffer_event) > batchSize || flush_event {
			if len(buffer_event) > 0 { // flush_event = true 但是又没有数据的时候, 不发送
				jdata, err := json.Marshal(buffer_event)
				if err != nil {
					c.err_chan <- "json encode login msg error " + err.Error()
				} else {
					msg := Msg{
						Content: jdata,
						Path:    "statistic/event",
					}
					c.PreSendMsg(&msg)
					buffer_event = buffer_event[:0]
				}
			}
			flush_event = false
		}

		// buf满了, 或者flush_event = true 都要发送
		if len(buffer_login) > batchSize || flush_login {
			if len(buffer_login) > 0 { // flush_* = true 但是又没有数据的时候, 不发送
				jdata, err := json.Marshal(buffer_login)
				if err != nil {
					c.err_chan <- "json encode login msg error " + err.Error()
				} else {
					msg := Msg{
						Content: jdata,
						Path:    "statistic/login",
					}
					c.PreSendMsg(&msg)
					buffer_login = buffer_login[:0]
				}
			}
			flush_login = false
		}

		// buf满了, 或者flush_event = true 都要发送
		if len(buffer_battle) > batchSize || flush_battle {
			if len(buffer_battle) > 0 { // flush_* = true 但是又没有数据的时候, 不发送
				jdata, err := json.Marshal(buffer_battle)
				if err != nil {
					c.err_chan <- "json encode login msg error " + err.Error()
				} else {
					msg := Msg{
						Content: jdata,
						Path:    "statistic/battle",
					}
					c.PreSendMsg(&msg)
					buffer_battle = buffer_battle[:0]
				}
			}
			flush_battle = false
		}

		if is_stop == true {
			break
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
