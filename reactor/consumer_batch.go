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
	serverUrl string           // 接收端地址
	AppId     string           // 项目 APP ID
	SecretId  string           // 项目 SecretId
	Timeout   time.Duration    // 网络请求超时时间, 单位毫秒
	compress  bool             //是否数据压缩
	event_ch  chan *Event_Plus // 数据传输信道
	login_ch  chan *Login      // 数据传输信道
	battle_ch chan *Battle     // 数据传输信道
	is_flush  chan struct{}
	wg        sync.WaitGroup
}

const (
	DEFAULT_TIME_OUT            = 3000  // 默认超时时长 3 秒
	MAX_BATCH_SIZE              = 30    // 最大批量发送条数
	BATCH_CHANNEL_SIZE          = 2000  // 数据缓冲区大小, 超过此值时会阻塞
	BATCH_CHANNEL_HRESHOLD_SIZE = 1000  // 超过这个阈值, 内存的数据就丢弃掉, 防止内存过大影响主业务
	DEFAULT_COMPRESS            = false //默认关闭压缩gzip
)

func NewBatchConsumerWithBatchSize(appId string, secretId string, batchSize int, url string) (Consumer, error) {
	return initBatchConsumer(url, appId, secretId, batchSize, DEFAULT_TIME_OUT, DEFAULT_COMPRESS)
}

func initBatchConsumer(serverUrl string, appId string, secretId string, batchSize int, timeout int, compress bool) (Consumer, error) {
	u, err := url.Parse(serverUrl)
	if err != nil {
		return nil, err
	}

	if batchSize > MAX_BATCH_SIZE {
		batchSize = MAX_BATCH_SIZE
	}
	c := &BatchConsumer{
		serverUrl: u.String(),
		AppId:     appId,
		SecretId:  secretId,
		Timeout:   time.Duration(timeout) * time.Millisecond,
		compress:  compress,
		is_flush:  make(chan struct{}, 10),
		event_ch:  make(chan *Event_Plus, BATCH_CHANNEL_SIZE),
		login_ch:  make(chan *Login, BATCH_CHANNEL_SIZE),
		battle_ch: make(chan *Battle, BATCH_CHANNEL_SIZE),
	}

	c.wg.Add(1)

	go func() {
		buffer := make([]*Event_Plus, 0, batchSize)
		defer func() {
			c.wg.Done()
		}()
		tiker := time.NewTicker(time.Second * time.Duration(10))
		is_stop := false
		for {
			flush := false
			select {
			case rec, ok := <-c.event_ch:
				if !ok {
					return
				}
				buffer = append(buffer, rec)
				if len(buffer) > batchSize {
					flush = true
				}
				// 超过阈值的时候, 丢掉老数据
				if len(buffer) > BATCH_CHANNEL_HRESHOLD_SIZE {
					for i := 0; i < len(buffer)-BATCH_CHANNEL_HRESHOLD_SIZE; i++ {
						<-c.event_ch
					}
				}
			case <-tiker.C:
				flush = true

			case <-c.is_flush:
				flush = true
				is_stop = true
			}

			// 上传数据到服务端, 不会重试
			if flush {
				if len(buffer) > 0 { // 有数据才发送
					jdata, err := json.Marshal(buffer)
					if err == nil {
						err = c.send("statistic/event", string(jdata))
						if err != nil {
							fmt.Println(err)
						}
					}
					buffer = buffer[:0]
					flush = false
				}
			}
			if is_stop {
				break
			}
		}
	}()
	return c, nil
}

func (c *BatchConsumer) AddEvent(d *Event) error {
	c.event_ch <- &Event_Plus{*d, c.AppId, Md5V(c.SecretId + "_" + c.AppId + "_" + d.DataTime)}
	return nil
}

func (c *BatchConsumer) AddLogin(l *Login) error {
	c.login_ch <- l
	return nil
}

func (c *BatchConsumer) AddBattle(b *Battle) error {
	c.battle_ch <- b
	return nil
}

func (c *BatchConsumer) Flush() error {
	c.is_flush <- struct{}{}
	return nil
}

func (c *BatchConsumer) Close() error {
	c.Flush()
	close(c.event_ch)
	//close(c.login_ch)
	//close(c.battle_ch)
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
	client := &http.Client{Timeout: c.Timeout}
	resp, err = client.Do(req)

	if err != nil {
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
