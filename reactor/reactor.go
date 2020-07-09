package reactor

import (
	"errors"
	"sync"
	"time"
)

const (
	TRACK         = "track"
	USER_SET      = "user_set"
	USER_UNSET    = "user_unset"
	USER_SET_ONCE = "user_setOnce"
	USER_ADD      = "user_add"
	USER_APPEND   = "user_append"
	USER_DEL      = "user_del"

	SDK_VERSION = "1.1.0"
	LIB_NAME    = "Golang"
)

// 自定义事件
type Event struct {
	DataTime string `json:"data_time"`
	EventId  string `json:"event_id"`
	UserId   string `json:"user_id"`
	DeviceId string `json:"device_id"`
	Params   string `json:"params"`
	Platform string `json:"platform"`
	Value1   string `json:"value1"`
	Value2   string `json:"value2"`
	Value3   string `json:"value3"`
	Value4   string `json:"value4"`
	Value5   string `json:"value5"`
	Value6   string `json:"value6"`
	Value7   string `json:"value7"`
	Value8   string `json:"value8"`
}

type Event_Plus struct {
	Event
	AppId string `json:"app_id"`
	Sign  string `json:"sign"`
}

// 登录
type Login struct {
	DataTime    string `json:"data_time"`
	AppId       string `json:"app_id,omitempty"`
	ChannelId   string `json:"channel_id"`
	ShareUid    string `json:"share_uid"`
	UserId      string `json:"user_id"`
	OpenId      string `json:"open_id"`
	DeviceId    string `json:"device_id"`
	Ip          string `json:"ip"`
	Country     string `json:"country"`
	Province    string `json:"province"`
	City        string `json:"city"`
	OsType      string `json:"os_type"`
	DeviceModel string `json:"device_model"`
	IsNew       string `json:"is_new"`
	Platform    string `json:"platform"`
	Sign        string `json:"sign"`
}

// 登录
type Battle struct {
	DataTime     string `json:"data_time"`
	AppId        string `json:"app_id,omitempty"`
	UserId       string `json:"user_id"`
	DeviceId     string `json:"device_id"`
	BattleId     string `json:"battle_id"`
	DeskType     string `json:"desk_type"`
	Category     string `json:"category"`
	Field        string `json:"field"`
	IsWin        string `json:"is_win"`
	Multiple     string `json:"multiple"`
	Score        string `json:"score"`
	CurrentScore string `json:"current_score"`
	PlayTime     string `json:"play_time"`
	Platform     string `json:"platform"`
	Sign         string `json:"sign"`
}

// Consumer 为数据实现 IO 操作（写入磁盘或者发送到接收端）
type Consumer interface {
	AddEvent(event *Event) error
	AddLogin(login *Login) error
	AddBattle(battle *Battle) error
	Flush() error
	Close() error
}

type Reactor struct {
	consumer        Consumer
	superProperties map[string]interface{}
	mutex           *sync.RWMutex
}

// 初始化 TDAnalytics
func New(c Consumer) *Reactor {
	return &Reactor{consumer: c,
		superProperties: make(map[string]interface{}),
		mutex:           new(sync.RWMutex)}
}

// 返回公共事件属性
func (rtr *Reactor) GetSuperProperties() map[string]interface{} {
	result := make(map[string]interface{})
	rtr.mutex.RLock()
	mergeProperties(result, rtr.superProperties)
	rtr.mutex.RUnlock()
	return result
}

// 对数值类型的属性做累加操作
func (r *Reactor) EventAdd(e *Event) error {
	if e.UserId == "" {
		return errors.New("user_id cann't empty")
	}
	if e.EventId == "" {
		return errors.New("event_id cann't empty")
	}
	if e.DataTime == "" {
		e.DataTime = time.Now().Format("2006-01-02 15:04:05")
	}
	return r.consumer.AddEvent(e)
}

// 对数值类型的属性做累加操作
func (r *Reactor) LoginAdd(e *Login) error {
	if e.UserId == "" {
		return errors.New("user_id cann't empty")
	}
	if e.ChannelId == "" {
		return errors.New("channel_id cann't empty")
	}
	if e.IsNew == "" {
		return errors.New("is_new cann't empty")
	}
	if e.DataTime == "" {
		e.DataTime = time.Now().Format("2006-01-02 15:04:05")
	}
	return r.consumer.AddLogin(e)
}

// 对数值类型的属性做累加操作
func (r *Reactor) BattleAdd(e *Battle) error {
	if e.UserId == "" {
		return errors.New("user_id cann't empty")
	}
	if e.DataTime == "" {
		e.DataTime = time.Now().Format("2006-01-02 15:04:05")
	}
	return r.consumer.AddBattle(e)
}

// 立即开始数据 IO 操作
func (rct *Reactor) Flush() {
	rct.consumer.Flush()
}

// 关闭 TDAnalytics
func (rct *Reactor) Close() {
	rct.consumer.Close()
}
