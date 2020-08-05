package reactor

import (
	"errors"
	"log"
	"strconv"
	"sync"
	"time"
)

var r *Reactor
var r_once sync.Once

const (
	INVENTORY = "inventory_events" // 库存事件
	CONSUME   = "consume_events"   // 充值成功事件
	SHARE     = "share_events"     // 分享事件
	VIDEO     = "video_events"     // 视频事件
	RESOURCE  = "resource_events"  // 货币产销事件
	NEW       = "new_events"       // 新手引导事件
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

// 自定义事件
type InventoryEvent struct {
	DataTime        string
	UserId          string
	CoinCount       int
	DiamondCount    int
	LiquanCount     int
	JipaiqiCount    int
	KandipaiCount   int
	CansaiquanCount int
	OtherProp1      int
	OtherProp2      int
	Params          string
	Platform        string
	DeviceId        string
}

type Event_Plus struct {
	Event
	AppId string `json:"app_id"`
	Sign  string `json:"sign"`
}

type Login_Plus struct {
	Login
	AppId string `json:"app_id"`
	Sign  string `json:"sign"`
}

type Battle_Plus struct {
	Battle
	AppId string `json:"app_id"`
	Sign  string `json:"sign"`
}

// 登录
//is_new o: old user, 1:new user
type Login struct {
	DataTime    string `json:"data_time"`
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
}

// 登录
type Battle struct {
	DataTime     string `json:"data_time"`
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
}

// Consumer 为数据实现 IO 操作（写入磁盘或者发送到接收端）
type Consumer interface {
	AddEvent(event *Event) error
	AddLogin(login *Login) error
	AddBattle(battle *Battle) error
	Close() error
	Flush() error
}

type EventInterface interface {
	Verify() error
	TransToStandardEvent() *Event
}

type Reactor struct {
	consumer        Consumer
	superProperties map[string]interface{}
	mutex           *sync.RWMutex
}

// 初始化 Reactor
func New(c Consumer) {
	r_once.Do(func() {
		r = &Reactor{consumer: c,
			superProperties: make(map[string]interface{}),
			mutex:           new(sync.RWMutex)}
	})
}

func GetReactor() (*Reactor, error) {
	if r != nil {
		return r, nil
	}
	return nil, errors.New("seems no init")
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
func (r *Reactor) EventAdd(e EventInterface) error {
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	err := e.Verify()
	if err != nil {
		return err
	}
	return r.consumer.AddEvent(e.TransToStandardEvent())
}

func (e Event) Verify() error {
	if e.UserId == "" {
		return errors.New("user_id cann't empty")
	}
	if e.EventId == "" {
		return errors.New("event_id cann't empty")
	}
	return nil
}

func (e InventoryEvent) Verify() error {
	if e.DataTime == "" {
		e.DataTime = time.Now().Format("2006-01-02 15:04:05")
	}
	if e.UserId == "" {
		return errors.New("user_id cann't empty")
	}
	return nil
}

func (e InventoryEvent) TransToStandardEvent() *Event {
	var event Event
	event.EventId = INVENTORY
	event.Value1 = strconv.Itoa(e.CoinCount)
	event.Value2 = strconv.Itoa(e.DiamondCount)
	event.Value3 = strconv.Itoa(e.LiquanCount)
	event.Value4 = strconv.Itoa(e.JipaiqiCount)
	event.Value5 = strconv.Itoa(e.KandipaiCount)
	event.Value6 = strconv.Itoa(e.CansaiquanCount)
	event.Value7 = strconv.Itoa(e.OtherProp1)
	event.Value8 = strconv.Itoa(e.OtherProp2)
	event.Params = e.Params
	event.UserId = e.UserId
	event.Platform = e.Platform
	event.DeviceId = e.DeviceId

	return &event
}

// 对数值类型的属性做累加操作
func (r *Reactor) LoginAdd(e *Login) error {
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
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
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if e.UserId == "" {
		return errors.New("user_id cann't empty")
	}
	if e.DataTime == "" {
		e.DataTime = time.Now().Format("2006-01-02 15:04:05")
	}
	return r.consumer.AddBattle(e)
}

// 立即开始数据 IO 操作
func (r *Reactor) Flush() {
	r.consumer.Flush()
}

//关闭 Reactor
func (r *Reactor) Close() {
	r.consumer.Close()
}
