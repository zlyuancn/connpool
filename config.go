package connpool

import (
	"errors"
	"time"
)

const (
	// 初始化时等待第一个链接
	defWaitFirstConn = false
	// 最小闲置
	defMinIdle = 2
	// 最大闲置
	defMaxIdle = defMinIdle * 2
	// 最大活跃连接数
	defMaxActive = 10
	// 批次增量
	defBatchIncrement = defMinIdle * 2
	// 批次缩容
	defBatchShrink = defBatchIncrement
	// 空闲链接超时时间
	defIdleTimeout = 0
	// 等待获取连接的超时时间
	defWaitTimeout = time.Second * 5
	// 最大等待conn的数量
	defMaxWaitConnCount = 0
	// 连接超时
	defConnectTimeout = time.Second * 5
	// 一个连接最大存活时间
	defMaxConnLifetime = 0

	// 检查空闲间隔, 包含最小空闲数, 最大空闲数, 空闲链接超时
	defCheckIdleInterval = time.Second * 5
)

type Config struct {
	WaitFirstConn     bool          // 初始化时等待第一个链接
	MinIdle           int           // 最小闲置
	MaxIdle           int           // 最大闲置
	MaxActive         int           // 最大活跃连接数, 小于1表示不限制
	BatchIncrement    int           // 批次增量, 当conn不够时, 一次性最多申请多少个链接
	BatchShrink       int           // 批次缩容, 当conn太多时(超过最大闲置), 一次性最多释放多少个链接
	IdleTimeout       time.Duration // 空闲链接超时时间, 如果一个连接长时间未使用将被视为连接无效, 小于1表示永不超时
	WaitTimeout       time.Duration // 等待获取连接的超时时间
	MaxWaitConnCount  int           // 最大等待conn的数量, 小于1表示不限制
	ConnectTimeout    time.Duration // 连接超时
	MaxConnLifetime   time.Duration // 一个连接最大存活时间, 小于1表示不限制
	CheckIdleInterval time.Duration // 检查空闲间隔
	Creator
	ConnClose
	ValidConnected
}

func NewConfig() *Config {
	return &Config{
		WaitFirstConn:     defWaitFirstConn,
		MinIdle:           defMinIdle,
		MaxIdle:           defMaxIdle,
		MaxActive:         defMaxActive,
		BatchIncrement:    defBatchIncrement,
		BatchShrink:       defBatchShrink,
		IdleTimeout:       defIdleTimeout,
		WaitTimeout:       defWaitTimeout,
		MaxWaitConnCount:  defMaxWaitConnCount,
		ConnectTimeout:    defConnectTimeout,
		MaxConnLifetime:   defMaxConnLifetime,
		CheckIdleInterval: defCheckIdleInterval,
		Creator:           nil,
		ConnClose:         nil,
		ValidConnected:    nil,
	}
}

func (conf *Config) Check() error {
	if conf.MinIdle < 1 {
		conf.MinIdle = defMinIdle
	}
	if conf.MaxIdle < 1 {
		conf.MaxIdle = defMaxIdle
	}
	if conf.MaxIdle < conf.MinIdle {
		conf.MaxIdle = conf.MinIdle * 2
	}
	if conf.BatchIncrement < 1 {
		conf.BatchIncrement = conf.MinIdle
	}
	if conf.BatchIncrement > conf.MaxIdle {
		conf.BatchIncrement = conf.MaxIdle
	}
	if conf.BatchShrink < 1 {
		conf.BatchShrink = conf.BatchIncrement
	}
	if conf.IdleTimeout < 1 {
		conf.IdleTimeout = 0
	}
	if conf.WaitTimeout < 1 {
		conf.WaitTimeout = defWaitTimeout
	}
	if conf.MaxWaitConnCount < 1 {
		conf.MaxWaitConnCount = 0
	}
	if conf.ConnectTimeout < 1 {
		conf.ConnectTimeout = defConnectTimeout
	}
	if conf.MaxConnLifetime < 1 {
		conf.MaxConnLifetime = 0
	}
	if conf.CheckIdleInterval < 1 {
		conf.CheckIdleInterval = defCheckIdleInterval
	}
	if conf.Creator == nil {
		return errors.New("未设置 Creator")
	}
	if conf.ConnClose == nil {
		return errors.New("未设置 ConnClose")
	}
	if conf.ValidConnected == nil {
		return errors.New("未设置 ValidConnected")
	}
	return nil
}
