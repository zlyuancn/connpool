package connpool

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type IConnectPool interface {
	// 获取
	Get(ctx context.Context) (*Conn, error)
	// 回收
	Put(conn *Conn)
	// 关闭连接池
	Close()
}

// 创造者
type Creator func(ctx context.Context) (interface{}, error)

// 关闭方式, 连接池会在空闲超时以及超过最大空闲数时调用 ConnClose, 连接池关闭时也会调用 ConnClose
type ConnClose func(conn *Conn)

// 检查连接是否有效
type ValidConnected func(conn *Conn) bool

var (
	ErrMaxActiveLimit     = errors.New("达到最大活跃连接数限制")
	ErrMaxWaitConnLimit   = errors.New("达到最大等待连接数")
	ErrPoolClosed         = errors.New("连接池已关闭")
	ErrWaitGetConnTimeout = errors.New("获取连接超时")
)

type ConnectPool struct {
	conf *Config

	waitList        *list.List    // 未取到活跃锁的等待请求列表, 元素为 *waitReq, 先进先出
	activeWaitList  *list.List    // 已经取到活跃锁的等待请求列表, 元素为 *waitReq, 先进先出
	connList        *list.List    // 已连接的conn列表, 元素为 *Conn, 后进先出
	connectingCount int           // 当前正在准备conn的数量, 连接无论成功与否都会-1, 这里不用atomic而是用锁确保精确
	activeNum       int           // 活跃计数
	activeLock      chan struct{} // 活跃锁
	mx              sync.Mutex

	close      chan struct{} // 关闭信号
	baseCtx    context.Context
	baseCancel context.CancelFunc
}

func NewConnectPool(conf *Config) (IConnectPool, error) {
	if err := conf.Check(); err != nil {
		return nil, fmt.Errorf("配置检查失败: %v", err)
	}

	pool := &ConnectPool{
		conf: conf,

		waitList:       list.New(),
		activeWaitList: list.New(),
		connList:       list.New(),

		close: make(chan struct{}),
	}
	pool.baseCtx, pool.baseCancel = context.WithCancel(context.Background())

	// 初始化连接
	err := pool.initConnect()
	if err != nil {
		return nil, fmt.Errorf("初始化连接失败: %v", err)
	}

	// 检查空闲循环
	go pool.checkIdleLoop()

	return pool, nil
}

func (c *ConnectPool) Get(ctx context.Context) (*Conn, error) {
	return c.getLoop(ctx)
}

// 放回conn, 每次放回都会导致活跃计数-1
func (c *ConnectPool) Put(conn *Conn) {
	c.mx.Lock()
	defer c.mx.Unlock()

	c.activeNum--

	if !c.isClose() {
		c.conf.ConnClose(conn)
		return
	}

	// 放入活跃锁
	c.putActiveLock()

	// 校验conn, 如果失败从conn列表中获取一个有效的
	for !c.validConn(conn) {
		conn = c.popFrontConn()
		if conn == nil {
			c.replenishLackConn()
			return
		}
		conn.putTimeSec = 0 // 取出后需要重置放入时间
	}

	// 立即使用这个conn
	if c.useConn(conn) {
		return
	}

	// 后进先出, 以便尽早被获取
	conn.putTimeSec = time.Now().Unix()
	c.connList.PushFront(conn)
}

func (c *ConnectPool) Close() {
	select {
	case <-c.close:
		return
	default:
		close(c.close)
		c.baseCancel()
	}

	// 释放当前所有已连接的conn
	c.mx.Lock()
	defer c.mx.Unlock()

	for c.connList.Len() > 0 {
		conn := c.connList.Remove(c.connList.Front()).(*Conn)
		c.conf.ConnClose(conn)
	}
	c.connList = list.New()
}

// 连接池是否已关闭
func (c *ConnectPool) isClose() bool {
	select {
	case <-c.close:
		return true
	default:
		return false
	}
}

func (c *ConnectPool) getLoop(ctx context.Context) (*Conn, error) {
	if c.isClose() {
		return nil, ErrPoolClosed
	}

	c.mx.Lock()

	// 如果有活跃限制, 需要拿到锁, 否则放入未取到活跃锁的等待请求列表
	if c.conf.MaxActive > 0 {
		select {
		case <-c.activeLock:
		default:
			req, err := c.addWaitReq(false) // 添加到等待队列
			c.mx.Unlock()

			if err != nil {
				return nil, err
			}

			return c.waitReqGetConnLoop(ctx, req)
		}
	}

	// 先从conn池中获取
	conn := c.popFrontConn()
	if conn != nil {
		c.activeNum++
		return conn, nil
	}

	// 否则加入已经取到活跃锁的等待请求列表
	req, err := c.addWaitReq(true)
	c.mx.Unlock()
	if err != nil {
		return nil, err
	}

	// 立即补充缺少的
	c.replenishLackConn()

	return c.waitReqGetConnLoop(ctx, req)
}

// 自动放入conn处理
func (c *ConnectPool) autoPutConn(conn *Conn) {
	if !c.validConn(conn) {
		return
	}

	c.mx.Lock()
	defer c.mx.Unlock()

	if !c.isClose() {
		c.conf.ConnClose(conn)
		return
	}

	// 立即使用这个conn
	if c.useConn(conn) {
		return
	}

	// 放入conn列表, 自动放入的conn应该放在列表末尾
	conn.putTimeSec = time.Now().Unix()
	c.connList.PushBack(conn)
}
