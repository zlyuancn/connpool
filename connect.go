package connpool

import (
	"context"
	"fmt"
	"time"
)

// 初始化连接
func (c *ConnectPool) initConnect() error {
	// 放入活跃锁
	if c.conf.MaxActive > 0 {
		c.activeLock = make(chan struct{}, c.conf.MaxActive)
		for i := 0; i < c.conf.MaxActive; i++ {
			c.activeLock <- struct{}{}
		}
	}

	// 等待第一个conn
	if c.conf.WaitFirstConn {
		err := c.applyConnectLoop()
		if err != nil {
			return fmt.Errorf("等待第一个conn失败: %v", err)
		}
	}

	// 立即补充缺少的conn
	c.replenishLackConn()
	return nil
}

// 检查空闲
func (c *ConnectPool) checkIdleLoop() {
	t := time.NewTicker(c.conf.CheckIdleInterval)
	for {
		select {
		case <-c.close:
			t.Stop()
			return
		case <-t.C:
			// 先释放, 再申请, 那么在缺conn的情况下就不会释放正常的conn
			c.releaseInvalidConn()
			c.releaseNeedlessConn()
			c.replenishLackConn()
		}
	}
}

// 申请一个连接
func (c *ConnectPool) applyConnectLoop() error {
	ctx, cancel := context.WithTimeout(c.baseCtx, c.conf.ConnectTimeout)
	defer cancel()

	var v interface{}
	var err error
	done := make(chan struct{})

	// 协程创建
	go func() {
		v, err = c.conf.Creator(ctx)
		select {
		case done <- struct{}{}: // 还在等待中, 直接处理
			return
		default: // 自行处理
			if err == nil {
				c.autoPutConn(makeConn(v)) // 仍然认可
			}
		}
	}()

	select {
	case <-done:
		if err == nil {
			c.autoPutConn(makeConn(v))
		}
		return err
	case <-c.close:
		return ErrPoolClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// 补充缺少的conn
func (c *ConnectPool) replenishLackConn() {
	if c.isClose() {
		return
	}

	c.mx.Lock()
	need := c.checkNeedConnCount()
	if need < 1 {
		c.mx.Unlock()
		return
	}
	c.connectingCount += need
	c.mx.Unlock()

	for i := 0; i < need; i++ {
		go func() {
			err := c.applyConnectLoop()
			c.mx.Lock()
			c.connectingCount-- // 不管申请连接结果如何都将正在申请数量-1
			c.mx.Unlock()
			if err != nil {
				// 创建失败时立即重新创建极有可能也会失败, 一般来说创建失败都是网络或者限流引起的, 等一会儿可能就好了
				time.Sleep(time.Second)
				c.replenishLackConn() // 重新申请
			}
		}()
	}
}

// 检查需要多少conn
func (c *ConnectPool) checkNeedConnCount() int {
	wait := c.getWaitConnCount()

	// 我们认为多余的MinIdle个conn是有必要的, 因为conn可能会超时等异常导致conn被释放, 此时这些"多余"的conn就派上用场了.
	// 即使 MacActive 限制, 也应该保持MinIdle个conn备用.
	need := c.conf.MinIdle + wait - c.connList.Len() // 需要的

	// 如果确实有需要申请的(wait数量>0), 批量申请
	if wait > 0 && need < c.conf.BatchIncrement {
		need = c.conf.BatchIncrement
	}

	need -= c.connectingCount // 实际需要的, 排除正在连接的conn
	return need
}

// 释放无效的conn
func (c *ConnectPool) releaseInvalidConn() {
	if c.conf.IdleTimeout < 1 && c.conf.MaxConnLifetime < 1 {
		return
	}

	c.mx.Lock()
	defer c.mx.Unlock()

	if c.connList.Len() == 0 {
		return
	}

	e := c.connList.Front()
	for e != nil {
		conn := e.Value.(*Conn)
		if c.validConn(conn) {
			e = e.Next()
			continue
		}

		validE := e
		e = e.Next()
		c.connList.Remove(validE)
	}
}

// 释放多余的conn
func (c *ConnectPool) releaseNeedlessConn() {
	c.mx.Lock()
	defer c.mx.Unlock()

	shrink := 0
	// 如果超过最大空闲并且未达到当前允许批次释放数量
	for c.connList.Len() > c.conf.MaxIdle && shrink < c.conf.BatchShrink {
		e := c.connList.Back()
		c.connList.Remove(e)
		shrink++

		conn := e.Value.(*Conn)
		go c.conf.ConnClose(conn)
	}
}
