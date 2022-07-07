package connpool

import (
	"time"
)

type Conn struct {
	v          interface{} // 通过 Creator 创建的真实连接
	createTime int64       // 创建时间, 秒级时间戳
	putTimeSec int64       // 放入时间, 秒级时间戳
}

// 获取通过 Creator 创建的真实连接
func (c *Conn) GetConn() interface{} {
	return c.v
}

// 传入一个真实连接以生成conn
func makeConn(v interface{}) *Conn {
	return &Conn{
		v:          v,
		createTime: time.Now().Unix(),
	}
}

// 校验conn
func (c *ConnectPool) validConn(conn *Conn) bool {
	// 无效的conn
	if !c.conf.ValidConnected(conn) {
		return false
	}

	// 最大存活时间超时
	if c.conf.MaxConnLifetime > 0 &&
		time.Duration(time.Now().Unix()-conn.createTime)*time.Second >= c.conf.MaxConnLifetime {
		go c.conf.ConnClose(conn)
		return false
	}

	// 空闲超时
	if c.conf.IdleTimeout > 1 && conn.putTimeSec > 0 &&
		time.Duration(time.Now().Unix()-conn.putTimeSec)*time.Second >= c.conf.IdleTimeout {
		go c.conf.ConnClose(conn)
		return false
	}

	return true
}

// 从已连接的conn列表弹出第一个有效的conn, 不存在时返回nil
func (c *ConnectPool) popFrontConn() *Conn {
	for c.connList.Len() > 0 {
		e := c.connList.Front()
		c.connList.Remove(e)

		conn := e.Value.(*Conn)
		if c.validConn(conn) {
			conn.putTimeSec = 0 // 重置放入时间
			return conn
		}
	}
	return nil
}
