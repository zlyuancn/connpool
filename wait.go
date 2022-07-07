package connpool

import (
	"container/list"
	"context"
)

type waitReq struct {
	ch            chan *Conn
	e             *list.Element
	hasActiveLock bool // 是否已获得活跃锁
}

// 等待conn的数量
func (c *ConnectPool) getWaitConnCount() int {
	return c.activeWaitList.Len()
}

/*立即使用这个conn
  循环从 activeWaitList 列表中取出一个 waitReq, 然后将 conn 交给 waitReq,
  交付失败会释放一个活跃锁并重新从列表中取出 waitReq.
*/
func (c *ConnectPool) useConn(conn *Conn) bool {
	for c.activeWaitList.Len() > 0 {
		// 先进先出
		e := c.activeWaitList.Front()
		req := c.activeWaitList.Remove(e).(*waitReq)

		select {
		case req.ch <- conn: // 这里可能由于 waitReq 超时而无法放入
			return true
		default: // 放入conn失败则释放锁, 因为这个 waitReq 占用了锁
			c.putActiveLock()
		}
	}
	return false
}

/*放入一个活跃锁
  如果有等待获取活跃锁的waitReq, 则直接给这个waitReq
*/
func (c *ConnectPool) putActiveLock() {
	if c.waitList.Len() == 0 {
		if c.conf.MaxActive > 0 { // 否则释放一个锁
			c.activeLock <- struct{}{}
		}
		return
	}

	// 从未取得锁的等待队列中取出一个等待请求, 放入已获取锁等待请求列表
	e := c.waitList.Front()
	req := c.waitList.Remove(e).(*waitReq)

	e = c.activeWaitList.PushBack(req)
	req.e = e
	req.hasActiveLock = true
}

// 添加等待req
func (c *ConnectPool) addWaitReq(hasActiveLock bool) (*waitReq, error) {
	l := c.waitList

	if hasActiveLock {
		l = c.activeWaitList
	} else if c.conf.MaxWaitConnCount > 0 && c.waitList.Len() >= c.conf.MaxWaitConnCount { // 检查最大等待数量
		return nil, ErrMaxWaitConnLimit
	}

	req := &waitReq{
		ch:            make(chan *Conn),
		hasActiveLock: hasActiveLock,
	}
	reqElement := l.PushBack(req) // 放入末尾, 先进先出
	req.e = reqElement
	return req, nil
}

// waitReq等待获取到conn, 一旦成功取到conn则活跃计数+1
func (c *ConnectPool) waitReqGetConnLoop(ctx context.Context, req *waitReq) (*Conn, error) {
	// 等待conn
	ctxWait, cancel := context.WithTimeout(ctx, c.conf.WaitTimeout)
	defer cancel()

	select {
	case <-c.close: // 已关闭
		return nil, ErrPoolClosed
	case <-ctxWait.Done(): // 超时
		c.mx.Lock()
		if req.hasActiveLock {
			c.activeWaitList.Remove(req.e)
			c.putActiveLock() // 将活跃锁交出去
		} else {
			c.waitList.Remove(req.e)
		}
		c.mx.Unlock()
		return nil, ErrWaitGetConnTimeout
	case conn := <-req.ch:
		c.activeNum++
		return conn, nil
	}
}
