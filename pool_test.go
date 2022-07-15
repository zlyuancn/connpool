package connpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testConn struct{}

func testCreator(ctx context.Context) (interface{}, error) { return testConn{}, nil }
func testConnClose(conn *Conn)                             {}
func testValidConnected(conn *Conn) bool                   { return true }

func makeTestConfig() *Config {
	conf := NewConfig()
	conf.Creator = testCreator
	conf.ConnClose = testConnClose
	conf.ValidConnected = testValidConnected
	return conf
}

// 等待第一个conn创建完毕
func TestWaitFirstConn(t *testing.T) {
	conf := makeTestConfig()
	conf.WaitFirstConn = true
	p, err := NewConnectPool(conf)
	require.Nil(t, err)
	p.Close()

	conf.Creator = func(ctx context.Context) (interface{}, error) {
		return nil, errors.New("")
	}
	_, err = NewConnectPool(conf)
	require.NotNil(t, err)
}

func TestGet(t *testing.T) {
	conf := makeTestConfig()
	p, err := NewConnectPool(conf)
	require.Nil(t, err)

	conn, err := p.Get(context.Background())
	require.Nil(t, err)
	require.Equal(t, testConn{}, conn.GetConn())
	p.Close()
}

// 获取时pool已关闭
func TestGetPoolClose(t *testing.T) {
	conf := makeTestConfig()
	p, err := NewConnectPool(conf)
	require.Nil(t, err)
	p.Close()

	_, err = p.Get(context.Background())
	require.Equal(t, ErrPoolClosed, err)
}

// 获取超时
func TestGetTimeout(t *testing.T) {
	conf := makeTestConfig()
	conf.MaxActive = 1
	conf.WaitTimeout = time.Millisecond * 300
	p, err := NewConnectPool(conf)
	require.Nil(t, err)

	_, err = p.Get(context.Background())
	require.Nil(t, err)

	_, err = p.Get(context.Background())
	require.Equal(t, ErrWaitGetConnTimeout, err)
	p.Close()
}

// 获取超时ctx
func TestGetTimeoutCtx(t *testing.T) {
	conf := makeTestConfig()
	conf.MaxActive = 1
	p, err := NewConnectPool(conf)
	require.Nil(t, err)

	_, err = p.Get(context.Background())
	require.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*300)
	defer cancel()
	_, err = p.Get(ctx)
	require.Equal(t, ErrWaitGetConnTimeout, err)
	p.Close()
}

// 获取时达到最大等待数
func TestGetLimitWaitCount(t *testing.T) {
	conf := makeTestConfig()
	conf.MaxActive = 1
	conf.MaxWaitConnCount = 1
	p, err := NewConnectPool(conf)
	require.Nil(t, err)

	_, err = p.Get(context.Background())
	require.Nil(t, err)

	go func() {
		// 占位
		_, err = p.Get(context.Background())
		require.Equal(t, ErrPoolClosed, err)
	}()

	time.Sleep(time.Millisecond * 200) // 这里让上面的goroutinue先执行
	_, err = p.Get(context.Background())
	require.Equal(t, ErrMaxWaitConnLimit, err)
	p.Close()
}

func TestPut(t *testing.T) {
	conf := makeTestConfig()
	conf.MinIdle = 1
	conf.CheckIdleInterval = time.Minute // 将自动补足时间变长
	p, err := NewConnectPool(conf)
	require.Nil(t, err)

	time.Sleep(time.Millisecond * 200)       // 等待初始化填充conn
	conn, err := p.Get(context.Background()) // 将空闲的1个conn消耗掉
	require.Nil(t, err)

	go func() {
		time.Sleep(time.Second)
		p.Put(conn)
	}()

	conf.Creator = func(ctx context.Context) (interface{}, error) {
		time.Sleep(time.Second * 5) // 这里延长创建conn时间让自动补足失效
		return testConn{}, nil
	}

	s := time.Now().Unix()
	_, err = p.Get(context.Background()) // 这里会触发立即补充conn
	if v := time.Now().Unix() - s; v < 1 {
		t.Errorf("实际等待了%d秒", v)
	}
	time.Sleep(time.Millisecond * 200) // 等待补充触发
	p.Close()
}

// 放入无效的conn
func TestPutInvalidConn(t *testing.T) {
	conf := makeTestConfig()
	conf.MinIdle = 1
	conf.MaxConnLifetime = time.Second
	conf.CheckIdleInterval = time.Minute // 将自动补足时间变长
	p, err := NewConnectPool(conf)
	require.Nil(t, err)

	time.Sleep(time.Millisecond * 500) // 等待初始化填充conn
	conn, err := p.Get(context.Background())
	require.Nil(t, err)

	time.Sleep(time.Second) // 等待conn达到最大存活时间

	closeNum := 0
	conf.ConnClose = func(conn *Conn) {
		closeNum++
	}
	p.Put(conn)
	time.Sleep(time.Millisecond * 200) // 调用关闭是通过goroutine的
	require.Equal(t, true, closeNum > 0)
	p.Close()
}

// 放入时将conn交给waitReq
func TestPutConnToWaitReq(t *testing.T) {
	conf := makeTestConfig()
	conf.MinIdle = 1
	conf.CheckIdleInterval = time.Minute // 将自动补足时间变长
	p, err := NewConnectPool(conf)
	require.Nil(t, err)
	defer p.Close()

	time.Sleep(time.Millisecond * 200)       // 等待初始化填充conn
	conn, err := p.Get(context.Background()) // 取出初始化创建的
	require.Nil(t, err)

	conf.Creator = func(ctx context.Context) (interface{}, error) {
		time.Sleep(time.Minute) // 将创建时间变长以让主动填充失效
		return testConn{}, nil
	}

	go func() {
		time.Sleep(time.Second * 1)
		p.Put(conn)
	}()
	s := time.Now().Unix()
	_, err = p.Get(context.Background()) // 再次获取
	require.Nil(t, err)
	if v := time.Now().Unix() - s; v < 1 {
		t.Errorf("实际等待了%d秒", v)
	}
}

// 放入时将活跃锁交给waitReq
func TestPutActiveToWaitReq(t *testing.T) {
	conf := makeTestConfig()
	conf.MinIdle = 1
	conf.MaxActive = 1
	conf.CheckIdleInterval = time.Minute // 将自动补足时间变长
	p, err := NewConnectPool(conf)
	require.Nil(t, err)
	defer p.Close()

	time.Sleep(time.Millisecond * 200)       // 等待初始化填充conn
	conn, err := p.Get(context.Background()) // 取出初始化创建的
	require.Nil(t, err)

	conf.Creator = func(ctx context.Context) (interface{}, error) {
		time.Sleep(time.Minute) // 将创建时间变长以让主动填充失效
		return testConn{}, nil
	}

	go func() {
		time.Sleep(time.Second * 1)
		p.Put(conn)
	}()
	s := time.Now().Unix()
	_, err = p.Get(context.Background()) // 再次获取
	require.Nil(t, err)
	if v := time.Now().Unix() - s; v < 1 {
		t.Errorf("实际等待了%d秒", v)
	}
}

func TestReplenishLackConn(t *testing.T) {
	conf := makeTestConfig()
	conf.MinIdle = 1
	conf.BatchIncrement = 5
	conf.MaxIdle = 10                    // BatchIncrement上限突破
	conf.CheckIdleInterval = time.Minute // 将自动补足时间变长
	p, err := NewConnectPool(conf)
	require.Nil(t, err)

	time.Sleep(time.Millisecond * 200)   // 等待初始化填充conn
	_, err = p.Get(context.Background()) // 将空闲的1个conn消耗掉
	require.Nil(t, err)

	creatorNum := 0
	conf.Creator = func(ctx context.Context) (interface{}, error) {
		creatorNum++
		time.Sleep(time.Second * 1)
		return testConn{}, nil
	}

	s := time.Now().Unix()
	_, err = p.Get(context.Background()) // 这里会立即触发补充conn
	if v := time.Now().Unix() - s; v < 1 {
		t.Errorf("实际等待了%d秒", v)
	}
	time.Sleep(time.Millisecond * 200) // 等待补充触发
	require.Equal(t, 5, creatorNum)    // 5 = 批次创建5个
	p.Close()
}

// 重复关闭
func TestRepeatClose(t *testing.T) {
	conf := makeTestConfig()
	p, err := NewConnectPool(conf)
	require.Nil(t, err)
	p.Close()
	p.Close()
	p.Close()
}

// 获取conn时conn存活超时
func TestConnLifetimeTimeout(t *testing.T) {
	conf := makeTestConfig()
	conf.MinIdle = 1
	conf.BatchIncrement = 1
	conf.MaxConnLifetime = time.Second
	conf.CheckIdleInterval = time.Minute // 将自动补足时间变长
	p, err := NewConnectPool(conf)
	require.Nil(t, err)
	time.Sleep(time.Millisecond * 200) // 等待主动创建完毕

	closeNum := 0
	conf.ConnClose = func(conn *Conn) {
		closeNum += 1
	}

	creatorNum := 0
	conf.Creator = func(ctx context.Context) (interface{}, error) {
		creatorNum++
		return testConn{}, nil
	}

	time.Sleep(time.Second * 2)          // 等待conn超时
	_, err = p.Get(context.Background()) // 重新获取
	require.Nil(t, err)
	time.Sleep(time.Millisecond * 200) // 等待主动补足完毕
	require.Equal(t, 2, creatorNum)    // 2 = 重新创建的1个 + 最小空闲1个
	require.Equal(t, 1, closeNum)
}

// 获取conn时无效
func TestGetConnValid(t *testing.T) {
	conf := makeTestConfig()
	conf.MinIdle = 1
	conf.BatchIncrement = 1
	conf.MaxConnLifetime = time.Second
	conf.CheckIdleInterval = time.Minute // 将自动补足时间变长
	p, err := NewConnectPool(conf)
	require.Nil(t, err)
	time.Sleep(time.Millisecond * 200) // 等待主动创建完毕

	creatorNum := 0
	conf.ValidConnected = func(conn *Conn) bool {
		if creatorNum == 0 { // 第一次拿到的是初始化时创建的
			return false
		}
		return true
	}
	conf.Creator = func(ctx context.Context) (interface{}, error) {
		creatorNum++
		return testConn{}, nil
	}

	_, err = p.Get(context.Background()) // 重新获取
	require.Nil(t, err)
	time.Sleep(time.Millisecond * 200) // 等待主动补足完毕
	require.Equal(t, 2, creatorNum)    // 2 = 重新创建的1个 + 最小空闲1个
}

// 获取conn时空闲超时
func TestGetConnIdleTimeout(t *testing.T) {
	conf := makeTestConfig()
	conf.MinIdle = 1
	conf.BatchIncrement = 1
	conf.IdleTimeout = time.Second
	conf.CheckIdleInterval = time.Minute // 将自动补足时间变长
	p, err := NewConnectPool(conf)
	require.Nil(t, err)
	time.Sleep(time.Millisecond * 200) // 等待主动创建完毕

	closeNum := 0
	conf.ConnClose = func(conn *Conn) {
		closeNum += 1
	}

	creatorNum := 0
	conf.Creator = func(ctx context.Context) (interface{}, error) {
		creatorNum++
		return testConn{}, nil
	}

	time.Sleep(time.Second * 2)          // 等待conn超时
	_, err = p.Get(context.Background()) // 重新获取
	require.Nil(t, err)
	time.Sleep(time.Millisecond * 200) // 等待主动补足完毕
	require.Equal(t, 2, creatorNum)    // 2 = 重新创建的1个 + 最小空闲1个
	require.Equal(t, 1, closeNum)
}

// conn自动空闲超时
func TestAutoConnIdleTimeout(t *testing.T) {
	conf := makeTestConfig()
	conf.MinIdle = 1
	conf.BatchIncrement = 1
	conf.IdleTimeout = time.Second
	conf.CheckIdleInterval = time.Second // 将自动补足时间变短
	p, err := NewConnectPool(conf)
	defer p.Close()
	require.Nil(t, err)
	time.Sleep(time.Millisecond * 200) // 等待主动创建完毕

	closeNum := 0
	conf.ConnClose = func(conn *Conn) {
		closeNum += 1
	}

	creatorNum := 0
	conf.Creator = func(ctx context.Context) (interface{}, error) {
		creatorNum++
		return testConn{}, nil
	}

	time.Sleep(time.Second * 1)        // 等待触发检查
	time.Sleep(time.Millisecond * 200) // 等待主动补足完毕
	require.Equal(t, 1, creatorNum)    // 最小空闲1个
	require.Equal(t, 1, closeNum)      // 自动关闭1个
}

// conn自动存活超时
func TestAutoConnLifetimeTimeout(t *testing.T) {
	conf := makeTestConfig()
	conf.MinIdle = 1
	conf.BatchIncrement = 1
	conf.MaxConnLifetime = time.Second
	conf.CheckIdleInterval = time.Second // 将自动补足时间变短
	p, err := NewConnectPool(conf)
	defer p.Close()
	require.Nil(t, err)
	time.Sleep(time.Millisecond * 200) // 等待主动创建完毕

	closeNum := 0
	conf.ConnClose = func(conn *Conn) {
		closeNum += 1
	}

	creatorNum := 0
	conf.Creator = func(ctx context.Context) (interface{}, error) {
		creatorNum++
		return testConn{}, nil
	}

	time.Sleep(time.Second * 1)        // 等待触发检查
	time.Sleep(time.Millisecond * 200) // 等待主动补足完毕
	require.Equal(t, 1, creatorNum)    // 最小空闲1个
	require.Equal(t, 1, closeNum)      // 自动关闭1个
}

// 触发检查时无需补充数量
func TestNoReplenish(t *testing.T) {
	conf := makeTestConfig()
	conf.MinIdle = 1
	conf.CheckIdleInterval = time.Second // 将自动补足时间变短
	p, err := NewConnectPool(conf)
	defer p.Close()
	require.Nil(t, err)
	time.Sleep(time.Millisecond * 200) // 等待主动创建完毕

	creatorNum := 0
	conf.Creator = func(ctx context.Context) (interface{}, error) {
		creatorNum++
		return testConn{}, nil
	}

	time.Sleep(time.Second * 1)        // 等待触发检查
	time.Sleep(time.Millisecond * 200) // 等待主动补足完毕
	require.Equal(t, 0, creatorNum)    // 没有补充
}

// 自动释放时没有conn
func TestAutoReleaseNoConn(t *testing.T) {
	conf := makeTestConfig()
	conf.MinIdle = 1
	conf.IdleTimeout = time.Second * 2
	conf.CheckIdleInterval = time.Second // 将自动补足时间变短
	p, err := NewConnectPool(conf)
	defer p.Close()
	require.Nil(t, err)

	time.Sleep(time.Millisecond * 200)   // 等待主动创建完毕
	_, err = p.Get(context.Background()) // 将空闲的1个conn消耗掉
	require.Nil(t, err)

	time.Sleep(time.Second * 1)        // 等待触发检查
	time.Sleep(time.Millisecond * 200) // 等待主动补足完毕
}

// 自动释放时没有无效的conn
func TestAutoReleaseNoInvalidConn(t *testing.T) {
	conf := makeTestConfig()
	conf.MinIdle = 1
	conf.IdleTimeout = time.Second * 2
	conf.CheckIdleInterval = time.Second // 将自动补足时间变短
	p, err := NewConnectPool(conf)
	defer p.Close()
	require.Nil(t, err)

	time.Sleep(time.Millisecond * 200) // 等待主动创建完毕

	time.Sleep(time.Second * 1)        // 等待触发检查
	time.Sleep(time.Millisecond * 200) // 等待主动补足完毕
}

// 自动释放多余的conn
func TestAutoReleaseNeedlessConn(t *testing.T) {
	conf := makeTestConfig()
	conf.MaxActive = 10
	conf.MinIdle = 1
	conf.BatchIncrement = 1
	conf.MaxIdle = 2
	conf.CheckIdleInterval = time.Second // 将自动补足时间变短
	conf.BatchShrink = 3                 // 一次最多释放3个
	p, err := NewConnectPool(conf)
	defer p.Close()
	require.Nil(t, err)

	time.Sleep(time.Millisecond * 200)   // 等待主动创建完毕
	_, err = p.Get(context.Background()) // 取出已存在的1个
	require.Nil(t, err)

	creatorNum := int32(0)
	conf.Creator = func(ctx context.Context) (interface{}, error) {
		atomic.AddInt32(&creatorNum, 1)
		return testConn{}, nil
	}
	connList := make([]*Conn, 5)
	for i := 0; i < len(connList); i++ {
		conn, err := p.Get(context.Background()) // 取出
		require.Nil(t, err)
		connList[i] = conn
	}
	time.Sleep(time.Millisecond * 100)     // 等待主动补足完毕
	require.Equal(t, int32(6), creatorNum) // 申请的5个 + 最小空闲1个

	for _, conn := range connList {
		p.Put(conn)
	}

	closeNum := 0
	conf.ConnClose = func(conn *Conn) {
		closeNum++
	}

	time.Sleep(time.Second)       // 等待触发
	require.Equal(t, 3, closeNum) // 最小空闲1个 + 回收的5个 - 最大空闲2个, 但是一次最多释放3个, 最后剩下3个
	closeNum = 0
	time.Sleep(time.Second)       // 再次等待触发
	require.Equal(t, 1, closeNum) // 当前数量3个 - 最大空闲2个
}
