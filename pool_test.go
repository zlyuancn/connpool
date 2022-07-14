package connpool

import (
	"context"
	"errors"
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

func TestGetPoolClose(t *testing.T) {
	conf := makeTestConfig()
	p, err := NewConnectPool(conf)
	require.Nil(t, err)
	p.Close()

	_, err = p.Get(context.Background())
	require.Equal(t, ErrPoolClosed, err)
}

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
