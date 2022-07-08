package connpool

import (
	"context"
	"errors"
	"testing"

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
