package connpool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkGet(b *testing.B) {
	conf := makeTestConfig()
	conf.WaitFirstConn = true
	p, err := NewConnectPool(conf)
	require.Nil(b, err)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := p.Get(context.Background())
			if err != nil {
				b.Fatal("获取失败", err)
			}
			p.Put(conn)
		}
	})
}

func BenchmarkGetNoActiveLimit(b *testing.B) {
	conf := makeTestConfig()
	conf.WaitFirstConn = true
	conf.MaxActive = 0
	p, err := NewConnectPool(conf)
	require.Nil(b, err)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := p.Get(context.Background())
			if err != nil {
				b.Fatal("获取失败", err)
			}
			p.Put(conn)
		}
	})
}
