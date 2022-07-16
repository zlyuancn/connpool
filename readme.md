
# 示例

```go
package main

import (
	"context"
	"net"

	"github.com/zlyuancn/connpool"
)

func main() {
	conf := connpool.NewConfig()
	// 设置创建函数
	conf.Creator = func(ctx context.Context) (interface{}, error) {
		return net.Dial("tcp", "127.0.0.1:8080")
	}
	// 设置关闭连接函数
	conf.ConnClose = func(conn *connpool.Conn) {
		v := conn.GetConn().(net.Conn)
		_ = v.Close()
	}

	// 创建连接池
	pool, _ := connpool.NewConnectPool(conf)

	// 获取conn
	conn, err := pool.Get(context.Background())
	if err != nil {
		panic(err)
	}

	// 放入conn
	pool.Put(conn)

	// 关闭连接池
	pool.Close()
}
```
