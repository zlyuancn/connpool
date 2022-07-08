package connpool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_Check(t *testing.T) {
	conf := &Config{
		Creator:        testCreator,
		ConnClose:      testConnClose,
		ValidConnected: testValidConnected,
	}
	err := conf.Check()
	require.Nil(t, err)
	t.Logf("%+v", conf)
}
