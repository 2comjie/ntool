package dirty_sync

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

//go:embed mark.lua
var markLua string

//go:embed claim.lua
var claimLua string

//go:embed renew.lua
var renewLua string

//go:embed ack.lua
var ackLua string

//go:embed release.lua
var releaseLua string

var (
	markScript    = redis.NewScript(markLua)
	claimScript   = redis.NewScript(claimLua)
	renewScript   = redis.NewScript(renewLua)
	ackScript     = redis.NewScript(ackLua)
	releaseScript = redis.NewScript(releaseLua)
)
