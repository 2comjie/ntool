package hash_sync

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

//go:embed read.lua
var readLua string

//go:embed fill.lua
var fillLua string

//go:embed write.lua
var writeLua string

//go:embed claim.lua
var claimLua string

//go:embed renew.lua
var renewLua string

//go:embed ack.lua
var ackLua string

//go:embed release.lua
var releaseLua string

var (
	readScript    = redis.NewScript(readLua)
	fillScript    = redis.NewScript(fillLua)
	writeScript   = redis.NewScript(writeLua)
	claimScript   = redis.NewScript(claimLua)
	renewScript   = redis.NewScript(renewLua)
	ackScript     = redis.NewScript(ackLua)
	releaseScript = redis.NewScript(releaseLua)
)
