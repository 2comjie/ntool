package string_sync

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

//go:embed ack.lua
var ackLua string

var (
	readScript  = redis.NewScript(readLua)
	fillScript  = redis.NewScript(fillLua)
	writeScript = redis.NewScript(writeLua)
	ackScript   = redis.NewScript(ackLua)
)
