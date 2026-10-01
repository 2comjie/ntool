// Copyright 2024 CloudWeGo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package linkbuffer

import (
	dirtmake "github.com/2comjie/ntool/lang/dirtymake"
	"github.com/2comjie/ntool/lang/mcache"
)

const (
	block1k  = 1 * 1024
	block2k  = 2 * 1024
	block4k  = 4 * 1024
	block8k  = 8 * 1024
	block32k = 32 * 1024

	pagesize  = block8k
	mallocMax = block8k * block1k // mallocMax is 8MB

	defaultLinkBufferMode = 0
)

// BinaryInplaceThreshold marks the minimum value of the nocopy slice length,
// which is the threshold to use copy to minimize overhead.
const BinaryInplaceThreshold = block4k

// LinkBufferCap that can be modified marks the minimum value of each node of LinkBuffer.
var LinkBufferCap = block4k

// malloc limits the cap of the buffer from mcache.
// 内存分配统一入口：小于等于 8MB 走 mcache 复用池，超过则直接堆分配。
func malloc(size, capacity int) []byte {
	if capacity > mallocMax {
		return dirtmake.Bytes(size, capacity)
	}
	return mcache.Malloc(size, capacity)
}

// free limits the cap of the buffer from mcache.
func free(buf []byte) {
	if cap(buf) > mallocMax {
		return
	}
	mcache.Free(buf)
}
