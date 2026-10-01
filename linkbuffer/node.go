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
	"sync"
	"sync/atomic"
)

const (
	// flagUnmanaged marks a buffer node whose memory is not allocated by the LinkBuffer
	// (e.g. user-provided data via WriteDirect, or a zero-size node).
	// Unmanaged nodes are not reusable and are skipped during buffer growth.
	flagUnmanaged uint8 = 1 << 0 // 0000 0001
	// flagReadExposed marks a buffer node whose underlying memory has been returned
	// directly to user code via a zero-copy Reader method (Next, Peek, Slice, GetBytes).
	// The buffer may still be referenced by user code until Release is called.
	flagReadExposed uint8 = 1 << 1 // 0000 0010
)

// newLinkBufferNode create or reuse linkBufferNode.
// Nodes with size <= 0 are marked as readonly, which means the node.buf is not allocated by this mcache.
// 节点的统一构造入口：从 sync.Pool 复用，size<=0 时打 flagUnmanaged 标记（不分配 buf）。
func newLinkBufferNode(size int) *linkBufferNode {
	node := linkedPool.Get().(*linkBufferNode)
	// reset node offset
	node.off, node.malloc, node.refer, node.mode = 0, 0, 1, defaultLinkBufferMode
	if size <= 0 {
		node.setFlag(flagUnmanaged)
		return node
	}
	if size < LinkBufferCap {
		size = LinkBufferCap
	}
	node.buf = malloc(0, size)
	return node
}

var linkedPool = sync.Pool{
	New: func() interface{} {
		return &linkBufferNode{
			refer: 1, // comes with 1 reference
		}
	},
}

// linkBufferNode 是链表的节点：buf 为底层内存，off 为读偏移，malloc 为写偏移，
// refer 为引用计数（Slice/Refer 派生的只读节点共享 origin 的内存）。
type linkBufferNode struct {
	buf    []byte          // buffer
	off    int             // read-offset
	malloc int             // write-offset
	refer  int32           // reference count
	mode   uint8           // mode store all bool bit status
	origin *linkBufferNode // the root node of the extends
	next   *linkBufferNode // the next node of the linked buffer
}

func (node *linkBufferNode) Len() (l int) {
	return len(node.buf) - node.off
}

func (node *linkBufferNode) IsEmpty() (ok bool) {
	return node.off == len(node.buf)
}

func (node *linkBufferNode) Reset() {
	if node.origin != nil || atomic.LoadInt32(&node.refer) != 1 {
		return
	}
	node.off, node.malloc = 0, 0
	node.buf = node.buf[:0]
}

func (node *linkBufferNode) Next(n int) (p []byte) {
	off := node.off
	node.off += n
	return node.buf[off:node.off:node.off]
}

func (node *linkBufferNode) Peek(n int) (p []byte) {
	return node.buf[node.off : node.off+n : node.off+n]
}

func (node *linkBufferNode) Malloc(n int) (buf []byte) {
	malloc := node.malloc
	node.malloc += n
	return node.buf[malloc:node.malloc:node.malloc]
}

// Refer holds a reference count at the same time as Next, and releases the real buffer after Release.
// The node obtained by Refer is read-only.
// Refer 派生一个共享底层内存的只读节点，同时给 origin 的引用计数 +1（Slice 的基础）。
func (node *linkBufferNode) Refer(n int) (p *linkBufferNode) {
	p = newLinkBufferNode(0)
	p.buf = node.Next(n)

	if node.origin != nil {
		p.origin = node.origin
	} else {
		p.origin = node
	}
	atomic.AddInt32(&p.origin.refer, 1)
	return p
}

// Release consists of two parts:
// 1. reduce the reference count of itself and origin.
// 2. recycle the buf when the reference count is 0.
func (node *linkBufferNode) Release() (err error) {
	if node.origin != nil {
		node.origin.Release()
	}
	// release self
	if atomic.AddInt32(&node.refer, -1) == 0 {
		// readonly nodes cannot recycle node.buf, other node.buf are recycled to mcache.
		if node.reusable() {
			free(node.buf)
		}
		node.buf, node.origin, node.next = nil, nil, nil
		linkedPool.Put(node)
	}
	return nil
}

func (node *linkBufferNode) getFlag(flag uint8) bool {
	return node.mode&flag > 0
}

func (node *linkBufferNode) setFlag(flag uint8) {
	node.mode |= flag
}

func (node *linkBufferNode) unsetFlag(flag uint8) {
	node.mode &^= flag
}

// reusable reports whether the node's buffer memory is owned by the LinkBuffer and can be recycled.
// Called during Release to decide if node.buf should be returned to mcache via free.
func (node *linkBufferNode) reusable() bool {
	return node.mode&flagUnmanaged == 0
}

// readExposed reports whether the node's buffer has been returned directly to user code
// via a zero-copy Reader method and may still be referenced externally.
func (node *linkBufferNode) readExposed() bool {
	return node.mode&flagReadExposed > 0
}
