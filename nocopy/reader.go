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

package nocopy

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/bytedance/gopkg/lang/dirtmake"
)

// ------------------------------------------ implement copy reader ------------------------------------------

// readCopy copies up to len(p) bytes from the buffer into p without exposing
// the underlying buffer to user code (flagReadExposed is not set).
// After copying, it releases consumed nodes where readExposed is false.
// Nodes with readExposed are left for the next Release call.
func (b *UnsafeLinkBuffer) readCopy(p []byte) (n int) {
	l := len(p)
	if l == 0 || b.Len() == 0 {
		return 0
	}
	if has := b.Len(); has < l {
		l = has
	}
	b.recalLen(-l)

	// copy from nodes
	for ack := l; ack > 0; {
		if b.read.Len() == 0 {
			b.read = b.read.next
			continue
		}
		rd := b.read.Len()
		if rd >= ack {
			n += copy(p[n:], b.read.buf[b.read.off:b.read.off+ack])
			b.read.off += ack
			break
		}
		n += copy(p[n:], b.read.buf[b.read.off:])
		ack -= rd
		b.read = b.read.next
	}

	// advance read past empty nodes
	for b.read != b.flush && b.read.Len() == 0 {
		b.read = b.read.next
	}
	// release consumed nodes that are not readExposed.
	// exposed nodes stay in the chain so Release() can free them later.
	//
	// Example: [exposed/consumed] → [not-exposed/consumed] → [read/partial]
	// After:   head → [exposed] → [read/partial]
	//          the middle node is detached and released.
	var prev *linkBufferNode
	newHead := b.read
	for cur := b.head; cur != b.read; {
		next := cur.next
		if cur.readExposed() {
			if prev == nil {
				newHead = cur
			}
			prev = cur
		} else {
			cur.Release()
			if prev != nil {
				prev.next = next
			}
		}
		cur = next
	}
	b.head = newHead
	return n
}

// ------------------------------------------ implement zero-copy reader ------------------------------------------

// Next implements Reader.
// 消费 n 字节：单节点内直接返回底层内存切片（零拷贝，打 flagReadExposed），跨节点时拷贝到 caches 里。
func (b *UnsafeLinkBuffer) Next(n int) (p []byte, err error) {
	if n <= 0 {
		return
	}
	// check whether enough or not.
	if b.Len() < n {
		return p, fmt.Errorf("link buffer next[%d] not enough", n)
	}
	b.recalLen(-n) // re-cal length

	// single node
	if b.isSingleNode(n) {
		b.read.setFlag(flagReadExposed)
		return b.read.Next(n), nil
	}
	// multiple nodes
	var pIdx int
	if block1k < n && n <= mallocMax {
		p = malloc(n, n)
		b.caches = append(b.caches, p)
	} else {
		p = dirtmake.Bytes(n, n)
	}
	var l int
	for ack := n; ack > 0; ack = ack - l {
		l = b.read.Len()
		if l >= ack {
			pIdx += copy(p[pIdx:], b.read.Next(ack))
			break
		} else if l > 0 {
			pIdx += copy(p[pIdx:], b.read.Next(l))
		}
		b.read = b.read.next
	}
	_ = pIdx
	return p, nil
}

// Peek does not have an independent lifecycle, and there is no signal to
// indicate that Peek content can be released, so Peek will not introduce mcache for now.
// 窥视 n 字节但不消费：单节点零拷贝，跨节点复用 cachePeek 缓存避免反复分配。
func (b *UnsafeLinkBuffer) Peek(n int) (p []byte, err error) {
	if n <= 0 {
		return
	}
	// check whether enough or not.
	if b.Len() < n {
		return p, fmt.Errorf("link buffer peek[%d] not enough", n)
	}
	// single node
	if b.isSingleNode(n) {
		b.read.setFlag(flagReadExposed)
		return b.read.Peek(n), nil
	}

	// multiple nodes

	// try to make use of the cap of b.cachePeek, if can't, free it.
	if b.cachePeek != nil && cap(b.cachePeek) < n {
		free(b.cachePeek)
		b.cachePeek = nil
	}
	if b.cachePeek == nil {
		b.cachePeek = malloc(0, n) // init with zero len, will append later
	}
	p = b.cachePeek
	if len(p) >= n {
		// in case we peek smaller than last time,
		// we can return cache data directly.
		// we will reset cachePeek when Next or Skip, no worries about stale data
		return p[:n], nil
	}

	// How it works >>>>>>
	// [ -------- node0 -------- ][ --------- node1 --------- ]  <- b.read
	// [ --------------- p --------------- ]
	//                                     ^ len(p)     ^ n here
	//                           ^ scanned
	// `scanned` var is the len of last nodes which we scanned and already copied to p
	// `len(p) - scanned` is the start pos of current node for p to copy from
	// `n - len(p)` is the len of bytes we're going to append to p
	// 		we copy `len(node1)` - `len(p) - scanned` bytes in case node1 doesn't have enough data
	for scanned, node := 0, b.read; len(p) < n; node = node.next {
		l := node.Len()
		if scanned+l <= len(p) { // already copied in p, skip
			scanned += l
			continue
		}
		start := len(p) - scanned // `start` must be smaller than l coz `scanned+l <= len(p)` is false
		copyn := n - len(p)
		if nodeLeftN := l - start; copyn > nodeLeftN {
			copyn = nodeLeftN
		}
		p = append(p, node.Peek(l)[start:start+copyn]...)
		scanned += l
	}
	b.cachePeek = p
	return p[:n], nil
}

// Skip implements Reader.
func (b *UnsafeLinkBuffer) Skip(n int) (err error) {
	if n <= 0 {
		return
	}
	// check whether enough or not.
	if b.Len() < n {
		return fmt.Errorf("link buffer skip[%d] not enough", n)
	}
	b.recalLen(-n) // re-cal length

	var l int
	for ack := n; ack > 0; ack = ack - l {
		l = b.read.Len()
		if l >= ack {
			b.read.off += ack
			break
		}
		b.read = b.read.next
	}
	return nil
}

// Release the node that has been read.
// b.flush == nil indicates that this LinkBuffer is created by LinkBuffer.Slice
// 回收 read 指针之前所有已读完的节点，以及 Next 跨节点拷贝产生的 caches / cachePeek。
func (b *UnsafeLinkBuffer) Release() (err error) {
	for b.read != b.flush && b.read.Len() == 0 {
		b.read = b.read.next
	}
	for b.head != b.read {
		node := b.head
		b.head = b.head.next
		node.Release()
	}
	for i := range b.caches {
		free(b.caches[i])
		b.caches[i] = nil
	}
	b.caches = b.caches[:0]
	if b.cachePeek != nil {
		free(b.cachePeek)
		b.cachePeek = nil
	}
	return nil
}

// ReadString implements Reader.
func (b *UnsafeLinkBuffer) ReadString(n int) (s string, err error) {
	if n <= 0 {
		return
	}
	// check whether enough or not.
	if b.Len() < n {
		return s, fmt.Errorf("link buffer read string[%d] not enough", n)
	}
	p := b.readBinary(n)
	return unsafe.String(unsafe.SliceData(p), len(p)), nil
}

// ReadBinary implements Reader.
func (b *UnsafeLinkBuffer) ReadBinary(n int) (p []byte, err error) {
	if n <= 0 {
		return
	}
	// check whether enough or not.
	if b.Len() < n {
		return p, fmt.Errorf("link buffer read binary[%d] not enough", n)
	}
	return b.readBinary(n), nil
}

// readBinary cannot use mcache, because the memory allocated by readBinary will not be recycled.
func (b *UnsafeLinkBuffer) readBinary(n int) (p []byte) {
	b.recalLen(-n) // re-cal length

	// single node
	if b.isSingleNode(n) {
		p = dirtmake.Bytes(n, n)
		copy(p, b.read.Next(n))
		return p
	}
	p = dirtmake.Bytes(n, n)
	// multiple nodes
	var pIdx int
	var l int
	for ack := n; ack > 0; ack = ack - l {
		l = b.read.Len()
		if l >= ack {
			pIdx += copy(p[pIdx:], b.read.Next(ack))
			break
		} else if l > 0 {
			pIdx += copy(p[pIdx:], b.read.Next(l))
		}
		b.read = b.read.next
	}
	_ = pIdx
	return p
}

// ReadByte implements Reader.
func (b *UnsafeLinkBuffer) ReadByte() (p byte, err error) {
	// check whether enough or not.
	if b.Len() < 1 {
		return p, errors.New("link buffer read byte is empty")
	}
	b.recalLen(-1) // re-cal length
	for {
		if b.read.Len() >= 1 {
			return b.read.Next(1)[0], nil
		}
		b.read = b.read.next
	}
}

// Until returns a slice ends with the delim in the buffer.
// 读到分隔符为止（含分隔符）：先在链上定位 delim 的下标，再走 Next 消费。
func (b *UnsafeLinkBuffer) Until(delim byte) (line []byte, err error) {
	n := b.indexByte(delim, 0)
	if n < 0 {
		return nil, untilErr
	}
	return b.Next(n + 1)
}

// Slice returns a new LinkBuffer, which is a zero-copy slice of this LinkBuffer,
// and only holds the ability of Reader.
//
// Slice will automatically execute a Release.
// 零拷贝切分：通过 Refer 派生共享底层内存的只读节点组成新 Reader，原 buffer 长度扣减并自动 Release。
func (b *UnsafeLinkBuffer) Slice(n int) (r Reader, err error) {
	if n <= 0 {
		return NewLinkBuffer(0), nil
	}
	// check whether enough or not.
	if b.Len() < n {
		return r, fmt.Errorf("link buffer readv[%d] not enough", n)
	}
	b.recalLen(-n) // re-cal length

	// just use for range
	p := new(LinkBuffer)
	p.length = int64(n)

	defer func() {
		// set to read-only
		p.flush = p.flush.next
		p.write = p.flush
	}()

	// single node
	if b.isSingleNode(n) {
		b.read.setFlag(flagReadExposed)
		node := b.read.Refer(n)
		p.head, p.read, p.flush = node, node, node
		return p, nil
	}
	// multiple nodes
	l := b.read.Len()
	b.read.setFlag(flagReadExposed)
	node := b.read.Refer(l)
	b.read = b.read.next

	p.head, p.read, p.flush = node, node, node
	for ack := n - l; ack > 0; ack = ack - l {
		l = b.read.Len()
		if l >= ack {
			b.read.setFlag(flagReadExposed)
			p.flush.next = b.read.Refer(ack)
			p.flush = p.flush.next
			break
		} else if l > 0 {
			b.read.setFlag(flagReadExposed)
			p.flush.next = b.read.Refer(l)
			p.flush = p.flush.next
		}
		b.read = b.read.next
	}
	return p, b.Release()
}
