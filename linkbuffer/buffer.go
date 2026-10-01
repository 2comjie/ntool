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
	"bytes"
	"errors"
	"sync/atomic"
)

var untilErr = errors.New("link buffer read slice cannot find delim")

// NewLinkBuffer size defines the initial capacity, but there is no readable data.
func NewLinkBuffer(size ...int) *LinkBuffer {
	buf := &LinkBuffer{}
	var l int
	if len(size) > 0 {
		l = size[0]
	}
	node := newLinkBufferNode(l)
	buf.head, buf.read, buf.flush, buf.write = node, node, node, node
	return buf
}

// UnsafeLinkBuffer implements ReadWriter.
type UnsafeLinkBuffer struct {
	length     int64
	mallocSize int

	head  *linkBufferNode // release head
	read  *linkBufferNode // read head
	flush *linkBufferNode // malloc head
	write *linkBufferNode // malloc tail

	// buf allocated by Next when cross-package, which should be freed when release
	caches [][]byte

	// for `Peek` only, avoid creating too many []byte in `caches`
	// fix the issue when we have a large buffer and we call `Peek` multiple times
	cachePeek []byte
}

// Len implements Reader.
func (b *UnsafeLinkBuffer) Len() int {
	l := atomic.LoadInt64(&b.length)
	return int(l)
}

// IsEmpty check if this LinkBuffer is empty.
func (b *UnsafeLinkBuffer) IsEmpty() (ok bool) {
	return b.Len() == 0
}

// book will grow and malloc buffer to hold data.
//
// bookSize: The size of data that can be read at once.
// maxSize: The maximum size of data between two Release(). In some cases, this can
//
//	guarantee all data allocated in one node to reduce copy.
//
// book 是网络读事件专用的写入入口：一次性 book 出一块可写内存供 read(2) 填充。
func (b *UnsafeLinkBuffer) book(bookSize, maxSize int) (p []byte) {
	l := cap(b.write.buf) - b.write.malloc
	// grow linkBuffer
	if l == 0 {
		l = maxSize
		b.write.next = newLinkBufferNode(maxSize)
		b.write = b.write.next
	}
	if l > bookSize {
		l = bookSize
	}
	return b.write.Malloc(l)
}

// bookAck will ack the first n malloc bytes and discard the rest.
//
// length: The size of data in inputBuffer. It is used to calculate the maxSize
func (b *UnsafeLinkBuffer) bookAck(n int) (length int, err error) {
	b.write.malloc = n + len(b.write.buf)
	b.write.buf = b.write.buf[:b.write.malloc]
	b.flush = b.write

	// re-cal length
	length = b.recalLen(n)
	return length, nil
}

// calcMaxSize will calculate the data size between two Release()
func (b *UnsafeLinkBuffer) calcMaxSize() (sum int) {
	for node := b.head; node != b.read; node = node.next {
		sum += len(node.buf)
	}
	sum += len(b.read.buf)
	return sum
}

// resetTail will reset tail node or add an empty tail node to
// guarantee the tail node is not larger than 8KB
func (b *UnsafeLinkBuffer) resetTail(maxSize int) {
	if maxSize <= pagesize {
		// no need to reset a small buffer tail node
		return
	}
	// set nil tail
	b.write.next = newLinkBufferNode(0)
	b.write = b.write.next
	b.flush = b.write
}

// indexByte returns the index of the first instance of c in buffer, or -1 if c is not present in buffer.
func (b *UnsafeLinkBuffer) indexByte(c byte, skip int) int {
	size := b.Len()
	if skip >= size {
		return -1
	}
	var unread, n, l int
	node := b.read
	for unread = size; unread > 0; unread -= n {
		l = node.Len()
		if l >= unread { // last node
			n = unread
		} else { // read full node
			n = l
		}

		// skip current node
		if skip >= n {
			skip -= n
			node = node.next
			continue
		}
		i := bytes.IndexByte(node.Peek(n)[skip:], c)
		if i >= 0 {
			return (size - unread) + skip + i // past_read + skip_read + index
		}
		skip = 0 // no skip bytes
		node = node.next
	}
	return -1
}

// ------------------------------------------ private function ------------------------------------------

// recalLen re-calculate the length
func (b *UnsafeLinkBuffer) recalLen(delta int) (length int) {
	if delta < 0 && len(b.cachePeek) > 0 {
		// b.cachePeek will contain stale data if we read out even a single byte from buffer,
		// so we need to reset it or the next Peek call will return invalid bytes.
		b.cachePeek = b.cachePeek[:0]
	}
	return int(atomic.AddInt64(&b.length, int64(delta)))
}

// growth directly create the next node, when b.write is not enough.
// 扩容：跳过 flagUnmanaged 节点（内存不归我们管），write 节点剩余容量不足时向后移动或新建节点。
func (b *UnsafeLinkBuffer) growth(n int) {
	if n <= 0 {
		return
	}
	// the memory of readonly node if not malloc by us so should skip them
	for b.write.getFlag(flagUnmanaged) || cap(b.write.buf)-b.write.malloc < n {
		if b.write.next == nil {
			b.write.next = newLinkBufferNode(n)
			b.write = b.write.next
			return
		}
		b.write = b.write.next
	}
}

// isSingleNode determines whether reading needs to cross nodes.
// isSingleNode will move b.read to latest non-empty node if there is a zero-size node
// Must require b.Len() > 0
func (b *UnsafeLinkBuffer) isSingleNode(readN int) (single bool) {
	if readN <= 0 {
		return true
	}
	l := b.read.Len()
	for l == 0 && b.read != b.flush {
		b.read = b.read.next
		l = b.read.Len()
	}
	return l >= readN
}

// memorySize return the real memory size in bytes the LinkBuffer occupied
func (b *LinkBuffer) memorySize() (bytes int) {
	for node := b.head; node != nil; node = node.next {
		bytes += cap(node.buf)
	}
	for _, c := range b.caches {
		bytes += cap(c)
	}
	bytes += cap(b.cachePeek)
	return bytes
}
