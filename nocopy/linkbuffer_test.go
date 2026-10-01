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
	"bytes"
	"testing"
)

func TestWriteReadLoopback(t *testing.T) {
	b := NewLinkBuffer()
	msg := []byte("hello world")

	buf, err := b.Malloc(len(msg))
	if err != nil {
		t.Fatal(err)
	}
	copy(buf, msg)
	if b.Len() != 0 {
		t.Fatalf("before flush: Len() = %d, want 0", b.Len())
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if b.Len() != len(msg) {
		t.Fatalf("after flush: Len() = %d, want %d", b.Len(), len(msg))
	}

	p, err := b.Next(len(msg))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p, msg) {
		t.Fatalf("Next() = %q, want %q", p, msg)
	}
	if b.Len() != 0 {
		t.Fatalf("after next: Len() = %d, want 0", b.Len())
	}
}

func TestCrossNode(t *testing.T) {
	b := NewLinkBuffer()
	const total = 3 * block4k // 12KB，必然跨节点

	// 分多次小段写入，保证节点内填不满而触发 growth 产生多个节点
	src := make([]byte, total)
	for i := range src {
		src[i] = byte(i % 256)
	}
	const chunk = 1024
	for off := 0; off < total; off += chunk {
		buf, err := b.Malloc(chunk)
		if err != nil {
			t.Fatal(err)
		}
		copy(buf, src[off:off+chunk])
		if err := b.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if b.Len() != total {
		t.Fatalf("Len() = %d, want %d", b.Len(), total)
	}
	if b.isSingleNode(total) {
		t.Skip("data fits in a single node, skip cross-node check")
	}

	// 跨节点 Peek
	p, err := b.Peek(total)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p, src) {
		t.Fatal("cross-node Peek content mismatch")
	}

	// 跨节点 Next
	p, err = b.Next(total)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p, src) {
		t.Fatal("cross-node Next content mismatch")
	}
	if b.Len() != 0 {
		t.Fatalf("after next: Len() = %d, want 0", b.Len())
	}
	if err := b.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestPeekNotConsume(t *testing.T) {
	b := NewLinkBuffer()
	msg := []byte("peek and next")

	buf, _ := b.Malloc(len(msg))
	copy(buf, msg)
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}

	p1, err := b.Peek(len(msg))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p1, msg) {
		t.Fatalf("Peek() = %q, want %q", p1, msg)
	}
	if b.Len() != len(msg) {
		t.Fatalf("after peek: Len() = %d, want %d (peek must not consume)", b.Len(), len(msg))
	}

	p2, err := b.Next(len(msg))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p2, msg) {
		t.Fatalf("Next() = %q, want %q", p2, msg)
	}
}

func TestUntil(t *testing.T) {
	b := NewLinkBuffer()
	data := "line1\r\nline2\r\nrest"

	buf, _ := b.Malloc(len(data))
	copy(buf, data)
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}

	line1, err := b.Until('\n')
	if err != nil {
		t.Fatal(err)
	}
	if string(line1) != "line1\r\n" {
		t.Fatalf("Until() = %q, want %q", line1, "line1\r\n")
	}

	line2, err := b.Until('\n')
	if err != nil {
		t.Fatal(err)
	}
	if string(line2) != "line2\r\n" {
		t.Fatalf("Until() = %q, want %q", line2, "line2\r\n")
	}

	// 剩余数据没有分隔符，Until 应返回错误
	if _, err := b.Until('\n'); err == nil {
		t.Fatal("Until() should fail when no delim exists")
	}
	if b.Len() != len("rest") {
		t.Fatalf("after failed until: Len() = %d, want %d", b.Len(), len("rest"))
	}
}

func TestSlice(t *testing.T) {
	b := NewLinkBuffer()
	msg := []byte("hello world")

	buf, _ := b.Malloc(len(msg))
	copy(buf, msg)
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}

	r, err := b.Slice(5)
	if err != nil {
		t.Fatal(err)
	}
	if r.Len() != 5 {
		t.Fatalf("slice Len() = %d, want 5", r.Len())
	}
	p, err := r.Next(5)
	if err != nil {
		t.Fatal(err)
	}
	if string(p) != "hello" {
		t.Fatalf("slice Next() = %q, want %q", p, "hello")
	}

	if b.Len() != len(msg)-5 {
		t.Fatalf("origin Len() = %d, want %d", b.Len(), len(msg)-5)
	}
	rest, err := b.Next(b.Len())
	if err != nil {
		t.Fatal(err)
	}
	if string(rest) != " world" {
		t.Fatalf("origin Next() = %q, want %q", rest, " world")
	}
}

func TestRelease(t *testing.T) {
	b := NewLinkBuffer()
	msg := []byte("release me")

	buf, _ := b.Malloc(len(msg))
	copy(buf, msg)
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}

	if _, err := b.Next(len(msg)); err != nil {
		t.Fatal(err)
	}
	if err := b.Release(); err != nil {
		t.Fatal(err)
	}
	if b.Len() != 0 {
		t.Fatalf("after release: Len() = %d, want 0", b.Len())
	}
	if !b.IsEmpty() {
		t.Fatal("IsEmpty() = false, want true")
	}
}

func TestMallocAck(t *testing.T) {
	b := NewLinkBuffer()

	buf, err := b.Malloc(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(buf) != 100 {
		t.Fatalf("Malloc(100) len = %d, want 100", len(buf))
	}
	copy(buf[:60], bytes.Repeat([]byte("a"), 60))
	if b.MallocLen() != 100 {
		t.Fatalf("MallocLen() = %d, want 100", b.MallocLen())
	}
	if err := b.MallocAck(60); err != nil {
		t.Fatal(err)
	}
	if b.MallocLen() != 60 {
		t.Fatalf("after ack: MallocLen() = %d, want 60", b.MallocLen())
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}

	if b.Len() != 60 {
		t.Fatalf("Len() = %d, want 60", b.Len())
	}
	p, err := b.Next(60)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p, bytes.Repeat([]byte("a"), 60)) {
		t.Fatal("content mismatch after MallocAck")
	}
}
