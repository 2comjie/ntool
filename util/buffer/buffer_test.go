package buffer

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// ============ 基础构造与状态 ============

func TestNewLinkBuffer(t *testing.T) {
	buf := NewLinkBuffer()
	if buf.Len() != 0 || !buf.IsEmpty() {
		t.Fatal("新 buffer 应为空")
	}

	buf = NewLinkBuffer(1024)
	if buf.Len() != 0 || !buf.IsEmpty() {
		t.Fatal("指定容量的新 buffer 也应为空（容量不等于长度）")
	}
}

// ============ 写路径：WriteString / WriteBinary / WriteByte / Malloc / Flush ============

func TestWriteAndFlush(t *testing.T) {
	buf := NewLinkBuffer()

	// Flush 之前数据不可读（只是 malloc 态）
	if _, err := buf.WriteString("hello"); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatal("Flush 前数据不应可读")
	}
	if err := buf.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 5 {
		t.Fatalf("Flush 后长度应为 5, 实际 %d", buf.Len())
	}

	if _, err := buf.WriteBinary([]byte(" world")); err != nil {
		t.Fatal(err)
	}
	if err := buf.WriteByte('!'); err != nil {
		t.Fatal(err)
	}
	if err := buf.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := string(buf.Bytes()); got != "hello world!" {
		t.Fatalf("Bytes = %q", got)
	}
}

func TestMallocAndFlush(t *testing.T) {
	buf := NewLinkBuffer()
	p, err := buf.Malloc(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(p) != 10 {
		t.Fatalf("Malloc 长度 = %d", len(p))
	}
	copy(p, "0123456789")
	if buf.MallocLen() != 10 {
		t.Fatalf("MallocLen = %d", buf.MallocLen())
	}
	if err := buf.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 10 {
		t.Fatalf("Len = %d", buf.Len())
	}
}

// MallocAck：只提交前 n 字节，其余丢弃
func TestMallocAck(t *testing.T) {
	buf := NewLinkBuffer()
	p, _ := buf.Malloc(10)
	copy(p, "0123456789")
	if err := buf.MallocAck(4); err != nil {
		t.Fatal(err)
	}
	if err := buf.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 4 {
		t.Fatalf("Len = %d, 期望 4", buf.Len())
	}
	if got := string(buf.Bytes()); got != "0123" {
		t.Fatalf("Bytes = %q", got)
	}
}

// ============ 读路径：Next / Peek / Skip / ReadByte / ReadString / ReadBinary / Until ============

func newBufWith(t *testing.T, s string) *LinkBuffer {
	t.Helper()
	buf := NewLinkBuffer()
	if _, err := buf.WriteBinary([]byte(s)); err != nil {
		t.Fatal(err)
	}
	if err := buf.Flush(); err != nil {
		t.Fatal(err)
	}
	return buf
}

func TestNext(t *testing.T) {
	buf := newBufWith(t, "hello world")
	p, err := buf.Next(5)
	if err != nil {
		t.Fatal(err)
	}
	if string(p) != "hello" {
		t.Fatalf("Next = %q", p)
	}
	if buf.Len() != 6 {
		t.Fatalf("剩余 = %d", buf.Len())
	}

	// 读超出现有数据应报错
	if _, err := buf.Next(100); err == nil {
		t.Fatal("超出长度应报错")
	}
}

func TestPeek(t *testing.T) {
	buf := newBufWith(t, "hello")
	p, err := buf.Peek(5)
	if err != nil {
		t.Fatal(err)
	}
	if string(p) != "hello" {
		t.Fatalf("Peek = %q", p)
	}
	if buf.Len() != 5 {
		t.Fatal("Peek 不应消耗数据")
	}
}

func TestSkip(t *testing.T) {
	buf := newBufWith(t, "hello world")
	if err := buf.Skip(6); err != nil {
		t.Fatal(err)
	}
	p, _ := buf.Next(5)
	if string(p) != "world" {
		t.Fatalf("Next = %q", p)
	}
}

func TestReadByte(t *testing.T) {
	buf := newBufWith(t, "ab")
	b1, _ := buf.ReadByte()
	b2, _ := buf.ReadByte()
	if b1 != 'a' || b2 != 'b' {
		t.Fatalf("ReadByte = %c %c", b1, b2)
	}
	if _, err := buf.ReadByte(); err == nil {
		t.Fatal("空 buffer 读字节应报错")
	}
}

func TestReadString(t *testing.T) {
	buf := newBufWith(t, "hello")
	s, err := buf.ReadString(5)
	if err != nil {
		t.Fatal(err)
	}
	if s != "hello" {
		t.Fatalf("ReadString = %q", s)
	}
}

func TestReadBinary(t *testing.T) {
	buf := newBufWith(t, "hello")
	p, err := buf.ReadBinary(5)
	if err != nil {
		t.Fatal(err)
	}
	if string(p) != "hello" {
		t.Fatalf("ReadBinary = %q", p)
	}
	// ReadBinary 是拷贝语义，返回的切片独立持有
	p[0] = 'X'
	if buf.Len() != 0 {
		t.Fatal("ReadBinary 应消费数据")
	}
}

func TestUntil(t *testing.T) {
	buf := newBufWith(t, "hello\nworld\n")
	line, err := buf.Until('\n')
	if err != nil {
		t.Fatal(err)
	}
	if string(line) != "hello\n" {
		t.Fatalf("Until = %q", line)
	}
	line, err = buf.Until('\n')
	if err != nil {
		t.Fatal(err)
	}
	if string(line) != "world\n" {
		t.Fatalf("Until = %q", line)
	}
	if buf.Len() != 0 {
		t.Fatal("应全部读完")
	}
}

// ============ Slice：零拷贝切片 ============

func TestSlice(t *testing.T) {
	buf := newBufWith(t, "hello world")
	r, err := buf.Slice(5)
	if err != nil {
		t.Fatal(err)
	}
	p, err := r.Next(5)
	if err != nil {
		t.Fatal(err)
	}
	if string(p) != "hello" {
		t.Fatalf("Slice 内容 = %q", p)
	}
	// 原 buffer 剩 " world"
	if got := string(buf.Bytes()); got != " world" {
		t.Fatalf("原 buffer 剩 %q", got)
	}
}

// ============ 跨节点读写（数据超过单个节点容量）============

func TestCrossNode(t *testing.T) {
	buf := NewLinkBuffer()
	// 写入超过 4KB（默认节点大小）的数据，强制跨节点
	big := bytes.Repeat([]byte("ab"), 4096) // 8KB
	if _, err := buf.WriteBinary(big); err != nil {
		t.Fatal(err)
	}
	if err := buf.Flush(); err != nil {
		t.Fatal(err)
	}

	got, err := buf.Next(len(big))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, big) {
		t.Fatal("跨节点读取内容不一致")
	}
}

// ============ Append / WriteBuffer：合并两个 buffer ============

func TestAppend(t *testing.T) {
	a := newBufWith(t, "hello")
	b := newBufWith(t, " world")
	if err := a.Append(b); err != nil {
		t.Fatal(err)
	}
	// Append 只合并不提交，需 Flush 后才可读（netpoll 语义）
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := string(a.Bytes()); got != "hello world" {
		t.Fatalf("Append 后 = %q", got)
	}
}

// ============ GetBytes：零拷贝拿到所有可读切片的引用 ============

func TestGetBytes(t *testing.T) {
	buf := newBufWith(t, "hello")
	vs := buf.GetBytes(nil)
	if len(vs) != 1 || string(vs[0]) != "hello" {
		t.Fatalf("GetBytes = %v", vs)
	}
}

// ============ Release / Close ============

func TestRelease(t *testing.T) {
	buf := newBufWith(t, "hello")
	if _, err := buf.Next(5); err != nil {
		t.Fatal(err)
	}
	if err := buf.Release(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatal("Release 后应为空")
	}
}

func TestClose(t *testing.T) {
	buf := newBufWith(t, "hello")
	if err := buf.Close(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatal("Close 后应为空")
	}
}

// ============ io 适配器：NewReader / NewWriter / NewReadWriter ============

func TestIOAdapters(t *testing.T) {
	// io.Writer → Writer（zcWriter）
	var sink bytes.Buffer
	w := NewWriter(&sink)
	if _, err := w.WriteString("hello"); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if sink.String() != "hello" {
		t.Fatalf("sink = %q", sink.String())
	}

	// io.Reader → Reader（zcReader）
	r := NewReader(strings.NewReader("world"))
	p, err := r.Next(5)
	if err != nil {
		t.Fatal(err)
	}
	if string(p) != "world" {
		t.Fatalf("read = %q", p)
	}
	// 再读应报 EOF
	if _, err := r.Next(1); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, 期望 io.EOF", err)
	}

	// io.ReadWriter → ReadWriter
	rw := NewReadWriter(struct {
		io.Reader
		io.Writer
	}{strings.NewReader("ping"), &sink})
	if _, err := rw.WriteString("x"); err != nil {
		t.Fatal(err)
	}
	if err := rw.Flush(); err != nil {
		t.Fatal(err)
	}
	q, err := rw.Next(4)
	if err != nil {
		t.Fatal(err)
	}
	if string(q) != "ping" {
		t.Fatalf("read = %q", q)
	}
}

// ============ 反向适配：NewIOReader / NewIOWriter / NewIOReadWriter ============

func TestReverseIOAdapters(t *testing.T) {
	// Reader → io.Reader
	buf := newBufWith(t, "hello")
	ior := NewIOReader(buf)
	out, err := io.ReadAll(ior)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "hello" {
		t.Fatalf("io.ReadAll = %q", out)
	}

	// Writer → io.Writer
	back := NewLinkBuffer()
	iow := NewIOWriter(back)
	if _, err := iow.Write([]byte("world")); err != nil {
		t.Fatal(err)
	}
	if got := string(back.Bytes()); got != "world" {
		t.Fatalf("buffer = %q", got)
	}

	// ReadWriter → io.ReadWriter
	buf2 := newBufWith(t, "ping")
	iorw := NewIOReadWriter(buf2)
	pong := make([]byte, 4)
	if _, err := iorw.Read(pong); err != nil {
		t.Fatal(err)
	}
	if string(pong) != "ping" {
		t.Fatalf("read = %q", pong)
	}
	if _, err := iorw.Write([]byte("!")); err != nil {
		t.Fatal(err)
	}
}
