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

// Package linkbuffer 是从 cloudwego/netpoll 中提取出来的 LinkBuffer 零拷贝
// 网络设施），用于独立阅读与学习。所有逻辑与原版逐行一致，未做重构。
//
// # 四指针分区模型
//
// LinkBuffer 由一条 linkBufferNode 单链表组成，链上有四个指针把节点划分成
// 四个区域：
//
//		head ──► [已读完的节点] read ──► [可读节点] flush ──► [malloc 中，未提交] write ──► [空尾节点]
//		          (待 Release)            (Next/Peek)          (Malloc 已申请)              (待增长)
//
//	  - head → read：已经读完、等待 Release 回收的节点；
//	  - read → flush：Flush 提交后、可被 Reader 读取的节点；
//	  - flush → write：已被 Malloc 申请、但尚未 Flush 提交的节点；
//	  - write 之后：为扩容预留的空闲节点。
//
// # 零拷贝两步式语义
//
// 写入是"先申请、后提交"的两步操作：
//
//	buf, _ := b.Malloc(n)   // 1. 申请一段内存（此时还不可读）
//	copy(buf, data)         //    往申请到的内存里填数据
//	b.Flush()               // 2. 提交，数据变得可读
//
// 读取（Next/Peek/Slice）在单节点内直接返回底层内存的切片（零拷贝），
// 返回的切片只在下一次 Release 之前有效；跨节点时才退化为拷贝。
//
// # 与原版 netpoll 的差异
//
// 仅省略了与 buffer 无关的内容：
//   - io 适配器（NewReader/NewWriter/NewReadWriter/NewIOReader/... 及其
//     zcReader/zcWriter/ioReader/ioWriter 实现，位于原版 nocopy_readwriter.go），
//     因为它们依赖 netpoll 内部的 Exception/ErrEOF 错误设施；
//   - 接口定义与 `var _ Reader = &LinkBuffer{}` 断言保留在 iface.go 中。
package nocopy
