// WebSocket 压测客户端 —— 最小实现的 RFC 6455 客户端（纯标准库）。
//
// 为什么手写而不引第三方库：与项目「Go 版零第三方依赖」的定位一致，
// 且压测只需要 text 帧的收发 + 握手校验。
//
// 覆盖范围（够压测用即可，不是通用实现）：
//   - 握手：HTTP Upgrade + Sec-WebSocket-Accept 校验
//   - 数据帧：text / continuation（处理分片）/ close / ping / pong
//   - 客户端 → 服务端一律加掩码（RFC 要求），服务端 → 客户端按协议不掩码
//
// 不支持：扩展协商、二进制帧、压缩、TLS（wss）。
package main

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"
)

const (
	opContinuation byte = 0x0
	opText         byte = 0x1
	opClose        byte = 0x8
	opPing         byte = 0x9
	opPong         byte = 0xA

	// RFC 6455 §1.3 规定的握手魔数
	wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
)

// ErrWSClosed 表示对端发送了 close 帧或连接已断
var ErrWSClosed = errors.New("websocket closed by peer")

// wsClient 是一个最小的 WebSocket 客户端连接
type wsClient struct {
	conn net.Conn
	r    *bufio.Reader
}

// wsDial 完成握手并返回已就绪的连接
func wsDial(rawURL string, timeout time.Duration) (*wsClient, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "ws" {
		return nil, fmt.Errorf("仅支持 ws://，收到 scheme=%q", u.Scheme)
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":80"
	}

	conn, err := net.DialTimeout("tcp", host, timeout)
	if err != nil {
		return nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		conn.Close()
		return nil, err
	}

	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		conn.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(nonce)

	req := "GET " + u.RequestURI() + " HTTP/1.1\r\n" +
		"Host: " + u.Host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		conn.Close()
		return nil, err
	}

	// 手工解析响应头：http.ReadResponse 对 101 的 body 处理有内部差异，
	// 手工解析更可预测，且能顺带严格校验握手结果。
	br := bufio.NewReaderSize(conn, 4096)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, err
	}
	if !strings.Contains(statusLine, "101") {
		conn.Close()
		return nil, fmt.Errorf("握手未升级：%s", strings.TrimSpace(statusLine))
	}

	headers := make(map[string]string, 8)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			conn.Close()
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if i := strings.Index(line, ":"); i > 0 {
			headers[strings.ToLower(strings.TrimSpace(line[:i]))] = strings.TrimSpace(line[i+1:])
		}
	}

	sum := sha1.Sum([]byte(key + wsGUID))
	want := base64.StdEncoding.EncodeToString(sum[:])
	if got := headers["sec-websocket-accept"]; got != want {
		conn.Close()
		return nil, fmt.Errorf("Sec-WebSocket-Accept 校验失败：got %q want %q", got, want)
	}

	// 握手完成后清掉超时，后续由每次读操作自行设置
	if err := conn.SetDeadline(time.Time{}); err != nil {
		conn.Close()
		return nil, err
	}

	return &wsClient{conn: conn, r: br}, nil
}

// writeFrame 发送一个帧（客户端发出的帧必须加掩码）
func (c *wsClient) writeFrame(opcode byte, payload []byte) error {
	n := len(payload)
	hdr := make([]byte, 0, 14)
	hdr = append(hdr, 0x80|opcode) // FIN=1

	switch {
	case n < 126:
		hdr = append(hdr, byte(n)|0x80)
	case n < 65536:
		hdr = append(hdr, 126|0x80, 0, 0)
		binary.BigEndian.PutUint16(hdr[len(hdr)-2:], uint16(n))
	default:
		hdr = append(hdr, 127|0x80, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(hdr[len(hdr)-8:], uint64(n))
	}

	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	hdr = append(hdr, mask[:]...)

	// 掩码后合并成一次 Write：避免多次系统调用污染延迟测量
	buf := make([]byte, 0, len(hdr)+n)
	buf = append(buf, hdr...)
	for i, b := range payload {
		buf = append(buf, b^mask[i%4])
	}
	_, err := c.conn.Write(buf)
	return err
}

// writeText 发送一个文本消息
func (c *wsClient) writeText(payload []byte) error {
	return c.writeFrame(opText, payload)
}

// readMessage 读取一条完整消息（处理分片），超时由 deadline 控制。
//
// 读到 ping 会自动回 pong；读到 close 返回 ErrWSClosed。
func (c *wsClient) readMessage(deadline time.Time) ([]byte, error) {
	if err := c.conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}

	var acc []byte
	started := false

	for {
		var h [2]byte
		if _, err := io.ReadFull(c.r, h[:]); err != nil {
			return nil, err
		}
		fin := h[0]&0x80 != 0
		opcode := h[0] & 0x0F
		masked := h[1]&0x80 != 0

		length := int64(h[1] & 0x7F)
		switch length {
		case 126:
			var ext [2]byte
			if _, err := io.ReadFull(c.r, ext[:]); err != nil {
				return nil, err
			}
			length = int64(binary.BigEndian.Uint16(ext[:]))
		case 127:
			var ext [8]byte
			if _, err := io.ReadFull(c.r, ext[:]); err != nil {
				return nil, err
			}
			length = int64(binary.BigEndian.Uint64(ext[:]))
		}

		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(c.r, mask[:]); err != nil {
				return nil, err
			}
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(c.r, payload); err != nil {
			return nil, err
		}
		if masked {
			for i := range payload {
				payload[i] ^= mask[i%4]
			}
		}

		switch opcode {
		case opClose:
			return nil, ErrWSClosed
		case opPing:
			// 控制帧可能夹在分片中间，回 pong 后继续累积
			if err := c.writeFrame(opPong, payload); err != nil {
				return nil, err
			}
			continue
		case opPong:
			continue
		case opText, opContinuation:
			acc = append(acc, payload...)
			started = true
			if fin {
				return acc, nil
			}
		default:
			// 忽略未知 opcode，避免测试卡死
			continue
		}
		_ = started
	}
}

// close 发送 close 帧并关闭连接（尽力而为，不保证对端回应）
func (c *wsClient) close() {
	_ = c.writeFrame(opClose, []byte{0x03, 0xE8}) // 1000 normal closure
	_ = c.conn.SetWriteDeadline(time.Now().Add(time.Second))
	_ = c.conn.Close()
}

// setReadDeadline 暴露给调用方，便于在批量排空时统一控制超时
func (c *wsClient) setReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}
