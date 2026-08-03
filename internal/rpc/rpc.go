// Package rpc 是对 mrpc 库的适配封装,提供类 net/rpc 的编程接口:
//
//	服务端: Listen / Register / Serve / Close
//	客户端: Dial / Call / Go / Close
//
// mrpc 的特性约束(见库源码):
//   - 客户端 Call 复用同一 TCP 连接,非并发安全,这里用互斥锁串行化;
//   - 服务端方法签名必须为 func(req T, reply *U) error;
//   - 单帧载荷上限 10MB,业务层通过 types.Config.MaxRPCDataSize 留出余量。
package rpc

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/23jdd/mrpc"
)

// ServiceMethod 组合服务名与方法名,如 "MasterService.Create"。
func ServiceMethod(service, method string) string {
	return service + "." + method
}

// ---- 客户端 ----

// Client 是并发安全的 mrpc 客户端封装。
type Client struct {
	mu   sync.Mutex
	raw  *mrpc.Client
	addr string
}

// Dial 建立到 addr(host:port)的 RPC 客户端,连接在首次调用时建立。
func Dial(addr string) (*Client, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("invalid port in address %q", addr)
	}
	return &Client{raw: mrpc.NewClient(host, port), addr: addr}, nil
}

// Addr 返回客户端目标地址。
func (c *Client) Addr() string {
	return c.addr
}

// Call 同步调用远端方法;失败时会关闭底层连接,下次调用自动重连。
func (c *Client) Call(method string, args, reply any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.raw == nil {
		return errors.New("rpc: client closed")
	}
	return c.raw.Call(method, args, reply)
}

// Call 表示一次异步调用。
type Call struct {
	Method string
	Args   any
	Reply  any
	Error  error
	Done   chan *Call
}

// Go 异步发起一次调用:完成(无论成败)后把结果写入 done 通道。
func (c *Client) Go(method string, args, reply any, done chan *Call) *Call {
	call := &Call{Method: method, Args: args, Reply: reply, Done: done}
	go func() {
		call.Error = c.Call(method, args, reply)
		if done != nil {
			done <- call
		}
	}()
	return call
}

// Close 关闭客户端连接。
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.raw == nil {
		return nil
	}
	err := c.raw.Close()
	c.raw = nil
	return err
}

// ---- 服务端 ----

// Server 封装 mrpc.Server,负责监听、注册与连接处理。
type Server struct {
	lis net.Listener
	raw *mrpc.Server
}

// Listen 在 addr 上创建 TCP 监听。
// 监听器会跟踪所有已建立的连接,Close 时一并关闭,
// 保证服务停止后残留连接不再被处理(避免"旧 Master 幽灵连接"问题)。
func Listen(addr string) (*Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	tl := &trackListener{Listener: lis, conns: make(map[net.Conn]bool)}
	return &Server{lis: tl, raw: mrpc.NewServer(tl)}, nil
}

// trackListener 跟踪已接受的连接,Close 时全部关闭。
type trackListener struct {
	net.Listener
	mu    sync.Mutex
	conns map[net.Conn]bool
}

func (t *trackListener) Accept() (net.Conn, error) {
	c, err := t.Listener.Accept()
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.conns[c] = true
	t.mu.Unlock()
	return &trackConn{Conn: c, t: t}, nil
}

func (t *trackListener) closeAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for c := range t.conns {
		_ = c.Close()
	}
	t.conns = make(map[net.Conn]bool)
}

type trackConn struct {
	net.Conn
	t *trackListener
}

func (c *trackConn) Close() error {
	c.t.mu.Lock()
	delete(c.t.conns, c.Conn)
	c.t.mu.Unlock()
	return c.Conn.Close()
}

// Addr 返回实际监听地址(addr 为 ":0" 时可取到真实端口)。
func (s *Server) Addr() string {
	return s.lis.Addr().String()
}

// Register 注册服务实现,等价于 mrpc.Server.Register。
func (s *Server) Register(name string, target any) error {
	return s.raw.Register(name, target)
}

// Serve 阻塞式处理连接(等价于 mrpc.Server.Run),监听关闭后返回。
func (s *Server) Serve() error {
	s.raw.Run()
	return nil
}

// Close 关闭监听器及全部已建立连接,使 Serve 返回。
func (s *Server) Close() error {
	if tl, ok := s.lis.(*trackListener); ok {
		tl.closeAll()
	}
	return s.lis.Close()
}
