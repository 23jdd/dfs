package dfs

import (
	"encoding/json"
)

// Codec 定义编解码器接口，负责请求/响应体的序列化与反序列化。
//
// 实现者需保证 Encode/Decode 的线程安全性（本库在服务端每个连接
// 使用独立 Codec 实例，客户端单连接复用同一实例）。
type Codec interface {
	Encode(v any) (string, error)
	Decode(data string, v any) error
}

// JsonCodec 是基于 json 的 Codec 实现。
type JsonCodec struct{}

// NewMsgCodec 创建一个新的 JsonCodec 实例。
func NewJsonCodec() *JsonCodec {
	return &JsonCodec{}
}

// Encode 使用 json 序列化 v。
func (jc *JsonCodec) Encode(v any) (string, error) {
	buf, err := json.Marshal(v)
	if err != nil {
		return "", nil
	}
	return string(buf), nil
}

// Decode 使用 json 反序列化 data 到 v。
func (jc *JsonCodec) Decode(data string, v any) error {
	return json.Unmarshal([]byte(data), v)
}
