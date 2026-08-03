// Package utils 提供 GFS 内部使用的工具函数:路径处理与校验和。
package utils

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"path"
	"strings"
	"time"

	"gfs/internal/types"
)

// CRC32 计算数据的 IEEE CRC32 校验和。
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// CRC32Block 计算 size 字节块的 CRC32,不足部分按零补齐,
// 保证写路径与读校验路径对同一块的计算结果一致。
func CRC32Block(data []byte, size int) uint32 {
	if len(data) >= size {
		return crc32.ChecksumIEEE(data[:size])
	}
	block := make([]byte, size)
	copy(block, data)
	return crc32.ChecksumIEEE(block)
}

// MD5Hex 返回数据的 MD5 十六进制摘要,用于测试中的数据一致性校验。
func MD5Hex(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}

// NormalizePath 规范化文件路径:保证以 "/" 开头、去除末尾斜杠、折叠重复斜杠。
func NormalizePath(p string) (string, error) {
	if p == "" {
		return "", fmt.Errorf("empty path")
	}
	if !strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("path must start with '/': %s", p)
	}
	clean := path.Clean(p)
	if clean == "." {
		clean = "/"
	}
	return clean, nil
}

// SplitPath 将路径拆分为各层组件(含根路径),如 "/a/b" -> ["/", "/a", "/a/b"]。
func SplitPath(p string) []string {
	parts := []string{"/"}
	if p == "/" {
		return parts
	}
	cur := ""
	for _, seg := range strings.Split(p, "/") {
		if seg == "" {
			continue
		}
		cur += "/" + seg
		parts = append(parts, cur)
	}
	return parts
}

// BaseName 返回路径的最后一段,如 "/a/b" -> "b"。
func BaseName(p string) string {
	last := SplitPath(p)[len(SplitPath(p))-1]
	if i := strings.LastIndex(last, "/"); i >= 0 {
		return last[i+1:]
	}
	return last
}

// IsDeletedPath 判断路径是否位于垃圾回收隐藏目录下。
func IsDeletedPath(p string) bool {
	return strings.HasPrefix(p, types.DeletedPrefix)
}

// HiddenPathFor 生成文件的隐藏路径:/.deleted/<时间戳>_<文件名>。
func HiddenPathFor(p string) string {
	name := BaseName(p)
	return types.DeletedPrefix + fmt.Sprintf("%d_%s", time.Now().UnixNano(), name)
}

// MinI64 返回若干 int64 中的最小值。
func MinI64(a int64, rest ...int64) int64 {
	for _, b := range rest {
		if b < a {
			a = b
		}
	}
	return a
}

// MaxI64 返回两个 int64 的较大值。
func MaxI64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
