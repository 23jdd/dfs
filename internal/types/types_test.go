package types

import (
	"errors"
	"testing"
)

// TestMatchError 验证跨 RPC 的哨兵错误还原:mrpc 以字符串传输错误,
// MatchError 应把已知消息映射回原哨兵,未知错误保持原样。
func TestMatchError(t *testing.T) {
	cases := []error{
		ErrFileExists,
		ErrFileNotFound,
		ErrChunkNotFound,
		ErrIndexOutOfRange,
		ErrChunkFull,
		ErrStaleVersion,
		ErrDataNotFound,
		ErrChecksum,
		ErrNoServer,
		ErrInvalidPath,
	}
	for _, want := range cases {
		got := MatchError(errors.New(want.Error()))
		if got != want {
			t.Errorf("MatchError(%q) = %v, want %v", want.Error(), got, want)
		}
		if !errors.Is(got, want) {
			t.Errorf("errors.Is(MatchError(%q), sentinel) = false", want.Error())
		}
	}
	// 未知错误原样返回(仍可 errors.Is 匹配原错误对象)
	unknown := errors.New("some unknown error")
	if MatchError(unknown) != unknown {
		t.Error("unknown error should be returned as-is")
	}
	// nil 输入返回 nil
	if MatchError(nil) != nil {
		t.Error("MatchError(nil) should be nil")
	}
}

// TestSentinelUniqueness 验证哨兵错误消息互不相同,否则 MatchError 会映射错乱。
func TestSentinelUniqueness(t *testing.T) {
	seen := make(map[string]error)
	for msg, e := range errorByMessage {
		if prev, ok := seen[msg]; ok {
			t.Errorf("duplicate error message %q: %v and %v", msg, prev, e)
		}
		seen[msg] = e
	}
}
