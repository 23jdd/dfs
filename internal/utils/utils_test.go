package utils

import (
	"strings"
	"testing"
)

// TestNormalizePath 验证路径规范化规则。
func TestNormalizePath(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "/a/b", want: "/a/b"},
		{in: "/a/b/", want: "/a/b"},
		{in: "//a//b", want: "/a/b"},
		{in: "/", want: "/"},
		{in: "/a/./b", want: "/a/b"},
		{in: "a/b", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, c := range cases {
		got, err := NormalizePath(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("NormalizePath(%q) should error", c.in)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("NormalizePath(%q) = %q, %v; want %q", c.in, got, err, c.want)
		}
	}
}

// TestSplitPath 验证路径拆分为各层组件。
func TestSplitPath(t *testing.T) {
	got := SplitPath("/a/b")
	want := []string{"/", "/a", "/a/b"}
	if len(got) != len(want) {
		t.Fatalf("SplitPath(/a/b) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SplitPath(/a/b) = %v, want %v", got, want)
		}
	}
	if root := SplitPath("/"); len(root) != 1 || root[0] != "/" {
		t.Fatalf("SplitPath(/) = %v", root)
	}
}

// TestBaseName 验证取路径最后一段。
func TestBaseName(t *testing.T) {
	if BaseName("/a/b/c.txt") != "c.txt" {
		t.Error("BaseName(/a/b/c.txt) wrong")
	}
	if BaseName("/") != "" {
		t.Error("BaseName(/) should be empty")
	}
}

// TestHiddenPathFor 验证隐藏目录路径格式。
func TestHiddenPathFor(t *testing.T) {
	p := HiddenPathFor("/data/logs.txt")
	if !strings.HasPrefix(p, "/.deleted/") {
		t.Errorf("hidden path %q should start with /.deleted/", p)
	}
	if !strings.HasSuffix(p, "_logs.txt") {
		t.Errorf("hidden path %q should end with _logs.txt", p)
	}
	if !IsDeletedPath(p) {
		t.Errorf("%q should be recognized as deleted path", p)
	}
}

// TestCRC32Block 验证块校验和:不足块大小按零补齐,计算结果与补零后一致。
func TestCRC32Block(t *testing.T) {
	data := []byte{1, 2, 3}
	short := CRC32Block(data, 8)
	padded := make([]byte, 8)
	copy(padded, data)
	if short != CRC32Block(padded, 8) {
		t.Error("CRC32Block should zero-pad to block size")
	}
	if CRC32Block(padded, 8) != CRC32(padded) {
		t.Error("CRC32Block full-size block should equal CRC32")
	}
	if CRC32Block(nil, 8) != CRC32(make([]byte, 8)) {
		t.Error("CRC32Block(nil) should equal CRC32 of zeros")
	}
}

// TestMD5Hex 验证 MD5 摘要格式。
func TestMD5Hex(t *testing.T) {
	// "abc" 的 MD5 是公认的测试向量
	if MD5Hex([]byte("abc")) != "900150983cd24fb0d6963f7d28e17f72" {
		t.Error("MD5 of abc wrong")
	}
	if len(MD5Hex([]byte("x"))) != 32 {
		t.Error("MD5 hex should be 32 chars")
	}
}

// TestMinMax 验证取整函数。
func TestMinMax(t *testing.T) {
	if MinI64(3, 5, 1, 4) != 1 {
		t.Error("MinI64 wrong")
	}
	if MaxI64(3, 5) != 5 {
		t.Error("MaxI64 wrong")
	}
}
