//go:build tinygo

package dirtmake

// Bytes allocates a byte slice for TinyGo environment.
// Uses make() to avoid go:linkname with runtime.mallocgc, which causes
// "undefined symbol: runtime.mallocgc" errors in TinyGo.
func Bytes(len, cap int) (b []byte) {
	return make([]byte, len, cap)
}
