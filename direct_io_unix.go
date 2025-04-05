// Direct IO for Unix

//go:build !windows && !darwin && !openbsd && !plan9
// +build !windows,!darwin,!openbsd,!plan9

package sarama

import (
	"os"
	"syscall"
)

const (
	// Size to align the buffer to
	AlignSize = 4096

	// Minimum block size
	BlockSize = 4096
)

// OpenFile is a modified version of os.OpenFile which sets O_DIRECT
func OpenFile(name string, direct bool, flag int, perm os.FileMode) (file *os.File, err error) {
	if !direct {
		return os.OpenFile(name, flag, perm)
	}
	return os.OpenFile(name, syscall.O_DIRECT|flag, perm)
}
