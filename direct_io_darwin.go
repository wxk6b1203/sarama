package sarama

import (
	"fmt"
	"os"
	"syscall"
)

const (
	// AlignSize AlignSizeOSX doesn't need any alignment
	AlignSize = 0

	// BlockSize Minimum block size
	BlockSize = 4096
)

func OpenFile(name string, direct bool, flag int, perm os.FileMode) (file *os.File, err error) {
	file, err = os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}

	if !direct {
		return
	}

	// Set F_NOCACHE to avoid caching
	// F_NOCACHE    Turns data caching off/on. A non-zero value in arg turns data caching off.  A value
	//              of zero in arg turns data caching on.
	_, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, file.Fd(), syscall.F_NOCACHE, 1)
	if e1 != 0 {
		err = fmt.Errorf("failed to set F_NOCACHE: %s", e1)
		err := file.Close()
		if err != nil {
			return nil, err
		}
		file = nil
	}

	return
}
