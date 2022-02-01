//go:build dragonfly || linux || solaris
// +build dragonfly linux solaris

package fs

import (
	"syscall"
)

// // Returns inode or file change time
func StatCtime(st *syscall.Stat_t) syscall.Timespec {
	return st.Ctim
}
