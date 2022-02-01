//go:build dragonfly || linux || solaris
// +build dragonfly linux solaris

package installer

import (
	"syscall"
)

// // Returns inode or file change time
func StatCtime(st *syscall.Stat_t) syscall.Timespec {
	return st.Ctim
}
