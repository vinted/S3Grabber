//go:build darwin || freebsd || netbsd || openbsd
// +build darwin freebsd netbsd openbsd

package installer

import (
	"syscall"
)

// Returns inode or file change time
func StatCtime(st *syscall.Stat_t) syscall.Timespec {
	return st.Ctimespec
}
