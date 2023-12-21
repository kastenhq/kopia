//go:build !windows
// +build !windows

package profilelog

import "syscall"

const dumpSignal = syscall.SIGUSR1
