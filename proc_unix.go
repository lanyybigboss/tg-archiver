//go:build linux || darwin

package main

import (
	"os/exec"
	"syscall"
)

// setProcGroup 让子进程自成一个进程组，便于整组回收。
func setProcGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killProcessGroup 杀掉整个进程组。
// 只杀主进程会留下 tdl 的子进程，长期运行会堆积成僵尸，故整组回收。
func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
