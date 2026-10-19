//go:build windows

package main

import "os/exec"

// Windows 仅用于本机编译检查，实际部署在 Linux。
func setProcGroup(cmd *exec.Cmd) {}

func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
