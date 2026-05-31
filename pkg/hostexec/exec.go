/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hostexec

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	utilexec "k8s.io/utils/exec"
)

const (
	// HelperCommand is the internal argv[1] used when etcd-manager re-execs itself
	// to run a command against the mounted host root.
	HelperCommand = "__hostexec"
)

var hostSearchPaths = []string{"/", "/bin", "/usr/sbin", "/usr/bin", "/sbin"}

var requiredHostBinaries = []string{
	"blkid",
	"mount",
	"findmnt",
	"umount",
	"mkfs.ext4",
	"resize2fs",
	"stat",
	"touch",
	"mkdir",
	"sh",
	"chmod",
	"realpath",
}

// Executor runs host commands by re-execing etcd-manager in a small helper mode.
type Executor struct {
	rootfs   string
	self     string
	executor utilexec.Interface
	paths    map[string]string
}

// New returns an executor that runs commands in the host mount namespace and
// chrooted to rootfs.
func New(rootfs string) (*Executor, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("finding etcd-manager executable: %w", err)
	}

	e := &Executor{
		rootfs:   filepath.Clean(rootfs),
		self:     self,
		executor: utilexec.New(),
		paths:    make(map[string]string),
	}
	if err := e.initPaths(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *Executor) initPaths() error {
	for _, binary := range requiredHostBinaries {
		p, ok := e.findHostBinary(binary)
		if !ok {
			return fmt.Errorf("unable to find %v", binary)
		}
		e.paths[binary] = p
	}

	if p, ok := e.findHostBinary("systemd-run"); ok {
		e.paths["systemd-run"] = p
	}

	return nil
}

func (e *Executor) findHostBinary(command string) (string, bool) {
	if filepath.IsAbs(command) {
		if _, err := os.Stat(filepath.Join(e.rootfs, command)); err == nil {
			return command, true
		}
		return "", false
	}

	for _, dir := range hostSearchPaths {
		p := filepath.Join(dir, command)
		if _, err := os.Stat(filepath.Join(e.rootfs, p)); err == nil {
			return p, true
		}
	}
	return "", false
}

// AbsHostPath returns the command path relative to the host root.
func (e *Executor) AbsHostPath(command string) string {
	if filepath.IsAbs(command) {
		return command
	}
	if p, ok := e.paths[command]; ok {
		return p
	}
	if p, ok := e.findHostBinary(command); ok {
		e.paths[command] = p
		return p
	}
	return command
}

// SupportsSystemd checks whether systemd-run exists in the host root.
func (e *Executor) SupportsSystemd() (string, bool) {
	p, ok := e.paths["systemd-run"]
	return p, ok && p != ""
}

// Exec executes a command through the helper.
func (e *Executor) Exec(cmd string, args []string) utilexec.Cmd {
	return e.Command(cmd, args...)
}

// Command implements exec.Interface.
func (e *Executor) Command(cmd string, args ...string) utilexec.Cmd {
	return e.executor.Command(e.self, e.helperArgs(cmd, args...)...)
}

// CommandContext implements exec.Interface.
func (e *Executor) CommandContext(ctx context.Context, cmd string, args ...string) utilexec.Cmd {
	return e.executor.CommandContext(ctx, e.self, e.helperArgs(cmd, args...)...)
}

// LookPath implements exec.Interface.
func (e *Executor) LookPath(file string) (string, error) {
	p, ok := e.findHostBinary(file)
	if !ok {
		return "", utilexec.ErrExecutableNotFound
	}
	return p, nil
}

func (e *Executor) helperArgs(cmd string, args ...string) []string {
	helperArgs := []string{HelperCommand, "--rootfs", e.rootfs, "--", e.AbsHostPath(cmd)}
	return append(helperArgs, args...)
}
