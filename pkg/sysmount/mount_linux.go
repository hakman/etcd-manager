//go:build linux
// +build linux

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

package sysmount

import (
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
	"k8s.io/mount-utils"
)

// Mounter implements mount.Interface using mount(2), avoiding a dependency on
// a mount binary in distroless-based images.
type Mounter struct{}

// New returns a syscall-backed mounter.
func New() *Mounter {
	return &Mounter{}
}

// Mount mounts source on target.
func Mount(source, target, fstype string, options []string) error {
	return doMount(source, target, fstype, options, nil, nil)
}

func doMount(source, target, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	if err := validateMountOptions(options, sensitiveOptions, mountFlags); err != nil {
		return err
	}

	allOptions := append(append([]string{}, options...), sensitiveOptions...)
	flags, data := mountOptions(allOptions)
	if err := unix.Mount(source, target, fstype, flags, data); err != nil {
		return mountError(source, target, fstype, options, sensitiveOptions, err)
	}
	return nil
}

// Unmount unmounts target.
func Unmount(target string) error {
	if err := unix.Unmount(target, 0); err != nil {
		return fmt.Errorf("unmount %q: %w", target, err)
	}
	return nil
}

func mountOptions(options []string) (uintptr, string) {
	var flags uintptr
	var data []string
	for _, option := range options {
		switch option {
		case "", "defaults", "rw":
		case "ro":
			flags |= unix.MS_RDONLY
		case "bind":
			flags |= unix.MS_BIND
		case "rbind":
			flags |= unix.MS_BIND | unix.MS_REC
		case "remount":
			flags |= unix.MS_REMOUNT
		case "noexec":
			flags |= unix.MS_NOEXEC
		case "nosuid":
			flags |= unix.MS_NOSUID
		case "nodev":
			flags |= unix.MS_NODEV
		case "sync":
			flags |= unix.MS_SYNCHRONOUS
		case "dirsync":
			flags |= unix.MS_DIRSYNC
		case "noatime":
			flags |= unix.MS_NOATIME
		case "nodiratime":
			flags |= unix.MS_NODIRATIME
		case "relatime":
			flags |= unix.MS_RELATIME
		default:
			data = append(data, option)
		}
	}
	return flags, strings.Join(data, ",")
}

func (m *Mounter) Mount(source string, target string, fstype string, options []string) error {
	return Mount(source, target, fstype, options)
}

func (m *Mounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return doMount(source, target, fstype, options, sensitiveOptions, nil)
}

func (m *Mounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return m.MountSensitive(source, target, fstype, options, sensitiveOptions)
}

func (m *Mounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	return doMount(source, target, fstype, options, sensitiveOptions, mountFlags)
}

func (m *Mounter) Unmount(target string) error {
	return Unmount(target)
}

func (m *Mounter) List() ([]mount.MountPoint, error) {
	return mount.ListProcMounts("/proc/mounts")
}

func (m *Mounter) IsLikelyNotMountPoint(file string) (bool, error) {
	return false, fmt.Errorf("IsLikelyNotMountPoint not implemented for syscall mounter")
}

func (m *Mounter) CanSafelySkipMountPointCheck() bool {
	return false
}

func (m *Mounter) IsMountPoint(file string) (bool, error) {
	return false, fmt.Errorf("IsMountPoint not implemented for syscall mounter")
}

func (m *Mounter) GetMountRefs(pathname string) ([]string, error) {
	return nil, fmt.Errorf("GetMountRefs not implemented for syscall mounter")
}
