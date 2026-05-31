//go:build !linux
// +build !linux

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
	return fmt.Errorf("syscall mount not implemented on this platform")
}

// Unmount unmounts target.
func Unmount(target string) error {
	return fmt.Errorf("syscall unmount not implemented on this platform")
}

func (m *Mounter) Mount(source string, target string, fstype string, options []string) error {
	return Mount(source, target, fstype, options)
}

func (m *Mounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return Mount(source, target, fstype, append(options, sensitiveOptions...))
}

func (m *Mounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return m.MountSensitive(source, target, fstype, options, sensitiveOptions)
}

func (m *Mounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	return m.MountSensitive(source, target, fstype, options, sensitiveOptions)
}

func (m *Mounter) Unmount(target string) error {
	return Unmount(target)
}

func (m *Mounter) List() ([]mount.MountPoint, error) {
	return nil, fmt.Errorf("List not implemented for syscall mounter")
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
