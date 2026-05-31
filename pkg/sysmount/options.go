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

import "fmt"

var supportedBindOptions = map[string]struct{}{
	"":         {},
	"defaults": {},
	"rw":       {},
	"bind":     {},
	"rbind":    {},
}

func validateMountOptions(options []string, sensitiveOptions []string, mountFlags []string) error {
	if len(mountFlags) != 0 {
		return fmt.Errorf("syscall mounter does not support mountFlags %v", mountFlags)
	}

	bind := hasBindOption(options) || hasBindOption(sensitiveOptions)
	if !bind {
		return nil
	}

	for _, option := range options {
		if _, ok := supportedBindOptions[option]; !ok {
			return fmt.Errorf("syscall mounter does not support bind mount option %q", option)
		}
	}
	for _, option := range sensitiveOptions {
		if _, ok := supportedBindOptions[option]; !ok {
			return fmt.Errorf("syscall mounter does not support sensitive bind mount options")
		}
	}

	return nil
}

func hasBindOption(options []string) bool {
	for _, option := range options {
		if option == "bind" || option == "rbind" {
			return true
		}
	}
	return false
}

func mountError(source, target, fstype string, options []string, sensitiveOptions []string, err error) error {
	if len(sensitiveOptions) == 0 {
		return fmt.Errorf("mount %q on %q type %q options %v: %w", source, target, fstype, options, err)
	}
	return fmt.Errorf("mount %q on %q type %q options %v sensitiveOptions <masked>: %w", source, target, fstype, options, err)
}
