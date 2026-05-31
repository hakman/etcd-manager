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
	"errors"
	"strings"
	"testing"
)

func TestValidateMountOptionsAllowsPlainBindMounts(t *testing.T) {
	testCases := []struct {
		name    string
		options []string
	}{
		{
			name:    "bind",
			options: []string{"bind"},
		},
		{
			name:    "recursive bind",
			options: []string{"rbind", "defaults", "rw"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateMountOptions(tc.options, nil, nil); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateMountOptionsRejectsUnsupportedBindOptions(t *testing.T) {
	testCases := []struct {
		name             string
		options          []string
		sensitiveOptions []string
		want             string
		forbidden        string
	}{
		{
			name:    "readonly bind",
			options: []string{"bind", "ro"},
			want:    `syscall mounter does not support bind mount option "ro"`,
		},
		{
			name:    "nodev recursive bind",
			options: []string{"rbind", "nodev"},
			want:    `syscall mounter does not support bind mount option "nodev"`,
		},
		{
			name:             "sensitive bind option",
			options:          []string{"bind"},
			sensitiveOptions: []string{"password=secret"},
			want:             "syscall mounter does not support sensitive bind mount options",
			forbidden:        "secret",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMountOptions(tc.options, tc.sensitiveOptions, nil)
			if err == nil {
				t.Fatal("expected error")
			}
			if got := err.Error(); got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
			if tc.forbidden != "" && strings.Contains(err.Error(), tc.forbidden) {
				t.Fatalf("error leaked %q: %v", tc.forbidden, err)
			}
		})
	}
}

func TestValidateMountOptionsRejectsMountFlags(t *testing.T) {
	err := validateMountOptions(nil, nil, []string{"--make-rprivate"})
	if err == nil {
		t.Fatal("expected error")
	}
	if got, want := err.Error(), "syscall mounter does not support mountFlags [--make-rprivate]"; got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestMountErrorMasksSensitiveOptions(t *testing.T) {
	sentinel := errors.New("boom")
	err := mountError("/dev/sda", "/mnt", "ext4", []string{"ro"}, []string{"password=secret"}, sentinel)

	if !errors.Is(err, sentinel) {
		t.Fatalf("expected error to wrap sentinel")
	}
	if got := err.Error(); strings.Contains(got, "password") || strings.Contains(got, "secret") {
		t.Fatalf("error leaked sensitive option: %v", err)
	}
	if got := err.Error(); !strings.Contains(got, "sensitiveOptions <masked>") {
		t.Fatalf("expected masked sensitive options, got %q", got)
	}
	if got := err.Error(); !strings.Contains(got, "options [ro]") {
		t.Fatalf("expected non-sensitive options, got %q", got)
	}
}
