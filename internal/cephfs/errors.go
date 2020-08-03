/*
Copyright 2019 The Ceph-CSI Authors.

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

package cephfs

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidVolID is returned when a CSI passed VolumeID is not conformant to any known volume ID
	// formats.
	ErrInvalidVolID = errors.New("invalid VolumeID")
	// ErrNonStaticVolume is returned when a volume is detected as not being
	// statically provisioned.
	ErrNonStaticVolume = errors.New("volume not static")
	// ErrVolNotEmpty is returned when a subvolume has snapshots in it.
	ErrVolNotEmpty = fmt.Errorf("%v", "Error ENOTEMPTY")
	// ErrVolumeNotFound is returned when a subvolume is not found in CephFS.
	ErrVolumeNotFound = fmt.Errorf("%v", "Error ENOENT")
	// ErrInvalidCommand is returned when a command is not known to the cluster
	ErrInvalidCommand = fmt.Errorf("%v", "Error EINVAL")
	// ErrSnapNotFound is returned when snap name passed is not found in the list of snapshots for the
	// given image.
	ErrSnapNotFound = fmt.Errorf("%v", "Error ENOENT")
	// ErrSnapProtectionExist is returned when the snapshot is already protected
	ErrSnapProtectionExist = fmt.Errorf("%v", "Error EEXIST")
)

// ErrCloneInProgress for cloned subvolume.
type ErrCloneInProgress struct {
	err error
}

// Error returns a user presentable string of the error.
func (e ErrCloneInProgress) Error() string {
	return e.err.Error()
}

// Unwrap returns the encapsulated error of ErrCloneInProgress.
func (e ErrCloneInProgress) Unwrap() error {
	return e.err
}
