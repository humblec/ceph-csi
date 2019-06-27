/*
Copyright 2016 The Kubernetes Authors.
Copyright 2018 The Ceph-CSI Authors.

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
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/fs"
)

const (
	// ErrCodeNotSupported code for NotSupported Errors.
	ErrCodeNotSupported int = iota + 1
	ErrCodeNoPathDefined
	ErrCodeFsInfoFailed
)

// NewNotSupportedError creates a new cephfsMetricsError with code NotSupported.
func NewNotSupportedError() *cephfsMetricsError {
	return &cephfsMetricsError{
		Code: ErrCodeNotSupported,
		Msg:  "metrics are not supported for MetricsNil Volumes",
	}
}

// NewNoPathDefined creates a new cephfsMetricsError with code NoPathDefined.
func NewNoPathDefinedError() *cephfsMetricsError {
	return &cephfsMetricsError{
		Code: ErrCodeNoPathDefined,
		Msg:  "no path defined for disk usage metrics.",
	}
}

// NewFsInfoFailedError creates a new cephfsMetricsError with code FsInfoFailed.
func NewFsInfoFailedError(err error) *cephfsMetricsError {
	return &cephfsMetricsError{
		Code: ErrCodeFsInfoFailed,
		Msg:  fmt.Sprintf("Failed to get FsInfo due to error %v", err),
	}
}

// cephfsMetricsError to distinguish different Metrics Errors.
type cephfsMetricsError struct {
	Code int
	Msg  string
}

func (e *cephfsMetricsError) Error() string {
	return fmt.Sprintf("%s", e.Msg)
}

// IsNotSupported returns true if and only if err is "key" not found error.
func IsNotSupported(err error) bool {
	return isErrCode(err, ErrCodeNotSupported)
}

func isErrCode(err error, code int) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*cephfsMetricsError); ok {
		return e.Code == code
	}
	return false
}

var _ volume.MetricsProvider = &cephfsStatFS{}

// cephfsStatFS represents a MetricsProvider that calculates the used and available
// Volume space by stat'ing and gathering filesystem info for the Volume path.
type cephfsStatFS struct {
	// the directory path the volume is mounted to.
	path string
}

// NewcephfsStatFS creates a new cephfsStatFS with the Volume path.
func NewcephfsStatFS(path string) volume.MetricsProvider {
	return &cephfsStatFS{path}
}

// See MetricsProvider.GetMetrics
// GetMetrics calculates the volume usage and device free space by executing "du"
// and gathering filesystem info for the Volume path.
func (md *cephfsStatFS) GetMetrics() (*(volume.Metrics), error) {
	metrics := &volume.Metrics{Time: metav1.Now()}
	if md.path == "" {
		return metrics, NewNoPathDefinedError()
	}

	err := md.getFsInfo(metrics)
	if err != nil {
		return metrics, err
	}

	return metrics, nil
}

// getFsInfo writes metrics.Capacity, metrics.Used and metrics.Available from the filesystem info
func (md *cephfsStatFS) getFsInfo(metrics *volume.Metrics) error {
	available, capacity, usage, inodes, inodesFree, inodesUsed, err := fs.FsInfo(md.path)
	if err != nil {
		return NewFsInfoFailedError(err)
	}
	metrics.Available = resource.NewQuantity(available, resource.BinarySI)
	metrics.Capacity = resource.NewQuantity(capacity, resource.BinarySI)
	metrics.Used = resource.NewQuantity(usage, resource.BinarySI)
	metrics.Inodes = resource.NewQuantity(inodes, resource.BinarySI)
	metrics.InodesFree = resource.NewQuantity(inodesFree, resource.BinarySI)
	metrics.InodesUsed = resource.NewQuantity(inodesUsed, resource.BinarySI)
	return nil
}
