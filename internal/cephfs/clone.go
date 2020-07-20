/*
Copyright 2020 The Ceph-CSI Authors.

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
	"context"
	"errors"
	"fmt"

	"github.com/ceph/ceph-csi/internal/util"

	klog "k8s.io/klog/v2"
)

const (
	// cephFSCloneCompleted indicates that clone is in complete state
	cephFSCloneCompleted = "complete"
)

func createCloneFromSubvolume(ctx context.Context, volID, cloneID volumeID, volOpt, parentvolOpt *volumeOptions, cr *util.Credentials) error {
	snapshotID := cloneID
	err := createSnapshot(ctx, parentvolOpt, cr, snapshotID, volID)
	if err != nil {
		klog.Errorf(util.Log(ctx, "failed to create snapshot %s %v"), snapshotID, err)
		return err
	}
	var (
		// if protectErr is not nil we will delete the snapshot as the protect fails
		protectErr error
		// if cloneErr is not nil we will unprotect the snapshot and delete the snapshot
		cloneErr error
	)
	defer func() {
		if protectErr != nil {
			err = deleteSnapshot(ctx, parentvolOpt, cr, snapshotID, volID)
			if err != nil {
				klog.Errorf(util.Log(ctx, "failed to delete snapshot %s %v"), snapshotID, err)
			}
		}

		if cloneErr != nil {
			if err = unprotectSnapshot(ctx, parentvolOpt, cr, snapshotID, volID); err != nil {
				klog.Errorf(util.Log(ctx, "failed to unprotect snapshot %s %v"), snapshotID, err)
			}
			if err = deleteSnapshot(ctx, parentvolOpt, cr, snapshotID, volID); err != nil {
				klog.Errorf(util.Log(ctx, "failed to delete snapshot %s %v"), snapshotID, err)
			}
		}
	}()
	protectErr = protectSnapshot(ctx, parentvolOpt, cr, snapshotID, volID)
	if protectErr != nil {
		klog.Errorf(util.Log(ctx, "failed to protect snapshot %s %v"), snapshotID, protectErr)
		return protectErr
	}

	protectErr = cloneSnapshot(ctx, parentvolOpt, cr, volID, snapshotID, cloneID, volOpt)
	if protectErr != nil {
		klog.Errorf(util.Log(ctx, "failed to clone snapshot %s %s to %s %v"), volID, snapshotID, cloneID, cloneErr)
		return protectErr
	}

	// returning cloneInProgress as we cannot do any undo operations if
	// getcloneInfo fails, Even we are not checking the clone status here, just
	// returning the error.
	return ErrCloneInProgress{err: fmt.Errorf("clone is in progress for %v", cloneID)}
}

func checkCloneFromSubvolumeExists(ctx context.Context, volID, cloneID volumeID, volOpt, parentVolOpt *volumeOptions, cr *util.Credentials) error {
	// snapshot name is same as clone name as we need a name which can be
	// identified during PVC-PVC cloning.
	snapShotID := cloneID
	// This is a work around to fix sizing issue for cloned images
	err := resizeVolume(ctx, volOpt, cr, cloneID, volOpt.Size)
	if err != nil {
		klog.Errorf(util.Log(ctx, "failed to expand volume %s: %v"), cloneID, err)
		return err
	}
	_, err = getSnapshotInfo(ctx, parentVolOpt, cr, snapShotID, volID)
	if err != nil {
		var evnf util.ErrSnapNotFound
		if errors.As(err, &evnf) {
			return nil
		}
		return err
	}
	err = unprotectSnapshot(ctx, parentVolOpt, cr, snapShotID, volID)
	if err != nil {
		klog.Errorf(util.Log(ctx, "failed to unprotect snapshot %s %v"), snapShotID, err)
		return err
	}
	err = deleteSnapshot(ctx, parentVolOpt, cr, snapShotID, volID)
	if err != nil {
		klog.Errorf(util.Log(ctx, "failed to delete snapshot %s %v"), snapShotID, err)
		return err
	}
	return nil
}
