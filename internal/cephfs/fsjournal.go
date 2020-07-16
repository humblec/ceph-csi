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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ceph/ceph-csi/internal/util"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
)

// volumeIdentifier structure contains an association between the CSI VolumeID to its subvolume
// name on the backing CephFS instance
type volumeIdentifier struct {
	FsSubvolName string
	VolumeID     string
}

type snapshotIdentifier struct {
	FsSnapshotName  string
	SnapshotID      string
	FsSubVolumeName string
}

/*
checkSnapExists checks to determine if passed in RequestName in volOptions exists on the backend.

**NOTE:** These functions manipulate the rados omaps that hold information regarding
volume names as requested by the CSI drivers. Hence, these need to be invoked only when the
respective CSI driver generated volume name based locks are held, as otherwise racy
access to these omaps may end up leaving them in an inconsistent state.

These functions also cleanup omap reservations that are stale. I.e when omap entries exist and
backing subvolumes are missing, or one of the omaps exist and the next is missing. This is
because, the order of omap creation and deletion are inverse of each other, and protected by the
request name lock, and hence any stale omaps are leftovers from incomplete transactions and are
hence safe to garbage collect.
*/
func checkSnapExists(ctx context.Context, volOptions *volumeOptions, parentSubVolName string, secret map[string]string) (*snapshotInfo, *snapshotIdentifier, error) {
	cr, err := util.NewAdminCredentials(secret)
	if err != nil {
		return nil, nil, err
	}
	defer cr.DeleteCredentials()

	j, err := snapJournal.Connect(volOptions.Monitors, cr)
	if err != nil {
		return nil, nil, err
	}
	defer j.Destroy()

	snapData, err := j.CheckReservation(
		ctx, volOptions.MetadataPool, volOptions.RequestName, volOptions.NamePrefix, parentSubVolName, "")
	if err != nil {
		return nil, nil, err
	}
	if snapData == nil {
		return nil, nil, nil
	}
	snapUUID := snapData.ImageUUID
	volOptions.SnapshotName = snapData.ImageAttributes.ImageName
	snapInfo, err := getSnapshotInfo(ctx, volOptions, cr, volumeID(parentSubVolName))
	if err != nil {
		var evnf util.ErrSnapNotFound
		if errors.As(err, &evnf) {
			err = j.UndoReservation(ctx, volOptions.MetadataPool,
				volOptions.MetadataPool, snapData.ImageAttributes.ImageName, volOptions.RequestName)
			return nil, nil, err
		}
		return nil, nil, err
	}

	var tm time.Time
	layout := "2006-01-02 15:04:05.000000"
	// TODO currently parsing of timestamp to time.ANSIC generate from ceph fs is failng
	tm, err = time.Parse(layout, snapInfo.CreatedAt)
	if err != nil {
		return nil, nil, err
	}
	snapInfo.CreationTime, err = ptypes.TimestampProto(tm)
	if err != nil {
		return nil, nil, err
	}

	if snapInfo.Protected == "no" {
		err = protectSnapshot(ctx, volOptions, cr, volumeID(parentSubVolName))
		if err != nil {
			return nil, nil, err
		}
	}

	// found a snapshot already available, process and return it!
	snapID, volIDErr := util.GenerateVolID(ctx, volOptions.Monitors, cr, volOptions.FscID,
		"", volOptions.ClusterID, snapUUID, volIDVersion)
	if volIDErr != nil {
		return nil, nil, volIDErr
	}

	snapIdfr := snapshotIdentifier{
		FsSnapshotName:  snapInfo.Name,
		SnapshotID:      snapID,
		FsSubVolumeName: parentSubVolName}
	util.DebugLog(ctx, "Found existing snapshot (%s) with subvolume name (%s) for request (%s)",
		snapInfo.Name, parentSubVolName, volOptions.RequestName)

	return &snapInfo, &snapIdfr, nil
}

/*
checkVolExists checks to determine if passed in RequestName in volOptions exists on the backend.

**NOTE:** These functions manipulate the rados omaps that hold information regarding
volume names as requested by the CSI drivers. Hence, these need to be invoked only when the
respective CSI driver generated volume name based locks are held, as otherwise racy
access to these omaps may end up leaving them in an inconsistent state.

These functions also cleanup omap reservations that are stale. I.e when omap entries exist and
backing subvolumes are missing, or one of the omaps exist and the next is missing. This is
because, the order of omap creation and deletion are inverse of each other, and protected by the
request name lock, and hence any stale omaps are leftovers from incomplete transactions and are
hence safe to garbage collect.
*/
// nolint:gocyclo // TODO: needs to get fixed later
func checkVolExists(ctx context.Context, volOptions *volumeOptions, req *csi.CreateVolumeRequest, secret map[string]string) (*volumeIdentifier, error) {
	var vid volumeIdentifier
	cr, err := util.NewAdminCredentials(secret)
	if err != nil {
		return nil, err
	}
	defer cr.DeleteCredentials()

	j, err := volJournal.Connect(volOptions.Monitors, cr)
	if err != nil {
		return nil, err
	}
	defer j.Destroy()

	imageData, err := j.CheckReservation(
		ctx, volOptions.MetadataPool, volOptions.RequestName, volOptions.NamePrefix, "", "")
	if err != nil {
		return nil, err
	}
	if imageData == nil {
		return nil, nil
	}
	imageUUID := imageData.ImageUUID
	vid.FsSubvolName = imageData.ImageAttributes.ImageName

	// check if topology constraints match what is found
	// TODO: we need an API to fetch subvolume attributes (size/datapool and others), based
	// on which we can evaluate which topology this belongs to.
	// TODO: CephFS topology support is postponed till we get the same
	// TODO: size checks

	// found a volume already available, process and return it!
	vid.VolumeID, err = util.GenerateVolID(ctx, volOptions.Monitors, cr, volOptions.FscID,
		"", volOptions.ClusterID, imageUUID, volIDVersion)
	if err != nil {
		return nil, err
	}

	util.DebugLog(ctx, "Found existing volume (%s) with subvolume name (%s) for request (%s)",
		vid.VolumeID, vid.FsSubvolName, volOptions.RequestName)

	if req.VolumeContentSource != nil {
		volumeSource := req.VolumeContentSource
		switch volumeSource.Type.(type) {
		case *csi.VolumeContentSource_Snapshot:
			var clone = CloneStatus{}
			// TODO check
			clone, err = getcloneInfo(ctx, volOptions, cr, volumeID(vid.FsSubvolName))
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			if clone.Status.State != cephFSCloneCompleted {
				return nil, ErrCloneInProgress{err: fmt.Errorf("clone is in progress for %v", vid.FsSubvolName)}
			}
			// This is a work around to fix sizing issue for cloned images
			err = resizeVolume(ctx, volOptions, cr, volumeID(vid.FsSubvolName), volOptions.Size)
			if err != nil {
				klog.Errorf(util.Log(ctx, "failed to expand volume %s: %v"), volumeID(vid.FsSubvolName), err)
				return nil, err
			}
		case *csi.VolumeContentSource_Volume:
			vol := req.VolumeContentSource.GetVolume()
			if vol == nil {
				return nil, status.Error(codes.NotFound, "volume cannot be empty")
			}
			volID := vol.GetVolumeId()
			if volID == "" {
				return nil, status.Errorf(codes.NotFound, "volume ID cannot be empty")
			}
			// Find the volume using the provided VolumeID
			_, pvID, err := newVolumeOptionsFromVolID(ctx, volID, nil, req.Secrets)
			if err != nil {
				var evnf ErrVolumeNotFound
				if !errors.As(err, &evnf) {
					return nil, status.Error(codes.NotFound, err.Error())
				}
				return nil, status.Error(codes.Internal, err.Error())
			}
			err = checkCloneFromSubvolumeExists(ctx, volumeID(pvID.FsSubvolName), volumeID(vid.FsSubvolName), volOptions, cr)
			if err != nil {
				return nil, err
			}
		default:
			return nil, status.Error(codes.InvalidArgument, "not a proper volume source")
		}
	}

	return &vid, nil
}

// undoVolReservation is a helper routine to undo a name reservation for a CSI VolumeName
func undoVolReservation(ctx context.Context, volOptions *volumeOptions, vid volumeIdentifier, secret map[string]string) error {
	cr, err := util.NewAdminCredentials(secret)
	if err != nil {
		return err
	}
	defer cr.DeleteCredentials()

	j, err := volJournal.Connect(volOptions.Monitors, cr)
	if err != nil {
		return err
	}
	defer j.Destroy()

	err = j.UndoReservation(ctx, volOptions.MetadataPool,
		volOptions.MetadataPool, vid.FsSubvolName, volOptions.RequestName)

	return err
}

func updateTopologyConstraints(volOpts *volumeOptions) error {
	// update request based on topology constrained parameters (if present)
	poolName, _, topology, err := util.FindPoolAndTopology(volOpts.TopologyPools, volOpts.TopologyRequirement)
	if err != nil {
		return err
	}
	if poolName != "" {
		volOpts.Pool = poolName
		volOpts.Topology = topology
	}

	return nil
}

// reserveVol is a helper routine to request a UUID reservation for the CSI VolumeName and,
// to generate the volume identifier for the reserved UUID
func reserveVol(ctx context.Context, volOptions *volumeOptions, secret map[string]string) (*volumeIdentifier, error) {
	var (
		vid       volumeIdentifier
		imageUUID string
		err       error
	)

	cr, err := util.NewAdminCredentials(secret)
	if err != nil {
		return nil, err
	}
	defer cr.DeleteCredentials()

	err = updateTopologyConstraints(volOptions)
	if err != nil {
		return nil, err
	}

	j, err := volJournal.Connect(volOptions.Monitors, cr)
	if err != nil {
		return nil, err
	}
	defer j.Destroy()

	imageUUID, vid.FsSubvolName, err = j.ReserveName(
		ctx, volOptions.MetadataPool, util.InvalidPoolID,
		volOptions.MetadataPool, util.InvalidPoolID, volOptions.RequestName,
		volOptions.NamePrefix, "", "")
	if err != nil {
		return nil, err
	}

	// generate the volume ID to return to the CO system
	vid.VolumeID, err = util.GenerateVolID(ctx, volOptions.Monitors, cr, volOptions.FscID,
		"", volOptions.ClusterID, imageUUID, volIDVersion)
	if err != nil {
		return nil, err
	}

	util.DebugLog(ctx, "Generated Volume ID (%s) and subvolume name (%s) for request name (%s)",
		vid.VolumeID, vid.FsSubvolName, volOptions.RequestName)

	return &vid, nil
}

// reserveSnap is a helper routine to request a UUID reservation for the CSI SnapName and,
// to generate the snapshot identifier for the reserved UUID
func reserveSnap(ctx context.Context, volOptions *volumeOptions, parentSubVolName string, secret map[string]string) (*snapshotIdentifier, error) {
	var (
		vid       snapshotIdentifier
		imageUUID string
		err       error
	)

	cr, err := util.NewAdminCredentials(secret)
	if err != nil {
		return nil, err
	}
	defer cr.DeleteCredentials()

	j, err := snapJournal.Connect(volOptions.Monitors, cr)
	if err != nil {
		return nil, err
	}
	defer j.Destroy()

	imageUUID, vid.FsSnapshotName, err = j.ReserveName(
		ctx, volOptions.MetadataPool, util.InvalidPoolID,
		volOptions.MetadataPool, util.InvalidPoolID, volOptions.RequestName,
		volOptions.NamePrefix, parentSubVolName, "")
	if err != nil {
		return nil, err
	}

	// generate the snapshot ID to return to the CO system
	vid.SnapshotID, err = util.GenerateVolID(ctx, volOptions.Monitors, cr, volOptions.FscID,
		"", volOptions.ClusterID, imageUUID, volIDVersion)
	if err != nil {
		return nil, err
	}

	util.DebugLog(ctx, "Generated Snapshot ID (%s) for request name (%s)",
		vid.SnapshotID, volOptions.RequestName)

	return &vid, nil
}

// undoSnapReservation is a helper routine to undo a name reservation for a CSI SnapshotName
func undoSnapReservation(ctx context.Context, volOptions *volumeOptions, vid snapshotIdentifier, secret map[string]string) error {
	cr, err := util.NewAdminCredentials(secret)
	if err != nil {
		return err
	}
	defer cr.DeleteCredentials()

	j, err := snapJournal.Connect(volOptions.Monitors, cr)
	if err != nil {
		return err
	}
	defer j.Destroy()

	err = j.UndoReservation(ctx, volOptions.MetadataPool,
		volOptions.MetadataPool, vid.FsSnapshotName, volOptions.RequestName)

	return err
}
