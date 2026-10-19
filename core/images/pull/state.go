/*
   Copyright The containerd Authors.

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

// Package pull coordinates image pulls so that an image name, its resolved
// manifest content and the unpacked layer snapshots become visible as a
// single, already complete state.
//
// A pull reserves the image name before downloading and unpacking anything.
// Only the pull owning the reservation performs the pull; concurrent pulls
// of the same name wait for its result instead of writing a second record.
// The image record is created or updated only after all manifest content has
// been re-confirmed by digest and every layer snapshot has been
// re-confirmed by chain ID. A pull which fails at any point releases the
// reservation without ever registering the name, in which case the next
// pull performs the work itself.
package pull

import (
	"context"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/platforms"
)

// State is a single-writer reservation for pulling one image name.
type State struct {
	// Name is the image name the reservation applies to.
	Name string

	// Token uniquely identifies the pull owning the reservation. Heartbeats
	// and releases carrying a different token are rejected, so a stalled pull
	// cannot invalidate a reservation taken over by another one.
	Token string

	// Target is the descriptor resolved for the name when the reservation was
	// created.
	Target ocispec.Descriptor

	CreatedAt time.Time

	// Heartbeat is the last time the owning pull reported progress.
	Heartbeat time.Time
}

// StateStore persists pull reservations. It is implemented by the metadata
// image store and exposed to in-process clients through the local images
// service. Remote clients coordinate through lease based reservations.
type StateStore interface {
	images.Store

	// CreatePullState reserves an image name for a pull. It returns
	// errdefs.ErrAlreadyExists when a pull for the name is in progress.
	CreatePullState(ctx context.Context, state State) (State, error)

	// GetPullState returns the current reservation for the name. It returns
	// errdefs.ErrNotFound when no pull is in progress for the name.
	GetPullState(ctx context.Context, name string) (State, error)

	// PullHeartbeat marks the reservation as alive. It fails when the
	// reservation no longer exists or is owned by another token.
	PullHeartbeat(ctx context.Context, name, token string) (State, error)

	// DeletePullState releases the reservation. It only deletes the
	// reservation when the token matches the current owner.
	DeletePullState(ctx context.Context, name, token string) error
}

// UnpackTarget describes a snapshotter the image must be unpacked into for
// the image to be considered complete.
type UnpackTarget struct {
	// SnapshotterName identifies the snapshotter for garbage collection
	// labels.
	SnapshotterName string

	// Snapshotter is used to confirm every layer snapshot is committed.
	Snapshotter snapshots.Snapshotter

	// Platform selects the platform manifest which gets unpacked.
	Platform platforms.MatchComparer
}
