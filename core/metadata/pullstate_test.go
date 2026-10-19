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

package metadata

import (
	"testing"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/containerd/containerd/v2/core/images/pull"
)

func TestPullStateReservation(t *testing.T) {
	ctx, db := testEnv(t)
	store := NewImageStore(NewDB(db, nil, nil)).(pull.StateStore)

	target := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromString("manifest"),
		Size:      42,
	}

	if _, err := store.CreatePullState(ctx, pull.State{
		Name:   "example.com/img:latest",
		Token:  "owner-1",
		Target: target,
	}); err != nil {
		t.Fatalf("create pull state: %v", err)
	}

	if _, err := store.CreatePullState(ctx, pull.State{
		Name:   "example.com/img:latest",
		Token:  "owner-2",
		Target: target,
	}); !errdefs.IsAlreadyExists(err) {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}

	state, err := store.GetPullState(ctx, "example.com/img:latest")
	if err != nil {
		t.Fatalf("get pull state: %v", err)
	}
	if state.Token != "owner-1" || state.Target.Digest != target.Digest || state.Target.Size != 42 {
		t.Fatalf("unexpected pull state: %+v", state)
	}
	if state.Heartbeat.IsZero() || state.CreatedAt.IsZero() {
		t.Fatal("pull state timestamps not recorded")
	}

	// A stale owner must not refresh the reservation.
	if _, err := store.PullHeartbeat(ctx, "example.com/img:latest", "intruder"); !errdefs.IsNotFound(err) {
		t.Fatalf("expected NotFound on heartbeat with the wrong token, got %v", err)
	}

	heartbeat, err := store.PullHeartbeat(ctx, "example.com/img:latest", "owner-1")
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !heartbeat.Heartbeat.After(state.Heartbeat) && !heartbeat.Heartbeat.Equal(state.Heartbeat) {
		t.Fatal("heartbeat timestamp not refreshed")
	}

	// A pull state must not interfere with image records of the same name.
	if _, err := store.Get(ctx, "example.com/img:latest"); !errdefs.IsNotFound(err) {
		t.Fatalf("pulling name must not be visible as an image, got %v", err)
	}

	// A stale owner must not release a reservation taken over by another pull.
	if err := store.DeletePullState(ctx, "example.com/img:latest", "intruder"); !errdefs.IsNotFound(err) {
		t.Fatalf("expected NotFound on release with the wrong token, got %v", err)
	}
	if _, err := store.GetPullState(ctx, "example.com/img:latest"); err != nil {
		t.Fatalf("reservation should still exist: %v", err)
	}

	if err := store.DeletePullState(ctx, "example.com/img:latest", "owner-1"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := store.GetPullState(ctx, "example.com/img:latest"); !errdefs.IsNotFound(err) {
		t.Fatalf("reservation not released, got %v", err)
	}
}

func TestPullStateRejectsInvalidTarget(t *testing.T) {
	ctx, db := testEnv(t)
	store := NewImageStore(NewDB(db, nil, nil)).(pull.StateStore)

	if _, err := store.CreatePullState(ctx, pull.State{
		Name:  "example.com/img:latest",
		Token: "owner",
		Target: ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageManifest,
			Digest:    "",
			Size:      42,
		},
	}); !errdefs.IsInvalidArgument(err) {
		t.Fatalf("expected InvalidArgument for an invalid target, got %v", err)
	}
}
