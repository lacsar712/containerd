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
	"context"
	"encoding/binary"
	"fmt"
	"time"

	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	bolt "go.etcd.io/bbolt"
	errbolt "go.etcd.io/bbolt/errors"

	"github.com/containerd/containerd/v2/core/images/pull"
	"github.com/containerd/containerd/v2/pkg/epoch"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
)

// CreatePullState reserves an image name for an in-progress pull. The
// reservation is stored separately from image records, so a pulling name is
// never visible through image queries.
func (s *imageStore) CreatePullState(ctx context.Context, state pull.State) (pull.State, error) {
	namespace, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return pull.State{}, err
	}
	if state.Name == "" {
		return pull.State{}, fmt.Errorf("pull state name must not be empty: %w", errdefs.ErrInvalidArgument)
	}
	if state.Token == "" {
		return pull.State{}, fmt.Errorf("pull state token must not be empty: %w", errdefs.ErrInvalidArgument)
	}
	if err := validateTarget(&state.Target); err != nil {
		return pull.State{}, err
	}

	if err := update(ctx, s.db, func(tx *bolt.Tx) error {
		bkt, err := createPullsBucket(tx, namespace)
		if err != nil {
			return err
		}

		pbkt, err := bkt.CreateBucket([]byte(state.Name))
		if err != nil {
			if err != errbolt.ErrBucketExists {
				return err
			}
			return fmt.Errorf("pull for image %q: %w", state.Name, errdefs.ErrAlreadyExists)
		}

		now := time.Now().UTC()
		if tm := epoch.FromContext(ctx); tm != nil {
			now = tm.UTC()
		}
		state.CreatedAt = now
		state.Heartbeat = now

		if err := writePullState(pbkt, &state); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return pull.State{}, err
	}

	return state, nil
}

// GetPullState returns the reservation for an image name.
func (s *imageStore) GetPullState(ctx context.Context, name string) (pull.State, error) {
	namespace, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return pull.State{}, err
	}

	var state pull.State
	if err := view(ctx, s.db, func(tx *bolt.Tx) error {
		bkt := getPullsBucket(tx, namespace)
		if bkt == nil {
			return fmt.Errorf("pull for image %q: %w", name, errdefs.ErrNotFound)
		}
		pbkt := bkt.Bucket([]byte(name))
		if pbkt == nil {
			return fmt.Errorf("pull for image %q: %w", name, errdefs.ErrNotFound)
		}
		state.Name = name
		return readPullState(pbkt, &state)
	}); err != nil {
		return pull.State{}, err
	}

	return state, nil
}

// PullHeartbeat refreshes the reservation heartbeat. The token must match the
// current owner, otherwise the reservation was taken over and the caller has
// lost the pull.
func (s *imageStore) PullHeartbeat(ctx context.Context, name, token string) (pull.State, error) {
	namespace, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return pull.State{}, err
	}

	var state pull.State
	if err := update(ctx, s.db, func(tx *bolt.Tx) error {
		bkt := getPullsBucket(tx, namespace)
		if bkt == nil {
			return fmt.Errorf("pull for image %q: %w", name, errdefs.ErrNotFound)
		}
		pbkt := bkt.Bucket([]byte(name))
		if pbkt == nil {
			return fmt.Errorf("pull for image %q: %w", name, errdefs.ErrNotFound)
		}

		state.Name = name
		if err := readPullState(pbkt, &state); err != nil {
			return err
		}
		if state.Token != token {
			return fmt.Errorf("pull for image %q is owned by another pull: %w", name, errdefs.ErrNotFound)
		}

		state.Heartbeat = time.Now().UTC()
		if tm := epoch.FromContext(ctx); tm != nil {
			state.Heartbeat = tm.UTC()
		}
		return pbkt.Put(bucketKeyHeartbeat, marshalTime(state.Heartbeat))
	}); err != nil {
		return pull.State{}, err
	}

	return state, nil
}

// DeletePullState removes a pull reservation, but only when the token matches
// the current owner.
func (s *imageStore) DeletePullState(ctx context.Context, name, token string) error {
	namespace, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return err
	}

	return update(ctx, s.db, func(tx *bolt.Tx) error {
		bkt := getPullsBucket(tx, namespace)
		if bkt == nil {
			return fmt.Errorf("pull for image %q: %w", name, errdefs.ErrNotFound)
		}
		pbkt := bkt.Bucket([]byte(name))
		if pbkt == nil {
			return fmt.Errorf("pull for image %q: %w", name, errdefs.ErrNotFound)
		}

		if v := pbkt.Get(bucketKeyToken); token == "" || string(v) != token {
			// The reservation was taken over by another pull; leave it.
			return fmt.Errorf("pull for image %q is owned by another pull: %w", name, errdefs.ErrNotFound)
		}

		if err := bkt.DeleteBucket([]byte(name)); err != nil {
			if err == errbolt.ErrBucketNotFound {
				err = fmt.Errorf("pull for image %q: %w", name, errdefs.ErrNotFound)
			}
			return err
		}
		s.db.dirty.Add(1)
		return nil
	})
}

func writePullState(bkt *bolt.Bucket, state *pull.State) error {
	if err := bkt.Put(bucketKeyToken, []byte(state.Token)); err != nil {
		return err
	}
	if err := bkt.Put(bucketKeyCreatedAt, marshalTime(state.CreatedAt)); err != nil {
		return err
	}
	if err := bkt.Put(bucketKeyHeartbeat, marshalTime(state.Heartbeat)); err != nil {
		return err
	}

	tbkt, err := bkt.CreateBucketIfNotExists(bucketKeyTarget)
	if err != nil {
		return err
	}
	sizeEncoded, err := encodeInt(state.Target.Size)
	if err != nil {
		return err
	}
	for _, v := range [][2][]byte{
		{bucketKeyDigest, []byte(state.Target.Digest)},
		{bucketKeyMediaType, []byte(state.Target.MediaType)},
		{bucketKeySize, sizeEncoded},
	} {
		if err := tbkt.Put(v[0], v[1]); err != nil {
			return err
		}
	}
	return nil
}

func readPullState(bkt *bolt.Bucket, state *pull.State) error {
	state.Token = string(bkt.Get(bucketKeyToken))
	if err := unmarshalTime(bkt.Get(bucketKeyCreatedAt), &state.CreatedAt); err != nil {
		return fmt.Errorf("reading createdat: %w", err)
	}
	if err := unmarshalTime(bkt.Get(bucketKeyHeartbeat), &state.Heartbeat); err != nil {
		return fmt.Errorf("reading heartbeat: %w", err)
	}

	tbkt := bkt.Bucket(bucketKeyTarget)
	if tbkt == nil {
		return fmt.Errorf("pull state for %q has no target: %w", state.Name, errdefs.ErrInvalidArgument)
	}
	state.Target = ocispec.Descriptor{
		Digest:    digest.Digest(tbkt.Get(bucketKeyDigest)),
		MediaType: string(tbkt.Get(bucketKeyMediaType)),
	}
	if v := tbkt.Get(bucketKeySize); v != nil {
		state.Target.Size, _ = binary.Varint(v)
	}
	return nil
}

func marshalTime(t time.Time) []byte {
	b, _ := t.MarshalBinary()
	return b
}

func unmarshalTime(v []byte, t *time.Time) error {
	if v == nil {
		return nil
	}
	return t.UnmarshalBinary(v)
}
