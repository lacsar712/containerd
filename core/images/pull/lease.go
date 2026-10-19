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

package pull

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/containerd/errdefs"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
)

// Remote clients cannot reach the metadata pull state store, which is not
// part of the gRPC API. They coordinate through a dedicated, otherwise empty
// lease with a deterministic ID: creating the lease makes the pull the owner,
// deleting it releases the name. A failed pull deletes the lease immediately;
// leases older than the stale timeout are treated as leftovers from a dead
// client and may be taken over.
const (
	leasePullLabel  = "containerd.io/image.pull"
	leasePullName   = "containerd.io/image.pull.name"
	leasePullToken  = "containerd.io/image.pull.token"
	leaseIDPrefix   = "imgpull-"
	remoteStaleTime = 5 * time.Minute
)

type leaseClaimer struct {
	leases leases.Manager
}

func leaseIDForName(name string) string {
	sum := sha256.Sum256([]byte(name))
	id := leaseIDPrefix + hex.EncodeToString(sum[:])
	// Lease identifiers are limited to 76 characters; the digest prefix is
	// already unique per image name.
	if len(id) > 76 {
		id = id[:76]
	}
	return id
}

func (c *leaseClaimer) acquire(ctx context.Context, img images.Image) (*claim, error) {
	id := leaseIDForName(img.Name)
	lease, err := c.leases.Create(ctx,
		leases.WithID(id),
		leases.WithLabels(map[string]string{
			leasePullLabel: "1",
			leasePullName:  img.Name,
			leasePullToken: newToken(),
		}),
	)
	if err != nil {
		return nil, err
	}
	return &claim{
		name:      img.Name,
		token:     lease.Labels[leasePullToken],
		heartbeat: lease.CreatedAt,
	}, nil
}

func (c *leaseClaimer) get(ctx context.Context, name string) (*claim, error) {
	id := leaseIDForName(name)
	list, err := c.leases.List(ctx, "id=="+id)
	if err != nil {
		return nil, err
	}
	for _, lease := range list {
		if lease.Labels[leasePullLabel] != "1" {
			continue
		}
		if lease.Labels[leasePullName] != name {
			continue
		}
		return &claim{
			name:      name,
			token:     lease.Labels[leasePullToken],
			heartbeat: lease.CreatedAt,
		}, nil
	}
	return nil, fmt.Errorf("pull lease %q for image %q: %w", id, name, errdefs.ErrNotFound)
}

// heartbeat cannot renew a lease through the leases gRPC API, so lease based
// reservations use the creation time as their heartbeat. Live pulls keep the
// lease regardless; only clients which died leave stale reservations.
func (c *leaseClaimer) heartbeat(_ context.Context, owner *claim) (*claim, error) {
	return owner, nil
}

func (c *leaseClaimer) release(ctx context.Context, owner *claim) error {
	return c.leases.Delete(ctx, leases.Lease{ID: leaseIDForName(owner.name)})
}
