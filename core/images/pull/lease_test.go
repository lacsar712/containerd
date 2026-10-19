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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/platforms"
)

// plainImageStore exposes only images.Store, forcing lease based
// coordination as used through the gRPC API.
type plainImageStore struct {
	inner *fakeImageStore
}

func (s *plainImageStore) Get(ctx context.Context, name string) (images.Image, error) {
	return s.inner.Get(ctx, name)
}
func (s *plainImageStore) List(ctx context.Context, filters ...string) ([]images.Image, error) {
	return s.inner.List(ctx, filters...)
}
func (s *plainImageStore) Create(ctx context.Context, img images.Image) (images.Image, error) {
	return s.inner.Create(ctx, img)
}
func (s *plainImageStore) Update(ctx context.Context, img images.Image, fieldpaths ...string) (images.Image, error) {
	return s.inner.Update(ctx, img, fieldpaths...)
}
func (s *plainImageStore) Delete(ctx context.Context, name string, opts ...images.DeleteOpt) error {
	return s.inner.Delete(ctx, name, opts...)
}

type fakeLeaseManager struct {
	mu     sync.Mutex
	leases map[string]leases.Lease
}

func (m *fakeLeaseManager) Create(_ context.Context, opts ...leases.Opt) (leases.Lease, error) {
	var l leases.Lease
	for _, opt := range opts {
		if err := opt(&l); err != nil {
			return leases.Lease{}, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.leases[l.ID]; ok {
		return leases.Lease{}, fmt.Errorf("lease %q: %w", l.ID, errdefs.ErrAlreadyExists)
	}
	l.CreatedAt = time.Now()
	m.leases[l.ID] = l
	return l, nil
}

func (m *fakeLeaseManager) Delete(_ context.Context, l leases.Lease, _ ...leases.DeleteOpt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.leases[l.ID]; !ok {
		return fmt.Errorf("lease %q: %w", l.ID, errdefs.ErrNotFound)
	}
	delete(m.leases, l.ID)
	return nil
}

func (m *fakeLeaseManager) List(_ context.Context, _ ...string) ([]leases.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []leases.Lease
	for _, l := range m.leases {
		out = append(out, l)
	}
	return out, nil
}

func (m *fakeLeaseManager) AddResource(context.Context, leases.Lease, leases.Resource) error {
	return nil
}

func (m *fakeLeaseManager) DeleteResource(context.Context, leases.Lease, leases.Resource) error {
	return nil
}

func (m *fakeLeaseManager) ListResources(context.Context, leases.Lease) ([]leases.Resource, error) {
	return nil, nil
}

func TestLeaseCoordinationLeaderAndWaiter(t *testing.T) {
	inner, cs, sn := newFakeImageStore(), newFakeContent(), newFakeSnapshotter()
	img := buildTestImage(cs)
	sn.addChain(img.chainIDs)

	store := &plainImageStore{inner: inner}
	verifier := NewVerifier(cs, platforms.Only(img.platform), UnpackTarget{
		SnapshotterName: testSnapshotter,
		Snapshotter:     sn,
		Platform:        platforms.Only(img.platform),
	})
	coordinator, err := New(store, verifier,
		WithLeaseManager(&fakeLeaseManager{leases: map[string]leases.Lease{}}),
		WithPollInterval(2*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	fn := func(context.Context) (images.Image, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return images.Image{Name: "example.com/lease:latest", Target: img.manifestDesc}, nil
	}

	leaderErr := make(chan error, 1)
	go func() {
		_, err := coordinator.Do(context.Background(),
			images.Image{Name: "example.com/lease:latest", Target: img.manifestDesc}, fn)
		leaderErr <- err
	}()
	<-started

	if _, err := inner.Get(context.Background(), "example.com/lease:latest"); !errdefs.IsNotFound(err) {
		t.Fatalf("name visible during lease-coordinated pull: %v", err)
	}

	waiterDone := make(chan struct{})
	go func() {
		_, err := coordinator.Do(context.Background(),
			images.Image{Name: "example.com/lease:latest", Target: img.manifestDesc}, fn)
		if err == nil {
			close(waiterDone)
		}
	}()

	select {
	case <-waiterDone:
		t.Fatal("waiter returned before the owning pull completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-waiterDone:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not adopt the lease-owning pull result")
	}
	if err := <-leaderErr; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected a single pull execution, got %d", calls.Load())
	}
	writes := inner.writes()
	if writes["creates"] != 1 {
		t.Fatalf("expected exactly one image write, got %+v", writes)
	}
}
