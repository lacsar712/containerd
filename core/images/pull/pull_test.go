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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/snapshots"
)

const testSnapshotter = "test-snapshotter"

// fakeImageStore is an in-memory implementation of images.Store plus the pull
// StateStore used by the coordinator.
type fakeImageStore struct {
	mu sync.Mutex

	images map[string]images.Image
	states map[string]State

	creates int
	updates int
}

func newFakeImageStore() *fakeImageStore {
	return &fakeImageStore{
		images: map[string]images.Image{},
		states: map[string]State{},
	}
}

func (s *fakeImageStore) Get(_ context.Context, name string) (images.Image, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	img, ok := s.images[name]
	if !ok {
		return images.Image{}, fmt.Errorf("image %q: %w", name, errdefs.ErrNotFound)
	}
	return img, nil
}

func (s *fakeImageStore) List(context.Context, ...string) ([]images.Image, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var imgs []images.Image
	for _, img := range s.images {
		imgs = append(imgs, img)
	}
	return imgs, nil
}

func (s *fakeImageStore) Create(_ context.Context, img images.Image) (images.Image, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.images[img.Name]; ok {
		return images.Image{}, fmt.Errorf("image %q: %w", img.Name, errdefs.ErrAlreadyExists)
	}
	s.creates++
	s.images[img.Name] = img
	return img, nil
}

func (s *fakeImageStore) Update(_ context.Context, img images.Image, _ ...string) (images.Image, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.images[img.Name]; !ok {
		return images.Image{}, fmt.Errorf("image %q: %w", img.Name, errdefs.ErrNotFound)
	}
	s.updates++
	s.images[img.Name] = img
	return img, nil
}

func (s *fakeImageStore) Delete(_ context.Context, name string, _ ...images.DeleteOpt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.images[name]; !ok {
		return fmt.Errorf("image %q: %w", name, errdefs.ErrNotFound)
	}
	delete(s.images, name)
	return nil
}

func (s *fakeImageStore) CreatePullState(_ context.Context, state State) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.states[state.Name]; ok {
		return State{}, fmt.Errorf("pull for image %q: %w", state.Name, errdefs.ErrAlreadyExists)
	}
	now := time.Now()
	state.CreatedAt = now
	state.Heartbeat = now
	s.states[state.Name] = state
	return state, nil
}

func (s *fakeImageStore) GetPullState(_ context.Context, name string) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.states[name]
	if !ok {
		return State{}, fmt.Errorf("pull for image %q: %w", name, errdefs.ErrNotFound)
	}
	return state, nil
}

func (s *fakeImageStore) PullHeartbeat(_ context.Context, name, token string) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.states[name]
	if !ok || state.Token != token {
		return State{}, fmt.Errorf("pull for image %q: %w", name, errdefs.ErrNotFound)
	}
	state.Heartbeat = time.Now()
	s.states[name] = state
	return state, nil
}

func (s *fakeImageStore) DeletePullState(_ context.Context, name, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.states[name]
	if !ok || state.Token != token {
		return fmt.Errorf("pull for image %q: %w", name, errdefs.ErrNotFound)
	}
	delete(s.states, name)
	return nil
}

func (s *fakeImageStore) setHeartbeat(name string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.states[name]
	state.Heartbeat = t
	s.states[name] = state
}

func (s *fakeImageStore) writes() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]int{"creates": s.creates, "updates": s.updates}
}

type fakeContent struct {
	content.Store

	mu    sync.Mutex
	blobs map[digest.Digest][]byte
	infos map[digest.Digest]content.Info
}

func newFakeContent() *fakeContent {
	return &fakeContent{
		blobs: map[digest.Digest][]byte{},
		infos: map[digest.Digest]content.Info{},
	}
}

func (c *fakeContent) addBlob(data []byte) ocispec.Descriptor {
	d := digest.FromBytes(data)
	c.mu.Lock()
	c.blobs[d] = data
	c.infos[d] = content.Info{Digest: d, Size: int64(len(data))}
	c.mu.Unlock()
	return ocispec.Descriptor{MediaType: ocispec.MediaTypeImageLayerGzip, Digest: d, Size: int64(len(data))}
}

func (c *fakeContent) setInfo(info content.Info) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.infos[info.Digest] = info
}

func (c *fakeContent) ReaderAt(_ context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	c.mu.Lock()
	data, ok := c.blobs[desc.Digest]
	c.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("content %s: %w", desc.Digest, errdefs.ErrNotFound)
	}
	return &fakeReaderAt{Reader: bytes.NewReader(data)}, nil
}

func (c *fakeContent) Info(_ context.Context, dgst digest.Digest) (content.Info, error) {
	c.mu.Lock()
	info, ok := c.infos[dgst]
	c.mu.Unlock()
	if !ok {
		return content.Info{}, fmt.Errorf("content %s: %w", dgst, errdefs.ErrNotFound)
	}
	return info, nil
}

type fakeReaderAt struct{ *bytes.Reader }

func (r *fakeReaderAt) Close() error { return nil }

type fakeSnapshotter struct {
	snapshots.Snapshotter

	mu   sync.Mutex
	stat map[string]struct{}
}

func newFakeSnapshotter() *fakeSnapshotter {
	return &fakeSnapshotter{stat: map[string]struct{}{}}
}

func (s *fakeSnapshotter) addChain(chainIDs []digest.Digest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, chainID := range chainIDs {
		s.stat[chainID.String()] = struct{}{}
	}
}

func (s *fakeSnapshotter) remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.stat, name)
}

func (s *fakeSnapshotter) Stat(_ context.Context, name string) (snapshots.Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.stat[name]; !ok {
		return snapshots.Info{}, fmt.Errorf("snapshot %q: %w", name, errdefs.ErrNotFound)
	}
	return snapshots.Info{Name: name}, nil
}

// testImage builds a manifest with two layers and content/snapshot fixtures
// presenting the image as complete when present is true.
type testImage struct {
	platform ocispec.Platform

	configDesc   ocispec.Descriptor
	manifestDesc ocispec.Descriptor
	layerDescs   []ocispec.Descriptor
	diffIDs      []digest.Digest
	chainIDs     []digest.Digest
}

func buildTestImage(cs *fakeContent) *testImage {
	platform := ocispec.Platform{OS: "linux", Architecture: "amd64"}

	diffIDs := []digest.Digest{digest.FromString("diff-0"), digest.FromString("diff-1")}

	configJSON, _ := json.Marshal(ocispec.Image{
		Platform: platform,
		RootFS:   ocispec.RootFS{Type: "layers", DiffIDs: diffIDs},
	})
	configDesc := cs.addBlob(configJSON)
	configDesc.MediaType = ocispec.MediaTypeImageConfig

	var layerDescs []ocispec.Descriptor
	for _, data := range [][]byte{[]byte("layer-0"), []byte("layer-1")} {
		layerDescs = append(layerDescs, cs.addBlob(data))
	}

	manifestJSON, _ := json.Marshal(ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    configDesc,
		Layers:    layerDescs,
	})
	manifestDigest := digest.FromBytes(manifestJSON)
	cs.mu.Lock()
	cs.blobs[manifestDigest] = manifestJSON
	cs.infos[manifestDigest] = content.Info{Digest: manifestDigest, Size: int64(len(manifestJSON))}
	cs.mu.Unlock()
	manifestDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    manifestDigest,
		Size:      int64(len(manifestJSON)),
	}

	// identity.ChainIDs mutates the passed slice, so derive it from a copy.
	chainIDs := identity.ChainIDs(append([]digest.Digest(nil), diffIDs...))
	// The unpacker labels the config with the chain top so GC retains it.
	cs.setInfo(content.Info{
		Digest: configDesc.Digest,
		Labels: map[string]string{
			"containerd.io/gc.ref.snapshot." + testSnapshotter: chainIDs[len(chainIDs)-1].String(),
		},
	})

	return &testImage{
		platform:     platform,
		configDesc:   configDesc,
		manifestDesc: manifestDesc,
		layerDescs:   layerDescs,
		diffIDs:      diffIDs,
		chainIDs:     chainIDs,
	}
}

func testCoordinator(t *testing.T, store *fakeImageStore, cs *fakeContent, sn *fakeSnapshotter, img *testImage, opts ...Opt) *Coordinator {
	t.Helper()
	c, err := New(store, NewVerifier(cs, platforms.Only(img.platform), UnpackTarget{
		SnapshotterName: testSnapshotter,
		Snapshotter:     sn,
		Platform:        platforms.Only(img.platform),
	}), append(opts,
		WithPollInterval(2*time.Millisecond),
		WithHeartbeatInterval(time.Hour),
	)...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestPullSerializesAndCommitsOnce(t *testing.T) {
	store, cs, sn := newFakeImageStore(), newFakeContent(), newFakeSnapshotter()
	img := buildTestImage(cs)
	sn.addChain(img.chainIDs)

	coordinator := testCoordinator(t, store, cs, sn, img)
	ctx := context.Background()

	var calls atomic.Int32
	leaderStarted := make(chan struct{})
	releaseLeader := make(chan struct{})

	pullFn := func(_ context.Context) (images.Image, error) {
		if calls.Add(1) == 1 {
			close(leaderStarted)
			<-releaseLeader
		}
		return images.Image{Name: "example.com/img:latest", Target: img.manifestDesc}, nil
	}

	leaderErr := make(chan error, 1)
	go func() {
		_, err := coordinator.Do(ctx, images.Image{Name: "example.com/img:latest", Target: img.manifestDesc}, pullFn)
		leaderErr <- err
	}()

	<-leaderStarted

	// While the pull runs the name must not be visible and the reservation
	// must be held.
	if _, err := store.Get(ctx, "example.com/img:latest"); !errdefs.IsNotFound(err) {
		t.Fatalf("image name visible during pull: %v", err)
	}
	if _, err := store.GetPullState(ctx, "example.com/img:latest"); err != nil {
		t.Fatalf("pull reservation missing while pull in progress: %v", err)
	}

	waiterDone := make(chan struct{})
	go func() {
		_, err := coordinator.Do(ctx, images.Image{Name: "example.com/img:latest", Target: img.manifestDesc}, pullFn)
		if err == nil {
			close(waiterDone)
		}
	}()

	// The waiter must wait, not run a second pull.
	select {
	case <-waiterDone:
		t.Fatal("waiter returned before the owning pull completed")
	case <-time.After(50 * time.Millisecond):
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected exactly one pull execution, got %d", got)
	}

	close(releaseLeader)

	select {
	case <-waiterDone:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not return after the owning pull completed")
	}
	if err := <-leaderErr; err != nil {
		t.Fatal(err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("waiter must not perform the pull itself, executions: %d", got)
	}
	writes := store.writes()
	if writes["creates"] != 1 || writes["updates"] != 0 {
		t.Fatalf("expected exactly one image record write, got %+v", writes)
	}
	if _, err := store.Get(ctx, "example.com/img:latest"); err != nil {
		t.Fatalf("image not visible after pull: %v", err)
	}
	if _, err := store.GetPullState(ctx, "example.com/img:latest"); !errdefs.IsNotFound(err) {
		t.Fatalf("pull reservation not released: %v", err)
	}
}

func TestPullFailureLeavesNoNameAndRetries(t *testing.T) {
	store, cs, sn := newFakeImageStore(), newFakeContent(), newFakeSnapshotter()
	img := buildTestImage(cs)
	sn.addChain(img.chainIDs)

	coordinator := testCoordinator(t, store, cs, sn, img)
	ctx := context.Background()

	failed := errors.New("registry reset by peer")
	var attempts atomic.Int32

	doPull := func() error {
		_, err := coordinator.Do(ctx, images.Image{Name: "example.com/fail:latest", Target: img.manifestDesc},
			func(context.Context) (images.Image, error) {
				if attempts.Add(1) == 1 {
					return images.Image{}, failed
				}
				return images.Image{Name: "example.com/fail:latest", Target: img.manifestDesc}, nil
			})
		return err
	}

	if err := doPull(); !errors.Is(err, failed) {
		t.Fatalf("expected registry error, got %v", err)
	}
	if _, err := store.Get(ctx, "example.com/fail:latest"); !errdefs.IsNotFound(err) {
		t.Fatal("image name must not be visible after a failed pull")
	}
	if _, err := store.GetPullState(ctx, "example.com/fail:latest"); !errdefs.IsNotFound(err) {
		t.Fatal("reservation must be released after a failed pull")
	}

	// A later, successful pull completes from scratch and re-verifies
	// everything by digest.
	if err := doPull(); err != nil {
		t.Fatalf("retry pull failed: %v", err)
	}
	if _, err := store.Get(ctx, "example.com/fail:latest"); err != nil {
		t.Fatalf("image not visible after successful retry: %v", err)
	}
}

func TestPullRejectedWhenContentIncomplete(t *testing.T) {
	store, cs, sn := newFakeImageStore(), newFakeContent(), newFakeSnapshotter()
	img := buildTestImage(cs)
	// Snapshots are there, but the second layer blob never arrived.
	sn.addChain(img.chainIDs)
	delete(cs.blobs, img.layerDescs[1].Digest)

	coordinator := testCoordinator(t, store, cs, sn, img)
	_, err := coordinator.Do(context.Background(),
		images.Image{Name: "example.com/partial:latest", Target: img.manifestDesc},
		func(context.Context) (images.Image, error) {
			// A pull function which thinks it succeeded with half the data.
			return images.Image{Name: "example.com/partial:latest", Target: img.manifestDesc}, nil
		})
	if err == nil {
		t.Fatal("pull committed although a layer blob is missing")
	}
	if _, getErr := store.Get(context.Background(), "example.com/partial:latest"); !errdefs.IsNotFound(getErr) {
		t.Fatal("incomplete image must not be registered")
	}
}

func TestPullRejectedWhenSnapshotMissing(t *testing.T) {
	store, cs, sn := newFakeImageStore(), newFakeContent(), newFakeSnapshotter()
	img := buildTestImage(cs)
	// Content is complete, but the last layer snapshot was never committed.
	sn.addChain(img.chainIDs[:len(img.chainIDs)-1])

	coordinator := testCoordinator(t, store, cs, sn, img)
	_, err := coordinator.Do(context.Background(),
		images.Image{Name: "example.com/unsnapped:latest", Target: img.manifestDesc},
		func(context.Context) (images.Image, error) {
			return images.Image{Name: "example.com/unsnapped:latest", Target: img.manifestDesc}, nil
		})
	if err == nil {
		t.Fatal("pull committed although a layer snapshot is missing")
	}
	if _, getErr := store.Get(context.Background(), "example.com/unsnapped:latest"); !errdefs.IsNotFound(getErr) {
		t.Fatal("image with a missing snapshot must not be registered")
	}
}

func TestWaiterTakesOverStaleReservation(t *testing.T) {
	store, cs, sn := newFakeImageStore(), newFakeContent(), newFakeSnapshotter()
	img := buildTestImage(cs)
	sn.addChain(img.chainIDs)

	now := time.Now()
	coordinator := testCoordinator(t, store, cs, sn, img,
		WithClock(func() time.Time { return now }),
		WithStaleTimeout(time.Minute),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stalled := make(chan struct{})
	leaderBlocked := make(chan struct{})
	go func() {
		_, _ = coordinator.Do(ctx, images.Image{Name: "example.com/stale:latest", Target: img.manifestDesc},
			func(ctx context.Context) (images.Image, error) {
				close(leaderBlocked)
				<-ctx.Done()
				return images.Image{}, ctx.Err()
			})
		close(stalled)
	}()

	<-leaderBlocked
	// Simulate a dead owner: the reservation stopped receiving heartbeats.
	store.setHeartbeat("example.com/stale:latest", now.Add(-2*time.Minute))

	// The waiter steals the stale reservation and completes the pull itself.
	committed, err := coordinator.Do(ctx, images.Image{Name: "example.com/stale:latest", Target: img.manifestDesc},
		func(context.Context) (images.Image, error) {
			return images.Image{Name: "example.com/stale:latest", Target: img.manifestDesc}, nil
		})
	if err != nil {
		t.Fatalf("takeover pull failed: %v", err)
	}
	if committed.Name != "example.com/stale:latest" {
		t.Fatalf("unexpected committed image %q", committed.Name)
	}
	writes := store.writes()
	if writes["creates"] != 1 {
		t.Fatalf("takeover must write the image exactly once, got %+v", writes)
	}

	// When the stalled owner finally exits, its release carries the old token
	// and must not affect the committed image.
	if _, err := store.Get(ctx, "example.com/stale:latest"); err != nil {
		t.Fatalf("committed image lost: %v", err)
	}
}

func TestExistingCompleteImageIsRefreshedByPull(t *testing.T) {
	store, cs, sn := newFakeImageStore(), newFakeContent(), newFakeSnapshotter()
	img := buildTestImage(cs)
	sn.addChain(img.chainIDs)

	committed := images.Image{Name: "example.com/cached:latest", Target: img.manifestDesc}
	if _, err := store.Create(context.Background(), committed); err != nil {
		t.Fatal(err)
	}

	coordinator := testCoordinator(t, store, cs, sn, img)
	var calls atomic.Int32
	_, err := coordinator.Do(context.Background(),
		images.Image{Name: "example.com/cached:latest", Target: img.manifestDesc},
		func(context.Context) (images.Image, error) {
			calls.Add(1)
			return committed, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	// Sequential pulls still run so that moved tags are refreshed; only
	// concurrent pulls take the wait-and-adopt path.
	if calls.Load() != 1 {
		t.Fatal("a sequential pull must run to pick up tag updates")
	}
	writes := store.writes()
	if writes["updates"] != 1 {
		t.Fatalf("existing image should be updated, got writes %+v", writes)
	}
}

func TestIncompleteVisibleImageIsFinishedByPull(t *testing.T) {
	store, cs, sn := newFakeImageStore(), newFakeContent(), newFakeSnapshotter()
	img := buildTestImage(cs)
	// The name was registered early but the top snapshot is missing.
	sn.addChain(img.chainIDs[:len(img.chainIDs)-1])
	if _, err := store.Create(context.Background(), images.Image{
		Name: "example.com/half:latest", Target: img.manifestDesc,
	}); err != nil {
		t.Fatal(err)
	}

	coordinator := testCoordinator(t, store, cs, sn, img)
	// The pull completes the missing snapshot.
	done := make(chan struct{})
	_, err := coordinator.Do(context.Background(),
		images.Image{Name: "example.com/half:latest", Target: img.manifestDesc},
		func(context.Context) (images.Image, error) {
			sn.addChain(img.chainIDs)
			close(done)
			return images.Image{Name: "example.com/half:latest", Target: img.manifestDesc}, nil
		})
	if err != nil {
		t.Fatalf("pull should finish the incomplete image: %v", err)
	}
	select {
	case <-done:
	default:
		t.Fatal("pull function was not executed for an incomplete visible image")
	}
}
