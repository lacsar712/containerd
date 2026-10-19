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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"time"

	"github.com/containerd/errdefs"
	"github.com/containerd/log"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
)

// PullFunc performs the actual fetch and unpack of an image. It must not
// register the image name: the coordinator does that only after verifying the
// returned image record points at complete content and snapshots. The
// context is canceled if another pull takes over the reservation, in which
// case the pull must stop.
type PullFunc func(ctx context.Context) (images.Image, error)

const (
	defaultPollInterval      = 100 * time.Millisecond
	defaultHeartbeatInterval = 2 * time.Second
	// defaultStaleTimeout is kept small relative to the heartbeat interval.
	// A pull which exits with an error always releases its reservation
	// immediately; the timeout only recovers reservations whose owner died
	// without releasing.
	defaultStaleTimeout = 30 * time.Second
)

// Options configures pull coordination.
type Options struct {
	now               func() time.Time
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	staleTimeout      time.Duration
	staleTimeoutSet   bool
	leases            leases.Manager
}

// Opt allows configuring a Coordinator.
type Opt func(*Options)

// WithClock overrides the time source, mainly for tests.
func WithClock(now func() time.Time) Opt {
	return func(o *Options) {
		o.now = now
	}
}

// WithPollInterval sets how long a waiting pull sleeps between checks for the
// owning pull's result.
func WithPollInterval(d time.Duration) Opt {
	return func(o *Options) {
		o.pollInterval = d
	}
}

// WithHeartbeatInterval sets how often the owning pull refreshes its
// reservation.
func WithHeartbeatInterval(d time.Duration) Opt {
	return func(o *Options) {
		o.heartbeatInterval = d
	}
}

// WithStaleTimeout sets how long a reservation may go without a heartbeat
// before a waiting pull takes it over.
func WithStaleTimeout(d time.Duration) Opt {
	return func(o *Options) {
		o.staleTimeout = d
		o.staleTimeoutSet = true
	}
}

// WithLeaseManager provides the leases manager used to coordinate remote
// clients, whose image store does not implement StateStore. It is ignored for
// in-process clients.
func WithLeaseManager(lm leases.Manager) Opt {
	return func(o *Options) {
		o.leases = lm
	}
}

// Coordinator serializes pulls per image name and makes the image record the
// final, already verified step of a successful pull.
type Coordinator struct {
	store    images.Store
	verifier *Verifier
	claims   claimer

	now               func() time.Time
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	staleTimeout      time.Duration
}

// New creates a Coordinator. In-process image stores implementing
// StateStore are coordinated through the metadata database; other stores are
// coordinated through a lease manager provided with WithLeaseManager.
func New(store images.Store, verifier *Verifier, opts ...Opt) (*Coordinator, error) {
	options := Options{
		now:               time.Now,
		pollInterval:      defaultPollInterval,
		heartbeatInterval: defaultHeartbeatInterval,
		staleTimeout:      defaultStaleTimeout,
	}
	for _, opt := range opts {
		opt(&options)
	}

	var claims claimer
	if stateStore, ok := store.(StateStore); ok {
		claims = &stateClaimer{states: stateStore}
		if options.leases != nil {
			// Stores reached over gRPC may accept the Go calls structurally
			// but return Unimplemented; coordinate through leases then.
			claims = &fallbackClaimer{
				primary:  claims,
				fallback: &leaseClaimer{leases: options.leases},
			}
		}
	} else {
		if options.leases == nil {
			return nil, errors.New("pull coordination requires a StateStore or a lease manager")
		}
		claims = &leaseClaimer{leases: options.leases}
		// Lease reservations cannot be renewed over the leases API, so their
		// creation time is their heartbeat. Give crashed clients more time
		// before their reservation is taken over.
		if !options.staleTimeoutSet {
			options.staleTimeout = remoteStaleTime
		}
	}

	return &Coordinator{
		store:             store,
		verifier:          verifier,
		claims:            claims,
		now:               options.now,
		pollInterval:      options.pollInterval,
		heartbeatInterval: options.heartbeatInterval,
		staleTimeout:      options.staleTimeout,
	}, nil
}

// Do runs fn as the single writer for img.Name.
//
//   - When no pull for the name is in progress, the caller owns the
//     reservation, runs fn and registers the image only after it has been
//     verified complete.
//   - When another pull is in progress, the caller waits and returns the
//     image committed by that pull once it is complete, without writing
//     anything itself.
//   - When the owning pull fails or goes stale and the name remains absent or
//     incomplete, the caller takes over and runs fn itself.
func (c *Coordinator) Do(ctx context.Context, img images.Image, fn PullFunc) (images.Image, error) {
	for {
		owner, err := c.claims.acquire(ctx, img)
		if err == nil {
			return c.lead(ctx, owner, fn)
		}
		if !errdefs.IsAlreadyExists(err) {
			return images.Image{}, err
		}

		committed, done, err := c.await(ctx, img.Name)
		if err != nil {
			return images.Image{}, err
		}
		if done {
			return committed, nil
		}

		// The reservation was released or taken over; try to become the owner.
	}
}

// await waits for another pull to finish. It returns the committed image when
// the pull completed successfully, or done=false when this caller should take
// over the pull itself.
func (c *Coordinator) await(ctx context.Context, name string) (image images.Image, done bool, err error) {
	for {
		// A visible, verified image means a pull completed: adopt its result.
		existing, getErr := c.store.Get(ctx, name)
		switch {
		case getErr == nil:
			if verifyErr := c.verifier.Verify(ctx, existing); verifyErr == nil {
				return existing, true, nil
			}
			// The record is visible but incomplete. Only take over when no
			// pull owns it any longer; otherwise the owning pull may still be
			// finalizing.
		case !errdefs.IsNotFound(getErr):
			return images.Image{}, false, getErr
		}

		owner, claimErr := c.claims.get(ctx, name)
		if claimErr != nil {
			if errdefs.IsNotFound(claimErr) {
				// The owning pull exited without leaving a complete image.
				return images.Image{}, false, nil
			}
			return images.Image{}, false, claimErr
		}

		if c.now().Sub(owner.heartbeat) > c.staleTimeout {
			// The owner appears dead: remove the stale reservation and take
			// over. The owner token makes a live owner's reservation safe.
			if delErr := c.claims.release(ctx, owner); delErr != nil && !errdefs.IsNotFound(delErr) {
				return images.Image{}, false, delErr
			}
			log.G(ctx).WithField("image", name).Warn("took over stale image pull reservation")
			return images.Image{}, false, nil
		}

		select {
		case <-ctx.Done():
			return images.Image{}, false, ctx.Err()
		case <-time.After(c.pollInterval):
		}
	}
}

// lead runs the fetch and unpack as the reservation owner and commits the
// image record as the final step.
func (c *Coordinator) lead(ctx context.Context, owner *claim, fn PullFunc) (images.Image, error) {
	pullCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Releasing the reservation after a canceled or timed out pull must keep
	// the request scope (namespace, gRPC metadata) without inheriting
	// cancellation.
	releaseCtx := context.WithoutCancel(ctx)

	heartbeatsDone := make(chan struct{})
	go c.heartbeatLoop(pullCtx, owner, cancel, heartbeatsDone)
	defer func() {
		cancel()
		<-heartbeatsDone
	}()

	final, err := fn(pullCtx)
	if err != nil {
		c.release(releaseCtx, owner)
		return images.Image{}, err
	}

	if err := c.verifier.Verify(ctx, final); err != nil {
		c.release(releaseCtx, owner)
		return images.Image{}, err
	}

	committed, err := c.commit(ctx, final)
	if err != nil {
		c.release(releaseCtx, owner)
		return images.Image{}, err
	}

	c.release(releaseCtx, owner)
	return committed, nil
}

// heartbeatLoop keeps the reservation alive while the pull runs. Losing the
// reservation (for example because a stalled pull was taken over) cancels the
// pull context so the old owner stops writing. Transient refresh errors are
// retried on the next tick.
func (c *Coordinator) heartbeatLoop(ctx context.Context, owner *claim, lost context.CancelFunc, done chan struct{}) {
	defer close(done)

	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updated, err := c.claims.heartbeat(ctx, owner)
			if err == nil {
				*owner = *updated
				continue
			}
			if errdefs.IsNotFound(err) {
				// The reservation was released or taken over.
				lost()
				return
			}
			log.G(ctx).WithError(err).WithField("image", owner.name).Error("image pull heartbeat failed")
		}
	}
}

// commit creates the visible image record, or updates an existing one (a tag
// which moved, or an incomplete record finished by a taking-over pull).
func (c *Coordinator) commit(ctx context.Context, image images.Image) (images.Image, error) {
	for {
		if _, err := c.store.Get(ctx, image.Name); err == nil {
			updated, err := c.store.Update(ctx, image)
			if errdefs.IsNotFound(err) {
				continue // removed concurrently; create it
			}
			return updated, err
		} else if !errdefs.IsNotFound(err) {
			return images.Image{}, err
		}

		created, err := c.store.Create(ctx, image)
		if errdefs.IsAlreadyExists(err) {
			continue // created concurrently; re-read and update
		}
		return created, err
	}
}

func (c *Coordinator) release(ctx context.Context, owner *claim) {
	// The reservation is deleted with the owner token, so a pull which was
	// taken over cannot release the new owner's reservation.
	if err := c.claims.release(ctx, owner); err != nil && !errdefs.IsNotFound(err) {
		log.G(ctx).WithError(err).WithField("image", owner.name).Error("failed to release image pull reservation")
	}
}

func newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read failing is not recoverable; panic rather than run with
		// colliding owner tokens.
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// claim is an internal view of a reservation.
type claim struct {
	name      string
	token     string
	heartbeat time.Time
}

// claimer abstracts reservation storage.
type claimer interface {
	// acquire reserves the name in img. It returns errdefs.ErrAlreadyExists
	// when a pull already owns the name.
	acquire(ctx context.Context, img images.Image) (*claim, error)
	// get returns the current reservation for the name.
	get(ctx context.Context, name string) (*claim, error)
	// heartbeat refreshes the reservation.
	heartbeat(ctx context.Context, owner *claim) (*claim, error)
	// release removes the reservation when it is owned by the claim's token.
	release(ctx context.Context, owner *claim) error
}

// stateClaimer coordinates through the metadata pull state store.
type stateClaimer struct {
	states StateStore
}

func (c *stateClaimer) acquire(ctx context.Context, img images.Image) (*claim, error) {
	state, err := c.states.CreatePullState(ctx, State{
		Name:   img.Name,
		Token:  newToken(),
		Target: img.Target,
	})
	if err != nil {
		return nil, err
	}
	return &claim{
		name:      state.Name,
		token:     state.Token,
		heartbeat: state.Heartbeat,
	}, nil
}

func (c *stateClaimer) get(ctx context.Context, name string) (*claim, error) {
	state, err := c.states.GetPullState(ctx, name)
	if err != nil {
		return nil, err
	}
	return &claim{
		name:      state.Name,
		token:     state.Token,
		heartbeat: state.Heartbeat,
	}, nil
}

func (c *stateClaimer) heartbeat(ctx context.Context, owner *claim) (*claim, error) {
	state, err := c.states.PullHeartbeat(ctx, owner.name, owner.token)
	if err != nil {
		return nil, err
	}
	owner.heartbeat = state.Heartbeat
	return owner, nil
}

func (c *stateClaimer) release(ctx context.Context, owner *claim) error {
	return c.states.DeletePullState(ctx, owner.name, owner.token)
}

// fallbackClaimer uses the metadata based primary claimer until it reports
// Unimplemented, then switches to the lease based fallback for the lifetime
// of the coordinator.
type fallbackClaimer struct {
	primary  claimer
	fallback claimer

	useFallback atomic.Bool
}

func (c *fallbackClaimer) acquire(ctx context.Context, img images.Image) (*claim, error) {
	if c.useFallback.Load() {
		return c.fallback.acquire(ctx, img)
	}
	cl, err := c.primary.acquire(ctx, img)
	if errdefs.IsNotImplemented(err) {
		c.useFallback.Store(true)
		return c.fallback.acquire(ctx, img)
	}
	return cl, err
}

func (c *fallbackClaimer) get(ctx context.Context, name string) (*claim, error) {
	if c.useFallback.Load() {
		return c.fallback.get(ctx, name)
	}
	cl, err := c.primary.get(ctx, name)
	if errdefs.IsNotImplemented(err) {
		c.useFallback.Store(true)
		return c.fallback.get(ctx, name)
	}
	return cl, err
}

func (c *fallbackClaimer) heartbeat(ctx context.Context, owner *claim) (*claim, error) {
	if c.useFallback.Load() {
		return c.fallback.heartbeat(ctx, owner)
	}
	cl, err := c.primary.heartbeat(ctx, owner)
	if errdefs.IsNotImplemented(err) {
		c.useFallback.Store(true)
		return c.fallback.heartbeat(ctx, owner)
	}
	return cl, err
}

func (c *fallbackClaimer) release(ctx context.Context, owner *claim) error {
	if c.useFallback.Load() {
		return c.fallback.release(ctx, owner)
	}
	if err := c.primary.release(ctx, owner); errdefs.IsNotImplemented(err) {
		c.useFallback.Store(true)
		return c.fallback.release(ctx, owner)
	} else {
		return err
	}
}
