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

package local

import (
	"context"
	"fmt"

	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	"github.com/containerd/platforms"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/diff"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/images/pull"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/containerd/v2/core/transfer"
	"github.com/containerd/containerd/v2/core/unpack"
	"github.com/containerd/containerd/v2/defaults"
	snpkg "github.com/containerd/containerd/v2/pkg/snapshotters"
)

func (ts *localTransferService) pull(ctx context.Context, ir transfer.ImageFetcher, is transfer.ImageStorer, tops *transfer.Config) error {
	ctx, done, err := ts.withLease(ctx)
	if err != nil {
		return err
	}
	defer done(ctx)

	if tops.Progress != nil {
		tops.Progress(transfer.Progress{
			Event: fmt.Sprintf("Resolving from %s", ir),
		})
	}

	if ir, ok := ir.(transfer.ImageResolverOptionSetter); ok {
		ir.SetResolverOptions(
			transfer.WithConcurrentLayerFetchBuffer(ts.config.ConcurrentLayerFetchBuffer),
			transfer.WithMaxConcurrentDownloads(ts.config.MaxConcurrentDownloads),
			transfer.WithDownloadLimiter(ts.limiterD),
		)
	}

	name, desc, err := ir.Resolve(ctx)
	if err != nil {
		return fmt.Errorf("failed to resolve image: %w", err)
	}
	if desc.MediaType == images.MediaTypeDockerSchema1Manifest {
		// Explicitly call out schema 1 as deprecated and not supported
		return fmt.Errorf("schema 1 image manifests are no longer supported: %w", errdefs.ErrInvalidArgument)
	}

	// Verify image before pulling.
	for vfName, vf := range ts.config.Verifiers {
		logger := log.G(ctx).WithFields(log.Fields{
			"name":     name,
			"digest":   desc.Digest.String(),
			"verifier": vfName,
		})
		logger.Debug("Verifying image pull")

		jdg, err := vf.VerifyImage(ctx, name, desc)
		if err != nil {
			logger.WithError(err).Error("No judgement received from verifier")
			return fmt.Errorf("blocking pull of %v with digest %v: image verifier %v returned error: %w", name, desc.Digest.String(), vfName, err)
		}
		logger = logger.WithFields(log.Fields{
			"ok":     jdg.OK,
			"reason": jdg.Reason,
		})

		if !jdg.OK {
			logger.Warn("Image verifier blocked pull")
			return fmt.Errorf("image verifier %s blocked pull of %v with digest %v for reason: %v", vfName, name, desc.Digest.String(), jdg.Reason)
		}
		logger.Debug("Image verifier allowed pull")
	}

	// TODO: Handle already exists
	if tops.Progress != nil {
		tops.Progress(transfer.Progress{
			Event: fmt.Sprintf("Pulling from %s", ir),
		})
		tops.Progress(transfer.Progress{
			Event: "fetching image content",
			Name:  name,
			Desc:  &desc,
		})
	}
	fetcher, err := ir.Fetcher(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to get fetcher for %q: %w", name, err)
	}

	var (
		handler images.Handler

		baseHandlers []images.Handler

		unpacker *unpack.Unpacker

		// has a config media type bug (distribution#1622)
		hasMediaTypeBug1622 bool

		store           = ts.content
		progressTracker *ProgressTracker
	)

	ctx, cancel := context.WithCancel(ctx)
	if tops.Progress != nil {
		progressTracker = NewProgressTracker(name, "downloading") // Pass in first name as root
		go progressTracker.HandleProgress(ctx, tops.Progress, NewContentStatusTracker(store))
		defer progressTracker.Wait()
	}
	defer cancel()

	// Get all the children for a descriptor
	childrenHandler := images.ChildrenHandler(store)

	if f, ok := is.(transfer.ImageFilterer); ok {
		childrenHandler = f.ImageFilter(childrenHandler, store)
	}

	checkNeedsFix := images.HandlerFunc(
		func(_ context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
			// set to true if there is application/octet-stream media type
			if desc.MediaType == docker.LegacyConfigMediaType {
				hasMediaTypeBug1622 = true
			}

			return []ocispec.Descriptor{}, nil
		},
	)

	appendDistSrcLabelHandler, err := docker.AppendDistributionSourceLabel(store, name)
	if err != nil {
		return err
	}

	// Set up baseHandlers from service configuration if present or create a new one
	if ts.config.BaseHandlers != nil {
		baseHandlers = ts.config.BaseHandlers
	} else {
		baseHandlers = []images.Handler{}
	}

	if tops.Progress != nil {
		baseHandlers = append(baseHandlers, images.HandlerFunc(
			func(_ context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
				progressTracker.Add(desc)

				return []ocispec.Descriptor{}, nil
			},
		))

		baseChildrenHandler := childrenHandler
		childrenHandler = images.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) (children []ocispec.Descriptor, err error) {
			children, err = baseChildrenHandler(ctx, desc)
			if err != nil {
				return
			}
			progressTracker.AddChildren(desc, children)
			return
		})
	}

	handler = images.Handlers(append(baseHandlers,
		fetchHandler(store, fetcher, progressTracker),
		checkNeedsFix,
		childrenHandler, // List children to track hierarchy
		appendDistSrcLabelHandler,
	)...)

	// First find suitable platforms to unpack into
	// If image storer is also an unpacker type, i.e implemented UnpackPlatforms() func
	var matchedUnpackPlatforms []unpack.Platform
	if iu, ok := is.(transfer.ImageUnpacker); ok {
		unpacks := iu.UnpackPlatforms()
		if len(unpacks) > 0 {
			uopts := []unpack.UnpackerOpt{}
			enableRemoteSnapshotAnnotations := false
			// Only unpack if requested unpackconfig matches default/supported unpackconfigs
			for _, u := range unpacks {
				matched, mu := getSupportedPlatform(ctx, u, ts.config.UnpackPlatforms)
				if matched {
					if v, ok := mu.SnapshotterExports["enable_remote_snapshot_annotations"]; ok && v == "true" {
						enableRemoteSnapshotAnnotations = true
					}
					if progressTracker != nil {
						mu.ApplyOpts = append(mu.ApplyOpts, diff.WithProgress(progressTracker.ExtractProgress))
					}
					uopts = append(uopts, unpack.WithUnpackPlatform(mu))
					matchedUnpackPlatforms = append(matchedUnpackPlatforms, mu)
				} else {
					log.G(ctx).WithFields(log.Fields{
						"platform":    platforms.FormatAll(u.Platform),
						"snapshotter": u.Snapshotter,
					}).Warn("Unpack configuration not supported, skipping")
				}
			}

			if ts.config.DuplicationSuppressor != nil {
				uopts = append(uopts, unpack.WithDuplicationSuppressor(ts.config.DuplicationSuppressor))
			}

			if ts.limiterP != nil {
				uopts = append(uopts, unpack.WithUnpackLimiter(ts.limiterP))
			}

			if enableRemoteSnapshotAnnotations {
				handler = snpkg.AppendInfoHandlerWrapper(name)(handler)
			}

			unpacker, err = unpack.NewUnpacker(ctx, ts.content, uopts...)
			if err != nil {
				return fmt.Errorf("unable to initialize unpacker: %w", err)
			}
			handler = unpacker.Unpack(handler)
		}
	}

	// fetchAndUnpack downloads the content and unpacks the configured
	// platforms without registering the image name.
	fetchAndUnpack := func(ctx context.Context) (images.Image, error) {
		if dispatchErr := images.Dispatch(ctx, handler, nil, desc); dispatchErr != nil {
			if unpacker != nil {
				// wait for unpacker to cleanup
				unpacker.Wait()
			}
			return images.Image{}, dispatchErr
		}

		// NOTE(fuweid): unpacker defers blobs download. before create image
		// record in ImageService, should wait for unpacking(including blobs
		// download).
		if unpacker != nil {
			if _, waitErr := unpacker.Wait(); waitErr != nil {
				return images.Image{}, waitErr
			}
			// TODO: Check results to make sure unpack was successful
		}

		if hasMediaTypeBug1622 {
			converted, convertErr := docker.ConvertManifest(ctx, store, desc)
			if convertErr != nil {
				return images.Image{}, convertErr
			}
			desc = converted
		}
		return images.Image{Name: name, Target: desc}, nil
	}

	var imgs []images.Image

	// Registry pulls which register under a fixed image name are
	// coordinated: the name is reserved while fetching and the image record
	// is written only once, after content and snapshots have been
	// re-confirmed complete. Concurrent pulls of the same name wait for this
	// result instead of writing the record again.
	var prName string
	var prLabels map[string]string
	var hasPullRef bool
	if pullRef, ok := is.(transfer.PullReference); ok {
		prName, prLabels, hasPullRef = pullRef.PullReference()
	}

	if hasPullRef {
		var contentPlatform platforms.MatchComparer
		if lister, ok := is.(interface{ Platforms() []ocispec.Platform }); ok && len(lister.Platforms()) > 0 {
			contentPlatform = platforms.Ordered(lister.Platforms()...)
		}

		targets := make([]pull.UnpackTarget, 0, len(matchedUnpackPlatforms))
		for _, mu := range matchedUnpackPlatforms {
			matcher, ok := mu.Platform.(platforms.MatchComparer)
			if !ok {
				matcher = platforms.All
			}
			targets = append(targets, pull.UnpackTarget{
				SnapshotterName: mu.SnapshotterKey,
				Snapshotter:     mu.Snapshotter,
				Platform:        matcher,
			})
		}

		coordinator, err := pull.New(ts.images, pull.NewVerifier(ts.content, contentPlatform, targets...))
		if err != nil {
			return err
		}

		committed, err := coordinator.Do(ctx, images.Image{
			Name:   prName,
			Target: desc,
			Labels: prLabels,
		}, fetchAndUnpack)
		if err != nil {
			return err
		}

		// Register additional references (digest refs, prefix refs), but do
		// not write the primary image record a second time.
		if refStorer, ok := is.(transfer.ReferenceStorer); ok {
			extra, err := refStorer.StoreReferences(ctx, committed.Target, ts.images, prName)
			if err != nil {
				return err
			}
			imgs = extra
		}
		imgs = append(imgs, committed)
	} else {
		image, err := fetchAndUnpack(ctx)
		if err != nil {
			return err
		}
		imgs, err = is.Store(ctx, image.Target, ts.images)
		if err != nil {
			return err
		}
	}

	if tops.Progress != nil {
		for _, img := range imgs {
			tops.Progress(transfer.Progress{
				Event: "saved",
				Name:  img.Name,
				Desc:  &img.Target,
			})
		}
		tops.Progress(transfer.Progress{
			Event: fmt.Sprintf("Completed pull from %s", ir),
		})
	}

	return nil
}

func fetchHandler(ingester content.Ingester, fetcher remotes.Fetcher, pt *ProgressTracker) images.HandlerFunc {
	return func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		ctx = log.WithLogger(ctx, log.G(ctx).WithFields(log.Fields{
			"digest":    desc.Digest,
			"mediatype": desc.MediaType,
			"size":      desc.Size,
		}))

		if desc.MediaType == images.MediaTypeDockerSchema1Manifest {
			return nil, fmt.Errorf("%v not supported", desc.MediaType)
		}
		err := remotes.Fetch(ctx, ingester, fetcher, desc)
		if errdefs.IsAlreadyExists(err) {
			pt.MarkExists(desc)
			return nil, nil
		}
		return nil, err
	}
}

// getSupportedPlatform returns a matched platform comparing input UnpackConfiguration to the supported platform/snapshotter combinations
// If input platform didn't specify snapshotter, default will be used if there is a match on platform.
func getSupportedPlatform(ctx context.Context, uc transfer.UnpackConfiguration, supportedPlatforms []unpack.Platform) (matched bool, u unpack.Platform) {
	for _, sp := range supportedPlatforms {
		// If both platform and snapshotter match, return the supportPlatform
		// If platform matched and SnapshotterKey is empty, we assume client didn't pass SnapshotterKey
		// use default Snapshotter
		if sp.Platform.Match(uc.Platform) {
			// Assume sp.SnapshotterKey is not empty
			//nolint:staticcheck // QF1003: could use tagged switch on base (staticcheck)
			if uc.Snapshotter == sp.SnapshotterKey {
				return true, sp
			} else if uc.Snapshotter == "" {
				if sp.SnapshotterKey == defaults.DefaultSnapshotter {
					// Return as best match immediately
					return true, sp
				} else if !matched {
					// Match but prefer default if present to prevent configuration
					// changes from altering default behavior
					matched = true
					u = sp
				}
			}
		} else if uc.Snapshotter == sp.SnapshotterKey {
			log.G(ctx).WithFields(log.Fields{
				"platform":    platforms.FormatAll(uc.Platform),
				"snapshotter": uc.Snapshotter,
			}).Info("Found unpack with matching snapshotter but platform does not match")
		}
	}
	return
}
