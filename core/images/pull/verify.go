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

	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
)

// Verifier re-confirms, purely by digest and chain ID, that a pulled image is
// complete before its name becomes visible:
//
//   - the target descriptor (index or manifest) is present in the content
//     store, together with every manifest, config and layer descriptor the
//     pull requested for its platform;
//   - for every unpack target, every layer snapshot identified by its chain
//     ID is committed to the snapshotter and the config content keeps the
//     garbage collection reference to the top of the snapshot chain.
//
// Nothing written by an interrupted previous pull is trusted: only content
// addressed by the digests of the resolved manifest and snapshots addressed
// by the chain IDs derived from its config count as complete.
type Verifier struct {
	content content.InfoReaderProvider

	// platform selects the index entries a single-platform pull requested.
	// nil means every entry, as used by all-platform pulls.
	platform platforms.MatchComparer

	unpacks []UnpackTarget
}

// NewVerifier creates a Verifier. When unpacks is empty, only content
// completeness is checked (content-only pulls).
func NewVerifier(cs content.InfoReaderProvider, platform platforms.MatchComparer, unpacks ...UnpackTarget) *Verifier {
	return &Verifier{
		content:  cs,
		platform: platform,
		unpacks:  unpacks,
	}
}

// Verify returns nil when the image is fully present, otherwise an error
// describing the first missing component.
func (v *Verifier) Verify(ctx context.Context, image images.Image) error {
	if err := verifyContent(ctx, v.content, v.platform, image.Target); err != nil {
		return fmt.Errorf("image %q content is incomplete: %w", image.Name, err)
	}

	for _, target := range v.unpacks {
		if err := verifyUnpacked(ctx, v.content, image.Target, target); err != nil {
			return fmt.Errorf("image %q is not unpacked for %s: %w", image.Name, target.SnapshotterName, err)
		}
	}

	return nil
}

// verifyContent walks the descriptor tree and opens a ReaderAt for every
// descriptor the pull requested. The content store only exposes committed
// blobs through ReaderAt, so a successful check proves every component is
// present and addressed by its digest.
func verifyContent(ctx context.Context, provider content.Provider, platform platforms.MatchComparer, target ocispec.Descriptor) error {
	return images.Walk(ctx, images.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		ra, err := provider.ReaderAt(ctx, desc)
		if err != nil {
			return nil, fmt.Errorf("content %s (%s): %w", desc.Digest, desc.MediaType, err)
		}
		if err := ra.Close(); err != nil {
			return nil, err
		}

		children, err := images.Children(ctx, provider, desc)
		if err != nil {
			return nil, err
		}

		// Mirror the platform filtering the fetch handler applies to index
		// entries: only the manifests selected for the pull are required.
		if images.IsIndexType(desc.MediaType) && platform != nil {
			filtered := children[:0]
			for _, child := range children {
				if child.Platform == nil || platform.Match(*child.Platform) {
					filtered = append(filtered, child)
				}
			}
			children = filtered
		}

		return children, nil
	}), target)
}

// verifyUnpacked confirms the platform manifest resolves to a fully committed
// snapshot chain.
func verifyUnpacked(ctx context.Context, cs content.InfoReaderProvider, target ocispec.Descriptor, unpack UnpackTarget) error {
	manifest, err := images.Manifest(ctx, cs, target, unpack.Platform)
	if err != nil {
		return err
	}

	diffIDs, err := images.RootFS(ctx, cs, manifest.Config)
	if err != nil {
		return err
	}

	if len(diffIDs) == 0 {
		// Images without layers (e.g. scratch) have no snapshots to mount.
		return nil
	}

	chainIDs := identity.ChainIDs(diffIDs)
	for _, chainID := range chainIDs {
		if _, err := unpack.Snapshotter.Stat(ctx, chainID.String()); err != nil {
			if errdefs.IsNotFound(err) {
				return fmt.Errorf("snapshot %s is not committed: %w", chainID, err)
			}
			return fmt.Errorf("stat snapshot %s: %w", chainID, err)
		}
	}

	// The unpacker labels the config with the chain top so garbage
	// collection keeps the snapshot chain. The image is not mountable after
	// the pull lease ends without it.
	top := chainIDs[len(chainIDs)-1].String()
	info, err := cs.Info(ctx, manifest.Config.Digest)
	if err != nil {
		return fmt.Errorf("image config %s: %w", manifest.Config.Digest, err)
	}
	if got := info.Labels["containerd.io/gc.ref.snapshot."+unpack.SnapshotterName]; got != top {
		return fmt.Errorf("config %s does not reference unpacked snapshot chain %s (got %q): %w",
			manifest.Config.Digest, top, got, errdefs.ErrNotFound)
	}

	return nil
}
