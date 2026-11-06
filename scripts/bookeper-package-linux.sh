#!/usr/bin/env bash
# Build the linux release archives of a channel tag from a CLEAN checkout of the tagged commit.
#
# Reuses the release tooling this tree already carries: build-release-runtimes.sh (the six
# runtime companions, frozen-source build ID), verify-go-release-binary.mjs (build provenance) and
# runtime-artifacts (manifest prepare/verify/extract). It does not sign, notarize or build the
# Apple helper, so it only produces the two linux controller archives:
#   crabbox_<version>_linux_amd64.tar.gz, crabbox_<version>_linux_arm64.tar.gz, checksums.txt
# Usage: scripts/bookeper-package-linux.sh vX.Y.Z-bookeper.N OUTPUT_DIR   (OUTPUT_DIR must not exist)
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
# shellcheck source=release-config.sh
source "$ROOT/scripts/release-config.sh"
TAG=${1:-}
OUT=${2:-}
if [[ ! "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-bookeper\.[0-9]+$ || -z "$OUT" ]]; then
  echo "usage: $0 vX.Y.Z-bookeper.N OUTPUT_DIR" >&2
  exit 2
fi
[[ ! -e "$OUT" ]] || { echo "refusing to reuse existing output directory: $OUT" >&2; exit 1; }
for tool in git go node python3; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
done
[[ -z "$(git -C "$ROOT" status --porcelain --untracked-files=all)" ]] || {
  echo "the checkout must be clean (a release is built from committed bytes)" >&2
  exit 1
}
COMMIT=$(git -C "$ROOT" rev-parse HEAD)
VERSION=${TAG#v}
COMMIT_TIME=$(git -C "$ROOT" show -s --format=%ct HEAD)

WORK=$(mktemp -d "${TMPDIR:-/tmp}/crabbox-channel-build.XXXXXX")
cleanup() {
  [[ -d "$WORK" ]] || return 0
  find -P "$WORK" -type d -exec chmod u+w {} + 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT
mkdir -m 700 "$WORK/home" "$WORK/tmp"
GO_ENV=(env -i GOCACHE="$WORK/gocache" GOMODCACHE="$WORK/gomodcache" GOPROXY=https://proxy.golang.org
  GOSUMDB=sum.golang.org GOTOOLCHAIN="$CRABBOX_RELEASE_GO_VERSION" GOWORK=off HOME="$WORK/home"
  PATH="$PATH" TMPDIR="$WORK/tmp")

# 1. The six runtime companions plus the frozen-source filesystem build ID.
"$ROOT/scripts/build-release-runtimes.sh" "$ROOT" "$COMMIT" "$WORK/runtime-pack" "$WORK/runtime-build"
BUILD_ID=$(cat "$WORK/runtime-build/filesystem-build-id")
[[ "$BUILD_ID" =~ ^[0-9a-f]{64}$ ]] || { echo "invalid filesystem build ID" >&2; exit 1; }

# 2. The manifest tool, built from the same tree.
(cd "$ROOT" && "${GO_ENV[@]}" go build -trimpath -buildvcs=false -o "$WORK/runtime-artifacts" ./scripts/runtime-artifacts)

# 3. One controller + pack + archive per linux architecture.
mkdir -m 755 "$OUT"
for arch in amd64 arm64; do
  stage="$WORK/stage-$arch"
  mkdir -p "$stage"
  cp -R "$WORK/runtime-pack" "$stage/crabbox-runtime"
  (
    cd "$ROOT"
    "${GO_ENV[@]}" CGO_ENABLED=0 GOOS=linux GOARCH="$arch" GOAMD64=v1 GOARM64=v8.0 \
      go build -trimpath -ldflags "-s -w -X github.com/openclaw/crabbox/internal/cli.version=$VERSION" \
      -o "$stage/crabbox" ./cmd/crabbox
  )
  "$WORK/runtime-artifacts" prepare --directory "$stage/crabbox-runtime" --controller "$stage/crabbox" \
    --filesystem-build-id "$BUILD_ID" --egress >"$stage/manifest.json"
  mv "$stage/manifest.json" "$stage/crabbox-runtime/manifest.json"
  "$WORK/runtime-artifacts" verify --directory "$stage/crabbox-runtime" --controller "$stage/crabbox" \
    --filesystem-build-id "$BUILD_ID" --egress >/dev/null
  archive="$OUT/crabbox_${VERSION}_linux_${arch}.tar.gz"
  python3 - "$stage" "$archive" "$COMMIT_TIME" <<'PY'
import gzip
import io
import os
import sys
import tarfile

stage, archive, mtime = sys.argv[1], sys.argv[2], int(sys.argv[3])
members = ["crabbox"] + sorted(
    os.path.join("crabbox-runtime", n) for n in os.listdir(os.path.join(stage, "crabbox-runtime"))
)
raw = io.BytesIO()
with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as tar:
    for name in members:
        path = os.path.join(stage, name)
        info = tarfile.TarInfo(name)
        info.size = os.path.getsize(path)
        info.mode = 0o644 if name.endswith("manifest.json") else 0o755
        info.mtime = mtime
        info.uid = info.gid = 0
        info.uname = info.gname = ""
        with open(path, "rb") as handle:
            tar.addfile(info, handle)
with open(archive, "wb") as out, gzip.GzipFile(fileobj=out, mode="wb", mtime=0, filename="") as gz:
    gz.write(raw.getvalue())
PY
  # The archive must extract and verify exactly as a consumer reads it.
  "$WORK/runtime-artifacts" extract --archive "$archive" --directory "$WORK/check-$arch" --os linux --arch "$arch" \
    --runtime-pack final-egress --filesystem-build-id "$BUILD_ID" >/dev/null
done

# 4. Checksums over the archives only; the controller digest is printed for the pin file.
(cd "$OUT" && if command -v sha256sum >/dev/null; then sha256sum crabbox_*.tar.gz; else shasum -a 256 crabbox_*.tar.gz; fi >checksums.txt)
for arch in amd64 arm64; do
  printf 'controller_sha256 linux_%s %s\n' "$arch" "$(python3 -c 'import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$WORK/stage-$arch/crabbox")"
done
printf 'source_commit %s\ntag %s\n' "$COMMIT" "$TAG"
