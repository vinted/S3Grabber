#!/usr/bin/env bash
set -euo pipefail

# Determine version from TAG env or CLI argument
if [ -n "${TAG:-}" ]; then
    VERSION="$TAG"
elif [ $# -eq 1 ]; then
    VERSION="$1"
else
    echo "Usage: TAG=<version> $0   or   $0 <version>"
    exit 1
fi

APP_NAME="s3grabber"
MAIN_PKG="./cmd/s3grabber"

# Build matrix
PLATFORMS=(
  "darwin amd64"
  "darwin arm64"
  "linux amd64"
  "linux arm64"
)

# Output directory
OUTDIR="release_$VERSION"
mkdir -p "$OUTDIR"

echo "Building release $VERSION"

for entry in "${PLATFORMS[@]}"; do
    read -r GOOS GOARCH <<< "$entry"

    BIN_NAME="${APP_NAME}"
    TAR_NAME="${APP_NAME}_${VERSION}_${GOOS}_${GOARCH}.tar.gz"

    echo "→ Building $GOOS/$GOARCH"

    GOOS=$GOOS GOARCH=$GOARCH go build -o "$OUTDIR/$BIN_NAME" "$MAIN_PKG"

    echo "→ Packaging $TAR_NAME"
    tar -C "$OUTDIR" -czf "$OUTDIR/$TAR_NAME" "$BIN_NAME"

    rm "$OUTDIR/$BIN_NAME"
done

echo "→ Generating checksums"
(
  cd "$OUTDIR"
  sha256sum *.tar.gz > "${APP_NAME}_${VERSION}_checksums.txt"
)

echo "Done. Files are in: $OUTDIR"
