#!/usr/bin/env bash
# Cross-compiles bunpo and bunpo-server for every supported platform into
# OUTDIR, with a SHA256SUMS file. For a tag such as v0.1.0, it also checks
# that every binary reports that version.
#
#   scripts/build-release.sh v0.1.0 dist
set -euo pipefail

version=${1:?usage: build-release.sh VERSION OUTDIR}
out=${2:?usage: build-release.sh VERSION OUTDIR}
targets=(linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64)
pkgs=(bunpo bunpo-server)

# OUTDIR must be new or empty, so the script never deletes anything it did
# not make.
if [ -e "$out" ] && [ -n "$(ls -A "$out")" ]; then
	echo "$out is not empty; give a new or empty directory" >&2
	exit 1
fi

# Binaries go to a directory outside the checkout first: an untracked file in
# the checkout makes Go stamp every later build as "+dirty".
build=$(mktemp -d)
trap 'rm -rf "$build"' EXIT

for target in "${targets[@]}"; do
	os=${target%/*}
	arch=${target#*/}
	ext=""
	[ "$os" = windows ] && ext=.exe

	for pkg in "${pkgs[@]}"; do
		bin="$build/$pkg-$os-$arch$ext"

		# No cgo and no local paths, so the binary is the same wherever it is built.
		CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -ldflags="-s -w" -o "$bin" "./cmd/$pkg"

		# Go stamps the version from the Git tag; a mismatch means the build did
		# not run on a clean checkout of that tag.
		if [[ $version == v* ]]; then
			stamped=$(go version -m "$bin" | awk '$1 == "mod" { print $3 }')
			[ "$stamped" = "$version" ] || { echo "$bin reports $stamped, want $version" >&2; exit 1; }
		fi
	done
done

if command -v sha256sum >/dev/null; then
	(cd "$build" && sha256sum bunpo-* >SHA256SUMS)
else
	(cd "$build" && shasum -a 256 bunpo-* >SHA256SUMS)
fi

mkdir -p "$out"
mv "$build"/* "$out"/
ls -l "$out"
