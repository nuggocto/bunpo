#!/usr/bin/env bash
# Regenerates the golden JUnit reports from the real runners. Afterwards run
# `go test ./internal/junit -update` and review the diff.
# Needs python3, npm and gem; installs only into the work directory.
set -euo pipefail

PYTEST=9.2.0
JEST=30.5.2
JEST_JUNIT=17.0.0
VITEST=5.1.1
PLAYWRIGHT=1.65.1
RSPEC=3.13.2
RSPEC_JUNIT=0.8.0
# RSpec needs erb, which some Ruby packages leave out.
ERB=6.0.7

here=$(cd "$(dirname "$0")" && pwd)
# A fixed path keeps home directories out of absolute paths in the reports.
work=/tmp/bunpo-fixtures

prepare() {
	rm -rf "${work:?}/$1"
	mkdir -p "$work/$1"
	cp -R "$here/$1/suite/." "$work/$1/"
	cd "$work/$1"
}

# Every suite fails on purpose, so keep checks for the report instead of the
# exit status, and scrubs host names and time zones.
keep() {
	test -s "$work/$1/report.xml" || { echo "$1 wrote no report" >&2; exit 1; }
	sed -E -e 's/hostname="[^"]*"/hostname=""/g' \
		-e 's/timestamp="[^"]*"/timestamp="2026-01-01T00:00:00"/g' \
		"$work/$1/report.xml" >"$here/$1/report.xml"
	echo "$1: $here/$1/report.xml"
}

npm_install() {
	npm install --no-audit --no-fund --no-save --silent "$@"
}

# Refuse to use a symlink, a directory owned by another user, or one writable
# by group or others, because tools installed into /tmp/bunpo-fixtures are run.
if [ ! -e "$work" ] && [ ! -L "$work" ]; then
	mkdir -m 700 "$work"
fi
if [ -L "$work" ] || [ ! -d "$work" ] || [ ! -O "$work" ]; then
	echo "refusing to use $work: must be a real directory owned by the current user" >&2
	exit 1
fi
mode=$(stat -c '%a' "$work" 2>/dev/null || stat -f '%Lp' "$work")
if (( (8#$mode & 8#022) != 0 )); then
	echo "refusing to use $work: mode $mode is writable by group or others" >&2
	exit 1
fi

prepare pytest
python3 -m venv "$work/.venv"
"$work/.venv/bin/pip" install --quiet "pytest==$PYTEST"
"$work/.venv/bin/pytest" tests -o junit_family=xunit1 --junitxml=report.xml >/dev/null || true
keep pytest

prepare jest
npm_install "jest@$JEST" "jest-junit@$JEST_JUNIT"
JEST_JUNIT_OUTPUT_NAME=report.xml JEST_JUNIT_ADD_FILE_ATTRIBUTE=true \
	npx jest --ci --reporters=default --reporters=jest-junit >/dev/null 2>&1 || true
keep jest

prepare vitest
npm_install "vitest@$VITEST"
npx vitest run >/dev/null 2>&1 || true
keep vitest

prepare playwright
npm_install "@playwright/test@$PLAYWRIGHT"
npx playwright test >/dev/null 2>&1 || true
keep playwright

prepare rspec
gems="$work/.gems"
gem install --install-dir "$gems" --no-document --silent \
	"erb:$ERB" "rspec:$RSPEC" "rspec_junit_formatter:$RSPEC_JUNIT"
GEM_HOME="$gems" GEM_PATH="$gems:$(gem env gempath)" "$gems/bin/rspec" \
	--require rspec_junit_formatter --format RspecJunitFormatter --out report.xml >/dev/null || true
keep rspec
