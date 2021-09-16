#!/bin/bash
# Bump the Python package version via Poetry then commit and push back to the branch
# Usage: ./bump-version.sh main my-new-branch
set -eo pipefail

BASE_BRANCH="$1"
NEW_BRANCH="$2"

git checkout "$BASE_BRANCH"
baseVersion="$(poetry version -s)"
git checkout "$NEW_BRANCH"
branchVersion="$(poetry version -s)"
result="$(python - "$baseVersion" "$branchVersion" <<EOF
from packaging.version import parse
import sys
base_version = parse(sys.argv[1])
branch_version = parse(sys.argv[2])
if branch_version > base_version:
    print("1")
elif branch_version < base_version:
    print("-1")
else:
    print("0")
EOF
)"
if [[ "$result" == "1" ]]; then
    echo "Branch version is newer than the base. Continuing..."
elif [[ "$result" == "-1" ]]; then
    echo "Branch version is older than the base. Failing..."
    exit 1
else
    echo "Branch version matches the base. Bumping..."
    poetry version minor
    branchVersion="$(poetry version -s)"
    sed -E -i "s/__version__ = \"[0-9\.]+\"/__version__ = \"${branchVersion}\"/" */__init__.py
    git add pyproject.toml */__init__.py
    git commit -m "Version bumped to ${branchVersion}"
    git push --set-upstream origin "$NEW_BRANCH"
fi
