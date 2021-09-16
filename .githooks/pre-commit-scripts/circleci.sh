#!/bin/bash

# pre-commit may run this script many times so we'll memoize it to only run once
if [[ "$CI" == 'true' ]]; then
  CRUMB="/tmp/${CIRCLE_BUILD_NUM}-$(basename "$0")"
  if [[ -f "$CRUMB" ]]; then
    echo "Already ran script for build ${CIRCLE_BUILD_NUM}"
    exit 0
  fi
  touch "$CRUMB"
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${DIR}/common.sh"

if ! (command -v circleci >/dev/null 2>&1); then
  echo "Installing circleci CLI"
  curl -fLSs https://circle.ci/cli | bash
fi

pushd "$REPO_ROOT" > /dev/null
circleci config pack .circleci > .circleci/config.yml
if [[ $? -ne 0 ]]; then
  echo "Failed to pack .circleci/config.yml. Please fix the errors above and try again."
  exit 1
fi
circleci config validate
if [[ $? -ne 0 ]]; then
  echo "Failed to validate .circleci/config.yml. Please fix the errors above and try again."
  exit 1
fi
if [[ "$CI" != 'true' ]]; then
  git add .circleci/config.yml
fi
popd > /dev/null
