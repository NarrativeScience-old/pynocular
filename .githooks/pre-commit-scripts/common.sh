#!/bin/bash

export REPO_ROOT="$(git rev-parse --show-toplevel)"

function checkPythonVersion() {
  local version="$(python --version 2>&1)"
  shopt -s nocasematch
  if [[ "$version" != *'Python 3.6.'* ]]; then
    echo "ERROR: The Python version in your shell ($version) is not supported by the git pre-commit hook. Please activate Python 3.6"
    exit 1
  fi
  shopt -u nocasematch
}

function checkAWSCLI() {
  if [[ ! $(which aws) ]]; then
    echo "Please install AWS CLI V2 following these instructions."
    echo "https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-mac.html#cliv2-mac-install-cmd"
  fi
}
