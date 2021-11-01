#!/bin/bash

export REPO_ROOT="$(git rev-parse --show-toplevel)"

function checkAWSCLI() {
  if [[ ! $(which aws) ]]; then
    echo "Please install AWS CLI V2 following these instructions."
    echo "https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-mac.html#cliv2-mac-install-cmd"
  fi
}
