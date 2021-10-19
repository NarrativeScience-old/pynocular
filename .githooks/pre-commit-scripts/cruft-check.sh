#!/bin/bash

# Run the cruft check and if it fails prompt user to run the manual update
cruft check
if [[ $? -ne 0 ]]; then
    echo "This project's cruft is not up to date."
    echo "Please run 'cruft update' and follow the prompts to update this repository."
fi
