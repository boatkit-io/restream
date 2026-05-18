#!/usr/bin/env bash
# Determines the next version.
set -euo pipefail

# Determine the next version as reported by the next-version command.
get-next-version --prefix v 2> /dev/null | tr -d '\n'
