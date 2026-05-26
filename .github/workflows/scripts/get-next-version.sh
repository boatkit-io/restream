#!/usr/bin/env bash
# Determines the next version.
set -euo pipefail

# In GitHub Actions, preserve get-next-version's release/no-release signal.
# Locally, keep the task output as the plain version string.
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  get-next-version --prefix v --target github-action
else
  get-next-version --prefix v 2> /dev/null | tr -d '\n'
fi
