#!/usr/bin/env bash
# Checks out the claybar-financials reference repo as the working directory for this case.
set -eu
git clone --depth 1 https://github.com/venugummadala/claybar-financials.git . 2>/dev/null || git clone --depth 1 https://github.com/venugummadala/claybar-financials.git repo
