#!/usr/bin/env bash

set -o errexit
set -o nounset
# set -o xtrace

#
# Run the robustness tests
# Run this script from the root of the repo
#

readonly kopia_robustness_dir=${PWD}

readonly test_duration=10s
readonly test_timeout=5m
readonly test_repo_path_prefix="${HOME}/tmp/kopia-robustness-repo/"

readonly kopia_git_revision=$(git rev-parse --short HEAD)
readonly kopia_git_branch="$(git describe --tags --always --dirty)"
readonly kopia_git_dirty=$(git diff-index --quiet HEAD -- || echo "*")
readonly kopia_build_time=$(date +%FT%T%z)

readonly ld_flags="\
-X github.com/kopia/kopia/tests/robustness/engine.repoBuildTime=${kopia_build_time} \
-X github.com/kopia/kopia/tests/robustness/engine.repoGitRevision=${kopia_git_dirty:-""}${kopia_git_revision} \
-X github.com/kopia/kopia/tests/robustness/engine.repoGitBranch=${kopia_git_branch} \
-X github.com/kopia/kopia/tests/robustness/engine.testBuildTime=${kopia_build_time} \
-X github.com/kopia/kopia/tests/robustness/engine.testGitRevision=${kopia_git_dirty:-""}${kopia_git_revision} \
-X github.com/kopia/kopia/tests/robustness/engine.testGitBranch=${kopia_git_branch}"

readonly test_flags="-v -timeout=${test_timeout}\
 --rand-test-duration=${test_duration}\
 --repo-path-prefix=${test_repo_path_prefix}\
 -ldflags '${ld_flags}'"

set -o verbose

make -C "${kopia_robustness_dir}" \
    FIO_EXE=fio \
    GO_TEST='go test' \
    TEST_FLAGS="${test_flags}" \
    robustness-tests
