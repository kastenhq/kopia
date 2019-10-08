#!/usr/bin/env sh

set -o errexit
set -o nounset
set -o xtrace

readonly KOPIA_BUILD_COMMIT_SHORT=$(git rev-parse --short=7 HEAD)
#readonly KOPIA_MAIN_COMMIT_SHORT=$(git rev-parse --short=7 master)
readonly IMAGE_TYPE=alpine
readonly DOCKER_DIR=docker/kopia-${IMAGE_TYPE}
readonly TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
readonly IMAGE_BUILD_VERSION="${TIMESTAMP}-$(git describe --long --candidates=1 --always HEAD)"
readonly REPO=${1-ghcr.io/kanisterio/kopia}
readonly TAG="${IMAGE_TYPE}-${IMAGE_BUILD_VERSION}"


REPO_DIR=$(realpath --logical --canonicalize-existing $(dirname "${0}")/../..)
cd "${REPO_DIR}"

docker build \
    --label "imageType=${IMAGE_TYPE}" \
    --label "kopiaCommitMain=$(git rev-parse master)" \
    --build-arg "imageVersion=${IMAGE_BUILD_VERSION}" \
    --build-arg "kopiaBuildCommit=$(git rev-parse HEAD)" \
    --tag "${REPO}:${IMAGE_TYPE}" \
    --tag "${REPO}:${IMAGE_TYPE}-${KOPIA_BUILD_COMMIT_SHORT}" \
    --tag "${REPO}:${IMAGE_TYPE}-${IMAGE_BUILD_VERSION}" \
    --file "${DOCKER_DIR}/Dockerfile" .

echo "Build tag: ${IMAGE_TYPE}-${KOPIA_BUILD_COMMIT_SHORT}"
echo "Run with: docker run --rm -it ${REPO}:${IMAGE_TYPE}-${KOPIA_BUILD_COMMIT_SHORT}"

# Minimum sanity check:
docker run --rm -it \
    "${REPO}:${IMAGE_TYPE}-${KOPIA_BUILD_COMMIT_SHORT}" \
    repo connect s3 --point-in-time 2021-07-01 |
    grep -q -v "unknown long flag '--point-in-time'"
