#!/usr/bin/env sh

set -o errexit
set -o nounset
set -o xtrace

readonly COMMIT_TAG=$(git rev-parse --short=7 HEAD)
readonly DOCKER_DIR=docker/kopia-alpine
readonly IMAGE_TYPE=alpine
readonly IMAGE_VERSION="$(git describe --long --candidates=1)-$(date +%Y%m%d-%H%M%S)"
readonly REPO=kopia/kopia
readonly TAG="alpine-${IMAGE_VERSION}"

cd ../..

docker build \
    --label "imageType=${IMAGE_TYPE}" \
    --label "imageVersion=${IMAGE_VERSION}" \
    --label "kopiaCommit=$(git rev-parse HEAD)" \
    --tag "${REPO}:${IMAGE_TYPE}" \
    --tag "${REPO}:${IMAGE_TYPE}-${COMMIT_TAG}" \
    --tag "${REPO}:${IMAGE_TYPE}-${IMAGE_VERSION}" \
    --file "${DOCKER_DIR}/Dockerfile" .

echo "Build tag: ${IMAGE_TYPE}-${COMMIT_TAG}"
echo "Run with: docker run --rm -it ${REPO}:${IMAGE_TYPE}-${COMMIT_TAG}"
