#!/usr/bin/env sh
set -o nounset
set -o errexit

echo "$DOCKER_PASSWORD" | docker login ghcr.io -u "$DOCKER_USERNAME" --password-stdin
docker push "${1-ghcr.io/kanisterio/kopia}"
