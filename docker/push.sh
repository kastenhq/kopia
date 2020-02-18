#!/usr/bin/env sh

set -o nounset
set -o errexit

docker login -u "${DOCKER_USERNAME}" --password-stdin <<EOF
${DOCKER_PASSWORD}
EOF

docker push --all-tags "${1-ghcr.io/kanisterio/kopia}"
