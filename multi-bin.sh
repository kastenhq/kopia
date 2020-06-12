#!/usr/bin/env bash

set -o xtrace

go build ./cmd/cbt

ln -sf cbt kopia

./kopia

./cbt
