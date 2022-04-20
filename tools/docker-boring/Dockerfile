FROM ghcr.io/kastenhq/goboring:1.17.9b7 AS builder

# PWD: /go

RUN cd /go/src && git clone https://github.com/kastenhq/kopia

WORKDIR /go/src/kopia

ARG kopiaCommit="kasten"

RUN git checkout "${kopiaCommit}"

# Install static executable, by default it ends up at /go/bin/kopia
RUN GO111MODULE=on GO_EXTLINK_ENABLED=0 go install \
  -ldflags="-s -w -X github.com/kopia/kopia/repo.BuildVersion=$(git show --no-patch --format='%cs-%h') -X github.com/kopia/kopia/repo.BuildInfo=$(git show --no-patch --format='%cI-%H')-${kopiaCommit}" .


FROM alpine

WORKDIR /kopia

# Add CA certs
RUN apk add --no-cache --verbose ca-certificates libc6-compat && \
  rm -rf /var/cache/apk/* && \
  adduser -D kopia && addgroup kopia kopia && \
  chown kopia /kopia

USER kopia:kopia

ENTRYPOINT [ "/bin/kopia" ]

COPY --from=builder --chown=kopia:kopia /go/bin/kopia /bin/kopia

ARG imageVersion="unknown"
# TODO: replace with build date + git commit info
LABEL imageVersion="${imageVersion}"
LABEL kopiaCommit="${kopiaCommit}"
