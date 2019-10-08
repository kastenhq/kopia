FROM golang:1.14-alpine3.12 AS builder

RUN adduser -D kopia && addgroup kopia kopia
USER kopia:kopia

COPY --chown=kopia . /kopia
WORKDIR /kopia
# Build static executable
RUN GO111MODULE=on CGO_ENABLED=0 GO_EXTLINK_ENABLED=0 go build .

FROM alpine:3.12

WORKDIR /kopia

# Add CA certs
RUN apk add --no-cache --verbose ca-certificates && \
  rm -rf /var/cache/apk/* && \
  adduser -D kopia && addgroup kopia kopia && \
  chown kopia /kopia

USER kopia:kopia

ENTRYPOINT [ "/kopia/kopia" ]

LABEL imageVersion="unknown"

COPY --from=builder --chown=kopia:kopia /kopia/kopia /kopia/kopia
