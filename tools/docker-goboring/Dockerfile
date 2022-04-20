FROM debian:stable-slim AS builder

ARG goVersion="1.17.9b7"
ARG tarSha256="9469d1b4c10f59c921c4666c52baba5f6ca63b1cce0eca95e03b5713ef27577c"

ADD "https://go-boringcrypto.storage.googleapis.com/go${goVersion}.linux-amd64.tar.gz" go.tgz

RUN echo "${tarSha256}  go.tgz" | sha256sum -c -

RUN tar -xzf go.tgz

FROM debian:stable-slim

CMD ["bash"]

ENV GOPATH=/go

# Add CA certs and dev libs
RUN apt-get update && \
  apt-get install --yes --no-install-recommends \
    ca-certificates curl netbase wget gnupg dirmngr git mercurial \
    openssh-client subversion g++ gcc libc6-dev make pkg-config && \
  apt-get autoclean && \
  apt-get clean && \
  rm -rf /var/cache/apt/* && \
  adduser --home /user --ingroup users --disabled-password --gecos "User" user && \
  mkdir -p "$GOPATH/src" "$GOPATH/bin" && \
  chmod -R 777 "$GOPATH"

ENV PATH=/go/bin:/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

COPY --from=builder /go/ /usr/local/go/

WORKDIR ${GOPATH}

ARG goVersion

LABEL org.golang.version="${goVersion}"

ENV GOLANG_VERSION="${goVersion}"
