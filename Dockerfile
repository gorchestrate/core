FROM ubuntu:18.04 as build

RUN apt-get update && apt-get install -y git make wget build-essential libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev libzstd-dev liblz4-dev 
RUN wget https://dl.google.com/go/go1.14.linux-amd64.tar.gz && tar -C /usr/local -xzf go1.14.linux-amd64.tar.gz 
ENV PATH="/usr/local/go/bin:${PATH}"

RUN git clone https://github.com/facebook/rocksdb.git 
RUN cd rocksdb && git checkout v5.18.3 && make static_lib

COPY go.mod go.sum ./
RUN go mod download

COPY *.go Makefile ./
COPY gorocksdb gorocksdb/.
RUN make build_docker_static


FROM alpine:latest
COPY --from=build /slct /slct
CMD ["/slct"]
