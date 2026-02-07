FROM golang:1.23 as golang
WORKDIR /app
COPY . .
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN go build -o monstache

FROM debian:12 AS runtime
RUN sed -i 's|deb.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list.d/debian.sources
RUN apt-get update && \
    apt-get install -y ca-certificates procps tmux vim && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app

# Copy the products
COPY --from=golang /app/monstache /bin/monstache
ENTRYPOINT ["/bin/monstache"]
