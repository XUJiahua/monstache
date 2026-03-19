FROM golang:1.23 as golang
WORKDIR /app
COPY . .
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN go build -o monstache

FROM debian:12 AS runtime
RUN sed -i 's|deb.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list.d/debian.sources
RUN apt-get update && \
    apt-get install -y ca-certificates procps tmux vim curl unzip && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*
RUN curl -o /tmp/ossutil.zip https://gosspublic.alicdn.com/ossutil/1.7.19/ossutil-v1.7.19-linux-amd64.zip && \
    unzip /tmp/ossutil.zip -d /tmp && \
    mv /tmp/ossutil-v1.7.19-linux-amd64/ossutil64 /usr/local/bin/ossutil && \
    chmod 755 /usr/local/bin/ossutil && \
    rm -rf /tmp/ossutil.zip /tmp/ossutil-v1.7.19-linux-amd64
WORKDIR /app

# Copy the products
COPY --from=golang /app/monstache /bin/monstache
ENTRYPOINT ["/bin/monstache"]
