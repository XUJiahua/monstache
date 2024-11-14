FROM golang:1.23 as golang
WORKDIR /app
COPY . .
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN go build -o monstache

FROM debian:12 AS runtime
RUN apt-get update
RUN apt-get install ca-certificates -y
RUN update-ca-certificates

# Copy the products
COPY --from=golang /app/monstache /bin/monstache
ENTRYPOINT ["/bin/monstache"]
