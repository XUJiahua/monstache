FROM golang:1.20
WORKDIR /app
COPY ./ ./
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o monstache

FROM debian:bullseye-slim AS runtime
RUN apt-get update
# may have issue
RUN apt-get install ca-certificates -y
RUN update-ca-certificates
WORKDIR /app
COPY --from=0 /app/monstache ./
ENTRYPOINT ["./monstache"]