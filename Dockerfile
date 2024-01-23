FROM golang:1.20
WORKDIR /app
COPY ./ ./
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN GOOS=linux GOARCH=amd64 go build -o monstache

FROM rwynn/monstache-alpine:3.17.3 AS final
WORKDIR /app
COPY --from=0 /app/monstache ./
ENTRYPOINT ["./monstache"]