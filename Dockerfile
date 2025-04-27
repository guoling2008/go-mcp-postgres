FROM golang:alpine AS builder

WORKDIR /build
# proxy
# ENV GO111MODULE=on \
#     GOPROXY=https://goproxy.cn,direct
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64  go build -o go-mcp-postgres

FROM alpine

WORKDIR /app
COPY --from=builder /build/go-mcp-postgres /app/go-mcp-postgres

CMD ["./go-mcp-postgres"]
