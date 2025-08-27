FROM golang:alpine AS builder

WORKDIR /build
# proxy
# ENV GO111MODULE=on \
#     GOPROXY=https://goproxy.cn,direct
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64  go build -o go-mcp-postgres

FROM alpine

# 创建非特权用户
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

WORKDIR /app
COPY --from=builder /build/go-mcp-postgres /app/go-mcp-postgres

# 确保应用程序可执行并设置正确的所有权
RUN chmod +x /app/go-mcp-postgres && \
    chown -R appuser:appgroup /app

# 切换到非特权用户
USER appuser

CMD ["./go-mcp-postgres"]
