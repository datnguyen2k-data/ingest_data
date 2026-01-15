# Multi-stage build cho production
# Stage 1: Build
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies (layer caching)
RUN go mod download

# Copy source code
COPY . .

# Build binary với optimizations
# CGO_ENABLED=0 để tạo static binary
# -ldflags="-w -s" để giảm binary size
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -extldflags '-static'" \
    -o pancake_ingest \
    ./cmd/pancake_ingest/

# Stage 2: Runtime (minimal image)
FROM alpine:latest

# Install ca-certificates để support HTTPS
RUN apk --no-cache add ca-certificates tzdata

# Set timezone
ENV TZ=Asia/Ho_Chi_Minh

# Create non-root user
RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser

WORKDIR /app

# Copy binary từ builder stage
COPY --from=builder /build/pancake_ingest /app/pancake_ingest

# Change ownership
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose ports (nếu cần)
# EXPOSE 8080

# Health check (optional)
# HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
#   CMD pgrep -f pancake_ingest || exit 1

# Run binary
CMD ["./pancake_ingest"]
