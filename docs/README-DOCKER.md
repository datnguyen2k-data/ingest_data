# Docker Setup cho Pancake Ingest Service

Service này chỉ build image và container, kết nối đến **Kafka external** (từ dự án data infra).

## 📋 Requirements

- Docker & Docker Compose
- Kafka cluster đã có sẵn (external)
- Environment variables đã config đúng

## 🚀 Quick Start

### 1. Cấu hình Environment Variables

Tạo file `.env` với các thông tin sau:

```bash
# Application
APP_NAME=ingest_data
APP_ENV=docker

# Kafka Configuration (external - từ data infra)
KAFKA_BOOTSTRAP_SERVERS=kafka-host1:9092,kafka-host2:9092,kafka-host3:9092
KAFKA_ORDER_TOPIC=pancake_chando_sale_order
KAFKA_CONSUMER_GROUP=ingest-data

# Pancake API
PANCAKE_CHANDO_BASE_URL=https://pos.pages.fm/api/v1
PANCAKE_CHANDO_API_KEY=your_api_key_here
PANCAKE_CHANDO_SHOP_ID=your_shop_id_here
PANCAKE_CHANDO_PAGE_SIZE=500
PANCAKE_CHANDO_SLEEP_MS=1000

# Logging
LOG_LEVEL=info
```

**Lưu ý:**
- `KAFKA_BOOTSTRAP_SERVERS`: Địa chỉ Kafka external (có thể là IP hoặc hostname)
- Nếu Kafka ở cùng Docker network, dùng hostname internal
- Nếu Kafka ở cùng host, có thể dùng `host.docker.internal:9092` (Mac/Windows) hoặc `host` network mode

### 2. Build Docker Image

```bash
# Build image
make docker-build

# Hoặc build với tag cụ thể
docker build -t ingest_data:1.0.0 .
```

### 3. Run với Docker Compose

```bash
# Start service
make docker-up

# Hoặc
docker-compose up -d

# Xem logs
make docker-logs-app

# Stop service
make docker-down
```

### 4. Run với Docker (không dùng compose)

```bash
# Build image
docker build -t ingest_data:latest .

# Run container
docker run -d \
  --name ingest_data \
  --restart unless-stopped \
  --env-file .env \
  ingest_data:latest

# Xem logs
docker logs -f ingest_data

# Stop container
docker stop ingest_data
docker rm ingest_data
```

## 🔧 Network Configuration

### Option 1: Kafka ở cùng Docker Network

Nếu Kafka ở cùng Docker network (từ data infra project):

```yaml
# docker-compose.yml
services:
  pancake-ingest:
    networks:
      - data-infra-network  # Network từ data infra project
```

Run với external network:
```bash
docker-compose up -d --network data-infra-network
```

### Option 2: Kafka ở cùng Host

Nếu Kafka chạy trên cùng host:

```yaml
# docker-compose.yml
services:
  pancake-ingest:
    network_mode: "host"  # Uncomment dòng này
```

Và trong `.env`:
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:19092,localhost:29092,localhost:39092
```

### Option 3: Kafka ở remote server

Nếu Kafka ở server khác (có thể truy cập qua network):

```bash
# .env
KAFKA_BOOTSTRAP_SERVERS=192.168.1.100:9092,192.168.1.101:9092,192.168.1.102:9092
```

## 📦 Production Deployment

### Build Production Image

```bash
# Build với version tag
make docker-build-prod IMAGE_TAG=v1.0.0

# Hoặc
docker build --build-arg BUILD_VERSION=v1.0.0 \
  -t pancake-ingest:v1.0.0 \
  -t pancake-ingest:latest .
```

### Deploy Production

```bash
# Sử dụng docker-compose.prod.yml
docker-compose -f docker-compose.prod.yml up -d

# Hoặc push lên registry và deploy
make prod-deploy DOCKER_REGISTRY=your-registry.io IMAGE_TAG=v1.0.0
```

### Production Environment Variables

Tạo file `.env.production`:

```bash
APP_NAME=ingest_data
APP_ENV=production

# Production Kafka
KAFKA_BOOTSTRAP_SERVERS=prod-kafka-1:9092,prod-kafka-2:9092,prod-kafka-3:9092
KAFKA_ORDER_TOPIC=pancake_chando_sale_order
KAFKA_CONSUMER_GROUP=ingest-data-prod

# Production Pancake API
PANCAKE_CHANDO_BASE_URL=https://pos.pages.fm/api/v1
PANCAKE_CHANDO_API_KEY=prod_api_key
PANCAKE_CHANDO_SHOP_ID=prod_shop_id
PANCAKE_CHANDO_PAGE_SIZE=500
PANCAKE_CHANDO_SLEEP_MS=1000

LOG_LEVEL=info
```

## 🛠️ Makefile Commands

```bash
# Build
make docker-build              # Build Docker image
make docker-build-prod         # Build production image

# Docker Compose
make docker-up                 # Start service
make docker-down               # Stop service
make docker-logs-app           # View logs
make docker-restart            # Restart service
make docker-rebuild            # Rebuild và restart

# Production
make docker-up-prod            # Start production
make docker-down-prod          # Stop production
make prod-deploy               # Build, tag, push

# Cleanup
make docker-clean              # Clean containers, networks, volumes
make docker-images             # List images
make docker-ps                 # List running containers
```

## 🔍 Debugging

### Xem logs

```bash
# Real-time logs
docker-compose logs -f pancake-ingest

# Hoặc
docker logs -f pancake-ingest
```

### Exec vào container

```bash
# Bash shell
docker-compose exec ingest_data sh

# Hoặc
make docker-exec
```

### Kiểm tra connection đến Kafka

```bash
# Exec vào container
docker-compose exec ingest-data sh

# Test network connection (nếu có telnet)
telnet kafka-host 9092

# Hoặc dùng nc
nc -zv kafka-host 9092
```

## 📝 Notes

1. **Image size**: Multi-stage build tạo image nhỏ (~20MB) từ Alpine
2. **Non-root user**: Container chạy với user `appuser` (UID 1000) để bảo mật
3. **Health check**: Container có health check tự động
4. **Log rotation**: Logs tự động rotate (max 10MB, 3 files)
5. **Resource limits**: Production có resource limits (CPU, Memory)

## 🚨 Troubleshooting

### Container không start

```bash
# Check logs
docker logs ingest_data

# Check environment variables
docker-compose config
```

### Không connect được Kafka

1. Kiểm tra `KAFKA_BOOTSTRAP_SERVERS` trong `.env`
2. Kiểm tra network connectivity từ container
3. Kiểm tra firewall rules
4. Nếu Kafka ở Docker network khác, cần join network đó

### Permission denied

Nếu có lỗi permission, kiểm tra:
- User trong container: `docker exec pancake-ingest id`
- File permissions: `ls -la /app`
