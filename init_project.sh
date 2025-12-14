#!/usr/bin/env bash

set -e

echo "Initializing DDD project structure in current Go module"

# cmd
mkdir -p cmd/api

# internal layers
mkdir -p internal/domain/order
mkdir -p internal/domain/repository

mkdir -p internal/application/order

mkdir -p internal/infrastructure/persistence/postgres
mkdir -p internal/infrastructure/messaging/kafka
mkdir -p internal/infrastructure/http/gin

mkdir -p internal/interfaces/http/handler
mkdir -p internal/interfaces/http/router
mkdir -p internal/interfaces/dto

mkdir -p internal/middleware
mkdir -p internal/config

# db
mkdir -p db/migrations

# pkg (shared)
mkdir -p pkg/utils
mkdir -p pkg/logger

# Files
touch cmd/api/main.go
touch README.md
touch Makefile

touch internal/domain/order/entity.go
touch internal/domain/order/value_object.go
touch internal/domain/order/errors.go

touch internal/domain/repository/order_repository.go

touch internal/application/order/service.go

touch internal/infrastructure/persistence/postgres/sqlc.yaml
touch internal/infrastructure/persistence/postgres/queries.sql
touch internal/infrastructure/persistence/postgres/order_repository.go

touch internal/infrastructure/messaging/kafka/producer.go
touch internal/infrastructure/messaging/kafka/consumer.go

touch internal/infrastructure/http/gin/server.go

touch internal/interfaces/http/handler/order_handler.go
touch internal/interfaces/http/router/router.go

touch internal/config/server_config.go

# .gitignore (only if not exists)
if [ ! -f .gitignore ]; then
cat <<EOF > .gitignore
# Go
/bin/
/vendor/
*.exe
*.out

# Env
.env

# IDE
.idea/
.vscode/

# OS
.DS_Store
EOF
fi

echo "DDD project structure created successfully!"
