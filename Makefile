.PHONY: help build build-docker build-local run-docker run-test stop clean test docker-build docker-run docker-stop docker-clean docker-push

# Variables
IMAGE_NAME ?= pancake-ingest
IMAGE_TAG ?= latest
DOCKER_REGISTRY ?= # Add your registry here, e.g., docker.io/yourusername or ghcr.io/yourusername

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Hiển thị help message
	@echo "$(GREEN)Available commands:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

# Build commands
build: ## Build Go binary locally
	@echo "$(GREEN)Building Go binary...$(NC)"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o pancake_ingest ./cmd/pancake_ingest/

build-local: ## Build Go binary cho local development
	@echo "$(GREEN)Building Go binary for local...$(NC)"
	go build -o pancake_ingest ./cmd/pancake_ingest/

# Docker commands
docker-build: ## Build Docker image
	@echo "$(GREEN)Building Docker image: $(IMAGE_NAME):$(IMAGE_TAG)$(NC)"
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .
	@echo "$(GREEN)Docker image built successfully!$(NC)"

docker-build-prod: ## Build Docker image for production
	@echo "$(GREEN)Building production Docker image...$(NC)"
	docker build --build-arg BUILD_VERSION=$(IMAGE_TAG) -t $(IMAGE_NAME):$(IMAGE_TAG) -t $(IMAGE_NAME):latest .
	@echo "$(GREEN)Production image built successfully!$(NC)"

docker-tag: ## Tag Docker image (use REGISTRY=xxx IMAGE_TAG=xxx)
	@if [ -z "$(DOCKER_REGISTRY)" ]; then \
		echo "$(RED)Error: DOCKER_REGISTRY is not set$(NC)"; \
		exit 1; \
	fi
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(DOCKER_REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(DOCKER_REGISTRY)/$(IMAGE_NAME):latest

docker-push: ## Push Docker image to registry (use REGISTRY=xxx IMAGE_TAG=xxx)
	@if [ -z "$(DOCKER_REGISTRY)" ]; then \
		echo "$(RED)Error: DOCKER_REGISTRY is not set$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)Pushing $(DOCKER_REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)...$(NC)"
	docker push $(DOCKER_REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)
	docker push $(DOCKER_REGISTRY)/$(IMAGE_NAME):latest
	@echo "$(GREEN)Image pushed successfully!$(NC)"

# Docker Compose commands
docker-up: ## Start pancake-ingest service (kết nối đến Kafka external)
	@echo "$(GREEN)Starting pancake-ingest service...$(NC)"
	@if [ ! -f .env ]; then \
		echo "$(RED)Error: .env file not found!$(NC)"; \
		echo "$(YELLOW)Please create .env file with Kafka and Pancake config$(NC)"; \
		exit 1; \
	fi
	docker-compose up -d
	@echo "$(GREEN)Service started!$(NC)"
	@echo "$(YELLOW)View logs: make docker-logs-app$(NC)"

docker-down: ## Stop all services
	@echo "$(YELLOW)Stopping services...$(NC)"
	docker-compose down

docker-logs: ## Xem logs của tất cả services
	docker-compose logs -f

docker-logs-app: ## Xem logs của pancake-ingest service
	docker-compose logs -f ingest-data

docker-restart: ## Restart pancake-ingest service
	@echo "$(YELLOW)Restarting pancake-ingest...$(NC)"
	docker-compose restart pancake-ingest

docker-rebuild: ## Rebuild và restart pancake-ingest service
	@echo "$(GREEN)Rebuilding và restarting pancake-ingest...$(NC)"
	docker-compose up -d --build pancake-ingest

# Production docker-compose
docker-up-prod: ## Start production với docker-compose.prod.yml
	@echo "$(GREEN)Starting production services...$(NC)"
	docker-compose -f docker-compose.prod.yml up -d
	@echo "$(GREEN)Production services started!$(NC)"

docker-down-prod: ## Stop production services
	@echo "$(YELLOW)Stopping production services...$(NC)"
	docker-compose -f docker-compose.prod.yml down

# Cleanup commands
docker-clean: ## Remove containers, networks và volumes
	@echo "$(YELLOW)Cleaning up Docker resources...$(NC)"
	docker-compose down -v --remove-orphans
	docker system prune -f

docker-clean-all: ## Remove images, containers, networks và volumes (CAUTION!)
	@echo "$(RED)WARNING: This will remove all Docker resources!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v --rmi all --remove-orphans; \
		docker system prune -af --volumes; \
		echo "$(GREEN)Cleanup completed!$(NC)"; \
	fi

# Development commands
run-local: build-local ## Build và run locally
	@echo "$(GREEN)Running locally...$(NC)"
	./pancake_ingest

test: ## Run tests
	@echo "$(GREEN)Running tests...$(NC)"
	go test -v ./...

test-cover: ## Run tests với coverage
	@echo "$(GREEN)Running tests with coverage...$(NC)"
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "$(GREEN)Coverage report: coverage.html$(NC)"

# Utility commands
docker-ps: ## List running containers
	docker ps

docker-images: ## List Docker images
	docker images | grep $(IMAGE_NAME)

docker-exec: ## Exec vào pancake-ingest container (bash)
	docker-compose exec pancake-ingest sh

docker-shell: docker-exec ## Alias cho docker-exec

# Quick commands
all: docker-build docker-up ## Build image và start services

prod-build: docker-build-prod ## Build production image

prod-deploy: docker-build-prod docker-tag docker-push ## Build, tag và push production image
