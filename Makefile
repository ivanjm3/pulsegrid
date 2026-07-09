# =============================================================================
# Makefile - PulseGrid Build Targets
# =============================================================================

# Configuration
APP_NAME := pulsegrid
REGISTRY ?= ghcr.io/pulsegrid
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GO := go
GOFLAGS := -ldflags="-s -w -X main.version=$(VERSION)"

# Docker
DOCKER := docker
PLATFORM ?= linux/amd64

# Binaries
API_BIN := bin/api
WORKER_BIN := bin/worker

.PHONY: all build test lint clean docker-build docker-push help

## help: Show available targets
help:
	@echo "PulseGrid Build Targets:"
	@echo ""
	@echo "  build          Build API and Worker binaries"
	@echo "  build-api      Build API binary only"
	@echo "  build-worker   Build Worker binary only"
	@echo "  test           Run all tests"
	@echo "  test-unit      Run unit tests only"
	@echo "  test-race      Run tests with race detector"
	@echo "  lint           Run Go vet"
	@echo "  clean          Remove build artifacts"
	@echo "  docker-build   Build Docker images"
	@echo "  docker-push    Push Docker images to registry"
	@echo "  ci             Full CI pipeline (lint + test + build)"
	@echo ""

## all: Default target — build everything
all: build

## build: Compile API and Worker binaries
build: build-api build-worker

## build-api: Compile API server binary
build-api:
	$(GO) build $(GOFLAGS) -o $(API_BIN) ./cmd/api

## build-worker: Compile Worker binary
build-worker:
	$(GO) build $(GOFLAGS) -o $(WORKER_BIN) ./cmd/worker

## test: Run all tests
test:
	$(GO) test ./... -v -count=1

## test-unit: Run unit tests (short mode, skip integration)
test-unit:
	$(GO) test ./... -v -short -count=1

## test-race: Run tests with race detector enabled
test-race:
	$(GO) test ./... -v -race -count=1

## lint: Static analysis
lint:
	$(GO) vet ./...

## clean: Remove build artifacts
clean:
	rm -rf bin/
	rm -f api.exe worker.exe

## docker-build: Build Docker images for API and Worker
docker-build:
	$(DOCKER) build \
		--platform $(PLATFORM) \
		-f Dockerfile.api \
		-t $(REGISTRY)/api:$(VERSION) \
		-t $(REGISTRY)/api:latest \
		.
	$(DOCKER) build \
		--platform $(PLATFORM) \
		-f Dockerfile.worker \
		-t $(REGISTRY)/worker:$(VERSION) \
		-t $(REGISTRY)/worker:latest \
		.

## docker-push: Push Docker images to registry
docker-push:
	$(DOCKER) push $(REGISTRY)/api:$(VERSION)
	$(DOCKER) push $(REGISTRY)/api:latest
	$(DOCKER) push $(REGISTRY)/worker:$(VERSION)
	$(DOCKER) push $(REGISTRY)/worker:latest

## ci: Full CI pipeline
ci: lint test build docker-build
