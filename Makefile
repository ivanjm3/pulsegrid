IMAGE_REGISTRY ?= pulsegrid
IMAGE_TAG ?= latest

.PHONY: build test docker-build docker-build-api docker-build-worker docker-push docker-push-api docker-push-worker

build:
	go build ./...

test:
	go test ./...

docker-build: docker-build-api docker-build-worker

docker-build-api:
	docker build -f Dockerfile.api -t $(IMAGE_REGISTRY)/pulsegrid-api:$(IMAGE_TAG) .

docker-build-worker:
	docker build -f Dockerfile.worker -t $(IMAGE_REGISTRY)/pulsegrid-worker:$(IMAGE_TAG) .

docker-push: docker-push-api docker-push-worker

docker-push-api:
	docker push $(IMAGE_REGISTRY)/pulsegrid-api:$(IMAGE_TAG)

docker-push-worker:
	docker push $(IMAGE_REGISTRY)/pulsegrid-worker:$(IMAGE_TAG)
