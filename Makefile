IMAGE_REGISTRY ?= pulsegrid
IMAGE_TAG ?= latest

.PHONY: build test docker-build docker-build-api docker-build-worker docker-build-analytics-consumer docker-push docker-push-api docker-push-worker docker-push-analytics-consumer

build:
	go build ./...

test:
	go test ./...

docker-build: docker-build-api docker-build-worker docker-build-analytics-consumer

docker-build-api:
	docker build -f Dockerfile.api -t $(IMAGE_REGISTRY)/pulsegrid-api:$(IMAGE_TAG) .

docker-build-worker:
	docker build -f Dockerfile.worker -t $(IMAGE_REGISTRY)/pulsegrid-worker:$(IMAGE_TAG) .

docker-build-analytics-consumer:
	docker build -f Dockerfile.analytics-consumer -t $(IMAGE_REGISTRY)/pulsegrid-analytics-consumer:$(IMAGE_TAG) .

docker-push: docker-push-api docker-push-worker docker-push-analytics-consumer

docker-push-api:
	docker push $(IMAGE_REGISTRY)/pulsegrid-api:$(IMAGE_TAG)

docker-push-worker:
	docker push $(IMAGE_REGISTRY)/pulsegrid-worker:$(IMAGE_TAG)

docker-push-analytics-consumer:
	docker push $(IMAGE_REGISTRY)/pulsegrid-analytics-consumer:$(IMAGE_TAG)
