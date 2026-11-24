.PHONY: build test run-mocks run-real demo ttft up down infra infra-down k6 clean fmt

build:
	mkdir -p bin
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/worker ./cmd/worker
	go build -o bin/grpc-client ./cmd/grpc-client

test:
	go test ./... -count=1

fmt:
	gofmt -w cmd gen internal

run-mocks: build
	bash scripts/run-mocks.sh

run-real: build
	bash scripts/run-real.sh

demo: build
	bash scripts/demo.sh

ttft: build
	bash scripts/ttft-proof.sh

up:
	docker compose up --build

down:
	docker compose down -v

infra:
	docker compose -f deploy/docker-compose.yaml up -d

infra-down:
	docker compose -f deploy/docker-compose.yaml down

k6:
	k6 run tests/k6/load.js

clean:
	rm -rf bin
