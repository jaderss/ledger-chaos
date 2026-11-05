include .env
export

COMPOSE := docker compose

.PHONY: help up ledger services chaos test-go down clean test test-java

help:
	@echo "make up      start Postgres, Kafka and Toxiproxy, create roles, databases and topics"
	@echo "make ledger  build and start the ledger behind Toxiproxy on port 8080"
	@echo "make services  ledger, relay and projector, all behind Toxiproxy"
	@echo "make chaos MODE=correct SCENARIO=kills   one chaos run and the checker, results in results/<run>"
	@echo "make test    Maven verify for the services"
	@echo "make down    stop everything"
	@echo "make clean   stop everything, drop volumes and results"

up:
	$(COMPOSE) up -d --wait postgres kafka toxiproxy
	$(COMPOSE) run --rm db-init
	$(COMPOSE) run --rm kafka-topics

ledger: up
	$(COMPOSE) up -d --build --wait ledger

services: up
	$(COMPOSE) up -d --build --wait ledger relay projector

chaos:
	scripts/chaos.sh $(or $(MODE),correct) $(or $(SCENARIO),contention)

test: test-java test-go

test-java:
	cd services && ./mvnw -B verify

GO_TEST := docker run --rm --network ledger-chaos_default -e TEST_POSTGRES_DSN=postgres://postgres:$(POSTGRES_PASSWORD)@postgres:5432/postgres \
	-e TEST_KAFKA_BROKERS=kafka:29092 -v ledger-chaos-gomod:/go/pkg/mod

test-go: up
	for m in chaos checker; do $(GO_TEST) -v "$$PWD/$$m":/src -w /src golang:$(GO_VERSION) go test -race -coverprofile=coverage.out ./... || exit 1; done

down:
	$(COMPOSE) down --remove-orphans

clean: down
	$(COMPOSE) down -v --remove-orphans
	find results -mindepth 1 -not -path 'results/sample*' -delete
