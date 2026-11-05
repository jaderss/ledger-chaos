include .env
export

COMPOSE := docker compose

.PHONY: help up ledger services down clean test test-java

help:
	@echo "make up      start Postgres, Kafka and Toxiproxy, create roles, databases and topics"
	@echo "make ledger  build and start the ledger behind Toxiproxy on port 8080"
	@echo "make services  ledger, relay and projector, all behind Toxiproxy"
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

test: test-java

test-java:
	cd services && ./mvnw -B verify

down:
	$(COMPOSE) down --remove-orphans

clean: down
	$(COMPOSE) down -v --remove-orphans
	find results -mindepth 1 -not -path 'results/sample*' -delete
