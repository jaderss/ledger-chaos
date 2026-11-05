include .env
export

COMPOSE := docker compose

.PHONY: help up ledger services chaos chaos-all sample report report-dev down clean test test-java

help:
	@echo "make up      start Postgres, Kafka and Toxiproxy, create roles, databases and topics"
	@echo "make ledger  build and start the ledger behind Toxiproxy on port 8080"
	@echo "make services  ledger, relay and projector, all behind Toxiproxy"
	@echo "make chaos MODE=correct SCENARIO=kills   one chaos run and the checker, results in results/<run>"
	@echo "make chaos-all   every mode and the locking comparison, results in results/<prefix>-<run>"
	@echo "make sample RUNS='a b'   copy those runs' report.json into results/sample for the report page"
	@echo "make report  build and serve the report page from results/sample on http://localhost:$(REPORT_PORT)"
	@echo "make report-dev   report page with live reload over every run in results"
	@echo "make test    Maven verify for the services, the runner and the checker"
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

chaos-all:
	scripts/experiments.sh

sample:
	for r in $(RUNS); do mkdir -p results/sample/$$r && cp results/$$r/report.json results/sample/$$r/; done

report:
	$(COMPOSE) --profile tools up -d --build report

report-dev:
	cd report && npm install --no-audit --no-fund && RESULTS_DIR=../results npm run dev

test: test-java

test-java:
	cd services && ./mvnw -B verify

down:
	$(COMPOSE) down --remove-orphans

clean: down
	$(COMPOSE) down -v --remove-orphans
	find results -mindepth 1 -not -path 'results/sample*' -delete
