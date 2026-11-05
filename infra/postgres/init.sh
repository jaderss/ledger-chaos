#!/bin/sh
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
create role ledger login password '${LEDGER_DB_PASSWORD}';
create role relay login password '${RELAY_DB_PASSWORD}';
create role projector login password '${PROJECTOR_DB_PASSWORD}';

alter role ledger set statement_timeout = '${STATEMENT_TIMEOUT}';
alter role relay set statement_timeout = '${STATEMENT_TIMEOUT}';
alter role relay set idle_in_transaction_session_timeout = '${IDLE_IN_TX_TIMEOUT}';
alter role projector set statement_timeout = '${STATEMENT_TIMEOUT}';

create database ledger owner ledger;
create database projector owner projector;
grant connect on database ledger to relay;
SQL
