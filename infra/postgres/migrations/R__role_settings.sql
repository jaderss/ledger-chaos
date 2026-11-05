alter role ledger set statement_timeout = '${statementTimeout}';
alter role relay set statement_timeout = '${statementTimeout}';
alter role relay set idle_in_transaction_session_timeout = '${idleInTxTimeout}';
alter role projector set statement_timeout = '${statementTimeout}';
grant connect on database ledger to relay;
