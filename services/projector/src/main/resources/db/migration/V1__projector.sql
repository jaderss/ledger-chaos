create table processed_event (
    event_id     uuid primary key,
    processed_at timestamptz not null default now()
);

create table balance (
    account_id    uuid primary key,
    currency      char(3) not null,
    balance_minor bigint  not null
);
