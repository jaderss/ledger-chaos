create table account (
    id              uuid primary key,
    currency        char(3) not null,
    allow_overdraft boolean not null default false,
    balance_minor   bigint  not null default 0,
    constraint balance_not_negative check (allow_overdraft or balance_minor >= 0)
);

create table transfer (
    id              uuid primary key,
    request_key     text        not null,
    correlation_id  text        not null,
    from_account    uuid        not null references account (id),
    to_account      uuid        not null references account (id),
    amount_minor    bigint      not null check (amount_minor > 0),
    currency        char(3)     not null,
    status          text        not null check (status in ('POSTED', 'REJECTED')),
    reject_reason   text,
    created_at      timestamptz not null default now(),
    check (from_account <> to_account)
);

create index transfer_request_key on transfer (request_key);

create table idempotency (
    key          text  primary key,
    request_hash bytea not null,
    transfer_id  uuid  not null references transfer (id) deferrable initially deferred
);

create table posting (
    id           bigint generated always as identity primary key,
    transfer_id  uuid   not null references transfer (id),
    account_id   uuid   not null references account (id),
    amount_minor bigint not null check (amount_minor <> 0)
);

create index posting_account on posting (account_id);
create index posting_transfer on posting (transfer_id);

create table outbox (
    id           uuid primary key,
    seq          bigint generated always as identity unique,
    transfer_id  uuid        not null references transfer (id) unique,
    payload      jsonb       not null,
    created_at   timestamptz not null default now(),
    published_at timestamptz
);

create index outbox_unpublished on outbox (seq) where published_at is null;
