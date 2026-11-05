create table if not exists outbox (
    id           uuid primary key,
    seq          bigint generated always as identity unique,
    transfer_id  uuid        not null unique,
    payload      jsonb       not null,
    created_at   timestamptz not null default now(),
    published_at timestamptz
);
