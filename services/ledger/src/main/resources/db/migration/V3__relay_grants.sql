do $$
begin
    if exists (select from pg_roles where rolname = 'relay') then
        grant usage on schema public to relay;
        grant select, update (published_at) on outbox to relay;
    end if;
end
$$;
