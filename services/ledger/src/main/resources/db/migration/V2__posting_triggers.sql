create function check_transfer_balanced() returns trigger
language plpgsql as $$
begin
    if (select sum(amount_minor) from posting where transfer_id = new.transfer_id) <> 0 then
        raise exception 'postings of transfer % do not sum to zero', new.transfer_id
            using errcode = 'check_violation';
    end if;
    return null;
end
$$;

create constraint trigger posting_sum_zero
    after insert on posting
    deferrable initially deferred
    for each row execute function check_transfer_balanced();

create function reject_change() returns trigger
language plpgsql as $$
begin
    raise exception '% on % is not allowed, postings are append only', tg_op, tg_table_name
        using errcode = 'restrict_violation';
end
$$;

create trigger posting_append_only
    before update or delete or truncate on posting
    for each statement execute function reject_change();
