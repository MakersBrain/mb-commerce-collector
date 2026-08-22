create table if not exists catalogue.proxy_attempt_authorizations (
    id uuid primary key default gen_random_uuid(),
    reservation_id uuid not null references catalogue.proxy_reservations(id) on delete cascade,
    estimated_bytes bigint not null,
    actual_bytes bigint,
    physical_requests integer,
    state text not null default 'authorized',
    created_at timestamptz not null default now(),
    resolved_at timestamptz,
    constraint proxy_attempt_authorizations_estimate_check check (estimated_bytes >= 0),
    constraint proxy_attempt_authorizations_actual_check check (actual_bytes is null or actual_bytes >= 0),
    constraint proxy_attempt_authorizations_requests_check check (
        physical_requests is null or physical_requests >= 0
    ),
    constraint proxy_attempt_authorizations_state_check check (
        state in ('authorized', 'reconciled', 'released')
    ),
    constraint proxy_attempt_authorizations_resolution_check check (
        (state = 'authorized' and actual_bytes is null and physical_requests is null and resolved_at is null)
        or (state = 'reconciled' and actual_bytes is not null and physical_requests is not null
            and resolved_at is not null)
        or (state = 'released' and actual_bytes is null and physical_requests is null
            and resolved_at is not null)
    )
);

create index if not exists proxy_attempt_authorizations_pending_idx
    on catalogue.proxy_attempt_authorizations (reservation_id)
    where state = 'authorized';
