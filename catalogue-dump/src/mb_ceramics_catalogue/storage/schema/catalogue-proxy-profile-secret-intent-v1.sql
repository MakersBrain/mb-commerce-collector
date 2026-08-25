-- Coordinate an operator-owned profile secret file with durable profile state.
--
-- The intent records only identities, generations, and recovery state. Gateway
-- credentials and provider capabilities remain exclusively in the private
-- secret file.

create table if not exists catalogue.proxy_profile_secret_intents (
    operation_id uuid primary key,
    provider text not null,
    profile_id uuid not null,
    logical_name text not null,
    cycle_start timestamp with time zone not null,
    expected_generation integer,
    target_generation integer not null,
    created_profile boolean not null,
    state text default 'prepared' not null,
    error_code text,
    created_at timestamp with time zone default now() not null,
    updated_at timestamp with time zone default now() not null,
    installed_at timestamp with time zone,
    completed_at timestamp with time zone,
    constraint proxy_profile_secret_intents_operation_fkey
        foreign key (operation_id)
        references catalogue.proxy_mutation_requests (operation_id),
    constraint proxy_profile_secret_intents_profile_identity_fkey
        foreign key (provider, profile_id, logical_name)
        references catalogue.proxy_profiles (provider, id, logical_name),
    constraint proxy_profile_secret_intents_cycle_fkey
        foreign key (provider, cycle_start)
        references catalogue.proxy_budget_cycles (provider, cycle_start),
    constraint proxy_profile_secret_intents_generation_check check (
        (created_profile and expected_generation is null and target_generation = 1)
        or
        (not created_profile and expected_generation is not null
         and expected_generation >= 1
         and target_generation::bigint = expected_generation::bigint + 1)
    ),
    constraint proxy_profile_secret_intents_state_check check (
        state in ('prepared', 'installed', 'completed', 'failed')
    ),
    constraint proxy_profile_secret_intents_state_timestamps_check check (
        (state = 'prepared' and installed_at is null and completed_at is null)
        or (state = 'installed' and installed_at is not null and completed_at is null)
        or (state = 'completed' and installed_at is not null and completed_at is not null)
        or (state = 'failed' and completed_at is not null)
    ),
    constraint proxy_profile_secret_intents_timestamp_order_check check (
        updated_at >= created_at
        and (installed_at is null or installed_at >= created_at)
        and (completed_at is null or completed_at >= created_at)
    )
);

create unique index if not exists proxy_profile_secret_intents_active_profile_key
    on catalogue.proxy_profile_secret_intents (provider, profile_id)
    where state in ('prepared', 'installed');
