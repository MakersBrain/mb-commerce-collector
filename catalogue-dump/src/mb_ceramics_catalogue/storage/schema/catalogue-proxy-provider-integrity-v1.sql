-- Bind every durable proxy identity to one provider/profile/route tuple.
--
-- The provider belongs to the profile and is copied onto routes under a
-- composite foreign key so it cannot drift. Composite foreign keys prevent
-- a writer that bypasses application checks
-- from charging one provider cycle while naming another provider's profile or
-- from pairing a reservation/probe with a route owned by another profile.

create unique index if not exists proxy_profiles_provider_id_key
    on catalogue.proxy_profiles (provider, id);

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_profiles'::regclass
           and conname = 'proxy_profiles_provider_id_key'
    ) then
        alter table catalogue.proxy_profiles
            add constraint proxy_profiles_provider_id_key
            unique using index proxy_profiles_provider_id_key;
    end if;
end
$$;

create unique index if not exists proxy_profiles_provider_id_logical_name_key
    on catalogue.proxy_profiles (provider, id, logical_name);

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_profiles'::regclass
           and conname = 'proxy_profiles_provider_id_logical_name_key'
    ) then
        alter table catalogue.proxy_profiles
            add constraint proxy_profiles_provider_id_logical_name_key
            unique using index proxy_profiles_provider_id_logical_name_key;
    end if;
end
$$;

alter table catalogue.proxy_routes
    add column if not exists provider text;

update catalogue.proxy_routes r
   set provider = p.provider
  from catalogue.proxy_profiles p
 where p.id = r.profile_id
   and r.provider is null;

alter table catalogue.proxy_routes
    alter column provider set not null;

create unique index if not exists proxy_routes_provider_profile_id_id_key
    on catalogue.proxy_routes (provider, profile_id, id);

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_routes'::regclass
           and conname = 'proxy_routes_provider_profile_id_id_key'
    ) then
        alter table catalogue.proxy_routes
            add constraint proxy_routes_provider_profile_id_id_key
            unique using index proxy_routes_provider_profile_id_id_key;
    end if;
end
$$;

create unique index if not exists proxy_routes_profile_id_id_key
    on catalogue.proxy_routes (profile_id, id);

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_routes'::regclass
           and conname = 'proxy_routes_profile_id_id_key'
    ) then
        alter table catalogue.proxy_routes
            add constraint proxy_routes_profile_id_id_key
            unique using index proxy_routes_profile_id_id_key;
    end if;
end
$$;

create unique index if not exists proxy_probes_id_profile_id_route_id_key
    on catalogue.proxy_probes (id, profile_id, route_id);

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_probes'::regclass
           and conname = 'proxy_probes_id_profile_id_route_id_key'
    ) then
        alter table catalogue.proxy_probes
            add constraint proxy_probes_id_profile_id_route_id_key
            unique using index proxy_probes_id_profile_id_route_id_key;
    end if;
end
$$;

create unique index if not exists proxy_reservations_id_provider_key
    on catalogue.proxy_reservations (id, provider);

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_reservations'::regclass
           and conname = 'proxy_reservations_id_provider_key'
    ) then
        alter table catalogue.proxy_reservations
            add constraint proxy_reservations_id_provider_key
            unique using index proxy_reservations_id_provider_key;
    end if;
end
$$;

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_routes'::regclass
           and conname = 'proxy_routes_provider_profile_id_fkey'
    ) then
        alter table catalogue.proxy_routes
            add constraint proxy_routes_provider_profile_id_fkey
            foreign key (provider, profile_id)
            references catalogue.proxy_profiles (provider, id) not valid;
    end if;
end
$$;

alter table catalogue.proxy_routes
    validate constraint proxy_routes_provider_profile_id_fkey;

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_profile_allocations'::regclass
           and conname = 'proxy_profile_allocations_provider_profile_id_fkey'
    ) then
        alter table catalogue.proxy_profile_allocations
            add constraint proxy_profile_allocations_provider_profile_id_fkey
            foreign key (provider, profile_id)
            references catalogue.proxy_profiles (provider, id) not valid;
    end if;
end
$$;

alter table catalogue.proxy_profile_allocations
    validate constraint proxy_profile_allocations_provider_profile_id_fkey;

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_reservations'::regclass
           and conname = 'proxy_reservations_provider_profile_name_fkey'
    ) then
        alter table catalogue.proxy_reservations
            add constraint proxy_reservations_provider_profile_name_fkey
            foreign key (provider, profile_id, profile)
            references catalogue.proxy_profiles (provider, id, logical_name) not valid;
    end if;
end
$$;

alter table catalogue.proxy_reservations
    validate constraint proxy_reservations_provider_profile_name_fkey;

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_reservations'::regclass
           and conname = 'proxy_reservations_provider_profile_route_fkey'
    ) then
        alter table catalogue.proxy_reservations
            add constraint proxy_reservations_provider_profile_route_fkey
            foreign key (provider, profile_id, route_id)
            references catalogue.proxy_routes (provider, profile_id, id) not valid;
    end if;
end
$$;

alter table catalogue.proxy_reservations
    validate constraint proxy_reservations_provider_profile_route_fkey;

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_reservations'::regclass
           and conname = 'proxy_reservations_durable_identity_check'
    ) then
        alter table catalogue.proxy_reservations
            add constraint proxy_reservations_durable_identity_check
            check (profile_id is not null and route_id is not null) not valid;
    end if;
end
$$;

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_probes'::regclass
           and conname = 'proxy_probes_profile_route_fkey'
    ) then
        alter table catalogue.proxy_probes
            add constraint proxy_probes_profile_route_fkey
            foreign key (profile_id, route_id)
            references catalogue.proxy_routes (profile_id, id) not valid;
    end if;
end
$$;

alter table catalogue.proxy_probes
    validate constraint proxy_probes_profile_route_fkey;

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_reservations'::regclass
           and conname = 'proxy_reservations_probe_identity_fkey'
    ) then
        alter table catalogue.proxy_reservations
            add constraint proxy_reservations_probe_identity_fkey
            foreign key (probe_id, profile_id, route_id)
            references catalogue.proxy_probes (id, profile_id, route_id)
            on delete cascade not valid;
    end if;
end
$$;

alter table catalogue.proxy_reservations
    validate constraint proxy_reservations_probe_identity_fkey;

do $$
begin
    if not exists (
        select 1 from pg_constraint
         where conrelid = 'catalogue.proxy_reconcile_requests'::regclass
           and conname = 'proxy_reconcile_requests_reservation_provider_fkey'
    ) then
        alter table catalogue.proxy_reconcile_requests
            add constraint proxy_reconcile_requests_reservation_provider_fkey
            foreign key (reservation_id, provider)
            references catalogue.proxy_reservations (id, provider) not valid;
    end if;
end
$$;

alter table catalogue.proxy_reconcile_requests
    validate constraint proxy_reconcile_requests_reservation_provider_fkey;
