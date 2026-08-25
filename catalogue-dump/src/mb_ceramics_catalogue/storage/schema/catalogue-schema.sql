--
-- The catalogue schema, in full.
--
-- Squashed on 2026-08-21 from the eleven incremental files that preceded it
-- (catalogue-reference-schema v1-v4, catalogue-ops-schema v1-v6, and
-- catalogue-canonical-promotion), taken as a pg_dump --schema-only of a
-- database that had applied all of them. Regenerate the same way rather than
-- editing by hand and hoping a deployed database agrees.
--
--
-- PostgreSQL database dump
--


-- Dumped from database version 17.10
-- Dumped by pg_dump version 17.10

SET check_function_bodies = false;

--
-- Name: catalogue; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA IF NOT EXISTS catalogue;

-- The loader functions run with search_path = pg_catalog, catalogue, so
-- pgcrypto's digest() only resolves when the extension lives in catalogue.
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA catalogue;


--
-- Name: SCHEMA catalogue; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA catalogue IS 'Reusable public ceramics catalogue imported from audited NDJSON sources.';


--
-- Name: clean_product_name(text); Type: FUNCTION; Schema: catalogue; Owner: -
--

CREATE FUNCTION catalogue.clean_product_name(p_name text) RETURNS text
    LANGUAGE sql IMMUTABLE
    SET search_path TO 'pg_catalog'
    AS $_$
  with stripped as (
    select coalesce(p_name, '') as n
  ), packed as (
    -- "Formato: 473 ml", "Poids: 1kg", "Size /Unit of Measure: SD-217 10lbs Dry"
    select regexp_replace(
      n,
      '\s*[–—|-]?\s*(Size\s*/\s*Unit of Measure|Unit of Measure|Size|Formato|Formaat|Format|Poids|Peso|T[uū]ris|Gr[oö]sse|Gr[oö]ße|Taille|Inhalt|Contenu|Content|Volume)\s*:\s*.*$',
      '', 'i') as n from stripped
  ), unlabelled as (
    -- The same clause written without a label: "– POT DE 500ML", "- SEAU DE 5KG".
    select regexp_replace(
      n,
      -- \y, not \b: in a POSIX regular expression \b is a backspace, and Postgres
      -- spells the word boundary \y.
      '\s*[–—|-]\s*(POT|SEAU|SACHET|SAC|BOITE|BO[iî]TE|FLACON|JAR|BUCKET|BAG)\y.*$',
      '', 'i') as n from packed
  ), fired as (
    -- "– 1200/1280°C", which firing_range already holds. Stripped after the pack
    -- clause, because these titles print the firing range before it.
    select regexp_replace(
      n,
      '\s*[–—-]\s*[0-9]{3,4}\s*[/-]\s*[0-9]{3,4}\s*°?\s*C\s*$',
      '', 'i') as n from unlabelled
  )
  select nullif(btrim(regexp_replace(n, '\s+', ' ', 'g'), ' –—-,/'), '') from fired;
$_$;


--
-- Name: load_record(jsonb, uuid, uuid); Type: FUNCTION; Schema: catalogue; Owner: -
--

CREATE FUNCTION catalogue.load_record(p_record jsonb, p_import_run_id uuid DEFAULT NULL::uuid, p_document_id uuid DEFAULT NULL::uuid) RETURNS uuid
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'catalogue'
    AS $$
declare
  v_source_product_id uuid;
  v_raw_record_id bigint;
  v_latest_offer_id bigint;
  v_latest_observed_at timestamptz;
  v_latest_context_hash bytea;
  v_existing_offer_id bigint;
  v_source text := p_record->>'source';
  v_external_id text := p_record->>'external_id';
  v_format text := p_record->>'format';
  v_is_v2 boolean := v_format like '%.v2';
  v_package jsonb := case when jsonb_typeof(p_record->'package_size') = 'object'
                          then p_record->'package_size' end;
  v_firing jsonb := case when jsonb_typeof(p_record->'firing') = 'object'
                         then p_record->'firing' end;
  v_unit_price jsonb := case when jsonb_typeof(p_record->'unit_price') = 'object'
                             then p_record->'unit_price' end;
  v_fetched_at timestamptz;
  v_price numeric;
  v_currency text;
  v_quantity numeric;
  v_unit text;
  v_vat_status text;
  v_sku text;
  v_firing_range text;
  v_record_hash bytea;
  v_context_hash bytea;
  v_price_refresh boolean := coalesce(p_record->>'collection_mode', 'full') = 'price';
begin
  if jsonb_typeof(p_record) <> 'object' then
    raise exception 'catalogue_record_must_be_an_object' using errcode = '22023';
  end if;
  if nullif(btrim(v_source), '') is null
     or nullif(btrim(v_external_id), '') is null
     or nullif(btrim(p_record->>'name'), '') is null
     or nullif(btrim(p_record->>'product_url'), '') is null then
    raise exception 'catalogue_record_missing_required_identity' using errcode = '22023';
  end if;
  if v_format not in (
    'ceramics.catalogue_item.v1', 'ceramics.catalogue_identity.v1',
    'ceramics.catalogue_item.v2', 'ceramics.catalogue_identity.v2'
  ) then
    raise exception 'unsupported_catalogue_record_format: %', v_format using errcode = '22023';
  end if;

  v_fetched_at := (p_record->>'fetched_at')::timestamptz;
  v_price := nullif(p_record->>'price', '')::numeric;
  v_currency := upper(nullif(btrim(p_record->>'currency'), ''));
  v_vat_status := lower(nullif(btrim(p_record->>'vat_status'), ''));
  if v_vat_status is not null and v_vat_status not in ('inclusive', 'exclusive', 'unknown') then
    v_vat_status := 'unknown';
  end if;

  if v_is_v2 then
    v_quantity := nullif(v_package->>'value', '')::numeric;
    v_unit := lower(nullif(btrim(v_package->>'unit'), ''));
    v_sku := coalesce(
      nullif(btrim(p_record->>'manufacturer_sku'), ''),
      nullif(btrim(p_record->>'supplier_reference'), '')
    );
    v_firing_range := nullif(btrim(coalesce(
      v_firing->>'evidence',
      case when v_firing->>'min_celsius' is not null
           then (v_firing->>'min_celsius') || '-' || (v_firing->>'max_celsius') || ' C' end
    )), '');
  else
    v_quantity := nullif(p_record->>'quantity', '')::numeric;
    v_unit := lower(nullif(btrim(p_record->>'unit'), ''));
    v_sku := nullif(btrim(p_record->>'sku'), '');
    v_firing_range := nullif(btrim(p_record->>'firing_range'), '');
  end if;

  insert into catalogue.sources (id) values (v_source)
  on conflict (id) do nothing;

  insert into catalogue.source_products (
    source_id, external_id, parent_external_id, record_format, name, brand,
    sku, manufacturer_sku, supplier_reference, family, description,
    firing_range, product_url, image_url, availability,
    first_seen_at, last_seen_at, attributes
  ) values (
    v_source, v_external_id, nullif(btrim(p_record->>'parent_external_id'), ''),
    v_format, p_record->>'name', nullif(btrim(p_record->>'brand'), ''), v_sku,
    nullif(btrim(p_record->>'manufacturer_sku'), ''),
    nullif(btrim(p_record->>'supplier_reference'), ''),
    nullif(btrim(p_record->>'family'), ''),
    nullif(btrim(p_record->>'description'), ''), v_firing_range,
    p_record->>'product_url', nullif(btrim(p_record->>'image_url'), ''),
    nullif(btrim(p_record->>'availability'), ''), v_fetched_at, v_fetched_at,
    p_record - array[
      'format','source','external_id','parent_external_id','name','brand','sku',
      'manufacturer_sku','supplier_reference','family','description',
      'firing_range','product_url','image_url','availability','price','currency',
      'price_text','vat_status','quantity','unit','fetched_at','raw'
    ]::text[]
  )
  on conflict (source_id, external_id) do update set
    record_format = case when excluded.last_seen_at >= catalogue.source_products.last_seen_at
                         then excluded.record_format else catalogue.source_products.record_format end,
    parent_external_id = case when excluded.last_seen_at >= catalogue.source_products.last_seen_at
                              then excluded.parent_external_id else catalogue.source_products.parent_external_id end,
    name = case when excluded.last_seen_at >= catalogue.source_products.last_seen_at
                then excluded.name else catalogue.source_products.name end,
    brand = case when excluded.last_seen_at >= catalogue.source_products.last_seen_at
                 then excluded.brand else catalogue.source_products.brand end,
    sku = case when excluded.last_seen_at >= catalogue.source_products.last_seen_at
               then excluded.sku else catalogue.source_products.sku end,
    manufacturer_sku = case when excluded.last_seen_at >= catalogue.source_products.last_seen_at
                            then excluded.manufacturer_sku else catalogue.source_products.manufacturer_sku end,
    supplier_reference = case when excluded.last_seen_at >= catalogue.source_products.last_seen_at
                              then excluded.supplier_reference else catalogue.source_products.supplier_reference end,
    family = case when v_price_refresh or excluded.last_seen_at < catalogue.source_products.last_seen_at
                  then catalogue.source_products.family else excluded.family end,
    description = case when v_price_refresh or excluded.last_seen_at < catalogue.source_products.last_seen_at
                       then catalogue.source_products.description else excluded.description end,
    firing_range = case when v_price_refresh or excluded.last_seen_at < catalogue.source_products.last_seen_at
                        then catalogue.source_products.firing_range else excluded.firing_range end,
    product_url = case when excluded.last_seen_at >= catalogue.source_products.last_seen_at
                       then excluded.product_url else catalogue.source_products.product_url end,
    image_url = case when v_price_refresh or excluded.last_seen_at < catalogue.source_products.last_seen_at
                     then catalogue.source_products.image_url else excluded.image_url end,
    availability = case when excluded.last_seen_at >= catalogue.source_products.last_seen_at
                        then excluded.availability else catalogue.source_products.availability end,
    first_seen_at = least(catalogue.source_products.first_seen_at, excluded.first_seen_at),
    last_seen_at = greatest(catalogue.source_products.last_seen_at, excluded.last_seen_at),
    active = case when excluded.last_seen_at >= catalogue.source_products.last_seen_at
                  then true else catalogue.source_products.active end,
    attributes = case when v_price_refresh or excluded.last_seen_at < catalogue.source_products.last_seen_at
                      then catalogue.source_products.attributes else excluded.attributes end
  returning id into v_source_product_id;

  -- Serialise even the first observation, for which there is no row to lock.
  perform pg_advisory_xact_lock(hashtextextended(v_source_product_id::text, 0));

  -- Collection time is an interval boundary, not semantic record content.
  v_record_hash := digest(convert_to((p_record - 'fetched_at')::text, 'UTF8'), 'sha256');
  insert into catalogue.raw_records (
    source_product_id, document_id, import_run_id, fetched_at,
    first_seen_at, last_seen_at, record_sha256, record
  ) values (
    v_source_product_id, p_document_id, p_import_run_id, v_fetched_at,
    v_fetched_at, v_fetched_at, v_record_hash, p_record
  )
  on conflict (source_product_id, record_sha256) do update set
    document_id = coalesce(catalogue.raw_records.document_id, excluded.document_id),
    import_run_id = coalesce(catalogue.raw_records.import_run_id, excluded.import_run_id),
    first_seen_at = least(catalogue.raw_records.first_seen_at, excluded.first_seen_at),
    last_seen_at = greatest(catalogue.raw_records.last_seen_at, excluded.last_seen_at)
  returning id into v_raw_record_id;

  if v_price is not null then
    if v_currency is null then
      raise exception 'priced_catalogue_record_requires_currency' using errcode = '22023';
    end if;
    if (v_quantity is null) <> (v_unit is null) then
      raise exception 'catalogue_quantity_and_unit_must_be_paired' using errcode = '22023';
    end if;
    v_context_hash := digest(convert_to(jsonb_build_object(
      'price', v_price, 'currency', v_currency,
      'vat_status', v_vat_status, 'quantity', v_quantity,
      'unit', v_unit, 'availability', p_record->>'availability'
    )::text, 'UTF8'), 'sha256');

    select id into v_existing_offer_id
      from catalogue.offer_observations
     where source_product_id = v_source_product_id
       and observed_at = v_fetched_at
       and context_sha256 = v_context_hash;

    if v_existing_offer_id is null then
      select id, observed_at, context_sha256
        into v_latest_offer_id, v_latest_observed_at, v_latest_context_hash
        from catalogue.offer_observations
       where source_product_id = v_source_product_id
       order by observed_at desc, id desc
       limit 1;

      if v_latest_offer_id is null then
        insert into catalogue.offer_observations (
          source_product_id, raw_record_id, observed_at, last_seen_at,
          price, currency, price_text, vat_status, quantity, unit,
          unit_price, unit_price_per, availability, context_sha256, attributes
        ) values (
          v_source_product_id, v_raw_record_id, v_fetched_at, v_fetched_at,
          v_price, v_currency, nullif(btrim(p_record->>'price_text'), ''),
          v_vat_status, v_quantity, v_unit,
          nullif(v_unit_price->>'value', '')::numeric,
          lower(nullif(btrim(v_unit_price->>'per'), '')),
          nullif(btrim(p_record->>'availability'), ''),
          v_context_hash, coalesce(p_record->'raw', '{}'::jsonb)
        );
      elsif v_fetched_at < v_latest_observed_at then
        insert into catalogue.out_of_order_observations (
          source_product_id, raw_record_id, observed_at, context_sha256, record
        ) values (
          v_source_product_id, v_raw_record_id, v_fetched_at, v_context_hash, p_record
        ) on conflict (source_product_id, observed_at, context_sha256) do nothing;
      elsif v_latest_context_hash = v_context_hash then
        update catalogue.offer_observations
           set last_seen_at = greatest(last_seen_at, v_fetched_at)
         where id = v_latest_offer_id;
      else
        insert into catalogue.offer_observations (
          source_product_id, raw_record_id, observed_at, last_seen_at,
          price, currency, price_text, vat_status, quantity, unit,
          unit_price, unit_price_per, availability, context_sha256, attributes
        ) values (
          v_source_product_id, v_raw_record_id, v_fetched_at, v_fetched_at,
          v_price, v_currency, nullif(btrim(p_record->>'price_text'), ''),
          v_vat_status, v_quantity, v_unit,
          nullif(v_unit_price->>'value', '')::numeric,
          lower(nullif(btrim(v_unit_price->>'per'), '')),
          nullif(btrim(p_record->>'availability'), ''),
          v_context_hash, coalesce(p_record->'raw', '{}'::jsonb)
        );
      end if;
    end if;
  end if;

  return v_source_product_id;
end
$$;


--
-- Name: notify_event_log(); Type: FUNCTION; Schema: catalogue; Owner: -
--

CREATE FUNCTION catalogue.notify_event_log() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  perform pg_notify('catalogue_ops', new.id::text);
  return null;
end;
$$;


--
-- Name: notify_job_progress(); Type: FUNCTION; Schema: catalogue; Owner: -
--

CREATE FUNCTION catalogue.notify_job_progress() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  perform pg_notify('catalogue_progress', new.job_id::text);
  return null;
end;
$$;


--
-- Name: promote_canonical_products(text); Type: FUNCTION; Schema: catalogue; Owner: -
--

CREATE FUNCTION catalogue.promote_canonical_products(p_manufacturer text DEFAULT NULL::text) RETURNS TABLE(manufacturers integer, canonical_created integer, canonical_updated integer, source_products_linked integer)
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'catalogue'
    AS $$
declare
  v_created int := 0;
  v_updated int := 0;
  v_linked int := 0;
  v_makers int := 0;
begin
  if p_manufacturer is not null
     and not exists (select 1 from catalogue.manufacturers where id = p_manufacturer) then
    raise exception 'unknown_manufacturer: %', p_manufacturer using errcode = '22023';
  end if;

  -- Adopt any hand-curated row that predates this file, so the upsert below has
  -- one arbiter index rather than two. Without this a curated ('Mayco','SC74')
  -- row with no manufacturer_id would not match the (manufacturer_id, sku_key)
  -- conflict target, and the insert would fail against the older brand+sku index
  -- instead of updating.
  update catalogue.canonical_products c
     set manufacturer_id = m.id,
         sku_key = catalogue.sku_key(c.manufacturer_sku),
         updated_at = now()
    from catalogue.manufacturer_aliases a
    join catalogue.manufacturers m on m.id = a.manufacturer_id
   where c.manufacturer_id is null
     and c.brand is not null
     and c.manufacturer_sku is not null
     and a.alias = lower(btrim(c.brand))
     and (p_manufacturer is null or m.id = p_manufacturer);

  with candidate as (
    select
      m.id   as manufacturer_id,
      m.name as manufacturer_name,
      upper(btrim(sp.manufacturer_sku)) as sku,
      catalogue.sku_key(sp.manufacturer_sku) as sku_key,
      sp.id, sp.source_id, sp.name, sp.description, sp.firing_range,
      sp.family, sp.image_url, sp.last_seen_at,
      coalesce(catalogue.clean_product_name(sp.name), sp.name) as clean_name,
      -- The maker's own listing beats every retailer's copy of it. Retailer
      -- titles are written for a search box: "POTTER'S CHOICE | PC-38 IRON
      -- YELLOW | AMACO 472 ml / Amarelo / Alta Temperatura" is one product name
      -- in this dump.
      (m.source_id is not null and sp.source_id = m.source_id) as from_maker,
      (sp.description is not null)::int
        + (sp.firing_range is not null)::int
        + (sp.family is not null)::int
        + (sp.image_url is not null)::int as completeness
    from catalogue.source_products sp
    join catalogue.manufacturer_aliases a on a.alias = lower(btrim(sp.brand))
    join catalogue.manufacturers m on m.id = a.manufacturer_id and m.active
    where sp.active
      and sp.brand is not null
      and catalogue.sku_key(sp.manufacturer_sku) is not null
      and (p_manufacturer is null or m.id = p_manufacturer)
  ),
  ranked as (
    select c.*,
           row_number() over (partition by manufacturer_id, sku_key
                              order by from_maker desc, completeness desc,
                                       last_seen_at desc, id) as rnk,
           -- The name is ranked by length and NOT by provenance, which is the
           -- opposite of every other field here. A maker's own storefront writes
           -- its titles for its own variant selector - Mayco publishes "Hot
           -- Tamale Size /Unit of Measure: Pint" - so preferring the maker picks
           -- the worst title on offer. Retailers name the product because that
           -- is what a customer searches for, and the shortest of their titles
           -- is reliably the bare product name.
           --
           -- The one thing shortest-wins gets wrong is a shop whose title is
           -- just the code, so a title that reduces to the SKU is ranked last.
           row_number() over (partition by manufacturer_id, sku_key
                              order by (catalogue.sku_key(clean_name) = sku_key),
                                       length(clean_name), from_maker desc,
                                       last_seen_at desc, id) as rnk_name
      from candidate c
  ),
  -- Field by field, the best non-null value rather than one row wholesale: the
  -- maker publishes the firing range and no price, a retailer publishes an image
  -- and no range, and a canonical identity wants both.
  merged as (
    select
      manufacturer_id,
      sku_key,
      (array_agg(manufacturer_name order by rnk))[1] as manufacturer_name,
      (array_agg(sku order by rnk))[1] as sku,
      (array_agg(clean_name order by rnk_name))[1] as name,
      (array_agg(description order by rnk) filter (where description is not null))[1] as description,
      (array_agg(firing_range order by rnk) filter (where firing_range is not null))[1] as firing_range,
      (array_agg(image_url order by rnk) filter (where image_url is not null))[1] as image_url,
      -- Family comes from a classifier per source, so the majority reading is
      -- steadier than any single shop's.
      mode() within group (order by family) as family,
      count(distinct source_id) as source_count,
      array_agg(distinct source_id) as sources,
      bool_or(from_maker) as has_maker_listing,
      max(last_seen_at) as last_seen_at
    from ranked
    group by manufacturer_id, sku_key
  ),
  upserted as (
    insert into catalogue.canonical_products (
      brand, manufacturer_sku, manufacturer_id, sku_key,
      name, family, description, firing_range, attributes, origin, updated_at
    )
    select
      m.manufacturer_name, m.sku, m.manufacturer_id, m.sku_key,
      m.name, m.family, m.description, m.firing_range,
      jsonb_strip_nulls(jsonb_build_object(
        'image_url', m.image_url,
        'promoted_from', jsonb_build_object(
          'sources', to_jsonb(m.sources),
          'source_count', m.source_count,
          'has_maker_listing', m.has_maker_listing,
          'last_seen_at', m.last_seen_at
        )
      )),
      'promoted', now()
    from merged m
    on conflict (manufacturer_id, sku_key)
      where manufacturer_id is not null and sku_key is not null
    do update set
      brand           = excluded.brand,
      manufacturer_sku = excluded.manufacturer_sku,
      name            = excluded.name,
      family          = coalesce(excluded.family, catalogue.canonical_products.family),
      description     = coalesce(excluded.description, catalogue.canonical_products.description),
      firing_range    = coalesce(excluded.firing_range, catalogue.canonical_products.firing_range),
      -- Merged, so a curator's own keys on a promoted row survive the next run.
      attributes      = catalogue.canonical_products.attributes || excluded.attributes,
      updated_at      = now()
    -- A row a person wrote is left exactly as they wrote it.
    where catalogue.canonical_products.origin = 'promoted'
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted)
    into v_created, v_updated
    from upserted;

  -- Link every supplier row that resolves to a promoted identity, including the
  -- ones whose own fields lost the merge - the link is what makes them one
  -- product's competing offers rather than fifteen unrelated listings.
  with linked as (
    update catalogue.source_products sp
       set canonical_product_id = c.id
      from catalogue.manufacturer_aliases a
      join catalogue.manufacturers m on m.id = a.manufacturer_id and m.active
      join catalogue.canonical_products c on c.manufacturer_id = m.id
     where a.alias = lower(btrim(sp.brand))
       and c.sku_key = catalogue.sku_key(sp.manufacturer_sku)
       and sp.brand is not null
       and sp.canonical_product_id is distinct from c.id
       and (p_manufacturer is null or m.id = p_manufacturer)
    returning 1
  )
  select count(*) into v_linked from linked;

  select count(distinct manufacturer_id) into v_makers
    from catalogue.canonical_products
   where origin = 'promoted'
     and (p_manufacturer is null or manufacturer_id = p_manufacturer);

  update catalogue.catalogue_generation
     set generation = generation + 1, promoted_at = now()
   where singleton;

  return query select v_makers, v_created, v_updated, v_linked;
end;
$$;


--
-- Name: proxy_audit_immutable(); Type: FUNCTION; Schema: catalogue; Owner: -
--

CREATE FUNCTION catalogue.proxy_audit_immutable() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'catalogue'
    AS $$
begin
  if (pg_has_role(current_user, 'catalogue_proxy_maintenance', 'member')
      and current_setting('catalogue.proxy_audit_maintenance', true) = 'on') is not true then
    raise exception 'proxy audit rows are immutable';
  end if;
  return old;
end;
$$;


--
-- Name: reconcile_host_slots(text); Type: FUNCTION; Schema: catalogue; Owner: -
--

CREATE FUNCTION catalogue.reconcile_host_slots(target_host text) RETURNS integer
    LANGUAGE plpgsql
    AS $$
declare
  limit_now smallint;
  created   integer := 0;
begin
  select max_concurrency into limit_now
    from catalogue.hosts where host = target_host for update;
  if limit_now is null then
    insert into catalogue.hosts (host) values (target_host)
      on conflict (host) do nothing;
    select max_concurrency into limit_now
      from catalogue.hosts where host = target_host for update;
  end if;

  insert into catalogue.host_leases (host, slot)
  select target_host, generate_series(1, limit_now)
  on conflict (host, slot) do nothing;
  get diagnostics created = row_count;

  -- Above the limit and idle: safe to drop. Above the limit and occupied: left
  -- alone, because taking a slot away from a running job does not stop the
  -- requests it is already making.
  delete from catalogue.host_leases
   where host = target_host
     and slot > limit_now
     and job_id is null;

  return created;
end;
$$;


--
-- Name: search_products(text, integer); Type: FUNCTION; Schema: catalogue; Owner: -
--

CREATE FUNCTION catalogue.search_products(p_query text, p_limit integer DEFAULT 50) RETURNS TABLE(source_product_id uuid, canonical_product_id uuid, source_id text, external_id text, name text, brand text, sku text, family text, firing_range text, product_url text, image_url text, price numeric, currency text, quantity numeric, unit text, observed_at timestamp with time zone, rank real)
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'catalogue'
    AS $$
  with query as (
    select websearch_to_tsquery('simple', p_query) as value
  )
  select p.id, p.canonical_product_id, p.source_id, p.external_id,
         p.name, p.brand, p.sku, p.family, p.firing_range,
         p.product_url, p.image_url,
         offer.price, offer.currency, offer.quantity, offer.unit,
         offer.observed_at,
         ts_rank_cd(
           to_tsvector(
             'simple',
             coalesce(p.name, '') || ' ' || coalesce(p.brand, '') || ' ' ||
             coalesce(p.sku, '') || ' ' || coalesce(p.family, '') || ' ' ||
             coalesce(p.description, '')
           ), query.value
         ) as rank
    from catalogue.source_products p
    cross join query
    left join catalogue.latest_offers offer on offer.source_product_id = p.id
   where p.active
     and (
       btrim(p_query) = ''
       or to_tsvector(
            'simple',
            coalesce(p.name, '') || ' ' || coalesce(p.brand, '') || ' ' ||
            coalesce(p.sku, '') || ' ' || coalesce(p.family, '') || ' ' ||
            coalesce(p.description, '')
          ) @@ query.value
       or lower(p.sku) = lower(p_query)
     )
   order by rank desc, p.name, p.source_id
   limit least(greatest(p_limit, 1), 200)
$$;


--
-- Name: sku_key(text); Type: FUNCTION; Schema: catalogue; Owner: -
--

CREATE FUNCTION catalogue.sku_key(p_sku text) RETURNS text
    LANGUAGE sql IMMUTABLE
    SET search_path TO 'pg_catalog'
    AS $$
  select nullif(upper(regexp_replace(coalesce(p_sku, ''), '[^A-Za-z0-9]', '', 'g')), '');
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: canonical_products; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.canonical_products (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    brand text,
    manufacturer_sku text,
    name text NOT NULL,
    family text,
    description text,
    firing_range text,
    attributes jsonb DEFAULT '{}'::jsonb NOT NULL,
    active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    manufacturer_id text,
    sku_key text,
    origin text DEFAULT 'curated'::text NOT NULL,
    CONSTRAINT canonical_products_attributes_check CHECK ((jsonb_typeof(attributes) = 'object'::text)),
    CONSTRAINT canonical_products_brand_check CHECK (((brand IS NULL) OR (btrim(brand) <> ''::text))),
    CONSTRAINT canonical_products_manufacturer_sku_check CHECK (((manufacturer_sku IS NULL) OR (btrim(manufacturer_sku) <> ''::text))),
    CONSTRAINT canonical_products_name_check CHECK ((btrim(name) <> ''::text)),
    CONSTRAINT canonical_products_origin_check CHECK ((origin = ANY (ARRAY['curated'::text, 'promoted'::text])))
);


--
-- Name: TABLE canonical_products; Type: COMMENT; Schema: catalogue; Owner: -
--

COMMENT ON TABLE catalogue.canonical_products IS 'Curated identities used to merge known-equivalent source products.';


--
-- Name: offer_observations; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.offer_observations (
    id bigint NOT NULL,
    source_product_id uuid NOT NULL,
    raw_record_id bigint,
    observed_at timestamp with time zone NOT NULL,
    price numeric(18,6) NOT NULL,
    currency text NOT NULL,
    price_text text,
    vat_status text,
    quantity numeric(18,6),
    unit text,
    availability text,
    context_sha256 bytea NOT NULL,
    attributes jsonb DEFAULT '{}'::jsonb NOT NULL,
    unit_price numeric(18,6),
    unit_price_per text,
    last_seen_at timestamp with time zone NOT NULL,
    CONSTRAINT offer_observations_attributes_check CHECK ((jsonb_typeof(attributes) = 'object'::text)),
    CONSTRAINT offer_observations_check CHECK (((quantity IS NULL) = (unit IS NULL))),
    CONSTRAINT offer_observations_context_sha256_check CHECK ((octet_length(context_sha256) = 32)),
    CONSTRAINT offer_observations_currency_check CHECK ((currency ~ '^[A-Z]{3}$'::text)),
    CONSTRAINT offer_observations_price_check CHECK ((price >= (0)::numeric)),
    CONSTRAINT offer_observations_quantity_check CHECK (((quantity IS NULL) OR (quantity > (0)::numeric))),
    CONSTRAINT offer_observations_seen_interval_check CHECK ((last_seen_at >= observed_at)),
    CONSTRAINT offer_observations_unit_check CHECK (((unit IS NULL) OR (btrim(unit) <> ''::text))),
    CONSTRAINT offer_observations_unit_price_per_check CHECK (((unit_price_per IS NULL) OR (unit_price_per = ANY (ARRAY['l'::text, 'kg'::text])))),
    CONSTRAINT offer_observations_vat_status_check CHECK (((vat_status IS NULL) OR (vat_status = ANY (ARRAY['inclusive'::text, 'exclusive'::text, 'unknown'::text]))))
);


--
-- Name: TABLE offer_observations; Type: COMMENT; Schema: catalogue; Owner: -
--

COMMENT ON TABLE catalogue.offer_observations IS 'Append-only package and price observations; absent for identity-only guides.';


--
-- Name: source_products; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.source_products (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    source_id text NOT NULL,
    external_id text NOT NULL,
    canonical_product_id uuid,
    record_format text NOT NULL,
    name text NOT NULL,
    brand text,
    sku text,
    family text,
    description text,
    firing_range text,
    product_url text NOT NULL,
    image_url text,
    availability text,
    first_seen_at timestamp with time zone NOT NULL,
    last_seen_at timestamp with time zone NOT NULL,
    active boolean DEFAULT true NOT NULL,
    attributes jsonb DEFAULT '{}'::jsonb NOT NULL,
    parent_external_id text,
    manufacturer_sku text,
    supplier_reference text,
    facet_colour text GENERATED ALWAYS AS (((attributes -> 'colour'::text) ->> 'name'::text)) STORED,
    facet_surface text GENERATED ALWAYS AS ((attributes ->> 'surface'::text)) STORED,
    facet_form text GENERATED ALWAYS AS ((attributes ->> 'form'::text)) STORED,
    facet_firing_max numeric GENERATED ALWAYS AS (
CASE
    WHEN (((attributes -> 'firing'::text) ->> 'max_celsius'::text) ~ '^-?[0-9]+(?:\.[0-9]+)?$'::text) THEN (((attributes -> 'firing'::text) ->> 'max_celsius'::text))::numeric
    ELSE NULL::numeric
END) STORED,
    facet_package_size numeric GENERATED ALWAYS AS (
CASE
    WHEN (((attributes -> 'package_size'::text) ->> 'millilitres'::text) ~ '^-?[0-9]+(?:\.[0-9]+)?$'::text) THEN (((attributes -> 'package_size'::text) ->> 'millilitres'::text))::numeric
    WHEN (((attributes -> 'package_size'::text) ->> 'grams'::text) ~ '^-?[0-9]+(?:\.[0-9]+)?$'::text) THEN (((attributes -> 'package_size'::text) ->> 'grams'::text))::numeric
    ELSE NULL::numeric
END) STORED,
    facet_application_methods jsonb GENERATED ALWAYS AS (
CASE
    WHEN (jsonb_typeof((attributes -> 'application_methods'::text)) = 'array'::text) THEN (attributes -> 'application_methods'::text)
    ELSE '[]'::jsonb
END) STORED,
    CONSTRAINT source_products_attributes_check CHECK ((jsonb_typeof(attributes) = 'object'::text)),
    CONSTRAINT source_products_brand_check CHECK (((brand IS NULL) OR (btrim(brand) <> ''::text))),
    CONSTRAINT source_products_check CHECK ((last_seen_at >= first_seen_at)),
    CONSTRAINT source_products_external_id_check CHECK ((btrim(external_id) <> ''::text)),
    CONSTRAINT source_products_name_check CHECK ((btrim(name) <> ''::text)),
    CONSTRAINT source_products_product_url_check CHECK ((btrim(product_url) <> ''::text)),
    CONSTRAINT source_products_record_format_check CHECK ((record_format = ANY (ARRAY['ceramics.catalogue_item.v1'::text, 'ceramics.catalogue_identity.v1'::text, 'ceramics.catalogue_item.v2'::text, 'ceramics.catalogue_identity.v2'::text]))),
    CONSTRAINT source_products_sku_check CHECK (((sku IS NULL) OR (btrim(sku) <> ''::text)))
);


--
-- Name: TABLE source_products; Type: COMMENT; Schema: catalogue; Owner: -
--

COMMENT ON TABLE catalogue.source_products IS 'Source-scoped identities; no automatic cross-supplier product merging.';


--
-- Name: canonical_catalogue; Type: VIEW; Schema: catalogue; Owner: -
--

CREATE VIEW catalogue.canonical_catalogue AS
 SELECT c.id AS canonical_product_id,
    c.manufacturer_id,
    c.brand,
    c.manufacturer_sku,
    c.name AS canonical_name,
    c.family,
    c.firing_range,
    c.attributes AS canonical_attributes,
    sp.id AS source_product_id,
    sp.source_id,
    sp.parent_external_id,
    sp.name AS supplier_name,
    sp.supplier_reference,
    sp.product_url,
    sp.image_url,
    sp.availability,
    sp.last_seen_at,
    o.observed_at,
    o.price,
    o.currency,
    o.vat_status,
    o.quantity AS package_quantity,
    o.unit AS package_unit,
    o.unit_price,
    o.unit_price_per,
    o.last_seen_at AS offer_last_seen_at
   FROM ((catalogue.canonical_products c
     JOIN catalogue.source_products sp ON (((sp.canonical_product_id = c.id) AND sp.active)))
     LEFT JOIN LATERAL ( SELECT o_1.observed_at,
            o_1.last_seen_at,
            o_1.price,
            o_1.currency,
            o_1.vat_status,
            o_1.quantity,
            o_1.unit,
            o_1.unit_price,
            o_1.unit_price_per
           FROM catalogue.offer_observations o_1
          WHERE (o_1.source_product_id = sp.id)
          ORDER BY o_1.observed_at DESC, o_1.id DESC
         LIMIT 1) o ON (true))
  WHERE c.active;


--
-- Name: catalogue_generation; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.catalogue_generation (
    singleton boolean DEFAULT true NOT NULL,
    generation bigint DEFAULT 0 NOT NULL,
    promoted_at timestamp with time zone,
    CONSTRAINT catalogue_generation_singleton_check CHECK (singleton)
);


--
-- Name: event_log; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.event_log (
    id bigint NOT NULL,
    at timestamp with time zone DEFAULT now() NOT NULL,
    topic text NOT NULL,
    type text NOT NULL,
    run_id uuid,
    job_id uuid,
    worker_id uuid,
    source_id text,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT event_log_topic_check CHECK ((topic = ANY (ARRAY['run'::text, 'job'::text, 'worker'::text, 'notification'::text, 'schedule'::text, 'source'::text, 'proxy'::text])))
);


--
-- Name: event_log_id_seq; Type: SEQUENCE; Schema: catalogue; Owner: -
--

ALTER TABLE catalogue.event_log ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME catalogue.event_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: host_leases; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.host_leases (
    host text NOT NULL,
    slot smallint NOT NULL,
    leased_by uuid,
    job_id uuid,
    leased_until timestamp with time zone,
    execution_token uuid,
    CONSTRAINT host_leases_slot_check CHECK ((slot > 0))
);


--
-- Name: hosts; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.hosts (
    host text NOT NULL,
    max_concurrency smallint DEFAULT 1 NOT NULL,
    delay_seconds numeric,
    CONSTRAINT hosts_max_concurrency_check CHECK ((max_concurrency > 0))
);


--
-- Name: import_runs; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.import_runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    status text DEFAULT 'running'::text NOT NULL,
    record_count integer DEFAULT 0 NOT NULL,
    error_count integer DEFAULT 0 NOT NULL,
    importer_version text,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    run_id uuid,
    CONSTRAINT import_runs_check CHECK (((finished_at IS NULL) OR (finished_at >= started_at))),
    CONSTRAINT import_runs_error_count_check CHECK ((error_count >= 0)),
    CONSTRAINT import_runs_metadata_check CHECK ((jsonb_typeof(metadata) = 'object'::text)),
    CONSTRAINT import_runs_record_count_check CHECK ((record_count >= 0)),
    CONSTRAINT import_runs_status_check CHECK ((status = ANY (ARRAY['running'::text, 'complete'::text, 'failed'::text])))
);


--
-- Name: job_artifacts; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.job_artifacts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    job_id uuid NOT NULL,
    dataset text NOT NULL,
    contract_version text NOT NULL,
    projector_version text NOT NULL,
    kind text NOT NULL,
    location text NOT NULL,
    sha256 text NOT NULL,
    size bigint NOT NULL,
    published_at timestamp with time zone DEFAULT now() NOT NULL,
    available boolean DEFAULT true NOT NULL,
    retained_at timestamp with time zone,
    CONSTRAINT job_artifacts_contract_version_check CHECK ((contract_version <> ''::text)),
    CONSTRAINT job_artifacts_dataset_check CHECK ((dataset <> ''::text)),
    CONSTRAINT job_artifacts_kind_check CHECK ((kind <> ''::text)),
    CONSTRAINT job_artifacts_location_check CHECK ((location <> ''::text)),
    CONSTRAINT job_artifacts_projector_version_check CHECK ((projector_version <> ''::text)),
    CONSTRAINT job_artifacts_sha256_check CHECK ((sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT job_artifacts_size_check CHECK ((size >= 0))
);


--
-- Name: job_checkpoint_lineages; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.job_checkpoint_lineages (
    job_id uuid NOT NULL,
    checkpoint_lineage uuid NOT NULL,
    source_id text NOT NULL,
    source_url text NOT NULL,
    connector text NOT NULL,
    connector_version text NOT NULL,
    connector_configuration jsonb DEFAULT '{}'::jsonb NOT NULL,
    connector_config_fingerprint text NOT NULL,
    dataset_fingerprint text NOT NULL,
    dataset_selection jsonb DEFAULT '[]'::jsonb NOT NULL,
    budget_state jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone,
    checksum text,
    CONSTRAINT job_checkpoint_lineages_check CHECK (((expires_at IS NULL) OR (expires_at > created_at))),
    CONSTRAINT job_checkpoint_lineages_check1 CHECK (((status <> ALL (ARRAY['completed'::text, 'limited'::text])) OR (checksum IS NOT NULL))),
    CONSTRAINT job_checkpoint_lineages_checksum_check CHECK (((checksum IS NULL) OR (checksum ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT job_checkpoint_lineages_connector_check CHECK ((connector <> ''::text)),
    CONSTRAINT job_checkpoint_lineages_connector_config_fingerprint_check CHECK ((connector_config_fingerprint ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT job_checkpoint_lineages_connector_version_check CHECK ((connector_version <> ''::text)),
    CONSTRAINT job_checkpoint_lineages_dataset_fingerprint_check CHECK ((dataset_fingerprint ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT job_checkpoint_lineages_source_id_check CHECK ((source_id <> ''::text)),
    CONSTRAINT job_checkpoint_lineages_source_url_check CHECK ((source_url <> ''::text)),
    CONSTRAINT job_checkpoint_lineages_status_check CHECK ((status = ANY (ARRAY['active'::text, 'completed'::text, 'limited'::text, 'rejected'::text, 'expired'::text])))
);


--
-- Name: job_datasets; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.job_datasets (
    job_id uuid NOT NULL,
    dataset text NOT NULL,
    contract_version text NOT NULL,
    projector_version text NOT NULL,
    state text DEFAULT 'pending'::text NOT NULL,
    complete boolean DEFAULT false NOT NULL,
    records bigint DEFAULT 0 NOT NULL,
    rejected bigint DEFAULT 0 NOT NULL,
    error text,
    promoted_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT job_datasets_contract_version_check CHECK ((contract_version <> ''::text)),
    CONSTRAINT job_datasets_dataset_check CHECK ((dataset <> ''::text)),
    CONSTRAINT job_datasets_projector_version_check CHECK ((projector_version <> ''::text)),
    CONSTRAINT job_datasets_records_check CHECK ((records >= 0)),
    CONSTRAINT job_datasets_rejected_check CHECK ((rejected >= 0)),
    CONSTRAINT job_datasets_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'projecting'::text, 'staged'::text, 'publishing'::text, 'published'::text, 'loading'::text, 'succeeded'::text, 'degraded'::text, 'failed'::text, 'cancelled'::text, 'skipped'::text])))
);


--
-- Name: job_events; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.job_events (
    id bigint NOT NULL,
    job_id uuid NOT NULL,
    at timestamp with time zone DEFAULT now() NOT NULL,
    level text NOT NULL,
    event text,
    message text NOT NULL,
    data jsonb,
    CONSTRAINT job_events_level_check CHECK ((level = ANY (ARRAY['debug'::text, 'info'::text, 'warning'::text, 'error'::text])))
);


--
-- Name: job_events_id_seq; Type: SEQUENCE; Schema: catalogue; Owner: -
--

ALTER TABLE catalogue.job_events ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME catalogue.job_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: job_page_batches; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.job_page_batches (
    job_id uuid NOT NULL,
    checkpoint_lineage uuid NOT NULL,
    partition_key text NOT NULL,
    page_id text NOT NULL,
    page_sequence bigint NOT NULL,
    dataset text NOT NULL,
    contract_version text NOT NULL,
    projector_version text NOT NULL,
    object_key text NOT NULL,
    sha256 text NOT NULL,
    size bigint NOT NULL,
    records bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT job_page_batches_contract_version_check CHECK ((contract_version <> ''::text)),
    CONSTRAINT job_page_batches_dataset_check CHECK ((dataset <> ''::text)),
    CONSTRAINT job_page_batches_object_key_check CHECK ((object_key <> ''::text)),
    CONSTRAINT job_page_batches_page_sequence_check CHECK ((page_sequence >= 0)),
    CONSTRAINT job_page_batches_projector_version_check CHECK ((projector_version <> ''::text)),
    CONSTRAINT job_page_batches_records_check CHECK ((records >= 0)),
    CONSTRAINT job_page_batches_sha256_check CHECK ((sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT job_page_batches_size_check CHECK ((size >= 0))
);


--
-- Name: job_page_dataset_outcomes; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.job_page_dataset_outcomes (
    job_id uuid NOT NULL,
    checkpoint_lineage uuid NOT NULL,
    partition_key text NOT NULL,
    page_id text NOT NULL,
    page_sequence bigint NOT NULL,
    dataset text NOT NULL,
    contract_version text NOT NULL,
    projector_version text NOT NULL,
    state text NOT NULL,
    records bigint DEFAULT 0 NOT NULL,
    error text,
    CONSTRAINT job_page_dataset_outcomes_check CHECK ((((state = 'failed'::text) AND (error IS NOT NULL)) OR (state <> 'failed'::text))),
    CONSTRAINT job_page_dataset_outcomes_page_sequence_check CHECK ((page_sequence >= 0)),
    CONSTRAINT job_page_dataset_outcomes_records_check CHECK ((records >= 0)),
    CONSTRAINT job_page_dataset_outcomes_state_check CHECK ((state = ANY (ARRAY['succeeded'::text, 'failed'::text, 'skipped'::text])))
);


--
-- Name: job_pages; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.job_pages (
    job_id uuid NOT NULL,
    checkpoint_lineage uuid NOT NULL,
    partition_key text NOT NULL,
    page_sequence bigint NOT NULL,
    page_id text NOT NULL,
    resume_after jsonb,
    terminal boolean DEFAULT false NOT NULL,
    enumeration_intact boolean DEFAULT true NOT NULL,
    connector_version text NOT NULL,
    committed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT job_pages_connector_version_check CHECK ((connector_version <> ''::text)),
    CONSTRAINT job_pages_page_id_check CHECK ((page_id <> ''::text)),
    CONSTRAINT job_pages_page_sequence_check CHECK ((page_sequence >= 0)),
    CONSTRAINT job_pages_partition_key_check CHECK ((partition_key <> ''::text))
);


--
-- Name: job_progress; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.job_progress (
    job_id uuid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    phase text,
    discovered integer DEFAULT 0 NOT NULL,
    records integer DEFAULT 0 NOT NULL,
    requests integer DEFAULT 0 NOT NULL,
    rendered_pages integer DEFAULT 0 NOT NULL,
    error_count integer DEFAULT 0 NOT NULL,
    truncated boolean DEFAULT false NOT NULL,
    in_flight jsonb DEFAULT '[]'::jsonb NOT NULL,
    http_tx_bytes_estimated bigint DEFAULT 0 NOT NULL,
    http_rx_bytes_estimated bigint DEFAULT 0 NOT NULL,
    browser_tx_bytes_estimated bigint DEFAULT 0 NOT NULL,
    browser_rx_bytes_estimated bigint DEFAULT 0 NOT NULL,
    cache_bytes_read bigint DEFAULT 0 NOT NULL,
    direct_requests integer DEFAULT 0 NOT NULL,
    impersonated_requests integer DEFAULT 0 NOT NULL,
    browser_requests integer DEFAULT 0 NOT NULL,
    proxy_requests integer DEFAULT 0 NOT NULL,
    proxy_bytes_reserved bigint DEFAULT 0 NOT NULL,
    proxy_bytes_estimated bigint DEFAULT 0 NOT NULL,
    browser_gain integer DEFAULT 0 NOT NULL,
    browser_zero_gain integer DEFAULT 0 NOT NULL,
    outcome_counts jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: jobs; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.jobs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    source_id text NOT NULL,
    host text NOT NULL,
    state text NOT NULL,
    priority smallint DEFAULT 100 NOT NULL,
    attempt smallint DEFAULT 0 NOT NULL,
    max_attempts smallint DEFAULT 3 NOT NULL,
    requires text[] DEFAULT '{}'::text[] NOT NULL,
    scheduled_for timestamp with time zone DEFAULT now() NOT NULL,
    lease_owner uuid,
    lease_expires_at timestamp with time zone,
    cancel_requested boolean DEFAULT false NOT NULL,
    pause_requested boolean DEFAULT false NOT NULL,
    resume_without_attempt boolean DEFAULT false NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    error text,
    trace_id text,
    artifact_path text,
    artifact_sha256 text,
    artifact_size bigint,
    summary jsonb,
    proxy_snapshot jsonb DEFAULT '{}'::jsonb NOT NULL,
    requires_any text[] DEFAULT '{}'::text[] NOT NULL,
    selected_browser_backend text,
    delivery_generation bigint DEFAULT 1 NOT NULL,
    execution_token uuid,
    paused_by_source boolean DEFAULT false NOT NULL,
    CONSTRAINT jobs_delivery_generation_check CHECK ((delivery_generation > 0)),
    CONSTRAINT jobs_selected_browser_backend_check CHECK (((selected_browser_backend IS NULL) OR (selected_browser_backend = ANY (ARRAY['camoufox'::text, 'cdp_extension_proxy'::text])))),
    CONSTRAINT jobs_state_check CHECK ((state = ANY (ARRAY['queued'::text, 'leased'::text, 'running'::text, 'paused'::text, 'succeeded'::text, 'degraded'::text, 'failed'::text, 'cancelled'::text, 'skipped'::text])))
);


--
-- Name: latest_offers; Type: VIEW; Schema: catalogue; Owner: -
--

CREATE VIEW catalogue.latest_offers AS
 SELECT DISTINCT ON (source_product_id) id,
    source_product_id,
    raw_record_id,
    observed_at,
    last_seen_at,
    price,
    currency,
    price_text,
    vat_status,
    quantity,
    unit,
    availability,
    attributes
   FROM catalogue.offer_observations
  ORDER BY source_product_id, observed_at DESC, id DESC;


--
-- Name: manufacturer_aliases; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.manufacturer_aliases (
    alias text NOT NULL,
    manufacturer_id text NOT NULL,
    CONSTRAINT manufacturer_aliases_alias_check CHECK (((btrim(alias) <> ''::text) AND (alias = lower(alias))))
);


--
-- Name: manufacturers; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.manufacturers (
    id text NOT NULL,
    name text NOT NULL,
    homepage_url text,
    source_id text,
    active boolean DEFAULT true NOT NULL,
    notes text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT manufacturers_homepage_url_check CHECK (((homepage_url IS NULL) OR (btrim(homepage_url) <> ''::text))),
    CONSTRAINT manufacturers_id_check CHECK ((id ~ '^[a-z0-9][a-z0-9-]*$'::text)),
    CONSTRAINT manufacturers_name_check CHECK ((btrim(name) <> ''::text))
);


--
-- Name: notifications; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.notifications (
    id bigint NOT NULL,
    at timestamp with time zone DEFAULT now() NOT NULL,
    severity text NOT NULL,
    kind text NOT NULL,
    title text NOT NULL,
    body text,
    run_id uuid,
    job_id uuid,
    source_id text,
    worker_id uuid,
    dedup_key text NOT NULL,
    resolved_at timestamp with time zone,
    acknowledged_at timestamp with time zone,
    acknowledged_by text,
    CONSTRAINT notifications_severity_check CHECK ((severity = ANY (ARRAY['info'::text, 'warning'::text, 'critical'::text])))
);


--
-- Name: notifications_id_seq; Type: SEQUENCE; Schema: catalogue; Owner: -
--

ALTER TABLE catalogue.notifications ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME catalogue.notifications_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: offer_comparison; Type: VIEW; Schema: catalogue; Owner: -
--

CREATE VIEW catalogue.offer_comparison AS
 SELECT p.manufacturer_sku,
    p.source_id,
    p.name,
    p.brand,
    p.family,
    p.product_url,
    o.price,
    o.currency,
    o.vat_status,
    o.quantity,
    o.unit,
    o.unit_price,
    o.unit_price_per,
    o.observed_at
   FROM (catalogue.source_products p
     JOIN LATERAL ( SELECT o_1.id,
            o_1.source_product_id,
            o_1.raw_record_id,
            o_1.observed_at,
            o_1.price,
            o_1.currency,
            o_1.price_text,
            o_1.vat_status,
            o_1.quantity,
            o_1.unit,
            o_1.availability,
            o_1.context_sha256,
            o_1.attributes,
            o_1.unit_price,
            o_1.unit_price_per
           FROM catalogue.offer_observations o_1
          WHERE (o_1.source_product_id = p.id)
          ORDER BY o_1.observed_at DESC
         LIMIT 1) o ON (true))
  WHERE ((p.manufacturer_sku IS NOT NULL) AND p.active);


--
-- Name: VIEW offer_comparison; Type: COMMENT; Schema: catalogue; Owner: -
--

COMMENT ON VIEW catalogue.offer_comparison IS 'Latest offer per source product, keyed by manufacturer code. Compare only rows with the same unit_price_per and a similar package quantity: a small jar always costs more per litre than a large tub.';


--
-- Name: offer_observations_id_seq; Type: SEQUENCE; Schema: catalogue; Owner: -
--

ALTER TABLE catalogue.offer_observations ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME catalogue.offer_observations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: out_of_order_observations; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.out_of_order_observations (
    id bigint NOT NULL,
    source_product_id uuid NOT NULL,
    raw_record_id bigint,
    observed_at timestamp with time zone NOT NULL,
    context_sha256 bytea NOT NULL,
    record jsonb NOT NULL,
    quarantined_at timestamp with time zone DEFAULT now() NOT NULL,
    reason text DEFAULT 'out_of_order_observation'::text NOT NULL,
    CONSTRAINT out_of_order_observations_context_sha256_check CHECK ((octet_length(context_sha256) = 32)),
    CONSTRAINT out_of_order_observations_record_check CHECK ((jsonb_typeof(record) = 'object'::text))
);


--
-- Name: out_of_order_observations_id_seq; Type: SEQUENCE; Schema: catalogue; Owner: -
--

ALTER TABLE catalogue.out_of_order_observations ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME catalogue.out_of_order_observations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: proxy_actor_nonces; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.proxy_actor_nonces (
    nonce uuid NOT NULL,
    actor text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    used_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: proxy_admin_audit; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.proxy_admin_audit (
    id bigint NOT NULL,
    operation_id uuid NOT NULL,
    actor text NOT NULL,
    actor_role text NOT NULL,
    request_id uuid NOT NULL,
    idempotency_key text,
    action text NOT NULL,
    resource_type text NOT NULL,
    resource_id text,
    at timestamp with time zone DEFAULT now() NOT NULL,
    state text DEFAULT 'started'::text NOT NULL,
    success boolean,
    error_code text,
    before_data jsonb,
    after_data jsonb,
    response_status integer,
    response_data jsonb,
    CONSTRAINT proxy_admin_audit_actor_role_check CHECK ((actor_role = ANY (ARRAY['viewer'::text, 'admin'::text, 'system'::text]))),
    CONSTRAINT proxy_admin_audit_state_check CHECK ((state = ANY (ARRAY['started'::text, 'succeeded'::text, 'failed'::text, 'ambiguous'::text, 'provider_changed_local_failed'::text])))
);


--
-- Name: proxy_admin_audit_id_seq; Type: SEQUENCE; Schema: catalogue; Owner: -
--

ALTER TABLE catalogue.proxy_admin_audit ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME catalogue.proxy_admin_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: proxy_budget_cycles; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.proxy_budget_cycles (
    provider text NOT NULL,
    cycle_start timestamp with time zone NOT NULL,
    cycle_end timestamp with time zone NOT NULL,
    purchased_bytes bigint DEFAULT '3000000000'::bigint NOT NULL,
    operational_bytes bigint DEFAULT '2400000000'::bigint NOT NULL,
    daily_bytes bigint DEFAULT 80000000 NOT NULL,
    pilot_bytes bigint DEFAULT 300000000 NOT NULL,
    pilot_active boolean DEFAULT false NOT NULL,
    provider_reported_bytes bigint DEFAULT 0 NOT NULL,
    application_bytes bigint DEFAULT 0 NOT NULL,
    reconciled_at timestamp with time zone,
    reconciliation_ok boolean DEFAULT false NOT NULL,
    kill_switch boolean DEFAULT true NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    lifecycle text DEFAULT 'active'::text NOT NULL,
    provider_resource_id text,
    unmanaged_allocation_bytes bigint DEFAULT 0 NOT NULL,
    proposed_at timestamp with time zone,
    proposed_by text,
    confirmed_at timestamp with time zone,
    confirmed_by text,
    opened_at timestamp with time zone,
    opened_by text,
    closed_at timestamp with time zone,
    closed_by text,
    CONSTRAINT proxy_budget_cycles_application_bytes_check CHECK ((application_bytes >= 0)),
    CONSTRAINT proxy_budget_cycles_check CHECK ((cycle_end > cycle_start)),
    CONSTRAINT proxy_budget_cycles_check1 CHECK ((operational_bytes <= purchased_bytes)),
    CONSTRAINT proxy_budget_cycles_daily_bytes_check CHECK ((daily_bytes > 0)),
    CONSTRAINT proxy_budget_cycles_lifecycle_check CHECK ((lifecycle = ANY (ARRAY['proposed'::text, 'active'::text, 'closed'::text, 'rejected'::text]))),
    CONSTRAINT proxy_budget_cycles_operational_bytes_check CHECK ((operational_bytes > 0)),
    CONSTRAINT proxy_budget_cycles_pilot_bytes_check CHECK ((pilot_bytes > 0)),
    CONSTRAINT proxy_budget_cycles_provider_reported_bytes_check CHECK ((provider_reported_bytes >= 0)),
    CONSTRAINT proxy_budget_cycles_purchased_bytes_check CHECK ((purchased_bytes > 0)),
    CONSTRAINT proxy_budget_cycles_unmanaged_allocation_check CHECK (((unmanaged_allocation_bytes >= 0) AND (unmanaged_allocation_bytes <= operational_bytes)))
);


--
-- Name: proxy_mutation_requests; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.proxy_mutation_requests (
    operation_id uuid DEFAULT gen_random_uuid() NOT NULL,
    actor text NOT NULL,
    action text NOT NULL,
    idempotency_key text NOT NULL,
    state text DEFAULT 'started'::text NOT NULL,
    response_status integer,
    response_data jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT proxy_mutation_requests_state_check CHECK ((state = ANY (ARRAY['started'::text, 'succeeded'::text, 'failed'::text, 'ambiguous'::text, 'provider_changed_local_failed'::text])))
);


--
-- Name: proxy_pilot_evidence; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.proxy_pilot_evidence (
    job_id uuid NOT NULL,
    source_id text NOT NULL,
    route_id uuid NOT NULL,
    succeeded boolean NOT NULL,
    estimated_bytes bigint DEFAULT 0 NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT proxy_pilot_evidence_estimated_bytes_check CHECK ((estimated_bytes >= 0))
);


--
-- Name: proxy_probes; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.proxy_probes (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    route_id uuid NOT NULL,
    profile_id uuid NOT NULL,
    state text DEFAULT 'pending'::text NOT NULL,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    error_category text,
    estimated_bytes bigint DEFAULT 0 NOT NULL,
    provider_requests integer DEFAULT 0 NOT NULL,
    exit_country text,
    exit_ip inet,
    exit_ip_expires_at timestamp with time zone,
    latency_ms integer,
    protocol text NOT NULL,
    actor text NOT NULL,
    request_id uuid NOT NULL,
    CONSTRAINT proxy_probes_estimated_bytes_check CHECK ((estimated_bytes >= 0)),
    CONSTRAINT proxy_probes_latency_ms_check CHECK (((latency_ms IS NULL) OR (latency_ms >= 0))),
    CONSTRAINT proxy_probes_provider_requests_check CHECK ((provider_requests >= 0)),
    CONSTRAINT proxy_probes_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'running'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: proxy_profile_allocations; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.proxy_profile_allocations (
    provider text NOT NULL,
    cycle_start timestamp with time zone NOT NULL,
    profile_id uuid NOT NULL,
    allocated_bytes bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by text NOT NULL,
    CONSTRAINT proxy_profile_allocations_allocated_bytes_check CHECK ((allocated_bytes >= 0))
);


--
-- Name: proxy_profile_retirements; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.proxy_profile_retirements (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    profile_id uuid NOT NULL,
    old_provider_resource_id text NOT NULL,
    replacement_resource_id text NOT NULL,
    target_limit_bytes bigint NOT NULL,
    temporary_allocation_bytes bigint NOT NULL,
    old_secret_generation integer NOT NULL,
    state text DEFAULT 'draining'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    error_code text,
    CONSTRAINT proxy_profile_retirements_old_secret_generation_check CHECK ((old_secret_generation >= 0)),
    CONSTRAINT proxy_profile_retirements_state_check CHECK ((state = ANY (ARRAY['creating'::text, 'draining'::text, 'finalizing'::text, 'completed'::text, 'failed'::text]))),
    CONSTRAINT proxy_profile_retirements_target_limit_bytes_check CHECK ((target_limit_bytes > 0)),
    CONSTRAINT proxy_profile_retirements_temporary_allocation_bytes_check CHECK ((temporary_allocation_bytes > 0))
);


--
-- Name: proxy_profiles; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.proxy_profiles (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    provider text DEFAULT 'decodo'::text NOT NULL,
    logical_name text NOT NULL,
    provider_resource_id text,
    display_name text NOT NULL,
    username_mask text,
    username_fingerprint text,
    provider_traffic_limit_bytes bigint,
    auto_disable boolean DEFAULT true NOT NULL,
    enabled boolean DEFAULT false NOT NULL,
    lifecycle text DEFAULT 'pending'::text NOT NULL,
    secret_generation integer DEFAULT 0 NOT NULL,
    secret_installed_at timestamp with time zone,
    provider_observed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by text NOT NULL,
    retired_at timestamp with time zone,
    pending_action text,
    CONSTRAINT proxy_profiles_lifecycle_check CHECK ((lifecycle = ANY (ARRAY['pending'::text, 'enabled'::text, 'draining'::text, 'disabled'::text, 'retired'::text, 'provider_changed_local_failed'::text]))),
    CONSTRAINT proxy_profiles_logical_name_check CHECK ((logical_name ~ '^[a-z0-9][a-z0-9_-]{0,63}$'::text)),
    CONSTRAINT proxy_profiles_pending_action_check CHECK (((pending_action IS NULL) OR (pending_action = ANY (ARRAY['disable'::text, 'rotate'::text, 'retire'::text])))),
    CONSTRAINT proxy_profiles_provider_traffic_limit_bytes_check CHECK ((provider_traffic_limit_bytes >= 0)),
    CONSTRAINT proxy_profiles_secret_generation_check CHECK ((secret_generation >= 0))
);


--
-- Name: proxy_provider_snapshots; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.proxy_provider_snapshots (
    id bigint NOT NULL,
    provider text NOT NULL,
    cycle_start timestamp with time zone NOT NULL,
    source_endpoint text NOT NULL,
    grouping_dimension text NOT NULL,
    grouping_key text NOT NULL,
    bucket_start timestamp with time zone NOT NULL,
    bucket_end timestamp with time zone NOT NULL,
    transmitted_bytes bigint DEFAULT 0 NOT NULL,
    received_bytes bigint DEFAULT 0 NOT NULL,
    total_bytes bigint DEFAULT 0 NOT NULL,
    request_count bigint DEFAULT 0 NOT NULL,
    provider_watermark text,
    first_observed_at timestamp with time zone DEFAULT now() NOT NULL,
    last_observed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT proxy_provider_snapshots_check CHECK ((bucket_end > bucket_start)),
    CONSTRAINT proxy_provider_snapshots_received_bytes_check CHECK ((received_bytes >= 0)),
    CONSTRAINT proxy_provider_snapshots_request_count_check CHECK ((request_count >= 0)),
    CONSTRAINT proxy_provider_snapshots_source_endpoint_check CHECK ((source_endpoint = ANY (ARRAY['traffic'::text, 'subuser_traffic'::text, 'subscription'::text]))),
    CONSTRAINT proxy_provider_snapshots_total_bytes_check CHECK ((total_bytes >= 0)),
    CONSTRAINT proxy_provider_snapshots_transmitted_bytes_check CHECK ((transmitted_bytes >= 0))
);


--
-- Name: proxy_provider_snapshots_id_seq; Type: SEQUENCE; Schema: catalogue; Owner: -
--

ALTER TABLE catalogue.proxy_provider_snapshots ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME catalogue.proxy_provider_snapshots_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: proxy_reconcile_requests; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.proxy_reconcile_requests (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    provider text DEFAULT 'decodo'::text NOT NULL,
    reason text NOT NULL,
    reservation_id uuid,
    mutation_request_id uuid,
    dedup_key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    claimed_at timestamp with time zone,
    completed_at timestamp with time zone,
    attempts integer DEFAULT 0 NOT NULL,
    error_code text,
    CONSTRAINT proxy_reconcile_requests_attempts_check CHECK ((attempts >= 0))
);


--
-- Name: proxy_reservations; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.proxy_reservations (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    job_id uuid,
    provider text NOT NULL,
    profile text NOT NULL,
    cycle_start timestamp with time zone NOT NULL,
    reserved_bytes bigint NOT NULL,
    estimated_bytes bigint DEFAULT 0 NOT NULL,
    request_count integer DEFAULT 0 NOT NULL,
    pilot boolean DEFAULT false NOT NULL,
    state text DEFAULT 'active'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    closed_at timestamp with time zone,
    probe_id uuid,
    profile_id uuid,
    route_id uuid,
    purpose text DEFAULT 'job'::text NOT NULL,
    secret_generation integer DEFAULT 0 NOT NULL,
    revocation_requested boolean DEFAULT false NOT NULL,
    CONSTRAINT proxy_reservations_consumer_check CHECK ((num_nonnulls(job_id, probe_id) = 1)),
    CONSTRAINT proxy_reservations_estimated_bytes_check CHECK ((estimated_bytes >= 0)),
    CONSTRAINT proxy_reservations_purpose_check CHECK ((((purpose = 'job'::text) AND (job_id IS NOT NULL) AND (probe_id IS NULL)) OR ((purpose = 'probe'::text) AND (probe_id IS NOT NULL) AND (job_id IS NULL)))),
    CONSTRAINT proxy_reservations_request_count_check CHECK ((request_count >= 0)),
    CONSTRAINT proxy_reservations_reserved_bytes_check CHECK ((reserved_bytes > 0)),
    CONSTRAINT proxy_reservations_state_check CHECK ((state = ANY (ARRAY['active'::text, 'closed'::text, 'cancelled'::text, 'revocation_requested'::text])))
);


--
-- Name: proxy_routes; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.proxy_routes (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    label text NOT NULL,
    profile_id uuid NOT NULL,
    protocol text DEFAULT 'http'::text NOT NULL,
    country text,
    state text,
    city text,
    session_mode text DEFAULT 'random'::text NOT NULL,
    session_minutes integer DEFAULT 30 NOT NULL,
    max_bytes bigint DEFAULT 25000000 NOT NULL,
    pilot boolean DEFAULT false NOT NULL,
    enabled boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by text NOT NULL,
    retired_at timestamp with time zone,
    CONSTRAINT proxy_routes_country_check CHECK (((country IS NULL) OR (country ~ '^[A-Z]{2}$'::text))),
    CONSTRAINT proxy_routes_max_bytes_check CHECK (((max_bytes >= 1) AND (max_bytes <= 25000000))),
    CONSTRAINT proxy_routes_protocol_check CHECK ((protocol = ANY (ARRAY['http'::text, 'https'::text, 'socks5'::text]))),
    CONSTRAINT proxy_routes_session_minutes_check CHECK (((session_minutes >= 1) AND (session_minutes <= 1440))),
    CONSTRAINT proxy_routes_session_mode_check CHECK ((session_mode = ANY (ARRAY['random'::text, 'sticky'::text])))
);


--
-- Name: queue_outbox; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.queue_outbox (
    id bigint NOT NULL,
    job_id uuid NOT NULL,
    generation bigint NOT NULL,
    payload jsonb NOT NULL,
    available_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    published_at timestamp with time zone,
    cancelled_at timestamp with time zone,
    publish_attempts integer DEFAULT 0 NOT NULL,
    last_error text,
    route text NOT NULL,
    envelope_schema text DEFAULT 'catalogue.job.v1'::text NOT NULL,
    deduplication_key text NOT NULL,
    CONSTRAINT queue_outbox_check CHECK (((published_at IS NULL) OR (cancelled_at IS NULL))),
    CONSTRAINT queue_outbox_generation_check CHECK ((generation > 0)),
    CONSTRAINT queue_outbox_payload_check CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT queue_outbox_publish_attempts_check CHECK ((publish_attempts >= 0)),
    CONSTRAINT queue_outbox_route_check CHECK ((route = ANY (ARRAY['plain.normal'::text, 'browser.auto.normal'::text, 'browser.camoufox.normal'::text, 'browser.cdp_extension_proxy.normal'::text]))),
    CONSTRAINT queue_outbox_schema_check CHECK ((envelope_schema = 'catalogue.job.v1'::text))
);


--
-- Name: queue_outbox_id_seq; Type: SEQUENCE; Schema: catalogue; Owner: -
--

ALTER TABLE catalogue.queue_outbox ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME catalogue.queue_outbox_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: raw_records; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.raw_records (
    id bigint NOT NULL,
    source_product_id uuid NOT NULL,
    document_id uuid,
    import_run_id uuid,
    fetched_at timestamp with time zone NOT NULL,
    record_sha256 bytea NOT NULL,
    record jsonb NOT NULL,
    first_seen_at timestamp with time zone NOT NULL,
    last_seen_at timestamp with time zone NOT NULL,
    CONSTRAINT raw_records_record_check CHECK ((jsonb_typeof(record) = 'object'::text)),
    CONSTRAINT raw_records_record_sha256_check CHECK ((octet_length(record_sha256) = 32)),
    CONSTRAINT raw_records_seen_interval_check CHECK ((last_seen_at >= first_seen_at))
);


--
-- Name: raw_records_id_seq; Type: SEQUENCE; Schema: catalogue; Owner: -
--

ALTER TABLE catalogue.raw_records ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME catalogue.raw_records_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: runs; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    kind text NOT NULL,
    schedule_id text,
    scheduled_fire_at timestamp with time zone,
    requested_by text,
    params jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    summary jsonb,
    CONSTRAINT runs_kind_check CHECK ((kind = ANY (ARRAY['scheduled'::text, 'manual'::text, 'retry'::text, 'backfill'::text]))),
    CONSTRAINT runs_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'complete'::text, 'degraded'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: schedules; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.schedules (
    id text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    cron text NOT NULL,
    timezone text DEFAULT 'Europe/Paris'::text NOT NULL,
    source_filter jsonb DEFAULT '{"all": true}'::jsonb NOT NULL,
    params jsonb DEFAULT '{}'::jsonb NOT NULL,
    last_fired_at timestamp with time zone,
    next_fire_at timestamp with time zone
);


--
-- Name: source_documents; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.source_documents (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    source_id text NOT NULL,
    import_run_id uuid,
    url text NOT NULL,
    title text,
    media_type text,
    published_on date,
    fetched_at timestamp with time zone NOT NULL,
    content_sha256 bytea,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT source_documents_content_sha256_check CHECK (((content_sha256 IS NULL) OR (octet_length(content_sha256) = 32))),
    CONSTRAINT source_documents_metadata_check CHECK ((jsonb_typeof(metadata) = 'object'::text)),
    CONSTRAINT source_documents_url_check CHECK ((btrim(url) <> ''::text))
);


--
-- Name: source_health_probes; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.source_health_probes (
    source_id text NOT NULL,
    url text NOT NULL,
    expect text DEFAULT 'json'::text NOT NULL,
    reason text,
    disabled_at timestamp with time zone DEFAULT now() NOT NULL,
    last_checked_at timestamp with time zone,
    last_status integer,
    last_error text,
    checks integer DEFAULT 0 NOT NULL,
    consecutive_ok integer DEFAULT 0 NOT NULL,
    required_ok integer DEFAULT 2 NOT NULL,
    recovered_at timestamp with time zone,
    CONSTRAINT source_health_probes_expect_check CHECK ((expect = ANY (ARRAY['json'::text, 'ok'::text])))
);


--
-- Name: source_proxy_policies; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.source_proxy_policies (
    source_id text NOT NULL,
    policy text DEFAULT 'never'::text NOT NULL,
    route_id uuid,
    max_bytes bigint DEFAULT 25000000 NOT NULL,
    pilot boolean DEFAULT true NOT NULL,
    evidence_count integer DEFAULT 0 NOT NULL,
    evidence_state text DEFAULT 'unproven'::text NOT NULL,
    revision bigint DEFAULT 1 NOT NULL,
    enabled_at timestamp with time zone,
    disabled_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by text NOT NULL,
    CONSTRAINT source_proxy_policies_check CHECK (((policy = 'never'::text) OR (route_id IS NOT NULL))),
    CONSTRAINT source_proxy_policies_check1 CHECK (((policy <> 'always'::text) OR ((evidence_state = 'promoted'::text) AND (evidence_count >= 3)))),
    CONSTRAINT source_proxy_policies_evidence_count_check CHECK ((evidence_count >= 0)),
    CONSTRAINT source_proxy_policies_evidence_state_check CHECK ((evidence_state = ANY (ARRAY['unproven'::text, 'eligible'::text, 'promoted'::text, 'rejected'::text]))),
    CONSTRAINT source_proxy_policies_max_bytes_check CHECK (((max_bytes >= 1) AND (max_bytes <= 25000000))),
    CONSTRAINT source_proxy_policies_policy_check CHECK ((policy = ANY (ARRAY['never'::text, 'fallback'::text, 'always'::text]))),
    CONSTRAINT source_proxy_policies_revision_check CHECK ((revision > 0))
);


--
-- Name: source_settings; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.source_settings (
    source_id text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    paused boolean DEFAULT false NOT NULL,
    schedule_id text,
    params jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by text
);


--
-- Name: sources; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.sources (
    id text NOT NULL,
    label text,
    homepage_url text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT sources_homepage_url_check CHECK (((homepage_url IS NULL) OR (btrim(homepage_url) <> ''::text))),
    CONSTRAINT sources_id_check CHECK ((id ~ '^[a-z0-9][a-z0-9-]*$'::text)),
    CONSTRAINT sources_label_check CHECK (((label IS NULL) OR (btrim(label) <> ''::text))),
    CONSTRAINT sources_metadata_check CHECK ((jsonb_typeof(metadata) = 'object'::text))
);


--
-- Name: workers; Type: TABLE; Schema: catalogue; Owner: -
--

CREATE TABLE catalogue.workers (
    id uuid NOT NULL,
    hostname text NOT NULL,
    pid integer NOT NULL,
    version text,
    capabilities text[] DEFAULT '{}'::text[] NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    last_heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    status text NOT NULL,
    desired_state text DEFAULT 'running'::text NOT NULL,
    current_job_id uuid,
    CONSTRAINT workers_desired_state_check CHECK ((desired_state = ANY (ARRAY['running'::text, 'paused'::text, 'draining'::text, 'stopping'::text]))),
    CONSTRAINT workers_status_check CHECK ((status = ANY (ARRAY['starting'::text, 'idle'::text, 'busy'::text, 'paused'::text, 'draining'::text, 'stopped'::text])))
);


--
-- Name: canonical_products canonical_products_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.canonical_products
    ADD CONSTRAINT canonical_products_pkey PRIMARY KEY (id);


--
-- Name: catalogue_generation catalogue_generation_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.catalogue_generation
    ADD CONSTRAINT catalogue_generation_pkey PRIMARY KEY (singleton);


--
-- Name: event_log event_log_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.event_log
    ADD CONSTRAINT event_log_pkey PRIMARY KEY (id);


--
-- Name: host_leases host_leases_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.host_leases
    ADD CONSTRAINT host_leases_pkey PRIMARY KEY (host, slot);


--
-- Name: hosts hosts_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.hosts
    ADD CONSTRAINT hosts_pkey PRIMARY KEY (host);


--
-- Name: import_runs import_runs_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.import_runs
    ADD CONSTRAINT import_runs_pkey PRIMARY KEY (id);


--
-- Name: job_artifacts job_artifacts_job_id_dataset_contract_version_projector_ver_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_artifacts
    ADD CONSTRAINT job_artifacts_job_id_dataset_contract_version_projector_ver_key UNIQUE (job_id, dataset, contract_version, projector_version, kind);


--
-- Name: job_artifacts job_artifacts_location_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_artifacts
    ADD CONSTRAINT job_artifacts_location_key UNIQUE (location);


--
-- Name: job_artifacts job_artifacts_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_artifacts
    ADD CONSTRAINT job_artifacts_pkey PRIMARY KEY (id);


--
-- Name: job_checkpoint_lineages job_checkpoint_lineages_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_checkpoint_lineages
    ADD CONSTRAINT job_checkpoint_lineages_pkey PRIMARY KEY (job_id, checkpoint_lineage);


--
-- Name: job_datasets job_datasets_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_datasets
    ADD CONSTRAINT job_datasets_pkey PRIMARY KEY (job_id, dataset, contract_version, projector_version);


--
-- Name: job_events job_events_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_events
    ADD CONSTRAINT job_events_pkey PRIMARY KEY (id);


--
-- Name: job_page_batches job_page_batches_object_key_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_page_batches
    ADD CONSTRAINT job_page_batches_object_key_key UNIQUE (object_key);


--
-- Name: job_page_batches job_page_batches_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_page_batches
    ADD CONSTRAINT job_page_batches_pkey PRIMARY KEY (job_id, checkpoint_lineage, partition_key, page_id, page_sequence, dataset, contract_version, projector_version);


--
-- Name: job_page_dataset_outcomes job_page_dataset_outcomes_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_page_dataset_outcomes
    ADD CONSTRAINT job_page_dataset_outcomes_pkey PRIMARY KEY (job_id, checkpoint_lineage, partition_key, page_id, page_sequence, dataset, contract_version, projector_version);


--
-- Name: job_pages job_pages_job_id_checkpoint_lineage_partition_key_page_id_p_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_pages
    ADD CONSTRAINT job_pages_job_id_checkpoint_lineage_partition_key_page_id_p_key UNIQUE (job_id, checkpoint_lineage, partition_key, page_id, page_sequence);


--
-- Name: job_pages job_pages_job_id_checkpoint_lineage_partition_key_page_sequ_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_pages
    ADD CONSTRAINT job_pages_job_id_checkpoint_lineage_partition_key_page_sequ_key UNIQUE (job_id, checkpoint_lineage, partition_key, page_sequence);


--
-- Name: job_pages job_pages_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_pages
    ADD CONSTRAINT job_pages_pkey PRIMARY KEY (job_id, checkpoint_lineage, partition_key, page_id);


--
-- Name: job_progress job_progress_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_progress
    ADD CONSTRAINT job_progress_pkey PRIMARY KEY (job_id);


--
-- Name: jobs jobs_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);


--
-- Name: jobs jobs_run_id_source_id_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.jobs
    ADD CONSTRAINT jobs_run_id_source_id_key UNIQUE (run_id, source_id);


--
-- Name: manufacturer_aliases manufacturer_aliases_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.manufacturer_aliases
    ADD CONSTRAINT manufacturer_aliases_pkey PRIMARY KEY (alias);


--
-- Name: manufacturers manufacturers_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.manufacturers
    ADD CONSTRAINT manufacturers_pkey PRIMARY KEY (id);


--
-- Name: notifications notifications_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.notifications
    ADD CONSTRAINT notifications_pkey PRIMARY KEY (id);


--
-- Name: offer_observations offer_observations_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.offer_observations
    ADD CONSTRAINT offer_observations_pkey PRIMARY KEY (id);


--
-- Name: offer_observations offer_observations_source_product_id_observed_at_context_sh_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.offer_observations
    ADD CONSTRAINT offer_observations_source_product_id_observed_at_context_sh_key UNIQUE (source_product_id, observed_at, context_sha256);


--
-- Name: out_of_order_observations out_of_order_observations_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.out_of_order_observations
    ADD CONSTRAINT out_of_order_observations_pkey PRIMARY KEY (id);


--
-- Name: out_of_order_observations out_of_order_observations_source_product_id_observed_at_con_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.out_of_order_observations
    ADD CONSTRAINT out_of_order_observations_source_product_id_observed_at_con_key UNIQUE (source_product_id, observed_at, context_sha256);


--
-- Name: proxy_actor_nonces proxy_actor_nonces_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_actor_nonces
    ADD CONSTRAINT proxy_actor_nonces_pkey PRIMARY KEY (nonce);


--
-- Name: proxy_admin_audit proxy_admin_audit_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_admin_audit
    ADD CONSTRAINT proxy_admin_audit_pkey PRIMARY KEY (id);


--
-- Name: proxy_budget_cycles proxy_budget_cycles_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_budget_cycles
    ADD CONSTRAINT proxy_budget_cycles_pkey PRIMARY KEY (provider, cycle_start);


--
-- Name: proxy_mutation_requests proxy_mutation_requests_actor_action_idempotency_key_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_mutation_requests
    ADD CONSTRAINT proxy_mutation_requests_actor_action_idempotency_key_key UNIQUE (actor, action, idempotency_key);


--
-- Name: proxy_mutation_requests proxy_mutation_requests_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_mutation_requests
    ADD CONSTRAINT proxy_mutation_requests_pkey PRIMARY KEY (operation_id);


--
-- Name: proxy_pilot_evidence proxy_pilot_evidence_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_pilot_evidence
    ADD CONSTRAINT proxy_pilot_evidence_pkey PRIMARY KEY (job_id);


--
-- Name: proxy_probes proxy_probes_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_probes
    ADD CONSTRAINT proxy_probes_pkey PRIMARY KEY (id);


--
-- Name: proxy_probes proxy_probes_request_id_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_probes
    ADD CONSTRAINT proxy_probes_request_id_key UNIQUE (request_id);


--
-- Name: proxy_profile_allocations proxy_profile_allocations_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_profile_allocations
    ADD CONSTRAINT proxy_profile_allocations_pkey PRIMARY KEY (provider, cycle_start, profile_id);


--
-- Name: proxy_profile_retirements proxy_profile_retirements_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_profile_retirements
    ADD CONSTRAINT proxy_profile_retirements_pkey PRIMARY KEY (id);


--
-- Name: proxy_profiles proxy_profiles_logical_name_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_profiles
    ADD CONSTRAINT proxy_profiles_logical_name_key UNIQUE (logical_name);


--
-- Name: proxy_profiles proxy_profiles_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_profiles
    ADD CONSTRAINT proxy_profiles_pkey PRIMARY KEY (id);


--
-- Name: proxy_provider_snapshots proxy_provider_snapshots_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_provider_snapshots
    ADD CONSTRAINT proxy_provider_snapshots_pkey PRIMARY KEY (id);


--
-- Name: proxy_provider_snapshots proxy_provider_snapshots_provider_cycle_start_source_endpoi_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_provider_snapshots
    ADD CONSTRAINT proxy_provider_snapshots_provider_cycle_start_source_endpoi_key UNIQUE (provider, cycle_start, source_endpoint, grouping_dimension, grouping_key, bucket_start, bucket_end);


--
-- Name: proxy_reconcile_requests proxy_reconcile_requests_dedup_key_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_reconcile_requests
    ADD CONSTRAINT proxy_reconcile_requests_dedup_key_key UNIQUE (dedup_key);


--
-- Name: proxy_reconcile_requests proxy_reconcile_requests_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_reconcile_requests
    ADD CONSTRAINT proxy_reconcile_requests_pkey PRIMARY KEY (id);


--
-- Name: proxy_reservations proxy_reservations_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_reservations
    ADD CONSTRAINT proxy_reservations_pkey PRIMARY KEY (id);


--
-- Name: proxy_reservations proxy_reservations_probe_id_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_reservations
    ADD CONSTRAINT proxy_reservations_probe_id_key UNIQUE (probe_id);


--
-- Name: proxy_routes proxy_routes_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_routes
    ADD CONSTRAINT proxy_routes_pkey PRIMARY KEY (id);


--
-- Name: queue_outbox queue_outbox_deduplication_key_unique; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.queue_outbox
    ADD CONSTRAINT queue_outbox_deduplication_key_unique UNIQUE (deduplication_key);


--
-- Name: queue_outbox queue_outbox_job_id_generation_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.queue_outbox
    ADD CONSTRAINT queue_outbox_job_id_generation_key UNIQUE (job_id, generation);


--
-- Name: queue_outbox queue_outbox_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.queue_outbox
    ADD CONSTRAINT queue_outbox_pkey PRIMARY KEY (id);


--
-- Name: raw_records raw_records_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.raw_records
    ADD CONSTRAINT raw_records_pkey PRIMARY KEY (id);


--
-- Name: raw_records raw_records_source_product_id_record_sha256_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.raw_records
    ADD CONSTRAINT raw_records_source_product_id_record_sha256_key UNIQUE (source_product_id, record_sha256);


--
-- Name: runs runs_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.runs
    ADD CONSTRAINT runs_pkey PRIMARY KEY (id);


--
-- Name: schedules schedules_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.schedules
    ADD CONSTRAINT schedules_pkey PRIMARY KEY (id);


--
-- Name: source_documents source_documents_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.source_documents
    ADD CONSTRAINT source_documents_pkey PRIMARY KEY (id);


--
-- Name: source_documents source_documents_source_id_url_fetched_at_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.source_documents
    ADD CONSTRAINT source_documents_source_id_url_fetched_at_key UNIQUE (source_id, url, fetched_at);


--
-- Name: source_health_probes source_health_probes_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.source_health_probes
    ADD CONSTRAINT source_health_probes_pkey PRIMARY KEY (source_id);


--
-- Name: source_products source_products_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.source_products
    ADD CONSTRAINT source_products_pkey PRIMARY KEY (id);


--
-- Name: source_products source_products_source_id_external_id_key; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.source_products
    ADD CONSTRAINT source_products_source_id_external_id_key UNIQUE (source_id, external_id);


--
-- Name: source_proxy_policies source_proxy_policies_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.source_proxy_policies
    ADD CONSTRAINT source_proxy_policies_pkey PRIMARY KEY (source_id);


--
-- Name: source_settings source_settings_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.source_settings
    ADD CONSTRAINT source_settings_pkey PRIMARY KEY (source_id);


--
-- Name: sources sources_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.sources
    ADD CONSTRAINT sources_pkey PRIMARY KEY (id);


--
-- Name: workers workers_pkey; Type: CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.workers
    ADD CONSTRAINT workers_pkey PRIMARY KEY (id);


--
-- Name: canonical_products_brand_sku; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE UNIQUE INDEX canonical_products_brand_sku ON catalogue.canonical_products USING btree (lower(brand), lower(manufacturer_sku)) WHERE ((brand IS NOT NULL) AND (manufacturer_sku IS NOT NULL));


--
-- Name: canonical_products_manufacturer_sku_key; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE UNIQUE INDEX canonical_products_manufacturer_sku_key ON catalogue.canonical_products USING btree (manufacturer_id, sku_key) WHERE ((manufacturer_id IS NOT NULL) AND (sku_key IS NOT NULL));


--
-- Name: event_log_id_topic_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX event_log_id_topic_idx ON catalogue.event_log USING btree (id) INCLUDE (topic);


--
-- Name: event_log_run_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX event_log_run_idx ON catalogue.event_log USING btree (run_id, id) WHERE (run_id IS NOT NULL);


--
-- Name: host_leases_free_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX host_leases_free_idx ON catalogue.host_leases USING btree (host) WHERE (job_id IS NULL);


--
-- Name: import_runs_run_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX import_runs_run_idx ON catalogue.import_runs USING btree (run_id) WHERE (run_id IS NOT NULL);


--
-- Name: job_artifacts_job_dataset_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX job_artifacts_job_dataset_idx ON catalogue.job_artifacts USING btree (job_id, dataset, published_at);


--
-- Name: job_checkpoint_lineages_status_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX job_checkpoint_lineages_status_idx ON catalogue.job_checkpoint_lineages USING btree (status, expires_at);


--
-- Name: job_datasets_state_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX job_datasets_state_idx ON catalogue.job_datasets USING btree (state, updated_at);


--
-- Name: job_events_tail_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX job_events_tail_idx ON catalogue.job_events USING btree (job_id, id);


--
-- Name: job_pages_lineage_commit_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX job_pages_lineage_commit_idx ON catalogue.job_pages USING btree (job_id, checkpoint_lineage, partition_key, page_sequence, committed_at);


--
-- Name: job_pages_one_terminal_per_lineage; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE UNIQUE INDEX job_pages_one_terminal_per_lineage ON catalogue.job_pages USING btree (job_id, checkpoint_lineage, partition_key) WHERE terminal;


--
-- Name: jobs_claimable_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX jobs_claimable_idx ON catalogue.jobs USING btree (state, scheduled_for, priority) WHERE (state = ANY (ARRAY['queued'::text, 'leased'::text, 'running'::text]));


--
-- Name: jobs_execution_expiry_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX jobs_execution_expiry_idx ON catalogue.jobs USING btree (lease_expires_at) WHERE (state = ANY (ARRAY['leased'::text, 'running'::text]));


--
-- Name: jobs_expiry_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX jobs_expiry_idx ON catalogue.jobs USING btree (lease_expires_at) WHERE (state = ANY (ARRAY['leased'::text, 'running'::text]));


--
-- Name: jobs_run_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX jobs_run_idx ON catalogue.jobs USING btree (run_id);


--
-- Name: jobs_source_history_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX jobs_source_history_idx ON catalogue.jobs USING btree (source_id, finished_at DESC) WHERE (finished_at IS NOT NULL);


--
-- Name: manufacturer_aliases_manufacturer; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX manufacturer_aliases_manufacturer ON catalogue.manufacturer_aliases USING btree (manufacturer_id);


--
-- Name: manufacturers_name; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE UNIQUE INDEX manufacturers_name ON catalogue.manufacturers USING btree (lower(name));


--
-- Name: notifications_feed_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX notifications_feed_idx ON catalogue.notifications USING btree (at DESC);


--
-- Name: notifications_open_key; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE UNIQUE INDEX notifications_open_key ON catalogue.notifications USING btree (dedup_key) WHERE ((resolved_at IS NULL) AND (acknowledged_at IS NULL));


--
-- Name: offer_observations_currency_price; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX offer_observations_currency_price ON catalogue.offer_observations USING btree (currency, price);


--
-- Name: offer_observations_product_time; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX offer_observations_product_time ON catalogue.offer_observations USING btree (source_product_id, observed_at DESC);


--
-- Name: out_of_order_observations_product_time; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX out_of_order_observations_product_time ON catalogue.out_of_order_observations USING btree (source_product_id, observed_at);


--
-- Name: proxy_admin_audit_recent_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX proxy_admin_audit_recent_idx ON catalogue.proxy_admin_audit USING btree (at DESC, id DESC);


--
-- Name: proxy_budget_cycles_id_key; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE UNIQUE INDEX proxy_budget_cycles_id_key ON catalogue.proxy_budget_cycles USING btree (id);


--
-- Name: proxy_budget_cycles_one_active; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE UNIQUE INDEX proxy_budget_cycles_one_active ON catalogue.proxy_budget_cycles USING btree (provider) WHERE (lifecycle = 'active'::text);


--
-- Name: proxy_profiles_provider_resource_key; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE UNIQUE INDEX proxy_profiles_provider_resource_key ON catalogue.proxy_profiles USING btree (provider, provider_resource_id) WHERE (provider_resource_id IS NOT NULL);


--
-- Name: proxy_provider_snapshots_series_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX proxy_provider_snapshots_series_idx ON catalogue.proxy_provider_snapshots USING btree (provider, cycle_start, bucket_start);


--
-- Name: proxy_reconcile_requests_pending_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX proxy_reconcile_requests_pending_idx ON catalogue.proxy_reconcile_requests USING btree (created_at) WHERE (completed_at IS NULL);


--
-- Name: proxy_reservations_accounting_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX proxy_reservations_accounting_idx ON catalogue.proxy_reservations USING btree (provider, cycle_start, created_at, state);


--
-- Name: proxy_reservations_one_active_job; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE UNIQUE INDEX proxy_reservations_one_active_job ON catalogue.proxy_reservations USING btree (job_id) WHERE ((job_id IS NOT NULL) AND (state = ANY (ARRAY['active'::text, 'revocation_requested'::text])));


--
-- Name: queue_outbox_pending_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX queue_outbox_pending_idx ON catalogue.queue_outbox USING btree (available_at, id) WHERE ((published_at IS NULL) AND (cancelled_at IS NULL));


--
-- Name: raw_records_product_time; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX raw_records_product_time ON catalogue.raw_records USING btree (source_product_id, fetched_at DESC);


--
-- Name: runs_active_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX runs_active_idx ON catalogue.runs USING btree (status) WHERE (status = ANY (ARRAY['queued'::text, 'running'::text]));


--
-- Name: runs_recent_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX runs_recent_idx ON catalogue.runs USING btree (created_at DESC);


--
-- Name: runs_scheduled_occurrence_key; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE UNIQUE INDEX runs_scheduled_occurrence_key ON catalogue.runs USING btree (schedule_id, scheduled_fire_at) WHERE (schedule_id IS NOT NULL);


--
-- Name: source_documents_source_time; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX source_documents_source_time ON catalogue.source_documents USING btree (source_id, fetched_at DESC);


--
-- Name: source_health_probes_pending_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX source_health_probes_pending_idx ON catalogue.source_health_probes USING btree (last_checked_at NULLS FIRST) WHERE (recovered_at IS NULL);


--
-- Name: source_products_brand_family; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX source_products_brand_family ON catalogue.source_products USING btree (lower(brand), lower(family));


--
-- Name: source_products_canonical; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX source_products_canonical ON catalogue.source_products USING btree (canonical_product_id) WHERE (canonical_product_id IS NOT NULL);


--
-- Name: source_products_manufacturer_sku; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX source_products_manufacturer_sku ON catalogue.source_products USING btree (upper(manufacturer_sku)) WHERE (manufacturer_sku IS NOT NULL);


--
-- Name: source_products_parent; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX source_products_parent ON catalogue.source_products USING btree (source_id, parent_external_id) WHERE (parent_external_id IS NOT NULL);


--
-- Name: source_products_search; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX source_products_search ON catalogue.source_products USING gin (to_tsvector('simple'::regconfig, ((((((((COALESCE(name, ''::text) || ' '::text) || COALESCE(brand, ''::text)) || ' '::text) || COALESCE(sku, ''::text)) || ' '::text) || COALESCE(family, ''::text)) || ' '::text) || COALESCE(description, ''::text))));


--
-- Name: source_products_sku; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX source_products_sku ON catalogue.source_products USING btree (lower(sku)) WHERE (sku IS NOT NULL);


--
-- Name: workers_live_idx; Type: INDEX; Schema: catalogue; Owner: -
--

CREATE INDEX workers_live_idx ON catalogue.workers USING btree (last_heartbeat_at DESC) WHERE (status <> 'stopped'::text);


--
-- Name: event_log event_log_notify; Type: TRIGGER; Schema: catalogue; Owner: -
--

CREATE TRIGGER event_log_notify AFTER INSERT ON catalogue.event_log FOR EACH ROW EXECUTE FUNCTION catalogue.notify_event_log();


--
-- Name: job_progress job_progress_notify; Type: TRIGGER; Schema: catalogue; Owner: -
--

CREATE TRIGGER job_progress_notify AFTER INSERT OR UPDATE ON catalogue.job_progress FOR EACH ROW EXECUTE FUNCTION catalogue.notify_job_progress();


--
-- Name: proxy_admin_audit proxy_admin_audit_immutable; Type: TRIGGER; Schema: catalogue; Owner: -
--

CREATE TRIGGER proxy_admin_audit_immutable BEFORE DELETE OR UPDATE ON catalogue.proxy_admin_audit FOR EACH ROW EXECUTE FUNCTION catalogue.proxy_audit_immutable();


--
-- Name: canonical_products canonical_products_manufacturer_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.canonical_products
    ADD CONSTRAINT canonical_products_manufacturer_id_fkey FOREIGN KEY (manufacturer_id) REFERENCES catalogue.manufacturers(id);


--
-- Name: host_leases host_leases_host_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.host_leases
    ADD CONSTRAINT host_leases_host_fkey FOREIGN KEY (host) REFERENCES catalogue.hosts(host) ON DELETE CASCADE;


--
-- Name: host_leases host_leases_job_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.host_leases
    ADD CONSTRAINT host_leases_job_id_fkey FOREIGN KEY (job_id) REFERENCES catalogue.jobs(id) ON DELETE SET NULL;


--
-- Name: host_leases host_leases_leased_by_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.host_leases
    ADD CONSTRAINT host_leases_leased_by_fkey FOREIGN KEY (leased_by) REFERENCES catalogue.workers(id);


--
-- Name: import_runs import_runs_run_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.import_runs
    ADD CONSTRAINT import_runs_run_id_fkey FOREIGN KEY (run_id) REFERENCES catalogue.runs(id);


--
-- Name: job_artifacts job_artifacts_job_id_dataset_contract_version_projector_ve_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_artifacts
    ADD CONSTRAINT job_artifacts_job_id_dataset_contract_version_projector_ve_fkey FOREIGN KEY (job_id, dataset, contract_version, projector_version) REFERENCES catalogue.job_datasets(job_id, dataset, contract_version, projector_version) ON DELETE CASCADE;


--
-- Name: job_artifacts job_artifacts_job_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_artifacts
    ADD CONSTRAINT job_artifacts_job_id_fkey FOREIGN KEY (job_id) REFERENCES catalogue.jobs(id) ON DELETE CASCADE;


--
-- Name: job_checkpoint_lineages job_checkpoint_lineages_job_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_checkpoint_lineages
    ADD CONSTRAINT job_checkpoint_lineages_job_id_fkey FOREIGN KEY (job_id) REFERENCES catalogue.jobs(id) ON DELETE CASCADE;


--
-- Name: job_datasets job_datasets_job_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_datasets
    ADD CONSTRAINT job_datasets_job_id_fkey FOREIGN KEY (job_id) REFERENCES catalogue.jobs(id) ON DELETE CASCADE;


--
-- Name: job_events job_events_job_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_events
    ADD CONSTRAINT job_events_job_id_fkey FOREIGN KEY (job_id) REFERENCES catalogue.jobs(id) ON DELETE CASCADE;


--
-- Name: job_page_batches job_page_batches_job_id_checkpoint_lineage_partition_key_p_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_page_batches
    ADD CONSTRAINT job_page_batches_job_id_checkpoint_lineage_partition_key_p_fkey FOREIGN KEY (job_id, checkpoint_lineage, partition_key, page_id, page_sequence) REFERENCES catalogue.job_pages(job_id, checkpoint_lineage, partition_key, page_id, page_sequence) ON DELETE CASCADE;


--
-- Name: job_page_batches job_page_batches_job_id_dataset_contract_version_projector_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_page_batches
    ADD CONSTRAINT job_page_batches_job_id_dataset_contract_version_projector_fkey FOREIGN KEY (job_id, dataset, contract_version, projector_version) REFERENCES catalogue.job_datasets(job_id, dataset, contract_version, projector_version) ON DELETE CASCADE;


--
-- Name: job_page_dataset_outcomes job_page_dataset_outcomes_job_id_checkpoint_lineage_partit_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_page_dataset_outcomes
    ADD CONSTRAINT job_page_dataset_outcomes_job_id_checkpoint_lineage_partit_fkey FOREIGN KEY (job_id, checkpoint_lineage, partition_key, page_id, page_sequence) REFERENCES catalogue.job_pages(job_id, checkpoint_lineage, partition_key, page_id, page_sequence) ON DELETE CASCADE;


--
-- Name: job_page_dataset_outcomes job_page_dataset_outcomes_job_id_dataset_contract_version__fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_page_dataset_outcomes
    ADD CONSTRAINT job_page_dataset_outcomes_job_id_dataset_contract_version__fkey FOREIGN KEY (job_id, dataset, contract_version, projector_version) REFERENCES catalogue.job_datasets(job_id, dataset, contract_version, projector_version) ON DELETE CASCADE;


--
-- Name: job_pages job_pages_job_id_checkpoint_lineage_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_pages
    ADD CONSTRAINT job_pages_job_id_checkpoint_lineage_fkey FOREIGN KEY (job_id, checkpoint_lineage) REFERENCES catalogue.job_checkpoint_lineages(job_id, checkpoint_lineage) ON DELETE CASCADE;


--
-- Name: job_progress job_progress_job_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.job_progress
    ADD CONSTRAINT job_progress_job_id_fkey FOREIGN KEY (job_id) REFERENCES catalogue.jobs(id) ON DELETE CASCADE;


--
-- Name: jobs jobs_lease_owner_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.jobs
    ADD CONSTRAINT jobs_lease_owner_fkey FOREIGN KEY (lease_owner) REFERENCES catalogue.workers(id);


--
-- Name: jobs jobs_run_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.jobs
    ADD CONSTRAINT jobs_run_id_fkey FOREIGN KEY (run_id) REFERENCES catalogue.runs(id) ON DELETE CASCADE;


--
-- Name: manufacturer_aliases manufacturer_aliases_manufacturer_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.manufacturer_aliases
    ADD CONSTRAINT manufacturer_aliases_manufacturer_id_fkey FOREIGN KEY (manufacturer_id) REFERENCES catalogue.manufacturers(id) ON DELETE CASCADE;


--
-- Name: manufacturers manufacturers_source_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.manufacturers
    ADD CONSTRAINT manufacturers_source_id_fkey FOREIGN KEY (source_id) REFERENCES catalogue.sources(id);


--
-- Name: offer_observations offer_observations_raw_record_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.offer_observations
    ADD CONSTRAINT offer_observations_raw_record_id_fkey FOREIGN KEY (raw_record_id) REFERENCES catalogue.raw_records(id) ON DELETE SET NULL;


--
-- Name: offer_observations offer_observations_source_product_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.offer_observations
    ADD CONSTRAINT offer_observations_source_product_id_fkey FOREIGN KEY (source_product_id) REFERENCES catalogue.source_products(id) ON DELETE CASCADE;


--
-- Name: out_of_order_observations out_of_order_observations_raw_record_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.out_of_order_observations
    ADD CONSTRAINT out_of_order_observations_raw_record_id_fkey FOREIGN KEY (raw_record_id) REFERENCES catalogue.raw_records(id) ON DELETE SET NULL;


--
-- Name: out_of_order_observations out_of_order_observations_source_product_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.out_of_order_observations
    ADD CONSTRAINT out_of_order_observations_source_product_id_fkey FOREIGN KEY (source_product_id) REFERENCES catalogue.source_products(id) ON DELETE CASCADE;


--
-- Name: proxy_pilot_evidence proxy_pilot_evidence_job_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_pilot_evidence
    ADD CONSTRAINT proxy_pilot_evidence_job_id_fkey FOREIGN KEY (job_id) REFERENCES catalogue.jobs(id) ON DELETE CASCADE;


--
-- Name: proxy_pilot_evidence proxy_pilot_evidence_route_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_pilot_evidence
    ADD CONSTRAINT proxy_pilot_evidence_route_id_fkey FOREIGN KEY (route_id) REFERENCES catalogue.proxy_routes(id);


--
-- Name: proxy_probes proxy_probes_profile_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_probes
    ADD CONSTRAINT proxy_probes_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES catalogue.proxy_profiles(id);


--
-- Name: proxy_probes proxy_probes_route_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_probes
    ADD CONSTRAINT proxy_probes_route_id_fkey FOREIGN KEY (route_id) REFERENCES catalogue.proxy_routes(id);


--
-- Name: proxy_profile_allocations proxy_profile_allocations_profile_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_profile_allocations
    ADD CONSTRAINT proxy_profile_allocations_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES catalogue.proxy_profiles(id);


--
-- Name: proxy_profile_allocations proxy_profile_allocations_provider_cycle_start_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_profile_allocations
    ADD CONSTRAINT proxy_profile_allocations_provider_cycle_start_fkey FOREIGN KEY (provider, cycle_start) REFERENCES catalogue.proxy_budget_cycles(provider, cycle_start);


--
-- Name: proxy_profile_retirements proxy_profile_retirements_profile_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_profile_retirements
    ADD CONSTRAINT proxy_profile_retirements_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES catalogue.proxy_profiles(id);


--
-- Name: proxy_provider_snapshots proxy_provider_snapshots_provider_cycle_start_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_provider_snapshots
    ADD CONSTRAINT proxy_provider_snapshots_provider_cycle_start_fkey FOREIGN KEY (provider, cycle_start) REFERENCES catalogue.proxy_budget_cycles(provider, cycle_start);


--
-- Name: proxy_reconcile_requests proxy_reconcile_requests_reservation_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_reconcile_requests
    ADD CONSTRAINT proxy_reconcile_requests_reservation_id_fkey FOREIGN KEY (reservation_id) REFERENCES catalogue.proxy_reservations(id);


--
-- Name: proxy_reservations proxy_reservations_job_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_reservations
    ADD CONSTRAINT proxy_reservations_job_id_fkey FOREIGN KEY (job_id) REFERENCES catalogue.jobs(id) ON DELETE CASCADE;


--
-- Name: proxy_reservations proxy_reservations_probe_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_reservations
    ADD CONSTRAINT proxy_reservations_probe_id_fkey FOREIGN KEY (probe_id) REFERENCES catalogue.proxy_probes(id) ON DELETE CASCADE;


--
-- Name: proxy_reservations proxy_reservations_profile_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_reservations
    ADD CONSTRAINT proxy_reservations_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES catalogue.proxy_profiles(id);


--
-- Name: proxy_reservations proxy_reservations_provider_cycle_start_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_reservations
    ADD CONSTRAINT proxy_reservations_provider_cycle_start_fkey FOREIGN KEY (provider, cycle_start) REFERENCES catalogue.proxy_budget_cycles(provider, cycle_start);


--
-- Name: proxy_reservations proxy_reservations_route_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_reservations
    ADD CONSTRAINT proxy_reservations_route_id_fkey FOREIGN KEY (route_id) REFERENCES catalogue.proxy_routes(id);


--
-- Name: proxy_routes proxy_routes_profile_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.proxy_routes
    ADD CONSTRAINT proxy_routes_profile_id_fkey FOREIGN KEY (profile_id) REFERENCES catalogue.proxy_profiles(id);


--
-- Name: queue_outbox queue_outbox_job_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.queue_outbox
    ADD CONSTRAINT queue_outbox_job_id_fkey FOREIGN KEY (job_id) REFERENCES catalogue.jobs(id) ON DELETE CASCADE;


--
-- Name: raw_records raw_records_document_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.raw_records
    ADD CONSTRAINT raw_records_document_id_fkey FOREIGN KEY (document_id) REFERENCES catalogue.source_documents(id) ON DELETE SET NULL;


--
-- Name: raw_records raw_records_import_run_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.raw_records
    ADD CONSTRAINT raw_records_import_run_id_fkey FOREIGN KEY (import_run_id) REFERENCES catalogue.import_runs(id) ON DELETE SET NULL;


--
-- Name: raw_records raw_records_source_product_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.raw_records
    ADD CONSTRAINT raw_records_source_product_id_fkey FOREIGN KEY (source_product_id) REFERENCES catalogue.source_products(id) ON DELETE CASCADE;


--
-- Name: runs runs_schedule_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.runs
    ADD CONSTRAINT runs_schedule_id_fkey FOREIGN KEY (schedule_id) REFERENCES catalogue.schedules(id);


--
-- Name: source_documents source_documents_import_run_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.source_documents
    ADD CONSTRAINT source_documents_import_run_id_fkey FOREIGN KEY (import_run_id) REFERENCES catalogue.import_runs(id) ON DELETE SET NULL;


--
-- Name: source_documents source_documents_source_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.source_documents
    ADD CONSTRAINT source_documents_source_id_fkey FOREIGN KEY (source_id) REFERENCES catalogue.sources(id);


--
-- Name: source_products source_products_canonical_product_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.source_products
    ADD CONSTRAINT source_products_canonical_product_id_fkey FOREIGN KEY (canonical_product_id) REFERENCES catalogue.canonical_products(id);


--
-- Name: source_products source_products_source_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.source_products
    ADD CONSTRAINT source_products_source_id_fkey FOREIGN KEY (source_id) REFERENCES catalogue.sources(id);


--
-- Name: source_proxy_policies source_proxy_policies_route_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.source_proxy_policies
    ADD CONSTRAINT source_proxy_policies_route_id_fkey FOREIGN KEY (route_id) REFERENCES catalogue.proxy_routes(id);


--
-- Name: source_settings source_settings_schedule_id_fkey; Type: FK CONSTRAINT; Schema: catalogue; Owner: -
--

ALTER TABLE ONLY catalogue.source_settings
    ADD CONSTRAINT source_settings_schedule_id_fkey FOREIGN KEY (schedule_id) REFERENCES catalogue.schedules(id);


--
-- PostgreSQL database dump complete
--


--
-- Seed data
--
-- `pg_dump --schema-only` does not carry rows, so the seeds that lived in the
-- files this baseline replaces are restored here, at the state they had reached
-- by the end of that sequence. All of them are idempotent: initdb and
-- `apply_schema` both run this file exactly once, but a hand-replay must not
-- duplicate or clobber curated rows.
--

insert into catalogue.catalogue_generation(singleton) values (true)
on conflict (singleton) do nothing;

-- The default schedule. Note `cache_mode: refresh`: a daily price run under the
-- old seven-day cache default would replay yesterday's pages and report success
-- while changing no prices at all, which would make the whole schedule a no-op.
--
-- Sources proven to time out or yield zero stay out of the daily freshness
-- promise and run in the weekly diagnostic/full-enrichment window instead. The
-- old sequence reached this filter by a follow-up `update`; it is inlined here.
insert into catalogue.schedules (id, cron, timezone, source_filter, params)
values (
  'daily-prices',
  '0 3 * * *',
  'Europe/Paris',
  '{"all": true, "except": ["countrylove", "mestrebras", "hobbyland", "toepferspass", "cromartie", "keramik-kriese"]}'::jsonb,
  '{"cache_mode": "refresh", "refresh_mode": "price", "sources": 4, "concurrency": 8}'::jsonb
)
on conflict (id) do nothing;

insert into catalogue.schedules (id, cron, timezone, source_filter, params)
values (
  'weekly-full', '0 2 * * 0', 'Europe/Paris', '{"all": true}'::jsonb,
  '{"cache_mode": "refresh", "refresh_mode": "full", "sources": 3, "concurrency": 6}'::jsonb
)
on conflict (id) do nothing;

-- Every name below was read off the loaded dump and judged one at a time. The
-- ones deliberately left out are as much a part of the curation as the ones
-- included: harry-ceradel, ceradel, les cousins, prodesco, taller gingell
-- barcelona, ulster ceramics pottery supplies, kettles pottery supplies,
-- penguin pottery and peter lavem are all shops, and several of them publish
-- article numbers that would pass for manufacturer codes.
insert into catalogue.manufacturers (id, name, homepage_url, source_id, notes)
values
  ('mayco', 'Mayco', 'https://www.maycocolors.com',
   (select id from catalogue.sources where id = 'mayco'),
   'Publishes specifications without prices; the identity rows in the dump are its own.'),
  ('amaco', 'AMACO', 'https://www.amaco.com',
   (select id from catalogue.sources where id = 'amaco'), null),
  ('botz', 'BOTZ', 'https://www.botz-glasuren.de', null, null),
  ('terracolor', 'Terracolor', 'https://www.terracolor.co.uk', null, null),
  ('spectrum', 'Spectrum Glazes', 'https://www.spectrumglazes.com',
   (select id from catalogue.sources where id = 'spectrum'), null),
  ('speedball', 'Speedball', 'https://www.speedballart.com',
   (select id from catalogue.sources where id = 'speedball'),
   'Owns AMACO; kept separate because the two publish separate code ranges.'),
  ('duncan', 'Duncan', 'https://www.duncanceramics.com', null, null),
  ('sio-2', 'SIO-2', 'https://www.sio-2.com',
   (select id from catalogue.sources where id = 'sio-2'), null),
  ('colorobbia', 'Colorobbia', 'https://www.colorobbia.com', null, null),
  ('gare', 'Gare', 'https://www.gareceramics.com', null, null),
  ('ferro', 'Ferro', 'https://www.ferro.com', null, null),
  ('laguna', 'Laguna Clay Company', 'https://www.lagunaclay.com', null, null),
  ('mason-color', 'Mason Color', 'https://www.masoncolor.com', null, null),
  ('carl-jaeger', 'Carl Jäger', 'https://www.carl-jaeger.de', null, null),
  ('goerg-schneider', 'Goerg & Schneider', 'https://www.goerg-schneider.de', null, null),
  ('sibelco', 'Sibelco', 'https://www.sibelco.com', null, null),
  ('chrysanthos', 'Chrysanthos', 'https://www.chrysanthos.com.au', null, null)
on conflict (id) do update
   set name = excluded.name,
       homepage_url = coalesce(excluded.homepage_url, catalogue.manufacturers.homepage_url),
       source_id = coalesce(excluded.source_id, catalogue.manufacturers.source_id),
       notes = coalesce(excluded.notes, catalogue.manufacturers.notes),
       updated_at = now();

insert into catalogue.manufacturer_aliases (alias, manufacturer_id)
values
  ('mayco', 'mayco'),
  ('mayco colors', 'mayco'),
  ('amaco', 'amaco'),
  ('botz', 'botz'),
  ('terracolor', 'terracolor'),
  ('terra color', 'terracolor'),
  ('spectrum', 'spectrum'),
  ('spectrum glazes', 'spectrum'),
  ('speedball', 'speedball'),
  ('speedball art', 'speedball'),
  ('duncan', 'duncan'),
  ('sio-2', 'sio-2'),
  ('sio2', 'sio-2'),
  ('colorobbia', 'colorobbia'),
  ('colorobbia art', 'colorobbia'),
  ('gare', 'gare'),
  ('ferro', 'ferro'),
  ('ferro frankfurt', 'ferro'),
  ('laguna', 'laguna'),
  ('laguna clay company', 'laguna'),
  ('mason color', 'mason-color'),
  ('carl jäger', 'carl-jaeger'),
  ('carl jaeger', 'carl-jaeger'),
  ('goerg & schneider', 'goerg-schneider'),
  ('g&s', 'goerg-schneider'),
  ('sibelco', 'sibelco'),
  ('chrysanthos', 'chrysanthos')
on conflict (alias) do update
   set manufacturer_id = excluded.manufacturer_id;
