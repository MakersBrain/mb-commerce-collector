-- Add exact, qualified stock state to the existing combined offer timeline.
--
-- Existing rows retain context version 1. New writes use version 2 and compare
-- semantic columns across the boundary so deployment itself is not recorded as
-- a price change.

alter table catalogue.offer_observations
    add column if not exists stock_quantity bigint,
    add column if not exists stock_quantity_kind text not null default 'unknown',
    add column if not exists context_version smallint not null default 1;

do $$
begin
  if not exists (
    select 1 from pg_constraint
     where conrelid = 'catalogue.offer_observations'::regclass
       and conname = 'offer_observations_stock_quantity_check'
  ) then
    alter table catalogue.offer_observations
      add constraint offer_observations_stock_quantity_check
      check (stock_quantity is null or stock_quantity >= 0);
  end if;
  if not exists (
    select 1 from pg_constraint
     where conrelid = 'catalogue.offer_observations'::regclass
       and conname = 'offer_observations_stock_quantity_kind_check'
  ) then
    alter table catalogue.offer_observations
      add constraint offer_observations_stock_quantity_kind_check
      check (stock_quantity_kind in (
        'exact', 'lower_bound', 'upper_bound', 'order_limit', 'unknown'
      ));
  end if;
  if not exists (
    select 1 from pg_constraint
     where conrelid = 'catalogue.offer_observations'::regclass
       and conname = 'offer_observations_stock_quantity_pair_check'
  ) then
    alter table catalogue.offer_observations
      add constraint offer_observations_stock_quantity_pair_check
      check ((stock_quantity is null) = (stock_quantity_kind = 'unknown'));
  end if;
  if not exists (
    select 1 from pg_constraint
     where conrelid = 'catalogue.offer_observations'::regclass
       and conname = 'offer_observations_context_version_check'
  ) then
    alter table catalogue.offer_observations
      add constraint offer_observations_context_version_check
      check (context_version in (1, 2));
  end if;
end
$$;

create or replace function catalogue.load_record(
  p_record jsonb,
  p_import_run_id uuid default null::uuid,
  p_document_id uuid default null::uuid
) returns uuid
language plpgsql
set search_path to 'pg_catalog', 'catalogue'
as $$
declare
  v_source_product_id uuid;
  v_raw_record_id bigint;
  v_latest_offer catalogue.offer_observations%rowtype;
  v_latest_observed_at timestamptz;
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
  v_unit_price_value numeric;
  v_unit_price_per text;
  v_price_text text;
  v_vat_status text;
  v_availability text;
  v_stock_quantity bigint;
  v_stock_quantity_kind text := 'unknown';
  v_sku text;
  v_firing_range text;
  v_record_hash bytea;
  v_context_hash bytea;
  v_price_refresh boolean := coalesce(p_record->>'collection_mode', 'full') = 'price';
  v_stock_trends_enabled boolean := coalesce(
    current_setting('catalogue.stock_trends_enabled', true), 'false'
  )::boolean;
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
  v_price_text := nullif(btrim(p_record->>'price_text'), '');
  v_vat_status := lower(nullif(btrim(p_record->>'vat_status'), ''));
  v_availability := nullif(btrim(p_record->>'availability'), '');
  v_unit_price_value := nullif(v_unit_price->>'value', '')::numeric;
  v_unit_price_per := lower(nullif(btrim(v_unit_price->>'per'), ''));
  if v_vat_status is not null and v_vat_status not in ('inclusive', 'exclusive', 'unknown') then
    v_vat_status := 'unknown';
  end if;
  if v_stock_trends_enabled then
    v_stock_quantity := nullif(p_record->>'stock_quantity', '')::bigint;
    v_stock_quantity_kind := lower(coalesce(
      nullif(btrim(p_record->>'stock_quantity_kind'), ''),
      case when v_stock_quantity is not null then 'exact' else 'unknown' end
    ));
    if v_stock_quantity_kind not in (
      'exact', 'lower_bound', 'upper_bound', 'order_limit', 'unknown'
    ) then
      raise exception 'unsupported_stock_quantity_kind: %', v_stock_quantity_kind
        using errcode = '22023';
    end if;
    if v_stock_quantity_kind = 'unknown' then
      v_stock_quantity := null;
    elsif v_stock_quantity is null then
      raise exception 'stock_quantity_required_for_kind: %', v_stock_quantity_kind
        using errcode = '22023';
    end if;
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
    v_availability, v_fetched_at, v_fetched_at,
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

  perform pg_advisory_xact_lock(hashtextextended(v_source_product_id::text, 0));

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
      'price', v_price,
      'currency', v_currency,
      'price_text', v_price_text,
      'vat_status', v_vat_status,
      'quantity', v_quantity,
      'unit', v_unit,
      'unit_price', v_unit_price_value,
      'unit_price_per', v_unit_price_per,
      'availability', v_availability,
      'stock_quantity', v_stock_quantity,
      'stock_quantity_kind', v_stock_quantity_kind
    )::text, 'UTF8'), 'sha256');

    select id into v_existing_offer_id
      from catalogue.offer_observations
     where source_product_id = v_source_product_id
       and observed_at = v_fetched_at
       and context_sha256 = v_context_hash;

    if v_existing_offer_id is null then
      select * into v_latest_offer
        from catalogue.offer_observations
       where source_product_id = v_source_product_id
       order by observed_at desc, id desc
       limit 1;
      v_latest_observed_at := v_latest_offer.observed_at;

      if v_latest_offer.id is null then
        insert into catalogue.offer_observations (
          source_product_id, raw_record_id, observed_at, last_seen_at,
          price, currency, price_text, vat_status, quantity, unit,
          unit_price, unit_price_per, availability, context_sha256, attributes,
          stock_quantity, stock_quantity_kind, context_version
        ) values (
          v_source_product_id, v_raw_record_id, v_fetched_at, v_fetched_at,
          v_price, v_currency, v_price_text, v_vat_status, v_quantity, v_unit,
          v_unit_price_value, v_unit_price_per, v_availability,
          v_context_hash, coalesce(p_record->'raw', '{}'::jsonb),
          v_stock_quantity, v_stock_quantity_kind, 2
        );
      elsif v_fetched_at < v_latest_observed_at then
        insert into catalogue.out_of_order_observations (
          source_product_id, raw_record_id, observed_at, context_sha256, record
        ) values (
          v_source_product_id, v_raw_record_id, v_fetched_at, v_context_hash, p_record
        ) on conflict (source_product_id, observed_at, context_sha256) do nothing;
      elsif v_latest_offer.price is not distinct from v_price
        and v_latest_offer.currency is not distinct from v_currency
        and v_latest_offer.price_text is not distinct from v_price_text
        and v_latest_offer.vat_status is not distinct from v_vat_status
        and v_latest_offer.quantity is not distinct from v_quantity
        and v_latest_offer.unit is not distinct from v_unit
        and v_latest_offer.unit_price is not distinct from v_unit_price_value
        and v_latest_offer.unit_price_per is not distinct from v_unit_price_per
        and v_latest_offer.availability is not distinct from v_availability
        and v_latest_offer.stock_quantity is not distinct from v_stock_quantity
        and v_latest_offer.stock_quantity_kind is not distinct from v_stock_quantity_kind then
        update catalogue.offer_observations
           set last_seen_at = greatest(last_seen_at, v_fetched_at)
         where id = v_latest_offer.id;
      else
        insert into catalogue.offer_observations (
          source_product_id, raw_record_id, observed_at, last_seen_at,
          price, currency, price_text, vat_status, quantity, unit,
          unit_price, unit_price_per, availability, context_sha256, attributes,
          stock_quantity, stock_quantity_kind, context_version
        ) values (
          v_source_product_id, v_raw_record_id, v_fetched_at, v_fetched_at,
          v_price, v_currency, v_price_text, v_vat_status, v_quantity, v_unit,
          v_unit_price_value, v_unit_price_per, v_availability,
          v_context_hash, coalesce(p_record->'raw', '{}'::jsonb),
          v_stock_quantity, v_stock_quantity_kind, 2
        );
      end if;
    end if;
  end if;

  return v_source_product_id;
end
$$;

create or replace view catalogue.latest_offers as
select distinct on (source_product_id)
       id, source_product_id, raw_record_id, observed_at, last_seen_at,
       price, currency, price_text, vat_status, quantity, unit, availability,
       attributes, unit_price, unit_price_per,
       stock_quantity, stock_quantity_kind, context_version
  from catalogue.offer_observations
 order by source_product_id, observed_at desc, id desc;
