import { band, measureUnit, type Measure } from '$lib/bands';
import { sql } from './db';
import { eurRate, fxRates, type Rates } from './fx';

// Every unit price shown here comes through catalogue.offer_comparison, which
// is the latest offer per source product and only exists for rows that carry a
// manufacturer code — the only honest cross-supplier key.
//
// Pack size is normalised here rather than in the view because the view keeps
// the supplier's own unit. A 59 ml jar always costs more per litre than a 473
// ml pot, and a 1 kg tub more per kilogram than a 25 kg sack, so anything that
// compares suppliers compares inside one pack band and never across bands.
//
// There are two normalisations because the schema quotes unit prices per litre
// or per kilogram, and only the liquid families are priced by volume. Which one
// applies is decided by the row's own `unit_price_per`, never guessed from the
// unit: a stain listed in grams and priced per kilogram must not be measured
// into litres and then banded against jars.
const PER_LITRE = sql`
	case unit
		when 'ml' then 1000 when 'l' then 1 when 'cl' then 100
		when 'fl oz' then 33.814 when 'pint' then 2.113 when 'gallon' then 0.2642
	end
`;

const PER_KILOGRAM = sql`
	case unit
		when 'g' then 1000 when 'gr' then 1000 when 'gram' then 1000 when 'grams' then 1000
		when 'kg' then 1 when 'lb' then 2.2046 when 'oz' then 35.274
	end
`;

/** Pack size in the base unit of one measure: litres, or kilograms. */
function packSize(measure: Measure) {
	return sql`quantity / nullif(${measure === 'mass' ? PER_KILOGRAM : PER_LITRE}, 0)`;
}

/** The same, for a query whose rows can be either measure. */
const packSizeOfRow = sql`
	case when unit_price_per = 'kg'
		then quantity / nullif(${PER_KILOGRAM}, 0)
		else quantity / nullif(${PER_LITRE}, 0)
	end
`;

export { BANDS, band } from '$lib/bands';

/**
 * Brand is matched case-insensitively: the same manufacturer is published as
 * "Mayco" by one storefront and "MAYCO" by the next, and they are one brand.
 */
function brandFilter(brand: string | null, column = sql`brand`) {
	return brand ? sql`upper(${column}) = ${brand.toUpperCase()}` : sql`true`;
}

/**
 * Product type is a promoted column, so it filters by equality rather than by
 * the case-folding brand needs. A row the importer could not classify is left
 * out of the filter entirely rather than being matched as an empty string.
 */
function familyFilter(family: string | null, column = sql`family`) {
	return family ? sql`${column} = ${family}` : sql`true`;
}

/** Product types with a count, most-stocked first, for the dashboard filter. */
export async function families(min = 20) {
	return sql<{ key: string; label: string; products: number }[]>`
		select btrim(family) as key, btrim(family) as label, count(*)::int as products
		from catalogue.source_products
		where family is not null and btrim(family) <> ''
		group by 1
		having count(*) >= ${min}
		order by 3 desc
	`;
}

/** Brands with a product count, most-stocked first, for the dashboard filter. */
export async function brands(min = 20) {
	return sql<{ key: string; label: string; products: number }[]>`
		select upper(brand) as key,
		       mode() within group (order by brand) as label,
		       count(*)::int as products
		from catalogue.source_products
		where brand is not null
		group by 1
		having count(*) >= ${min}
		order by 3 desc
	`;
}

export async function overview(brand: string | null = null, family: string | null = null) {
	const scope = sql`${brandFilter(brand, sql`p.brand`)} and ${familyFilter(family, sql`p.family`)}`;
	const [row] = await sql<
		{ products: number; suppliers: number; offers: number; codes: number; observed: Date | null }[]
	>`
		select
			(select count(*)::int from catalogue.source_products p where ${scope}) as products,
			(select count(distinct p.source_id)::int from catalogue.source_products p
				where ${scope}) as suppliers,
			(select count(*)::int from catalogue.source_products p
				where ${scope} and exists (
					select 1 from catalogue.offer_observations o where o.source_product_id = p.id
				)) as offers,
			(select count(distinct upper(p.manufacturer_sku))::int from catalogue.source_products p
				where p.manufacturer_sku is not null and ${scope}) as codes,
			(select max(o.observed_at) from catalogue.offer_observations o
				join catalogue.source_products p on p.id = o.source_product_id
				where ${scope}) as observed
	`;
	return row;
}

export async function supplierCoverage(brand: string | null = null, family: string | null = null) {
	return sql<{ supplier: string; products: number; coded: number }[]>`
		select source_id as supplier,
		       count(*)::int as products,
		       count(*) filter (where manufacturer_sku is not null)::int as coded
		from catalogue.source_products
		where ${brandFilter(brand)} and ${familyFilter(family)}
		group by 1
		order by 2 desc
	`;
}

/**
 * Top families by product count, with the tail folded into one "other" slice.
 *
 * The one panel the product-type filter does not scope, and deliberately: this
 * is the panel that shows what the types are, and narrowed to one type it would
 * only ever report 100% of itself.
 */
export async function familyMix(brand: string | null = null, keep = 6) {
	const rows = await sql<{ family: string; products: number }[]>`
		select coalesce(nullif(btrim(family), ''), 'unclassified') as family,
		       count(*)::int as products
		from catalogue.source_products
		where ${brandFilter(brand)}
		group by 1
		order by 2 desc
	`;
	const head = rows.slice(0, keep);
	const tail = rows.slice(keep);
	if (tail.length) {
		head.push({ family: 'other', products: tail.reduce((sum, row) => sum + row.products, 0) });
	}
	return head;
}

/**
 * The families that actually carry a comparable unit price, with the measure
 * they are quoted in and how many offers there are.
 *
 * This is what replaced a hard-coded default of `glaze`. Glaze is the type most
 * suppliers publish a unit price for, so it is what this returns first today -
 * but it returns it because the data says so, and a catalogue that grows a
 * thousand priced clay bodies moves the default without an edit here.
 */
export async function pricedFamilies(brand: string | null = null, minOffers = 5) {
	const rows = await sql<{ family: string; unit: string; offers: number }[]>`
		select family, unit_price_per as unit, count(*)::int as offers
		from catalogue.offer_comparison
		where unit_price > 0
		  and unit_price_per in ('l', 'kg')
		  and quantity is not null
		  and family is not null
		  and ${brandFilter(brand)}
		group by 1, 2
		having count(*) >= ${minOffers}
		order by 3 desc
	`;
	return rows.map((row) => ({
		family: row.family,
		measure: (row.unit === 'kg' ? 'mass' : 'volume') as Measure,
		offers: row.offers
	}));
}

/** Median EUR per litre or per kilogram per supplier, in one band and family. */
export async function medianUnitPrice(
	bandId: string,
	brand: string | null,
	family: string,
	measure: Measure,
	minOffers = 5,
	rates: Rates | null = null
) {
	const chosen = band(measure, bandId);
	const fx = eurRate(rates ?? (await fxRates()));
	return sql<{ supplier: string; median: number; offers: number }[]>`
		with priced as (
			select source_id, family, (unit_price / ${fx})::float8 as unit_price,
			       ${packSize(measure)} as size
			from catalogue.offer_comparison
			-- Strictly positive, not merely present. A 0.00 listing is a
			-- storefront saying "ask us", not a price of nothing, and a median
			-- that counts it reports a shop as cheaper than it is.
			where unit_price > 0
			  and unit_price_per = ${measureUnit(measure)} and quantity is not null
			  and ${fx} is not null
			  and ${brandFilter(brand)}
		)
		select source_id as supplier,
		       (percentile_cont(0.5) within group (order by unit_price))::float8 as median,
		       count(*)::int as offers
		from priced
		where size >= ${chosen.low} and size < ${chosen.high} and family = ${family}
		group by 1
		having count(*) >= ${minOffers}
		order by 2
	`;
}

/** Products several suppliers sell in the same pack band, widest gap first. */
export async function widestSpread(
	bandId: string,
	brand: string | null,
	family: string | null,
	measure: Measure,
	limit = 12,
	minSuppliers = 3,
	rates: Rates | null = null
) {
	const chosen = band(measure, bandId);
	const fx = eurRate(rates ?? (await fxRates()));
	return sql<
		{
			code: string;
			name: string;
			family: string | null;
			suppliers: number;
			low: number;
			high: number;
			ratio: number;
		}[]
	>`
		with priced as (
			select upper(manufacturer_sku) as code, source_id, name, family,
			       (unit_price / ${fx})::float8 as unit_price, ${packSize(measure)} as size
			from catalogue.offer_comparison
			-- As above, and here it is also what keeps the ratio finite: a group
			-- whose cheapest listing is 0.00 divides by zero and reports a null
			-- spread, which is not a wider spread but an absent one.
			where unit_price > 0
			  and unit_price_per = ${measureUnit(measure)} and quantity is not null
			  and ${fx} is not null
			  and ${brandFilter(brand)}
			  and ${familyFilter(family)}
		)
		select code,
		       min(name) as name,
		       min(family) as family,
		       count(distinct source_id)::int as suppliers,
		       min(unit_price)::float8 as low,
		       max(unit_price)::float8 as high,
		       (max(unit_price) / nullif(min(unit_price), 0))::float8 as ratio
		from priced
		where size >= ${chosen.low} and size < ${chosen.high}
		group by code
		having count(distinct source_id) >= ${minSuppliers}
		order by ratio desc, suppliers desc
		limit ${limit}
	`;
}

export type Offer = {
	/** The source_products row, so the compare page can open its detail panel. */
	id: string;
	code: string;
	supplier: string;
	name: string;
	brand: string | null;
	family: string | null;
	url: string;
	price: number;
	currency: string;
	/** The same price at the ECB reference rate; null for an unlisted currency. */
	price_eur: number | null;
	vat_status: string | null;
	availability: string | null;
	stock_quantity: number | null;
	stock_quantity_kind: 'exact' | 'lower_bound' | 'upper_bound' | 'order_limit' | 'unknown';
	quantity: number | null;
	unit: string | null;
	unit_price: number | null;
	unit_price_eur: number | null;
	unit_price_per: string | null;
	/** The pack in the base unit of its own measure: litres, or kilograms. */
	pack_size: number | null;
	observed_at: Date;
};

/**
 * Offers for one manufacturer code, or for the codes whose product name
 * matches. An empty query returns nothing: the dashboard, not this route,
 * is where browsing starts.
 */
export async function searchOffers(query: string, limit = 200, rates: Rates | null = null) {
	const term = query.trim();
	if (!term) return [] as Offer[];
	const pattern = `%${term}%`;
	const fx = eurRate(rates ?? (await fxRates()));
	// Read source_products with its own latest offer rather than the
	// offer_comparison view: the view drops the stock state, and joining it
	// back on name and URL would collapse the sizes of one product together.
	return sql<Offer[]>`
		with matched as (
			select distinct upper(manufacturer_sku) as code
			from catalogue.source_products
			where manufacturer_sku is not null
			  and (upper(manufacturer_sku) = upper(${term})
			       or replace(upper(manufacturer_sku), '-', '') = replace(upper(${term}), '-', '')
			       or name ilike ${pattern})
			limit 40
		)
		select p.id, upper(p.manufacturer_sku) as code, p.source_id as supplier, p.name, p.brand, p.family,
		       p.product_url as url, o.availability,
		       case when o.context_version >= 2 then o.stock_quantity::float8
		            else nullif(p.attributes->>'stock_quantity', '')::float8
		       end as stock_quantity,
		       case when o.context_version >= 2 then o.stock_quantity_kind
		            when nullif(p.attributes->>'stock_quantity', '') is not null then 'exact'
		            else 'unknown'
		       end as stock_quantity_kind,
		       o.price::float8 as price, o.currency, o.vat_status,
		       (o.price / ${fx})::float8 as price_eur,
		       o.quantity::float8 as quantity, o.unit,
		       o.unit_price::float8 as unit_price, o.unit_price_per,
		       (o.unit_price / ${fx})::float8 as unit_price_eur,
		       (${packSizeOfRow})::float8 as pack_size,
		       o.observed_at
		from catalogue.source_products p
		join matched m on m.code = upper(p.manufacturer_sku)
		join lateral (
			select price, currency, vat_status, quantity, unit, unit_price, unit_price_per,
			       availability, stock_quantity, stock_quantity_kind, context_version, observed_at
			from catalogue.offer_observations o
			where o.source_product_id = p.id
			order by o.observed_at desc
			limit 1
		) o on true
		where p.active
		order by code, (o.unit_price / ${fx}) nulls last
		limit ${limit}
	`;
}
