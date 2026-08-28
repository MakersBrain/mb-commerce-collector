import type { Observation, Product, ProductDetail, Sort, SortKey } from '$lib/catalogue';
import { sql } from './db';
import { eurRate, fxRates, type Rates } from './fx';
import { stable } from './cache';

/**
 * Faceted browse over catalogue.source_products.
 *
 * Everything except family, brand and supplier lives in the importer's
 * `attributes` jsonb rather than in a promoted column, so the facets read out
 * of the document. Coverage is uneven by nature — a storefront that never
 * publishes a surface leaves that facet null — so every facet count is shown
 * and "unspecified" is never silently folded into a value.
 */

export type FiringBand = { id: string; label: string; min: number; max: number };

export const FIRING_BANDS: FiringBand[] = [
	{ id: 'low', label: 'low fire - under 1100 C (around cone 06-04)', min: 0, max: 1100 },
	{ id: 'mid', label: 'mid fire - 1100 to 1240 C (around cone 4-6)', min: 1100, max: 1240 },
	{ id: 'high', label: 'high fire - 1240 C and up (around cone 7-10)', min: 1240, max: 100000 }
];

export type Filters = {
	q: string;
	family: string | null;
	brand: string | null;
	series: string | null;
	colour: string | null;
	surface: string | null;
	firing: string | null;
	/** brushing, dipping, pouring, spraying - as published by the supplier. */
	application: string | null;
	/** liquid (ready to use) or powder (a dry mix to be made up). */
	form: string | null;
	/** 'in' keeps only what a supplier currently has on the shelf. */
	stock: string | null;
	/** Suppliers to keep. Empty means all of them, not none of them. */
	suppliers: string[];
	/**
	 * Suppliers to drop, applied after the include list. Kept apart from it
	 * because "everyone except these two" is the question people actually ask,
	 * and it cannot be written as a list of the other eighteen that stays right
	 * when a nineteenth supplier is imported.
	 */
	excluded: string[];
	/** ISO 3166-1 alpha-2 codes of the countries suppliers ship from. */
	countries: string[];
};

export const EMPTY: Filters = {
	q: '',
	family: null,
	brand: null,
	series: null,
	colour: null,
	surface: null,
	firing: null,
	application: null,
	form: null,
	stock: null,
	suppliers: [],
	excluded: [],
	countries: []
};

/**
 * A product line (Celadon, Cosmos, Potter's Choice, Stoneware, Foundations)
 * is not a published field anywhere. What every retailer does keep is the
 * manufacturer's code, and the letters in front of the number ARE the line:
 * PC-40 is Potter's Choice, C-10 is Celadon, SW-104 is Mayco Stoneware. So the
 * line is derived from the code prefix, which is why it survives the trip from
 * the manufacturer to a French or Dutch storefront that never names the series.
 */
const seriesPrefix = sql`upper((regexp_match(p.manufacturer_sku, '^([A-Za-z]+)'))[1])`;

/**
 * The human name for each prefix is harvested from the manufacturers that do
 * publish it, as "(PC) Potter's Choice" category entries. Nothing is invented:
 * a prefix nobody labels is shown as the bare prefix.
 */
const seriesLabels = sql`
	select distinct
		upper((regexp_match(cat, '^\\(([A-Z]+)\\)\\s*(.+)$'))[1]) as prefix,
		(regexp_match(cat, '^\\(([A-Z]+)\\)\\s*(.+)$'))[2] as label
	from (
		select jsonb_array_elements_text(attributes->'category_path') as cat
		from catalogue.source_products
		where jsonb_typeof(attributes->'category_path') = 'array'
	) paths
	where cat ~ '^\\([A-Z]+\\)'
`;

export function readFilters(params: URLSearchParams): Filters {
	const get = (key: string) => {
		const value = params.get(key);
		return value && value.trim() ? value.trim() : null;
	};
	/**
	 * Repeated params (?country=FR&country=IT) or one comma-separated list, so a
	 * hand-shortened URL works as well as one the checkboxes produced.
	 */
	const list = (key: string, upper = false) => {
		const seen = new Set<string>();
		for (const raw of params.getAll(key)) {
			for (const part of raw.split(',')) {
				const value = part.trim();
				if (value) seen.add(upper ? value.toUpperCase() : value);
			}
		}
		return [...seen];
	};
	return {
		q: params.get('q')?.trim() ?? '',
		family: get('family'),
		brand: get('brand'),
		series: get('series'),
		colour: get('colour'),
		surface: get('surface'),
		firing: get('firing'),
		application: get('application'),
		form: get('form'),
		stock: get('stock'),
		suppliers: list('supplier'),
		excluded: list('no_supplier'),
		countries: list('country', true)
	};
}

const TRUE = sql`true`;

/**
 * The WHERE clause for one facet's own count leaves that facet out, so a
 * dropdown always shows the alternatives the reader could switch to rather
 * than only the one they already picked. The supplier panel leaves out both of
 * its own lists, so an excluded supplier can still be found and put back.
 */
function conditions(filters: Filters, ...exclude: (keyof Filters)[]) {
	const parts = [];
	const use = (key: keyof Filters) => {
		if (exclude.includes(key)) return false;
		const value = filters[key];
		return Array.isArray(value) ? value.length > 0 : Boolean(value);
	};

	if (use('q')) parts.push(sql`p.name ilike ${'%' + filters.q + '%'}`);
	if (use('family')) parts.push(sql`p.family = ${filters.family}`);
	if (use('brand')) parts.push(sql`upper(p.brand) = ${filters.brand!.toUpperCase()}`);
	if (use('series')) parts.push(sql`${seriesPrefix} = ${filters.series!.toUpperCase()}`);
	if (use('colour')) parts.push(sql`p.facet_colour = ${filters.colour}`);
	if (use('surface')) parts.push(sql`p.facet_surface = ${filters.surface}`);
	// application_methods is a jsonb array, so membership rather than equality.
	if (use('application'))
		parts.push(sql`p.facet_application_methods ? ${filters.application!}`);
	if (use('form')) parts.push(sql`p.facet_form = ${filters.form}`);
	if (use('stock')) parts.push(sql`p.availability = 'https://schema.org/InStock'`);
	if (use('suppliers')) parts.push(sql`p.source_id = any(${filters.suppliers})`);
	if (use('excluded')) parts.push(sql`p.source_id <> all(${filters.excluded})`);
	// Written against catalogue.sources rather than as a join, so that adding a
	// country filter needs no change to the six queries that call this.
	if (use('countries')) {
		parts.push(
			sql`p.source_id in (
				select id from catalogue.sources
				where metadata->>'country' = any(${filters.countries})
			)`
		);
	}
	if (use('firing')) {
		const chosen = FIRING_BANDS.find((entry) => entry.id === filters.firing);
		if (chosen) {
			parts.push(
				sql`p.facet_firing_max >= ${chosen.min} and p.facet_firing_max < ${chosen.max}`
			);
		}
	}

	return parts.length ? parts.reduce((left, right) => sql`${left} and ${right}`) : TRUE;
}

export type Facet = { value: string; label: string; products: number };

async function facet(filters: Filters, key: keyof Filters, value: ReturnType<typeof sql>, limit = 30) {
	return sql<Facet[]>`
		select ${value} as value, ${value} as label, count(*)::int as products
		from catalogue.source_products p
		where ${conditions(filters, key)} and ${value} is not null
		group by 1
		order by 3 desc
		limit ${limit}
	`;
}

/**
 * Both supplier lists are left out of their own counts: the panel has to show
 * every supplier, including the ones currently excluded, or there would be no
 * way back from an exclusion.
 */
async function supplierFacet(filters: Filters) {
	return sql<Facet[]>`
		select p.source_id as value,
		       coalesce(s.label, p.source_id) as label,
		       count(*)::int as products
		from catalogue.source_products p
		left join catalogue.sources s on s.id = p.source_id
		where ${conditions(filters, 'suppliers', 'excluded')}
		group by 1, 2
		order by 3 desc
	`;
}

/** Counted over suppliers, so a country with no importer never appears. */
async function countryFacet(filters: Filters) {
	return sql<Facet[]>`
		select s.metadata->>'country' as value,
		       s.metadata->>'country' as label,
		       count(*)::int as products
		from catalogue.source_products p
		join catalogue.sources s on s.id = p.source_id
		where ${conditions(filters, 'countries')} and s.metadata->>'country' is not null
		group by 1
		order by 3 desc
	`;
}

async function uncachedFacets(filters: Filters) {
	const [family, brand, series, colour, surface, supplier, application, form, firing, country] =
		await Promise.all([
		facet(filters, 'family', sql`p.family`),
		// One brand, two spellings ("Mayco" / "MAYCO"): keyed on the upper-case
		// form, labelled with the spelling that appears most often.
		sql<Facet[]>`
			select upper(p.brand) as value,
			       mode() within group (order by p.brand) as label,
			       count(*)::int as products
			from catalogue.source_products p
			where ${conditions(filters, 'brand')} and p.brand is not null
			group by 1
			order by 3 desc
			limit 40
		`,
		sql<Facet[]>`
			with labels as (${seriesLabels}),
			counted as (
				select ${seriesPrefix} as value, count(*)::int as products
				from catalogue.source_products p
				where ${conditions(filters, 'series')} and p.manufacturer_sku is not null
				group by 1
			)
			select c.value,
			       case when l.label is null then c.value else c.value || ' - ' || l.label end as label,
			       c.products
			from counted c
			left join labels l on l.prefix = c.value
			where c.value is not null
			order by c.products desc
			limit 30
		`,
		facet(filters, 'colour', sql`p.facet_colour`, 40),
		facet(filters, 'surface', sql`p.facet_surface`),
		supplierFacet(filters),
		// How the glaze is put on the pot. Published as an array, so a product
		// that can be brushed and dipped counts under both.
		sql<Facet[]>`
			select method as value, method as label, count(*)::int as products
			from catalogue.source_products p
			cross join lateral jsonb_array_elements_text(
				coalesce(p.facet_application_methods, '[]'::jsonb)
			) as method
			where ${conditions(filters, 'application')}
			group by 1
			order by 3 desc
			limit 12
		`,
		facet(filters, 'form', sql`p.facet_form`, 8),
		sql<Facet[]>`
			select case
				when p.facet_firing_max < 1100 then 'low'
				when p.facet_firing_max < 1240 then 'mid'
				else 'high'
			end as value,
			'' as label,
			count(*)::int as products
			from catalogue.source_products p
			where ${conditions(filters, 'firing')}
			  and p.facet_firing_max is not null
			group by 1
		`,
		countryFacet(filters)
	]);

	return {
		family,
		brand,
		series,
		colour,
		surface,
		supplier,
		country,
		application,
		// "powder" is the dry mix a potter makes up; "liquid" is ready to use.
		form: form.map((row) => ({
			...row,
			label: row.value === 'powder' ? 'powder (dry mix)' : row.value
		})),
		firing: FIRING_BANDS.map((entry) => ({
			value: entry.id,
			label: entry.label,
			products: firing.find((row) => row.value === entry.id)?.products ?? 0
		}))
	};
}

function unfiltered(filters: Filters) {
	return Object.values(filters).every((value) =>
		Array.isArray(value) ? value.length === 0 : value === null || value === ''
	);
}

export async function facets(filters: Filters) {
	return unfiltered(filters)
		? stable('explore:facets:unfiltered', () => uncachedFacets(filters))
		: uncachedFacets(filters);
}


/**
 * The expression each sortable column orders by. Typed as a total map of
 * SortKey, so adding a key to the shared allowlist without giving it an
 * expression here is a compile error rather than a query that silently
 * falls back to sorting by name.
 */
const SORTS: Record<SortKey, ReturnType<typeof sql>> = {
	name: sql`p.name`,
	stock: sql`p.availability`,
	code: sql`p.manufacturer_sku`,
	supplier: sql`p.source_id`,
	country: sql`(select metadata->>'country' from catalogue.sources where id = p.source_id)`,
	brand: sql`p.brand`,
	family: sql`p.family`,
	colour: sql`p.facet_colour`,
	surface: sql`p.facet_surface`,
	firing: sql`p.facet_firing_max`,
	// The published array as text, so the order the cell shows is the order it
	// sorts by: everything brushable together, then dipping, then pouring.
	application: sql`p.facet_application_methods::text`,
	// Volumes and masses on one scale. Glazes and slips are close enough to
	// water that a millilitre and a gram rank a container the same way, which is
	// what this sort is for - putting the small pots before the buckets.
	size: sql`p.facet_package_size`,
	form: sql`p.facet_form`,
	price: sql`price_eur`,
	unit_price: sql`unit_price_eur`
};


/**
 * One page of products, without the matching total. The infinite-scroll table
 * asks for a block at a time and already knows the total from the page load, so
 * counting 27k rows again on every scroll would be work nobody reads.
 */
export async function productRows(
	filters: Filters,
	limit = 60,
	offset = 0,
	sort: Sort = { key: 'name', dir: 'asc' },
	rates: Rates | null = null
) {
	const where = conditions(filters);
	const fx = eurRate(rates ?? (await fxRates()));
	// Nulls last in both directions: a product with no price is not the cheapest
	// one, and it is not the dearest one either.
	const order =
		sort.dir === 'desc'
			? sql`${SORTS[sort.key]} desc nulls last`
			: sql`${SORTS[sort.key]} asc nulls last`;
	return sql<Product[]>`
			select p.id, p.source_id as supplier,
			       coalesce(src.label, p.source_id) as supplier_label,
			       src.metadata->>'country' as country,
			       p.name, p.brand, p.family,
			       p.manufacturer_sku as code, p.product_url as url, p.image_url,
			       p.attributes->'colour'->>'name' as colour,
			       p.attributes->>'surface' as surface,
			       (p.attributes->'firing'->>'min_celsius')::float8 as min_celsius,
			       (p.attributes->'firing'->>'max_celsius')::float8 as max_celsius,
			       p.attributes->'firing'->>'cone_min' as cone_min,
			       p.attributes->'firing'->>'cone_max' as cone_max,
			       o.price::float8 as price, o.currency,
			       (o.price / ${fx})::float8 as price_eur,
			       (o.unit_price / ${fx})::float8 as unit_price_eur,
			       o.quantity::float8 as quantity, o.unit,
			       o.unit_price::float8 as unit_price, o.unit_price_per,
			       p.availability,
			       (p.attributes->>'stock_quantity')::int as stock_quantity,
			       case when jsonb_typeof(p.attributes->'application_methods') = 'array'
			            then array(select jsonb_array_elements_text(p.attributes->'application_methods'))
			       end as application_methods,
			       p.attributes->>'form' as form,
			       (p.attributes->'package_size'->>'value')::float8 as size_value,
			       p.attributes->'package_size'->>'unit' as size_unit,
			       p.attributes->'package_size'->>'dimension' as size_dimension,
			       (p.attributes->'package_size'->>'millilitres')::float8 as size_ml,
			       (p.attributes->'package_size'->>'grams')::float8 as size_g
			from catalogue.source_products p
			left join catalogue.sources src on src.id = p.source_id
			left join lateral (
				select price, currency, quantity, unit, unit_price, unit_price_per
				from catalogue.offer_observations o
				where o.source_product_id = p.id
				order by o.observed_at desc
				limit 1
			) o on true
			where ${where}
			order by ${order}, p.name, p.source_id
			limit ${limit} offset ${offset}
	`;
}


/**
 * Everything held about one product, for the detail panel.
 *
 * The promoted columns and the whole `attributes` document both come back
 * untouched. The panel's job is to show what was actually imported, including
 * the fields no facet reads and the ones a given storefront never filled in, so
 * filtering the document here would defeat the point of opening it.
 *
 * The offers are the full observed history rather than the latest one. A price
 * that moved is the most interesting thing the catalogue knows about a product,
 * and it is invisible everywhere else in the app.
 */
export async function productDetail(id: string): Promise<ProductDetail> {
	const [[product], offers] = await Promise.all([
		sql<Record<string, unknown>[]>`
			select p.*, src.label as source_label, src.homepage_url as source_homepage,
			       src.metadata as source_metadata
			from catalogue.source_products p
			left join catalogue.sources src on src.id = p.source_id
			where p.id = ${id}
		`,
		sql<Observation[]>`
			select observed_at, price::float8 as price, currency, price_text, vat_status,
			       quantity::float8 as quantity, unit,
			       unit_price::float8 as unit_price, unit_price_per,
			       availability, stock_quantity::float8 as stock_quantity,
			       stock_quantity_kind, context_version, attributes
			from catalogue.offer_observations
			where source_product_id = ${id}
			order by observed_at desc, id desc
			limit 200
		`
	]);
	if (!product) return { product: null, source: null, offers: [] };

	// The joined source columns are lifted out of the product row so the panel
	// can show "the shop" and "the product" as the two separate things they are.
	const { source_label, source_homepage, source_metadata, ...rest } = product;
	return {
		product: rest,
		source: {
			id: rest.source_id,
			label: source_label ?? rest.source_id,
			homepage_url: source_homepage,
			...(source_metadata as Record<string, unknown> | null)
		},
		offers
	};
}

/** The same page, plus how many rows the filters match in total. */
export async function products(
	filters: Filters,
	limit = 60,
	offset = 0,
	sort: Sort = { key: 'name', dir: 'asc' },
	rates: Rates | null = null
) {
	const [rows, [{ total }]] = await Promise.all([
		productRows(filters, limit, offset, sort, rates),
		sql<{ total: number }[]>`
			select count(*)::int as total
			from catalogue.source_products p
			where ${conditions(filters)}
		`
	]);
	return { rows, total };
}

/**
 * A shop, and what it turns out to be carrying.
 *
 * Assembled here rather than read from a view because none of it is stored: the
 * catalogue holds products, and "who is this supplier" is a question about the
 * shape of the rows they contributed. Every figure is therefore a count over
 * `source_products`, which also means every figure is as current as the last
 * successful crawl and no more — hence `last_seen`, which says when that was.
 *
 * Coverage is the part worth reading. A shop that publishes a firing schedule
 * and one that publishes a price and a photograph are both in this catalogue,
 * and the difference between them decides what can be asked of their rows.
 */
export type SupplierTotals = {
	products: number;
	active: number;
	brands: number;
	with_code: number;
	observations: number;
	first_seen: string | null;
	last_seen: string | null;
};

export type SupplierDetail = {
	source: Record<string, unknown> | null;
	totals: SupplierTotals | null;
	families: Facet[];
	brands: Facet[];
	coverage: { field: string; products: number }[];
	currencies: { value: string; products: number }[];
};

export async function supplierDetail(id: string): Promise<SupplierDetail> {
	const [[source], [totals], families, brands, [coverage], currencies] = await Promise.all([
		sql<Record<string, unknown>[]>`
			select id, label, homepage_url, created_at, updated_at, metadata
			from catalogue.sources where id = ${id}
		`,
		sql<SupplierTotals[]>`
			select count(*)::int as products,
			       count(*) filter (where active)::int as active,
			       count(distinct brand)::int as brands,
			       count(manufacturer_sku)::int as with_code,
			       (select count(*)::int from catalogue.offer_observations o
			         join catalogue.source_products sp on sp.id = o.source_product_id
			        where sp.source_id = ${id}) as observations,
			       min(first_seen_at) as first_seen,
			       max(last_seen_at) as last_seen
			from catalogue.source_products where source_id = ${id}
		`,
		sql<Facet[]>`
			select family as value, family as label, count(*)::int as products
			from catalogue.source_products
			where source_id = ${id} and family is not null
			group by 1 order by 3 desc limit 12
		`,
		sql<Facet[]>`
			select brand as value, brand as label, count(*)::int as products
			from catalogue.source_products
			where source_id = ${id} and brand is not null and brand <> ''
			group by 1 order by 3 desc limit 12
		`,
		// One row of counts rather than a row per field: the panel reads them as
		// a set of bars against the same total, so they have to come from one
		// scan of the same rows or they would not add up against each other.
		sql<Record<string, number>[]>`
			select count(*) filter (where manufacturer_sku is not null)::int as "manufacturer code",
			       count(*) filter (where brand is not null and brand <> '')::int as brand,
			       count(*) filter (where image_url is not null)::int as image,
			       count(*) filter (where description is not null and description <> '')::int as description,
			       count(*) filter (where firing_range is not null)::int as "firing range",
			       count(*) filter (where family is not null)::int as family,
			       count(*) filter (where availability is not null)::int as "stock state"
			from catalogue.source_products where source_id = ${id}
		`,
		sql<{ value: string; products: number }[]>`
			select o.currency as value, count(distinct sp.id)::int as products
			from catalogue.offer_observations o
			join catalogue.source_products sp on sp.id = o.source_product_id
			where sp.source_id = ${id} and o.currency is not null
			group by 1 order by 2 desc limit 6
		`
	]);

	if (!source) return { source: null, totals: null, families: [], brands: [], coverage: [], currencies: [] };

	return {
		source,
		totals: totals ?? null,
		families,
		brands,
		coverage: Object.entries(coverage ?? {})
			.map(([field, products]) => ({ field, products: Number(products) }))
			.sort((a, b) => b.products - a.products),
		currencies
	};
}
