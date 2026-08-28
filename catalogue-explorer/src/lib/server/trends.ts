import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { env } from '$env/dynamic/private';
import { sql } from '$lib/server/db';
import type {
	ProductTrend,
	ProviderTrend,
	StockQuantityKind,
	TrendObservation,
	TrendRange
} from '$lib/trends';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const MAX_OBSERVATIONS_PER_PROVIDER = 500;
const DEFAULT_CONFIG = resolve(process.cwd(), 'config/tracked-products.json');

type TrackedProductConfig = {
	canonical_product_id: string;
	label: string;
	purchase_references: string[];
};

type ProviderRow = {
	canonical_product_id: string;
	canonical_name: string;
	canonical_brand: string | null;
	canonical_sku: string | null;
	source_product_id: string;
	source_id: string;
	source_label: string;
	name: string;
	manufacturer_sku: string | null;
	product_url: string;
	package_label: string | null;
	current_id: number | null;
	current_observed_at: string | null;
	current_last_seen_at: string | null;
	current_price: number | null;
	current_currency: string | null;
	current_quantity: number | null;
	current_unit: string | null;
	current_unit_price: number | null;
	current_unit_price_per: string | null;
	current_availability: string | null;
	current_stock_quantity: number | null;
	current_stock_quantity_kind: StockQuantityKind | null;
};

type ObservationRow = TrendObservation & { source_product_id: string };

function object(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Strict on purpose: a typo must not unexpectedly expose a feature. */
export function trendsEnabled(value = env.CATALOGUE_EXPLORER_TRENDS_ENABLED): boolean {
	if (value === undefined || value === '' || value === 'false') return false;
	if (value === 'true') return true;
	throw new Error('CATALOGUE_EXPLORER_TRENDS_ENABLED must be true or false');
}

export function trendRange(value: string | null, now = new Date()): TrendRange {
	const parsed = Number(value ?? 30);
	const days = ([7, 30, 90, 365] as const).find((candidate) => candidate === parsed) ?? 30;
	const to = new Date(now);
	const from = new Date(to);
	from.setUTCDate(from.getUTCDate() - days);
	return { days, from: from.toISOString(), to: to.toISOString() };
}

export async function trackedProductsConfig(
	path = env.CATALOGUE_TRACKED_PRODUCTS_FILE || DEFAULT_CONFIG
): Promise<TrackedProductConfig[]> {
	let parsed: unknown;
	try {
		parsed = JSON.parse(await readFile(path, 'utf8'));
	} catch (error) {
		throw new Error(`Cannot read tracked-products configuration at ${path}`, { cause: error });
	}
	if (!object(parsed) || !Array.isArray(parsed.products)) {
		throw new Error('Tracked-products configuration must contain a products array');
	}

	const seen = new Set<string>();
	return parsed.products.map((entry, index) => {
		if (!object(entry)) throw new Error(`Tracked product ${index + 1} must be an object`);
		const id = entry.canonical_product_id;
		const label = entry.label;
		if (typeof id !== 'string' || !UUID.test(id)) {
			throw new Error(`Tracked product ${index + 1} has an invalid canonical_product_id`);
		}
		if (seen.has(id)) throw new Error(`Duplicate tracked canonical product: ${id}`);
		seen.add(id);
		if (typeof label !== 'string' || label.trim() === '') {
			throw new Error(`Tracked product ${id} must have a non-empty label`);
		}
		const references = entry.purchase_references ?? [];
		if (
			!Array.isArray(references) ||
			references.some((reference) => typeof reference !== 'string' || reference.trim() === '')
		) {
			throw new Error(`Tracked product ${id} has invalid purchase_references`);
		}
		return {
			canonical_product_id: id,
			label: label.trim(),
			purchase_references: references.map((reference) => reference.trim())
		};
	});
}

function observation(row: ProviderRow): TrendObservation | null {
	if (row.current_id === null || row.current_observed_at === null || row.current_last_seen_at === null) {
		return null;
	}
	return {
		id: row.current_id,
		observed_at: row.current_observed_at,
		last_seen_at: row.current_last_seen_at,
		price: row.current_price!,
		currency: row.current_currency!,
		quantity: row.current_quantity,
		unit: row.current_unit,
		unit_price: row.current_unit_price,
		unit_price_per: row.current_unit_price_per,
		availability: row.current_availability,
		stock_quantity: row.current_stock_quantity,
		stock_quantity_kind: row.current_stock_quantity_kind ?? 'unknown'
	};
}

/**
 * One bounded, deterministic read model for the dashboard. Empty configuration
 * returns before touching PostgreSQL, which lets the feature be deployed before
 * the reviewed product list exists.
 */
export async function productTrends(
	configured: TrackedProductConfig[],
	range: TrendRange
): Promise<ProductTrend[]> {
	if (configured.length === 0) return [];
	const ids = configured.map((entry) => entry.canonical_product_id);

	const providers = await sql<ProviderRow[]>`
		select c.id::text as canonical_product_id, c.name as canonical_name,
		       c.brand as canonical_brand, c.manufacturer_sku as canonical_sku,
		       sp.id::text as source_product_id, sp.source_id,
		       coalesce(s.label, sp.source_id) as source_label,
		       sp.name, sp.manufacturer_sku, sp.product_url,
		       case when current.quantity is not null and current.unit is not null
		            then current.quantity::text || ' ' || current.unit end as package_label,
		       current.id::float8 as current_id, current.observed_at as current_observed_at,
		       current.last_seen_at as current_last_seen_at,
		       current.price::float8 as current_price, current.currency as current_currency,
		       current.quantity::float8 as current_quantity, current.unit as current_unit,
		       current.unit_price::float8 as current_unit_price,
		       current.unit_price_per as current_unit_price_per,
		       current.availability as current_availability,
		       current.stock_quantity::float8 as current_stock_quantity,
		       current.stock_quantity_kind as current_stock_quantity_kind
		from catalogue.canonical_products c
		join catalogue.source_products sp on sp.canonical_product_id = c.id and sp.active
		left join catalogue.sources s on s.id = sp.source_id
		left join lateral (
			select o.* from catalogue.offer_observations o
			where o.source_product_id = sp.id
			order by o.observed_at desc, o.id desc limit 1
		) current on true
		where c.active and c.id = any(${ids}::uuid[])
		order by c.id, sp.source_id, sp.name, sp.id
	`;

	const found = new Set(providers.map((row) => row.canonical_product_id));
	const missing = configured.filter((entry) => !found.has(entry.canonical_product_id));
	if (missing.length) {
		throw new Error(
			`Tracked canonical products are unknown, inactive, or have no active provider variants: ${missing.map((entry) => entry.canonical_product_id).join(', ')}`
		);
	}

	const sourceProductIds = providers.map((row) => row.source_product_id);
	const rows = await sql<ObservationRow[]>`
		select sp.id::text as source_product_id,
		       history.id::float8 as id, history.observed_at, history.last_seen_at,
		       history.price::float8 as price, history.currency,
		       history.quantity::float8 as quantity, history.unit,
		       history.unit_price::float8 as unit_price, history.unit_price_per,
		       history.availability, history.stock_quantity::float8 as stock_quantity,
		       history.stock_quantity_kind
		from catalogue.source_products sp
		join lateral (
			select o.* from catalogue.offer_observations o
			where o.source_product_id = sp.id
			  and o.last_seen_at >= ${range.from}::timestamptz
			  and o.observed_at <= ${range.to}::timestamptz
			order by o.observed_at desc, o.id desc
			limit ${MAX_OBSERVATIONS_PER_PROVIDER + 1}
		) history on true
		where sp.id = any(${sourceProductIds}::uuid[])
		order by sp.id, history.observed_at desc, history.id desc
	`;

	const histories = new Map<string, ObservationRow[]>();
	for (const row of rows) {
		const values = histories.get(row.source_product_id) ?? [];
		values.push(row);
		histories.set(row.source_product_id, values);
	}

	const configById = new Map(configured.map((entry) => [entry.canonical_product_id, entry]));
	const products = new Map<string, ProductTrend>();
	for (const row of providers) {
		let product = products.get(row.canonical_product_id);
		if (!product) {
			product = {
				canonical_product_id: row.canonical_product_id,
				label: configById.get(row.canonical_product_id)!.label,
				canonical_name: row.canonical_name,
				brand: row.canonical_brand,
				manufacturer_sku: row.canonical_sku,
				providers: []
			};
			products.set(row.canonical_product_id, product);
		}
		const newestFirst = histories.get(row.source_product_id) ?? [];
		const provider: ProviderTrend = {
			source_product_id: row.source_product_id,
			source_id: row.source_id,
			source_label: row.source_label,
			name: row.name,
			manufacturer_sku: row.manufacturer_sku,
			product_url: row.product_url,
			package_label: row.package_label,
			current: observation(row),
			history: newestFirst.slice(0, MAX_OBSERVATIONS_PER_PROVIDER).reverse(),
			truncated: newestFirst.length > MAX_OBSERVATIONS_PER_PROVIDER
		};
		product.providers.push(provider);
	}
	return configured.map((entry) => products.get(entry.canonical_product_id)!);
}
