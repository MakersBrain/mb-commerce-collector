/**
 * What a catalogue row is, said once for both sides of the wire.
 *
 * These types used to live beside the queries in $lib/server/explore, which
 * works right up until a component imports one: SvelteKit types every
 * `$lib/server/*` import from client code as `never`, so the component compiles
 * against nothing and every field access silently passes. The shapes therefore
 * live here, where the browser is allowed to know them, and the query module
 * imports them like anybody else.
 */

export type Product = {
	id: string;
	supplier: string;
	/** The shop's own name for itself, falling back to its catalogue id. */
	supplier_label: string;
	/** ISO 3166-1 alpha-2, or null for a supplier nobody has placed yet. */
	country: string | null;
	name: string;
	brand: string | null;
	family: string | null;
	code: string | null;
	url: string;
	image_url: string | null;
	colour: string | null;
	surface: string | null;
	min_celsius: number | null;
	max_celsius: number | null;
	cone_min: string | null;
	cone_max: string | null;
	price: number | null;
	currency: string | null;
	/** The same figures at the ECB reference rate, for sorting and comparison. */
	price_eur: number | null;
	unit_price_eur: number | null;
	quantity: number | null;
	unit: string | null;
	unit_price: number | null;
	unit_price_per: string | null;
	availability: string | null;
	stock_quantity: number | null;
	application_methods: string[] | null;
	form: string | null;
	/** How much product is in the container, as the supplier published it. */
	size_value: number | null;
	size_unit: string | null;
	/** 'volume' or 'weight' - a pint of dipping glaze, a kilo of dry mix. */
	size_dimension: string | null;
	/** The importer's metric normalisation of the two, for non-metric units. */
	size_ml: number | null;
	size_g: number | null;
};

/**
 * What the detail panel needs to draw its header before the fetch lands.
 *
 * A subset rather than the whole `Product`, so a page holding something else —
 * /compare holds `Offer`, which is a different shape for a different job — can
 * open the panel without inventing thirty fields it has no answer for.
 */
export type ProductSeed = Pick<
	Product,
	'id' | 'name' | 'url' | 'brand' | 'code' | 'supplier_label' | 'country' | 'image_url'
>;

/**
 * One reading of a shop's offer, at one moment. Typed, unlike the product and
 * source beside it, because these columns are this codebase's own: they come
 * from a fixed select over `catalogue.offer_observations`, not from whatever a
 * storefront chose to publish, and the charts have to do arithmetic on them.
 */
export type Observation = {
	observed_at: string;
	price: number | null;
	currency: string | null;
	price_text: string | null;
	vat_status: string | null;
	quantity: number | null;
	unit: string | null;
	unit_price: number | null;
	unit_price_per: string | null;
	availability: string | null;
	stock_quantity: number | null;
	stock_quantity_kind: 'exact' | 'lower_bound' | 'upper_bound' | 'order_limit' | 'unknown';
	context_version: number;
	attributes: Record<string, unknown> | null;
};

/**
 * The whole imported record for one product, as the detail panel shows it.
 * The product and the source are deliberately untyped past the top level: the
 * point of the panel is to render whatever a given storefront published, and a
 * schema here would only describe the suppliers that happened to exist when it
 * was written.
 */
export type ProductDetail = {
	product: Record<string, unknown> | null;
	source: Record<string, unknown> | null;
	offers: Observation[];
};

/**
 * Sortable columns, as an allowlist: the sort key arrives in the URL, so it
 * indexes a fixed table rather than reaching a query as text. The query module
 * maps every one of these to an expression, and the compiler holds it to that.
 */
export const SORT_KEYS = [
	'name',
	'stock',
	'code',
	'supplier',
	'country',
	'brand',
	'family',
	'colour',
	'surface',
	'firing',
	'application',
	'size',
	'form',
	'price',
	'unit_price'
] as const;

export type SortKey = (typeof SORT_KEYS)[number];

export type Sort = { key: SortKey; dir: 'asc' | 'desc' };

export function readSort(value: string | null): SortKey {
	return SORT_KEYS.includes(value as SortKey) ? (value as SortKey) : 'name';
}
