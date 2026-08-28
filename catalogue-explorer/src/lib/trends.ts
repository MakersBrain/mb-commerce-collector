export type StockQuantityKind =
	| 'exact'
	| 'lower_bound'
	| 'upper_bound'
	| 'order_limit'
	| 'unknown';

export type TrendObservation = {
	id: number;
	observed_at: string;
	last_seen_at: string;
	price: number;
	currency: string;
	quantity: number | null;
	unit: string | null;
	unit_price: number | null;
	unit_price_per: string | null;
	availability: string | null;
	stock_quantity: number | null;
	stock_quantity_kind: StockQuantityKind;
};

export type ProviderTrend = {
	source_product_id: string;
	source_id: string;
	source_label: string;
	name: string;
	manufacturer_sku: string | null;
	product_url: string;
	package_label: string | null;
	current: TrendObservation | null;
	history: TrendObservation[];
	truncated: boolean;
};

export type ProductTrend = {
	canonical_product_id: string;
	label: string;
	canonical_name: string;
	brand: string | null;
	manufacturer_sku: string | null;
	providers: ProviderTrend[];
};

export type TrendRange = {
	days: 7 | 30 | 90 | 365;
	from: string;
	to: string;
};
