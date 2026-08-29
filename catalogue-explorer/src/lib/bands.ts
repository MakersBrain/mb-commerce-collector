/**
 * Pack-size bands, per measure.
 *
 * A 59 ml jar always costs more per litre than a 473 ml pot, so every
 * supplier-to-supplier comparison in this app happens inside one band. The same
 * is true by weight: a 1 kg tub of stain is dearer per kilogram than a 25 kg
 * sack of feldspar, and for four of the seven families the catalogue carries -
 * clay bodies, raw materials, oxides and stains - weight is the only unit a
 * storefront publishes.
 *
 * So the bands come in two sets that share their ids. The id survives a change
 * of family in the URL: choosing clay after glaze keeps you in the middle band
 * rather than resetting to the first one, and only the label changes.
 *
 * Shared between the SQL that aggregates and the pages that group offers, so
 * the two never drift apart.
 */

/** What a price is quoted against. `unit_price_per` is 'l' or 'kg' in the schema. */
export type Measure = 'volume' | 'mass';

export type Band = { id: string; label: string; low: number; high: number };

/** Litres for volume, kilograms for mass: the base unit each band is cut in. */
export const BANDS: Record<Measure, Band[]> = {
	volume: [
		{ id: 'small', label: 'under 150 ml', low: 0, high: 0.15 },
		{ id: 'medium', label: '150 ml - 600 ml', low: 0.15, high: 0.6 },
		{ id: 'large', label: '0.6 L - 2 L', low: 0.6, high: 2 },
		{ id: 'bulk', label: 'over 2 L', low: 2, high: 1e9 }
	],
	// Cut where the trade actually packs: retail tubs under a kilogram, the
	// 1-5 kg dry-material bag, the 10 and 12.5 kg clay box, and the 25 kg sack.
	mass: [
		{ id: 'small', label: 'under 1 kg', low: 0, high: 1 },
		{ id: 'medium', label: '1 kg - 5 kg', low: 1, high: 5 },
		{ id: 'large', label: '5 kg - 15 kg', low: 5, high: 15 },
		{ id: 'bulk', label: 'over 15 kg', low: 15, high: 1e9 }
	]
};

/** The measure a stored `unit_price_per` names, or null for anything else. */
export function measureOf(unitPricePer: string | null | undefined): Measure | null {
	if (unitPricePer === 'l') return 'volume';
	if (unitPricePer === 'kg') return 'mass';
	return null;
}

/** The `unit_price_per` value a measure is stored under. */
export function measureUnit(measure: Measure): 'l' | 'kg' {
	return measure === 'mass' ? 'kg' : 'l';
}

/** How a price per that measure is written to a reader: "EUR/L", "per kg". */
export function unitLabel(measure: Measure): 'L' | 'kg' {
	return measure === 'mass' ? 'kg' : 'L';
}

/** The measure spelled out, for a sentence rather than a column head. */
export function measureWord(measure: Measure): 'litre' | 'kilogram' {
	return measure === 'mass' ? 'kilogram' : 'litre';
}

export function band(measure: Measure, id: string): Band {
	const set = BANDS[measure];
	return set.find((entry) => entry.id === id) ?? set[1];
}

export function bandOf(measure: Measure, size: number | null | undefined): Band | null {
	if (size == null) return null;
	return BANDS[measure].find((entry) => size >= entry.low && size < entry.high) ?? null;
}
