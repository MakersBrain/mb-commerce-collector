import { BANDS, band, measureWord, unitLabel, type Measure } from '$lib/bands';
import {
	brands,
	families,
	familyMix,
	medianUnitPrice,
	overview,
	pricedFamilies,
	supplierCoverage,
	widestSpread
} from '$lib/server/queries';
import { fxRates } from '$lib/server/fx';
import { stable } from '$lib/server/cache';
import type { PageServerLoad } from './$types';

/**
 * The size control carries the measure as well as the band: `mass:large` is a
 * 12.5 kg box and `volume:large` is a two-litre tub, and a catalogue where the
 * same clay body is sold both ways has to let a reader say which they meant.
 * The measure is only ever one of the two the data actually holds for that
 * family, so an invented query string lands on the family's busiest one.
 */
function parseBand(raw: string | null, available: Measure[]) {
	const [first, second] = (raw ?? '').split(':');
	const measure = available.find((entry) => entry === first) ?? available[0] ?? 'volume';
	return { measure, id: band(measure, second ?? first ?? 'medium').id };
}

/**
 * The same value reduced to one of the eight strings it can legitimately be,
 * before it reaches a cache key. Which measure is available needs the database,
 * but which strings exist does not, and an unfiltered query parameter in a key
 * is an unbounded cache.
 */
function canonical(raw: string | null) {
	const [first, second] = (raw ?? '').split(':');
	const measure: Measure = first === 'mass' ? 'mass' : 'volume';
	return `${measure}:${band(measure, second ?? first ?? 'medium').id}`;
}

export const load: PageServerLoad = async ({ url }) => {
	const requested = url.searchParams.get('band');
	const brand = url.searchParams.get('brand')?.trim() || null;
	const family = url.searchParams.get('family')?.trim() || null;

	// The filter row scopes every panel below it, so the numbers always agree.
	// The one exception is the family mix, which is the panel that shows what the
	// product types are; see familyMix.
	const rates = await fxRates();
	const build = async () => {
		// The price panels compare like with like, so they sit inside one product
		// type and one measure. Which measures exist is asked of the data rather
		// than assumed: a glaze is usually sold by the litre and a clay body by
		// the kilogram, and plenty of both are sold the other way.
		const priced = await pricedFamilies(brand);
		const pricedFamily = family ?? priced[0]?.family ?? null;
		const available = priced
			.filter((row) => row.family === pricedFamily)
			.map((row) => row.measure);
		const { measure, id } = parseBand(requested, available);
		const size = band(measure, id);

		const [totals, suppliers, mix, medians, spread, brandOptions, familyOptions] =
			await Promise.all([
				overview(brand, family),
				supplierCoverage(brand, family),
				familyMix(brand),
				pricedFamily
					? medianUnitPrice(size.id, brand, pricedFamily, measure, 5, rates)
					: Promise.resolve([]),
				widestSpread(size.id, brand, family, measure, 12, 3, rates),
				brands(),
				families()
			]);

		return {
			totals,
			suppliers,
			families: mix,
			medians,
			spread,
			bandLabel: size.label,
			/** What the select submits: the measure and the band, together. */
			bandValue: `${measure}:${size.id}`,
			/** Only the measures this family is actually priced in. */
			bandOptions: (available.length ? available : (['volume'] as Measure[])).map((entry) => ({
				measure: entry,
				label: entry === 'mass' ? 'by weight' : 'by volume',
				bands: BANDS[entry]
			})),
			brandOptions,
			familyOptions,
			/** What the two price panels are actually about, chosen or defaulted. */
			pricedFamily,
			measure,
			/** "L" or "kg", and "litre" or "kilogram", for the panel copy. */
			unit: unitLabel(measure),
			measureWord: measureWord(measure)
		};
	};

	const loaded =
		brand === null && family === null
			? await stable(`homepage:${canonical(requested)}`, build)
			: await build();

	return {
		...loaded,
		brand,
		family,
		fx: { date: rates.date, stale: rates.stale }
	};
};
