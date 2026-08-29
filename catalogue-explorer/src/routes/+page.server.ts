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

export const load: PageServerLoad = async ({ url }) => {
	// Normalised before it reaches the cache key: the band ids are shared between
	// the two measures, so resolving against either set rejects anything a query
	// string invented without having to know the measure yet.
	const bandId = band('volume', url.searchParams.get('band') ?? 'medium').id;
	const brand = url.searchParams.get('brand')?.trim() || null;
	const family = url.searchParams.get('family')?.trim() || null;

	// The filter row scopes every panel below it, so the numbers always agree.
	// The one exception is the family mix, which is the panel that shows what the
	// product types are; see familyMix.
	const rates = await fxRates();
	const build = async () => {
		// The price panels compare like with like, so they sit inside one product
		// type and one measure. Which measure is a property of the type, not a
		// setting: a glaze is sold by the litre and a clay body by the kilogram,
		// and the catalogue is asked which rather than told.
		const priced = await pricedFamilies(brand);
		const chosen = family ? priced.find((row) => row.family === family) : priced[0];
		const measure: Measure = chosen?.measure ?? 'volume';
		const pricedFamily = chosen?.family ?? family;
		const size = band(measure, bandId);

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
			bandId: size.id,
			bands: BANDS[measure],
			bandLabel: size.label,
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
		brand === null && family === null ? await stable(`homepage:${bandId}`, build) : await build();

	return {
		...loaded,
		brand,
		family,
		fx: { date: rates.date, stale: rates.stale }
	};
};
