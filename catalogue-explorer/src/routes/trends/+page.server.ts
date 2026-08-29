import { error } from '@sveltejs/kit';
import {
	productTrends,
	trackedProductsConfig,
	trendRange,
	trendsEnabled
} from '$lib/server/trends';
import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ url }) => {
	if (!trendsEnabled()) error(404, 'Price and stock trends are not enabled');
	const range = trendRange(url.searchParams.get('days'));
	// The overview always shows both 7- and 30-day movement. A shorter chart
	// range must not make the 30-day column silently change meaning.
	const queryRange = trendRange(String(Math.max(range.days, 30)));
	const configured = await trackedProductsConfig();
	const products = await productTrends(configured, queryRange);

	return { products, range, configEmpty: configured.length === 0 };
};
