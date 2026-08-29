import { error } from '@sveltejs/kit';
import {
	productTrends,
	trackedProductsConfig,
	trendRange,
	trendsEnabled
} from '$lib/server/trends';
import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ params, url }) => {
	if (!trendsEnabled()) error(404, 'Price and stock trends are not enabled');

	const configured = await trackedProductsConfig();
	const tracked = configured.find((product) => product.canonical_product_id === params.id);
	if (!tracked) error(404, 'No such tracked product');

	const range = trendRange(url.searchParams.get('days'));
	const [product] = await productTrends([tracked], range);
	if (!product) error(404, 'No such tracked product');

	const requestedProvider = url.searchParams.get('provider');
	const provider = product.providers.some(
		(entry) => entry.source_product_id === requestedProvider
	)
		? requestedProvider
		: null;

	return { product, provider, range };
};
