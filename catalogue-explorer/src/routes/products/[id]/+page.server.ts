import { error } from '@sveltejs/kit';
import { productDetail } from '$lib/server/explore';
import type { PageServerLoad } from './$types';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export const load: PageServerLoad = async ({ params }) => {
	if (!UUID.test(params.id)) error(404, 'No such product');
	const detail = await productDetail(params.id);
	if (!detail.product) error(404, 'No such product');
	return { detail };
};
