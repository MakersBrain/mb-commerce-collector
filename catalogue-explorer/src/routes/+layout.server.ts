import { trendsEnabled } from '$lib/server/trends';
import type { LayoutServerLoad } from './$types';

export const load: LayoutServerLoad = () => ({ trendsEnabled: trendsEnabled() });
