<script lang="ts">
	import { productSlug } from '$lib/product-slug.js';
	import EmptyState from '$lib/components/ui/empty-state/empty-state.svelte';
	import { Metric, PageHeader } from '@makersbrain/ui/svelte';
	import { Card, CardContent, CardHeader, CardTitle } from '$lib/components/ui/card';
	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$lib/components/ui/table';
	import NativeSelect from '$lib/components/ui/native-select/native-select.svelte';
	import type { ProductTrend, ProviderTrend, TrendObservation } from '$lib/trends';

	let { data } = $props();

	const ageHours = (at: string) => (Date.now() - new Date(at).getTime()) / 3_600_000;
	const stale = (provider: ProviderTrend) => !provider.current || ageHours(provider.current.last_seen_at) > 36;
	const stockState = (observation: TrendObservation | null) => {
		if (!observation) return 'unknown';
		if (observation.stock_quantity_kind === 'exact') {
			return observation.stock_quantity === 0 ? 'out' : 'in';
		}
		const tail = observation.availability?.split('/').at(-1);
		if (tail === 'OutOfStock' || tail === 'SoldOut') return 'out';
		if (tail === 'InStock' || tail === 'LimitedAvailability') return 'in';
		return 'unknown';
	};
	const rangeStart = (days: number) => Date.now() - days * 86_400_000;
	const historyFor = (provider: ProviderTrend, days: number = data.range.days) =>
		provider.history.filter((entry) => new Date(entry.last_seen_at).getTime() >= rangeStart(days));
	const first = (provider: ProviderTrend, days: number = data.range.days) => historyFor(provider, days)[0] ?? null;
	const changedTo = (provider: ProviderTrend, state: 'in' | 'out') =>
		stockState(provider.current) === state && stockState(first(provider)) !== state;
	const priceDelta = (provider: ProviderTrend, days: number = data.range.days) => {
		const before = first(provider, days);
		const current = provider.current;
		if (!before || !current || before.currency !== current.currency || before.price === 0) return null;
		return ((current.price - before.price) / before.price) * 100;
	};
	const productDelta = (product: ProductTrend, days: number) => {
		const values = product.providers
			.map((provider) => priceDelta(provider, days))
			.filter((value): value is number => value !== null);
		if (!values.length) return '—';
		const average = values.reduce((sum, value) => sum + value, 0) / values.length;
		return `${average > 0 ? '+' : ''}${average.toFixed(1)}%`;
	};
	const providers = $derived(data.products.flatMap((product: ProductTrend) => product.providers));
	const drops = $derived(providers.filter((provider: ProviderTrend) => (priceDelta(provider) ?? 0) < 0).length);
	const out = $derived(providers.filter((provider: ProviderTrend) => changedTo(provider, 'out')).length);
	const restocked = $derived(providers.filter((provider: ProviderTrend) => changedTo(provider, 'in')).length);
	const staleProviders = $derived(providers.filter(stale).length);

	function best(product: ProductTrend) {
		const offers = product.providers.flatMap((provider) => (provider.current ? [provider.current] : []));
		const comparable = new Map<string, TrendObservation[]>();
		for (const offer of offers) {
			if (offer.unit_price === null || !offer.unit_price_per) continue;
			const key = `${offer.currency}/${offer.unit_price_per}`;
			comparable.set(key, [...(comparable.get(key) ?? []), offer]);
		}
		const group = [...comparable.entries()].find(([, values]) => values.length >= 2);
		if (!group) return 'Not comparable';
		const [key, values] = group;
		const value = Math.min(...values.map((offer) => offer.unit_price!));
		return `${value.toFixed(2)} ${key}`;
	}

	const moment = (value: string) => new Date(value).toLocaleString();
</script>

<svelte:head><title>Price & stock trends · Catalogue</title></svelte:head>

<PageHeader
	eyebrow="Tracked catalogue"
	title="Price & stock trends"
	description="Reviewed products across providers. Missing inventory remains unknown, never zero."
>
	{#snippet actions()}
		<form method="GET" class="flex items-center gap-2">
			<label for="days" class="text-muted-foreground text-xs">Range</label>
			<NativeSelect id="days" name="days" value={String(data.range.days)} fit onchange={(event) => event.currentTarget.form?.requestSubmit()}>
				<option value="7">7 days</option><option value="30">30 days</option>
				<option value="90">90 days</option><option value="365">1 year</option>
			</NativeSelect>
		</form>
	{/snippet}
</PageHeader>

{#if data.configEmpty}
	<EmptyState
		class="mt-8"
		title="No products are tracked yet"
		description="Add reviewed canonical product IDs to config/tracked-products.json, rebuild Explorer, and this page will start with the history already collected by the catalogue."
	/>
{:else}
	<div class="mb-metrics mb-metrics-ruled mt-6">
		<Metric label="Tracked products" value={data.products.length} />
		<Metric label="Provider price drops" value={drops} detail="{data.range.days}-day range" />
		<Metric label="Newly out of stock" value={out} detail="{data.range.days}-day range" />
		<Metric label="Restocked" value={restocked} detail="{data.range.days}-day range" />
		<Metric
			label="Stale providers"
			value={staleProviders}
			detail="no observation in 36 hours"
		/>
	</div>

	<Card class="mt-6">
		<CardHeader><CardTitle>Tracked products</CardTitle></CardHeader>
		<CardContent>
			<Table>
				<TableHeader><TableRow>
					<TableHead>Product</TableHead><TableHead>Best comparable unit price</TableHead><TableHead>7-day change</TableHead><TableHead>30-day change</TableHead>
					<TableHead>Stocked providers</TableHead><TableHead>Last checked</TableHead>
				</TableRow></TableHeader>
				<TableBody>
					{#each data.products as product (product.canonical_product_id)}
						{@const currentProviders = product.providers.filter((provider: ProviderTrend) => provider.current)}
						{@const last = currentProviders.map((provider: ProviderTrend) => provider.current!.last_seen_at).sort().at(-1)}
						<TableRow>
							<TableCell><a class="font-medium underline-offset-4 hover:underline" href="/trends/{productSlug(product.label)}?days={data.range.days}">{product.label}</a><div class="text-muted-foreground text-xs">{product.providers.length} provider variants</div></TableCell>
							<TableCell class="tabular-nums">{best(product)}</TableCell>
							<TableCell class="tabular-nums">{productDelta(product, 7)}</TableCell>
							<TableCell class="tabular-nums">{productDelta(product, 30)}</TableCell>
							<TableCell>{product.providers.filter((provider: ProviderTrend) => stockState(provider.current) === 'in').length} of {product.providers.length}</TableCell>
							<TableCell>{last ? moment(last) : 'Never'}</TableCell>
						</TableRow>
					{/each}
				</TableBody>
			</Table>
		</CardContent>
	</Card>
{/if}
