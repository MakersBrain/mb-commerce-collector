<script lang="ts">
	import AvailabilityBand from '$lib/charts/AvailabilityBand.svelte';
	import PriceHistory from '$lib/charts/PriceHistory.svelte';
	import StockHistory from '$lib/charts/StockHistory.svelte';
	import StatTile from '$lib/components/StatTile.svelte';
	import EmptyState from '$lib/components/ui/empty-state/empty-state.svelte';
	import PageHeader from '$lib/components/ui/page-header/page-header.svelte';
	import StatusBadge from '$lib/components/ui/status-badge/status-badge.svelte';
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
	const visibleProviders = $derived(
		data.selected?.providers.filter(
			(provider: ProviderTrend) => !data.provider || provider.source_product_id === data.provider
		) ?? []
	);
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

	function currentLabel(observation: TrendObservation | null) {
		if (!observation) return 'No priced observation';
		const pack = observation.quantity && observation.unit ? ` / ${observation.quantity} ${observation.unit}` : '';
		return `${observation.price.toFixed(2)} ${observation.currency}${pack}`;
	}

	function stockLabel(observation: TrendObservation | null) {
		if (!observation) return 'Unknown';
		if (observation.stock_quantity !== null) {
			return `${observation.stock_quantity} (${observation.stock_quantity_kind.replace('_', ' ')})`;
		}
		return observation.availability?.split('/').at(-1) ?? 'Unknown';
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
			{#if data.selected}<input type="hidden" name="product" value={data.selected.canonical_product_id} />{/if}
			{#if data.provider}<input type="hidden" name="provider" value={data.provider} />{/if}
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
	<div class="mt-6 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
		<StatTile label="Tracked products" value={String(data.products.length)} />
		<StatTile label="Provider price drops" value={String(drops)} note={`${data.range.days}-day range`} />
		<StatTile label="Newly out of stock" value={String(out)} note={`${data.range.days}-day range`} />
		<StatTile label="Restocked" value={String(restocked)} note={`${data.range.days}-day range`} />
		<StatTile label="Stale providers" value={String(staleProviders)} note="No observation in 36 hours" />
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
							<TableCell><a class="font-medium underline-offset-4 hover:underline" href="?days={data.range.days}&product={product.canonical_product_id}">{product.label}</a><div class="text-muted-foreground text-xs">{product.providers.length} provider variants</div></TableCell>
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

	{#if data.selected}
		<section class="mt-8" aria-labelledby="selected-product">
			<div class="flex flex-wrap items-end justify-between gap-3">
				<div><p class="text-muted-foreground text-xs uppercase tracking-wide">Product detail</p><h2 id="selected-product" class="text-xl font-semibold">{data.selected.label}</h2><p class="text-muted-foreground text-sm">{data.selected.brand ?? 'Unknown brand'} · {data.selected.manufacturer_sku ?? 'No manufacturer SKU'}</p></div>
				<div class="flex items-center gap-2">
					<form method="GET" class="flex items-center gap-2">
						<input type="hidden" name="days" value={data.range.days} />
						<input type="hidden" name="product" value={data.selected.canonical_product_id} />
						<label for="provider" class="text-muted-foreground text-xs">Provider</label>
						<NativeSelect id="provider" name="provider" value={data.provider ?? ''} fit onchange={(event) => event.currentTarget.form?.requestSubmit()}>
							<option value="">All providers</option>
							{#each data.selected.providers as provider (provider.source_product_id)}<option value={provider.source_product_id}>{provider.source_label} · {provider.name}</option>{/each}
						</NativeSelect>
					</form>
					{#if data.selected.providers.some((provider: ProviderTrend) => provider.truncated)}<StatusBadge tone="warn">Some series limited to 500 observations</StatusBadge>{/if}
				</div>
			</div>

			<Card class="mt-4" size="sm">
				<CardHeader><CardTitle>Current provider comparison</CardTitle></CardHeader>
				<CardContent><Table>
					<TableHeader><TableRow><TableHead>Provider / variant</TableHead><TableHead>Listed price</TableHead><TableHead>Unit price</TableHead><TableHead>Stock</TableHead><TableHead>Last checked</TableHead></TableRow></TableHeader>
					<TableBody>{#each visibleProviders as provider (provider.source_product_id)}<TableRow><TableCell>{provider.source_label}<div class="text-muted-foreground text-xs">{provider.name}{provider.package_label ? ` · ${provider.package_label}` : ''}</div></TableCell><TableCell>{currentLabel(provider.current)}</TableCell><TableCell>{provider.current?.unit_price !== null && provider.current?.unit_price_per ? `${provider.current.unit_price.toFixed(2)} ${provider.current.currency}/${provider.current.unit_price_per}` : 'Not available'}</TableCell><TableCell>{stockLabel(provider.current)}</TableCell><TableCell>{provider.current ? moment(provider.current.last_seen_at) : 'Never'}</TableCell></TableRow>{/each}</TableBody>
				</Table></CardContent>
			</Card>

			<div class="mt-4 grid gap-4 lg:grid-cols-2">
				{#each visibleProviders as provider (provider.source_product_id)}
					<Card size="sm">
						<CardHeader><CardTitle><a href={provider.product_url} rel="noreferrer" target="_blank" class="underline-offset-4 hover:underline">{provider.source_label}</a></CardTitle></CardHeader>
						<CardContent class="grid gap-4">
							<div class="flex flex-wrap gap-2"><StatusBadge tone={stale(provider) ? 'warn' : 'good'}>{stale(provider) ? 'Stale' : 'Fresh'}</StatusBadge><StatusBadge tone={stockState(provider.current) === 'out' ? 'bad' : stockState(provider.current) === 'in' ? 'good' : 'neutral'}>{stockLabel(provider.current)}</StatusBadge><span class="text-muted-foreground text-xs self-center">{provider.name}{provider.package_label ? ` · ${provider.package_label}` : ''}</span></div>
							<PriceHistory points={historyFor(provider).map((entry: TrendObservation) => ({ at: entry.observed_at, value: entry.price }))} currency={provider.current?.currency ?? ''} />
							{#if historyFor(provider).some((entry: TrendObservation) => entry.unit_price !== null)}
								<PriceHistory points={historyFor(provider).filter((entry: TrendObservation) => entry.unit_price !== null).map((entry: TrendObservation) => ({ at: entry.observed_at, value: entry.unit_price! }))} currency={`${provider.current?.currency ?? ''}/${provider.current?.unit_price_per ?? 'unit'}`} label="Unit price" />
							{/if}
							<StockHistory points={historyFor(provider)} />
							<AvailabilityBand points={historyFor(provider).map((entry: TrendObservation) => ({ at: entry.observed_at, state: entry.availability }))} />
							<Table>
								<TableHeader><TableRow><TableHead>Observed</TableHead><TableHead>Price</TableHead><TableHead>Stock</TableHead><TableHead>Available until</TableHead></TableRow></TableHeader>
								<TableBody>{#each [...historyFor(provider)].reverse() as entry (entry.id)}<TableRow><TableCell>{moment(entry.observed_at)}</TableCell><TableCell>{currentLabel(entry)}</TableCell><TableCell>{stockLabel(entry)}</TableCell><TableCell>{moment(entry.last_seen_at)}</TableCell></TableRow>{/each}</TableBody>
							</Table>
						</CardContent>
					</Card>
				{/each}
			</div>
		</section>
	{/if}
{/if}
