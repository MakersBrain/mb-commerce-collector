<script lang="ts">
	import AvailabilityBand from '$lib/charts/AvailabilityBand.svelte';
	import PriceHistory from '$lib/charts/PriceHistory.svelte';
	import StockHistory from '$lib/charts/StockHistory.svelte';
	import { PageHeader } from '@makersbrain/ui/svelte';
	import StatusBadge from '$lib/components/ui/status-badge/status-badge.svelte';
	import { Card, CardContent, CardHeader, CardTitle } from '$lib/components/ui/card';
	import NativeSelect from '$lib/components/ui/native-select/native-select.svelte';
	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$lib/components/ui/table';
	import type { ProviderTrend, TrendObservation } from '$lib/trends';

	let { data } = $props();

	const visibleProviders = $derived(
		data.product.providers.filter(
			(provider: ProviderTrend) => !data.provider || provider.source_product_id === data.provider
		)
	);
	const ageHours = (at: string) => (Date.now() - new Date(at).getTime()) / 3_600_000;
	const stale = (provider: ProviderTrend) =>
		!provider.current || ageHours(provider.current.last_seen_at) > 36;
	const historyFor = (provider: ProviderTrend) => provider.history;
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
	const moment = (value: string) => new Date(value).toLocaleString();

	function currentLabel(observation: TrendObservation | null) {
		if (!observation) return 'No priced observation';
		const pack =
			observation.quantity && observation.unit
				? ` / ${observation.quantity} ${observation.unit}`
				: '';
		return `${observation.price.toFixed(2)} ${observation.currency}${pack}`;
	}

	function stockLabel(observation: TrendObservation | null) {
		if (!observation) return 'Unknown';
		if (observation.stock_quantity !== null) {
			return `${observation.stock_quantity} (${observation.stock_quantity_kind.replace('_', ' ')})`;
		}
		return observation.availability?.split('/').at(-1) ?? 'Unknown';
	}
</script>

<svelte:head><title>{data.product.label} trends · Catalogue</title></svelte:head>

<PageHeader
	backHref="/trends?days={data.range.days}"
	backLabel="← Back to trends"
	eyebrow="Tracked product"
	title={data.product.label}
	description={`${data.product.brand ?? 'Unknown brand'} · ${data.product.manufacturer_sku ?? 'No manufacturer SKU'}`}
>
	{#snippet actions()}
		<form method="GET" class="flex items-center gap-2">
			<label for="days" class="text-muted-foreground text-xs">Range</label>
			<NativeSelect id="days" name="days" value={String(data.range.days)} fit onchange={(event) => event.currentTarget.form?.requestSubmit()}>
				<option value="7">7 days</option><option value="30">30 days</option>
				<option value="90">90 days</option><option value="365">1 year</option>
			</NativeSelect>
			{#if data.provider}<input type="hidden" name="provider" value={data.provider} />{/if}
		</form>
	{/snippet}
</PageHeader>

<div class="mt-6 flex flex-wrap items-center justify-between gap-3">
	<form method="GET" class="flex items-center gap-2">
		<input type="hidden" name="days" value={data.range.days} />
		<label for="provider" class="text-muted-foreground text-xs">Provider</label>
		<NativeSelect id="provider" name="provider" value={data.provider ?? ''} fit onchange={(event) => event.currentTarget.form?.requestSubmit()}>
			<option value="">All providers</option>
			{#each data.product.providers as provider (provider.source_product_id)}<option value={provider.source_product_id}>{provider.source_label} · {provider.name}</option>{/each}
		</NativeSelect>
	</form>
	{#if data.product.providers.some((provider: ProviderTrend) => provider.truncated)}
		<StatusBadge tone="warn">Some series limited to 500 observations</StatusBadge>
	{/if}
</div>

<Card class="mt-4" size="sm">
	<CardHeader><CardTitle>Current provider comparison</CardTitle></CardHeader>
	<CardContent>
		<Table>
			<TableHeader><TableRow><TableHead>Provider / variant</TableHead><TableHead>Listed price</TableHead><TableHead>Unit price</TableHead><TableHead>Stock</TableHead><TableHead>Last checked</TableHead></TableRow></TableHeader>
			<TableBody>
				{#each visibleProviders as provider (provider.source_product_id)}
					<TableRow><TableCell>{provider.source_label}<div class="text-muted-foreground text-xs">{provider.name}{provider.package_label ? ` · ${provider.package_label}` : ''}</div></TableCell><TableCell>{currentLabel(provider.current)}</TableCell><TableCell>{provider.current?.unit_price !== null && provider.current?.unit_price_per ? `${provider.current.unit_price.toFixed(2)} ${provider.current.currency}/${provider.current.unit_price_per}` : 'Not available'}</TableCell><TableCell>{stockLabel(provider.current)}</TableCell><TableCell>{provider.current ? moment(provider.current.last_seen_at) : 'Never'}</TableCell></TableRow>
				{/each}
			</TableBody>
		</Table>
	</CardContent>
</Card>

<div class="mt-4 grid gap-4 lg:grid-cols-2">
	{#each visibleProviders as provider (provider.source_product_id)}
		<Card size="sm">
			<CardHeader><CardTitle><a href={provider.product_url} rel="noreferrer" target="_blank" class="underline-offset-4 hover:underline">{provider.source_label}</a></CardTitle></CardHeader>
			<CardContent class="grid gap-4">
				<div class="flex flex-wrap gap-2"><StatusBadge tone={stale(provider) ? 'warn' : 'good'}>{stale(provider) ? 'Stale' : 'Fresh'}</StatusBadge><StatusBadge tone={stockState(provider.current) === 'out' ? 'bad' : stockState(provider.current) === 'in' ? 'good' : 'neutral'}>{stockLabel(provider.current)}</StatusBadge><span class="text-muted-foreground self-center text-xs">{provider.name}{provider.package_label ? ` · ${provider.package_label}` : ''}</span></div>
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
