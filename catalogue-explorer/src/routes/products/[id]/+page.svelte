<script lang="ts">
	import AvailabilityBand from '$lib/charts/AvailabilityBand.svelte';
	import PriceHistory from '$lib/charts/PriceHistory.svelte';
	import StockHistory from '$lib/charts/StockHistory.svelte';
	import PageHeader from '$lib/components/ui/page-header/page-header.svelte';
	import { Card, CardContent, CardHeader, CardTitle } from '$lib/components/ui/card';
	import type { Observation } from '$lib/catalogue';
	import type { PageData } from './$types';

	let { data }: { data: PageData } = $props();
	const product = $derived(data.detail.product!);
	const source = $derived(data.detail.source ?? {});
	const offers = $derived(data.detail.offers);
	const chronological = $derived([...offers].reverse());
	const attributes = $derived(object(product.attributes) ? product.attributes : null);
	const current = $derived(offers[0] ?? null);

	const value = (record: Record<string, unknown>, key: string) => record[key];
	const text = (record: Record<string, unknown>, key: string, fallback = '') => {
		const found = value(record, key);
		return typeof found === 'string' && found ? found : fallback;
	};
	const name = $derived(text(product, 'name', 'Storefront product'));
	const sourceId = $derived(text(product, 'source_id'));
	const sourceLabel = $derived(text(source, 'label', sourceId));
	const storefrontUrl = $derived(text(product, 'product_url'));
	const brand = $derived(text(product, 'brand'));
	const code = $derived(text(product, 'manufacturer_sku'));

	const pictures = $derived.by(() => [...new Set([
		text(product, 'image_url'),
		...(Array.isArray(attributes?.all_image_urls) ? attributes.all_image_urls : [])
	].filter((entry): entry is string => typeof entry === 'string' && entry.length > 0))]);

	const COVERED = new Set([
		'id', 'source_id', 'attributes', 'name', 'product_url', 'image_url',
		'created_at', 'updated_at', 'last_seen_at'
	]);
	const fields = $derived(Object.entries(product).filter(
		([key, entry]) => !COVERED.has(key) && entry !== null && entry !== ''
	));

	const priceSeries = $derived(chronological
		.filter((offer) => typeof offer.price === 'number')
		.map((offer) => ({ at: offer.observed_at, value: offer.price! })));
	const unitPriceSeries = $derived(chronological
		.filter((offer) => typeof offer.unit_price === 'number')
		.map((offer) => ({ at: offer.observed_at, value: offer.unit_price! })));
	const availabilitySeries = $derived(chronological.map((offer) => ({
		at: offer.observed_at,
		state: offer.availability
	})));
	const currency = $derived.by(() => {
		const values = new Set(offers.map((offer) => offer.currency).filter(Boolean));
		return values.size === 1 ? String([...values][0]) : '';
	});
	const unitPer = $derived.by(() => {
		const values = new Set(offers.map((offer) => offer.unit_price_per).filter(Boolean));
		return values.size === 1 ? String([...values][0]) : '';
	});

	let copied = $state(false);
	async function copyLink() {
		await navigator.clipboard.writeText(window.location.href);
		copied = true;
		setTimeout(() => (copied = false), 1600);
	}

	function object(entry: unknown): entry is Record<string, unknown> {
		return entry !== null && typeof entry === 'object' && !Array.isArray(entry);
	}
	function label(key: string) {
		return key.replace(/_/g, ' ');
	}
	function scalar(entry: unknown) {
		if (entry === null || entry === undefined || entry === '') return '—';
		if (typeof entry === 'boolean') return entry ? 'yes' : 'no';
		if (typeof entry === 'number') return entry.toLocaleString('en-US', { maximumFractionDigits: 4 });
		return String(entry);
	}
	function moment(entry: string) {
		return new Date(entry).toLocaleString();
	}
	function stockLabel(offer: Observation | null) {
		if (!offer) return 'unknown';
		if (offer.stock_quantity !== null) {
			const quantity = scalar(offer.stock_quantity);
			if (offer.stock_quantity_kind === 'exact') {
				return offer.stock_quantity === 0 ? '0 · out of stock' : `${quantity} in stock`;
			}
			if (offer.stock_quantity_kind === 'lower_bound') return `at least ${quantity}`;
			if (offer.stock_quantity_kind === 'upper_bound') return `up to ${quantity}`;
			if (offer.stock_quantity_kind === 'order_limit') return `order limit ${quantity}`;
		}
		const state = offer.availability?.split('/').at(-1);
		if (state === 'InStock') return 'in stock · quantity unknown';
		if (state === 'LimitedAvailability') return 'limited availability · quantity unknown';
		if (state === 'OutOfStock' || state === 'SoldOut') return 'out of stock';
		return 'unknown';
	}
</script>

<svelte:head><title>{name} · Catalogue</title></svelte:head>

<a href="/explore" class="text-muted-foreground text-xs underline-offset-4 hover:underline">← Back to catalogue</a>

<PageHeader
	eyebrow="Storefront product"
	title={name}
	description={[sourceLabel, brand, code].filter(Boolean).join(' · ')}
>
	{#snippet actions()}
		<div class="flex gap-2">
			<button type="button" class="rounded-lg px-3 py-2 text-xs" style="border: 1px solid var(--hairline)" onclick={copyLink}>
				{copied ? 'Link copied' : 'Copy link'}
			</button>
			{#if storefrontUrl}
				<a href={storefrontUrl} target="_blank" rel="noreferrer noopener" class="rounded-lg px-3 py-2 text-xs" style="background: var(--primary); color: var(--primary-foreground)">Open storefront ↗</a>
			{/if}
		</div>
	{/snippet}
</PageHeader>

<div class="mt-6 grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(18rem,1fr)]">
	<div class="grid content-start gap-4">
		<Card>
			<CardHeader><CardTitle>Stock history</CardTitle></CardHeader>
			<CardContent class="grid gap-4">
				<p class="text-sm" style="color: var(--text-primary)">Current: {stockLabel(current)}</p>
				<StockHistory points={chronological} height={132} />
				<AvailabilityBand points={availabilitySeries} />
				<p class="text-muted-foreground text-xs">Exact quantity history begins when a provider first publishes it to this catalogue. Availability history remains visible when quantity is unknown.</p>
			</CardContent>
		</Card>

		<Card>
			<CardHeader><CardTitle>Price history</CardTitle></CardHeader>
			<CardContent class="grid gap-5">
				<PriceHistory points={priceSeries} {currency} />
				{#if unitPriceSeries.length}
					<PriceHistory points={unitPriceSeries} currency={currency && unitPer ? `${currency}/${unitPer}` : currency} label="Unit price" />
				{/if}
			</CardContent>
		</Card>
	</div>

	<Card class="self-start">
		<CardHeader><CardTitle>Storefront record</CardTitle></CardHeader>
		<CardContent>
			{#if pictures.length}
				<div class="mb-5 grid grid-cols-2 gap-2">
					{#each pictures as picture (picture)}
						<img src={picture} alt="" referrerpolicy="no-referrer" class="aspect-square w-full rounded-lg object-contain" style="background: color-mix(in srgb, var(--recede) 30%, transparent)" />
					{/each}
				</div>
			{/if}
			<dl class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-xs">
				{#each fields as [key, entry] (key)}
					<dt class="text-muted-foreground whitespace-nowrap">{label(key)}</dt>
					<dd class="min-w-0 break-words">{scalar(entry)}</dd>
				{/each}
			</dl>
			{#if attributes && Object.keys(attributes).length}
				<h3 class="mt-6 text-xs font-semibold">Imported attributes</h3>
				<div class="mt-2">{@render tree(attributes)}</div>
			{/if}
		</CardContent>
	</Card>
</div>

<Card class="mt-4">
	<CardHeader><CardTitle>Observed price and stock ({offers.length})</CardTitle></CardHeader>
	<CardContent class="overflow-x-auto">
		<table class="w-full min-w-[48rem] text-xs" style="color: var(--text-secondary)">
			<thead><tr class="text-left" style="border-bottom: 1px solid var(--hairline)">
				<th class="py-2 pr-4">Observed</th><th class="py-2 pr-4">Price</th><th class="py-2 pr-4">Pack</th><th class="py-2 pr-4">Unit price</th><th class="py-2 pr-4">Stock / quantity</th><th class="py-2">Availability</th>
			</tr></thead>
			<tbody>
				{#each offers as offer, index (index)}
					<tr style="border-bottom: 1px solid var(--hairline)">
						<td class="py-2 pr-4 whitespace-nowrap">{moment(offer.observed_at)}</td>
						<td class="py-2 pr-4 tabular-nums">{scalar(offer.price)} {offer.currency ?? ''}</td>
						<td class="py-2 pr-4">{offer.quantity ? `${scalar(offer.quantity)} ${offer.unit ?? ''}` : '—'}</td>
						<td class="py-2 pr-4 tabular-nums">{offer.unit_price ? `${scalar(offer.unit_price)}/${offer.unit_price_per ?? ''}` : '—'}</td>
						<td class="py-2 pr-4">{stockLabel(offer)}</td>
						<td class="py-2">{offer.availability?.split('/').at(-1) ?? 'unknown'}</td>
					</tr>
				{:else}
					<tr><td colspan="6" class="text-muted-foreground py-4">No price or stock observations yet.</td></tr>
				{/each}
			</tbody>
		</table>
	</CardContent>
</Card>

{#snippet tree(node: Record<string, unknown>)}
	<dl class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-xs">
		{#each Object.entries(node) as [key, entry] (key)}
			<dt class="text-muted-foreground whitespace-nowrap">{label(key)}</dt>
			<dd class="min-w-0 break-words">
				{#if Array.isArray(entry)}
					{entry.map(scalar).join(', ') || '—'}
				{:else if object(entry)}
					<div class="pl-2" style="border-left: 1px solid var(--hairline)">{@render tree(entry)}</div>
				{:else}
					{scalar(entry)}
				{/if}
			</dd>
		{/each}
	</dl>
{/snippet}
