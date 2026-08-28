<script lang="ts">
	import { BANDS, bandOf } from '$lib/bands';
	import BarChart from '$lib/charts/BarChart.svelte';
	import ChartCard from '$lib/charts/ChartCard.svelte';
	import SupplierDetail from '$lib/grid/SupplierDetail.svelte';
	import { trim } from '$lib/format';
	import type { PageData } from './$types';

	let { data }: { data: PageData } = $props();

	/**
	 * The comparison answers "who is cheapest"; the next question is always "and
	 * what is it, exactly" — the pack, the firing range, whether that price has
	 * moved. Product names now link to the same first-class detail page used by
	 * Explore, so the record and its history have a stable, shareable address.
	 */
	let openedSupplier = $state<{ id: string; label?: string } | null>(null);

	type Row = PageData['groups'][number]['offers'][number];

	const euros = (value: number) => `EUR ${value.toFixed(2)}`;

	function pack(quantity: number | null, unit: string | null) {
		if (quantity == null || !unit) return '-';
		return `${trim(quantity)} ${unit}`;
	}

	/**
	 * One chart per pack band, cheapest first. Prices convert to EUR at the ECB
	 * reference rate so a USD listing takes part instead of being dropped from
	 * the comparison, and a gallon is never ranked against a jar:
	 * per-litre pricing is not linear in pack size, so that comparison would
	 * always crown the biggest pack.
	 */
	function stockState(offer: Row): 'in' | 'out' | 'unknown' {
		if (offer.stock_quantity_kind === 'exact' && offer.stock_quantity !== null) {
			return offer.stock_quantity === 0 ? 'out' : 'in';
		}
		const state = offer.availability?.split('/').at(-1);
		if (state === 'InStock' || state === 'LimitedAvailability') return 'in';
		if (state === 'OutOfStock' || state === 'SoldOut') return 'out';
		return 'unknown';
	}

	function stockLabel(offer: Row) {
		if (offer.stock_quantity !== null) {
			const quantity = trim(offer.stock_quantity);
			switch (offer.stock_quantity_kind) {
				case 'exact':
					return offer.stock_quantity === 0 ? '0 · out of stock' : `${quantity} in stock`;
				case 'lower_bound':
					return `at least ${quantity}`;
				case 'upper_bound':
					return `up to ${quantity}`;
				case 'order_limit':
					return `order limit ${quantity}`;
			}
		}
		const state = offer.availability?.split('/').at(-1);
		if (state === 'InStock') return 'in stock';
		if (state === 'LimitedAvailability') return 'limited availability';
		if (state === 'OutOfStock' || state === 'SoldOut') return 'out of stock';
		return 'unknown';
	}

	function banded(offers: PageData['groups'][number]['offers']) {
		const plottable = offers.filter((offer) => offer.unit_price_eur && offer.unit_price_per === 'l');
		return BANDS.map((entry) => ({
			band: entry,
			bars: plottable
				.filter((offer) => bandOf(offer.litres)?.id === entry.id)
				.sort((a, b) => (a.unit_price_eur ?? 0) - (b.unit_price_eur ?? 0))
				.map((offer) => ({
					label: `${offer.supplier} - ${pack(offer.quantity, offer.unit)}`,
					value: offer.unit_price_eur ?? 0,
					note:
						`${euros(offer.price_eur ?? offer.price)} per pack` +
						(offer.currency === 'EUR'
							? ''
							: ` (listed ${offer.price.toFixed(2)} ${offer.currency})`) +
						` - ${offer.vat_status ?? 'VAT basis unknown'}` +
						(stockState(offer) === 'out' ? ' - out of stock' : '')
				}))
		})).filter((entry) => entry.bars.length > 0);
	}
</script>

<h1 class="text-xl font-semibold sm:text-2xl" style="color: var(--text-primary)">
	Compare suppliers
</h1>
<p class="measure mt-1 text-sm" style="color: var(--text-secondary)">
	Search a manufacturer code (<code>PC-20</code>, <code>CG-1013</code>, <code>UG51</code>) or a
	product name.
</p>
<p class="measure mt-1 text-xs" style="color: var(--text-muted)">
	Every price is converted to EUR at the ECB reference rate{data.fx.date
		? ` of ${data.fx.date}`
		: ''}{data.fx.stale ? ' (last stored rates - the ECB was unreachable)' : ''}; the listed
	currency stays in the table.
</p>

<form method="GET" class="mt-6 flex gap-2">
	<input
		type="search"
		name="q"
		value={data.query}
		placeholder="PC-20"
		autocomplete="off"
		aria-label="Manufacturer code or product name"
		class="min-w-0 flex-1 rounded-lg px-3 py-2 text-sm sm:w-72 sm:flex-none"
		style="background: var(--surface-1); color: var(--text-primary); border: 1px solid var(--hairline)"
	/>
	<button
		type="submit"
		class="shrink-0 rounded-lg px-4 py-2 text-sm font-medium"
		style="background: var(--primary); color: var(--primary-foreground)">Search</button
	>
</form>

{#if data.query && !data.groups.length}
	<p class="mt-8 text-sm" style="color: var(--text-muted)">
		Nothing matched "{data.query}". Only products carrying a manufacturer code are comparable
		across suppliers.
	</p>
{/if}

<div class="mt-6 flex flex-col gap-4">
	{#each data.groups as group (group.code)}
		{@const panels = banded(group.offers)}
		<ChartCard
			title="{group.code} - {group.name}"
			subtitle="{group.suppliers} supplier{group.suppliers === 1 ? '' : 's'}, {group.offers
				.length} offers{group.family ? ` - ${group.family}` : ''}"
			note="Unit prices are computed from the observed pack price and converted at the ECB reference rate, which is indicative rather than a transaction rate. VAT basis, stock and shipping differ between storefronts, so the cheapest bar is not automatically the cheapest purchase."
			tableAlwaysVisible
		>
			{#snippet chart()}
				{#if panels.length}
					<div class="flex flex-col gap-5">
						{#each panels as panel (panel.band.id)}
							<div>
								<div class="mb-2 text-xs" style="color: var(--text-muted)">
									{panel.band.label} packs
								</div>
								<BarChart
									items={panel.bars}
									format={euros}
									emphasis={panel.bars.length > 1 ? 0 : -1}
									labelWidth={240}
								/>
							</div>
						{/each}
					</div>
				{:else}
					<p class="text-sm" style="color: var(--text-muted)">
						No EUR offer in this group normalises to a price per litre. The table below has the raw
						observations.
					</p>
				{/if}
			{/snippet}
			{#snippet table()}
				<table class="w-full min-w-[46rem] text-xs" style="color: var(--text-secondary)">
					<thead>
						<tr class="text-left">
							<th class="py-1 pr-4">Supplier</th>
							<th class="py-1 pr-4">Variant</th>
							<th class="py-1 pr-4">Pack</th>
							<th class="py-1 pr-4 text-right">Price (EUR)</th>
							<th class="py-1 pr-4">Listed</th>
							<th class="py-1 pr-4">VAT</th>
							<th class="py-1 pr-4">Stock / quantity</th>
							<th class="py-1 pr-4 text-right">Unit price (EUR)</th>
							<th class="py-1">Source</th>
						</tr>
					</thead>
					<tbody>
						{#each group.offers as offer}
							{@const state = stockState(offer)}
							<tr>
								<td class="py-1 pr-4">
									<button
										type="button"
										class="text-left underline decoration-dotted underline-offset-2"
										style="color: var(--primary)"
										onclick={() => (openedSupplier = { id: offer.supplier })}
									>
										{offer.supplier}
									</button>
								</td>
								<td class="py-1 pr-4">
									<a
										href="/products/{offer.id}"
										class="text-left underline decoration-dotted underline-offset-2"
										style="color: var(--primary)"
									>
										{offer.name}
									</a>
								</td>
								<td class="py-1 pr-4">{pack(offer.quantity, offer.unit)}</td>
								<td class="py-1 pr-4 text-right tabular-nums">
									{offer.price_eur ? offer.price_eur.toFixed(2) : '-'}
								</td>
								<td class="py-1 pr-4 whitespace-nowrap tabular-nums">
									{offer.price.toFixed(2)}
									{offer.currency}
								</td>
								<td class="py-1 pr-4">{offer.vat_status ?? '-'}</td>
								<td class="py-1 pr-4 whitespace-nowrap">
									<span
										aria-hidden="true"
										style={state === 'in'
											? 'color: var(--good)'
											: state === 'out'
												? 'color: var(--critical)'
												: 'color: var(--text-muted)'}>{state === 'in' ? '\u25CF' : state === 'out' ? '\u25CB' : '\u2014'}</span
									>
									<span style={state === 'unknown' ? 'color: var(--text-muted)' : ''}>{stockLabel(offer)}</span>
								</td>
								<td class="py-1 pr-4 text-right tabular-nums">
									{offer.unit_price_eur
										? `${offer.unit_price_eur.toFixed(2)} /${offer.unit_price_per}`
										: '-'}
								</td>
								<td class="py-1">
									<a
										href={offer.url}
										target="_blank"
										rel="noreferrer noopener"
										style="color: var(--primary)">open</a
									>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			{/snippet}
		</ChartCard>
	{/each}
</div>

<SupplierDetail supplier={openedSupplier} onClose={() => (openedSupplier = null)} />
