<script lang="ts">
	import { Metric, PageHeader } from '@makersbrain/ui/svelte';
	import BarChart from '$lib/charts/BarChart.svelte';
	import ChartCard from '$lib/charts/ChartCard.svelte';
	import RangeChart from '$lib/charts/RangeChart.svelte';
	import StackedBar from '$lib/charts/StackedBar.svelte';
	import { NativeSelect } from '$lib/components/ui/native-select';
	import type { PageData } from './$types';

	let { data }: { data: PageData } = $props();

	const count = (value: number) => value.toLocaleString('en-US');
	const euros = (value: number) => `EUR ${value.toFixed(2)}`;

	/**
	 * The chart takes the busiest dozen, the table below it takes all of them.
	 *
	 * There are eighty-three storefronts, and eighty-three bars is not a chart -
	 * it is a list drawn slowly, three screens tall, in which the shape that a
	 * chart exists to show is the first thing lost.
	 */
	const TOP_SUPPLIERS = 12;
	const supplierBars = $derived(
		data.suppliers.slice(0, TOP_SUPPLIERS).map((row) => ({
			label: row.supplier,
			value: row.products,
			note: `${count(row.coded)} carry a manufacturer code`
		}))
	);
	const otherSuppliers = $derived(Math.max(0, data.suppliers.length - TOP_SUPPLIERS));
	const otherMedians = $derived(Math.max(0, data.medians.length - TOP_SUPPLIERS));

	const familySegments = $derived(
		data.families.map((row) => ({ label: row.family, value: row.products }))
	);

	// The same cap, and for the same reason: forty bars of a median is a table.
	// The cheapest end is the one worth drawing, and it is the end the emphasis
	// mark sits at; the table below keeps every supplier.
	const medianBars = $derived(
		data.medians.slice(0, TOP_SUPPLIERS).map((row) => ({
			label: row.supplier,
			value: row.median,
			note: `${count(row.offers)} comparable offers`
		}))
	);

	const spreadRows = $derived(
		data.spread.map((row) => ({
			label: row.code,
			sublabel: row.name,
			low: row.low,
			high: row.high,
			note: `${row.suppliers} suppliers`,
			href: `/compare?q=${encodeURIComponent(row.code)}`
		}))
	);

	const bandLabel = $derived(data.bandLabel);
	/** "packs" for a jar of glaze, "bags" for a box of clay. */
	const packWord = $derived(data.measure === 'mass' ? 'bags' : 'packs');
	const observed = $derived(
		data.totals.observed ? new Date(data.totals.observed).toISOString().slice(0, 10) : undefined
	);
</script>

<PageHeader
	title="Reference catalogue"
	description="Public listings collected from {data.totals
		.suppliers} ceramics suppliers, loaded into the local PostgreSQL catalogue schema."
/>

<!--
	Four numbers, ruled rather than boxed. They are one statement about the
	catalogue; four bordered tiles said they were four separate things, and the
	first of them was set five sizes larger than the rest for no reason a reader
	could act on.
-->
<div class="mb-metrics mb-metrics-ruled">
	<Metric
		label="Supplier products"
		value={count(data.totals.products)}
		detail="one row per purchasable variant"
	/>
	<Metric
		label="Price observations"
		value={count(data.totals.offers)}
		detail="append-only history"
	/>
	<Metric
		label="Manufacturer codes"
		value={count(data.totals.codes)}
		detail="the cross-supplier key"
	/>
	<Metric
		label="Suppliers"
		value={count(data.totals.suppliers)}
		detail={observed ? `last observed ${observed}` : undefined}
	/>
</div>

<p class="text-muted-foreground measure mt-3 text-xs">
	Prices are converted to EUR at the ECB reference rate{data.fx.date
		? ` of ${data.fx.date}`
		: ''}{data.fx.stale ? ' (last stored rates - the ECB was unreachable)' : ''}, which is
	indicative and not a transaction rate.
</p>

<!-- One filter row, above everything it scopes. On a phone the label sits above
     its control and the three stack into two columns, rather than the label and
     the select being torn onto separate lines by a wrap. -->
<form
	method="GET"
	class="border-border text-muted-foreground mt-6 grid grid-cols-2 items-end gap-3 border-y py-3 text-sm sm:flex sm:flex-wrap sm:items-center"
>
	<div class="flex min-w-0 flex-col gap-1 sm:flex-row sm:items-center sm:gap-2">
		<label for="band" class="text-xs whitespace-nowrap">Pack size</label>
		<NativeSelect
			id="band"
			name="band"
			class="h-8 text-xs"
			fit
			value={data.bandValue}
			onchange={(event) => event.currentTarget.form?.requestSubmit()}
		>
			<!-- Both measures when the type is sold both ways, which several are:
			     the same clay body comes as a 12.5 kg box from one storefront and a
			     two-litre tub from the next, and the reader has to be able to say
			     which comparison they wanted. -->
			{#each data.bandOptions as group (group.measure)}
				<optgroup label={group.label}>
					{#each group.bands as entry (entry.id)}
						<option value="{group.measure}:{entry.id}">{entry.label}</option>
					{/each}
				</optgroup>
			{/each}
		</NativeSelect>
	</div>

	<div class="flex min-w-0 flex-col gap-1 sm:flex-row sm:items-center sm:gap-2">
		<label for="brand" class="text-xs whitespace-nowrap">Brand</label>
		<NativeSelect
			id="brand"
			name="brand"
			class="h-8 max-w-56 text-xs"
			fit
			value={data.brand ?? ''}
			onchange={(event) => event.currentTarget.form?.requestSubmit()}
		>
			<option value="">all brands</option>
			{#each data.brandOptions as option (option.key)}
				<option value={option.key}>{option.label} ({count(option.products)})</option>
			{/each}
		</NativeSelect>
	</div>

	<div class="flex min-w-0 flex-col gap-1 sm:flex-row sm:items-center sm:gap-2">
		<label for="family" class="text-xs whitespace-nowrap">Product type</label>
		<NativeSelect
			id="family"
			name="family"
			class="h-8 max-w-56 text-xs"
			fit
			value={data.family ?? ''}
			onchange={(event) => event.currentTarget.form?.requestSubmit()}
		>
			<option value="">all types</option>
			{#each data.familyOptions as option (option.key)}
				<option value={option.key}>{option.label} ({count(option.products)})</option>
			{/each}
		</NativeSelect>
	</div>

	{#if data.brand || data.family}
		<a href="/?band={data.bandValue}" class="text-xs underline-offset-4 hover:underline">clear</a>
	{/if}

	<span class="text-muted-foreground measure col-span-2 text-xs sm:ml-auto">
		the size scopes both price panels: a small jar always costs more per {data.measureWord} than a
		large one, so suppliers are only compared inside one band
	</span>
	<noscript><button type="submit" class="rounded-lg px-3 py-1.5">Apply</button></noscript>
</form>

<!--
	Panels, in one column of two and then two full-width. A chart card inside a
	column beside a taller chart card is what the page used to be, and it left
	the reader deciding whether the two on the right were subordinate to the one
	on the left. They are not; they are four panels of one page.
-->
<div class="mt-8 grid gap-8 lg:grid-cols-2">
	<ChartCard
		title="Products per supplier"
		subtitle={otherSuppliers
			? `The ${TOP_SUPPLIERS} largest storefronts; the table has all ${data.suppliers.length}`
			: 'Variants collected from each storefront'}
	>
		{#snippet chart()}
			<BarChart items={supplierBars} format={count} labelWidth={144} />
		{/snippet}
		{#snippet table()}
			<table class="text-muted-foreground w-full text-xs">
				<thead>
					<tr class="text-left">
						<th class="py-1 pr-4">Supplier</th>
						<th class="py-1 pr-4 text-right">Products</th>
						<th class="py-1 text-right">With code</th>
					</tr>
				</thead>
				<tbody>
					{#each data.suppliers as row (row.supplier)}
						<tr>
							<td class="py-1 pr-4">{row.supplier}</td>
							<td class="py-1 pr-4 text-right tabular-nums">{count(row.products)}</td>
							<td class="py-1 text-right tabular-nums">{count(row.coded)}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/snippet}
	</ChartCard>

	<ChartCard
		title="Catalogue by family"
		subtitle="Share of all collected variants"
		note="Families are derived by the importer from the product name and its categories, not published by the suppliers. This panel always shows the whole mix - it is the one that says what the types are - so the product type filter does not scope it."
	>
		{#snippet chart()}
			<StackedBar segments={familySegments} format={count} />
		{/snippet}
		{#snippet table()}
			<table class="text-muted-foreground w-full text-xs">
				<thead>
					<tr class="text-left">
						<th class="py-1 pr-4">Family</th>
						<th class="py-1 text-right">Products</th>
					</tr>
				</thead>
				<tbody>
					{#each data.families as row (row.family)}
						<tr>
							<td class="py-1 pr-4">{row.family}</td>
							<td class="py-1 text-right tabular-nums">{count(row.products)}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/snippet}
	</ChartCard>
</div>

<div class="mt-8 grid gap-8">
	<ChartCard
		title="Median {data.pricedFamily ?? 'unit'} price per {data.measureWord}"
		subtitle="{bandLabel} {packWord}, EUR offers only{data.family
			? ''
			: ` - ${data.pricedFamily ?? 'no priced type'} unless another type is chosen`}{otherMedians
			? ` - the ${TOP_SUPPLIERS} cheapest of ${data.medians.length} suppliers`
			: ''}"
		note="Medians over the latest offer per product. Prices are supplier observations that can be VAT-inclusive or exclusive, and non-EUR listings are converted at the ECB reference rate."
	>
		{#snippet chart()}
			{#if medianBars.length}
				<BarChart items={medianBars} format={euros} emphasis={0} labelWidth={144} />
			{:else}
				<p class="text-muted-foreground text-sm">
					No supplier publishes enough comparable offers in this band.
				</p>
			{/if}
		{/snippet}
		{#snippet table()}
			<table class="text-muted-foreground w-full text-xs">
				<thead>
					<tr class="text-left">
						<th class="py-1 pr-4">Supplier</th>
						<th class="py-1 pr-4 text-right">Median EUR/{data.unit}</th>
						<th class="py-1 text-right">Offers</th>
					</tr>
				</thead>
				<tbody>
					{#each data.medians as row (row.supplier)}
						<tr>
							<td class="py-1 pr-4">{row.supplier}</td>
							<td class="py-1 pr-4 text-right tabular-nums">{row.median.toFixed(2)}</td>
							<td class="py-1 text-right tabular-nums">{count(row.offers)}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/snippet}
	</ChartCard>

	<ChartCard
		title="Widest price spread between suppliers"
		subtitle="{bandLabel} {packWord} sold by three or more suppliers, EUR per {data.measureWord}{data.family
			? ` - ${data.family} only`
			: ''}"
		note="A similar name does not prove two products are equivalent. These rows share a manufacturer code and a pack band; VAT basis and shipping still differ between suppliers."
	>
		{#snippet chart()}
			{#if spreadRows.length}
				<RangeChart items={spreadRows} />
			{:else}
				<p class="text-muted-foreground text-sm">
					No product in this size band is sold by three or more suppliers in EUR.
				</p>
			{/if}
		{/snippet}
		{#snippet table()}
			<table class="text-muted-foreground w-full text-xs">
				<thead>
					<tr class="text-left">
						<th class="py-1 pr-4">Code</th>
						<th class="py-1 pr-4">Product</th>
						<th class="py-1 pr-4 text-right">Suppliers</th>
						<th class="py-1 pr-4 text-right">Low</th>
						<th class="py-1 pr-4 text-right">High</th>
						<th class="py-1 text-right">Ratio</th>
					</tr>
				</thead>
				<tbody>
					{#each data.spread as row (row.code)}
						<tr>
							<td class="py-1 pr-4">{row.code}</td>
							<td class="py-1 pr-4">{row.name}</td>
							<td class="py-1 pr-4 text-right tabular-nums">{row.suppliers}</td>
							<td class="py-1 pr-4 text-right tabular-nums">{row.low.toFixed(2)}</td>
							<td class="py-1 pr-4 text-right tabular-nums">{row.high.toFixed(2)}</td>
							<td class="py-1 text-right tabular-nums">{row.ratio.toFixed(2)}x</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/snippet}
	</ChartCard>
</div>
