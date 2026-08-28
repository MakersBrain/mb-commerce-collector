<script lang="ts">
	import { goto, replaceState } from '$app/navigation';
	import { page } from '$app/state';
	import BarChart from '$lib/charts/BarChart.svelte';
	import ChartCard from '$lib/charts/ChartCard.svelte';
	import {
		COLUMNS,
		DEFAULT_COLUMNS,
		arrange,
		fit,
		isDefaultColumns,
		merge,
		readColumns,
		type ColumnKey
	} from '$lib/columns';
	import { SCHENGEN, countryName } from '$lib/countries';
	import { count, eur, firing, listedPrice as price, size, stock } from '$lib/format';
	import ProductGrid from '$lib/grid/ProductGrid.svelte';
	import { readSort, type Sort } from '$lib/catalogue';
	import { onMount } from 'svelte';
	import type { PageData } from './$types';

	let { data }: { data: PageData } = $props();

	/**
	 * What the sheet sends with every block request. It is this page's own query
	 * string rather than a rebuild of it, so the filters the rows come back for
	 * are by construction the filters the page is showing - there is no second
	 * serialisation to fall out of step with the first.
	 */
	const pageQuery = $derived(page.url.search);

	/**
	 * The sort in force, read off the URL rather than off the load.
	 *
	 * Sorting the sheet does not re-run the load - the grid has already asked for
	 * the re-ordered blocks by the time we hear about it, so a round trip would
	 * only re-fetch facets nobody changed. That leaves `data.sort` behind, and
	 * everything on the page that has to agree with the sheet reads this instead:
	 * the URL is rewritten first and is never stale.
	 */
	const sort = $derived<Sort>({
		key: readSort(page.url.searchParams.get('sort')),
		dir: page.url.searchParams.get('dir') === 'desc' ? 'desc' : 'asc'
	});

	/** Told by the sheet when the reader sorts it. */
	function sorted(next: Sort) {
		if (next.key === sort.key && next.dir === sort.dir) return;
		replaceState(href({ sort: next.key, dir: next.dir, page: null }), page.state);
	}

	/**
	 * The arrangement outlives the link.
	 *
	 * Which columns to show and where to put them is a preference, not a query:
	 * a reader who has trimmed fifteen columns to six and moved price next to the
	 * name wants that layout tomorrow, not only for as long as the tab is open.
	 *
	 * A URL that names its columns still wins, so a link someone sent arrives
	 * showing the table they were looking at rather than the recipient's. The
	 * store is only what a bare /explore falls back to, and every link built on
	 * the page carries the layout forward, so the moment the reader does anything
	 * their arrangement is in the URL and shareable again.
	 */
	const REMEMBERED = 'explore.columns';

	let remembered = $state<ColumnKey[] | null>(null);

	// Read once the page is in the browser. Reading it into state rather than
	// rewriting the URL on arrival keeps SvelteKit's router out of it: a bare
	// /explore stays bare until the reader changes something.
	onMount(() => {
		const saved = localStorage.getItem(REMEMBERED);
		if (saved) remembered = readColumns(new URLSearchParams([['col', saved]]));
	});

	/**
	 * Which columns are shown and in what order. Read off the URL for the same
	 * reason the sort is - dragging a column rewrites the URL without re-running
	 * the load, so `data.columns` is a snapshot of how the page arrived rather
	 * than of how it looks now - and off the store when the URL is silent.
	 */
	const columns = $derived(
		page.url.searchParams.has('col')
			? readColumns(page.url.searchParams)
			: (remembered ?? DEFAULT_COLUMNS)
	);

	function remember(chosen: readonly string[]) {
		try {
			localStorage.setItem(REMEMBERED, chosen.join(','));
		} catch {
			// Private browsing, or a full quota. The layout is still in the URL.
		}
	}

	function forget() {
		remembered = null;
		try {
			localStorage.removeItem(REMEMBERED);
		} catch {
			// Nothing to do: the reset navigation below is what the reader sees.
		}
	}

	/**
	 * Told by the sheet when the reader drags a column somewhere else.
	 *
	 * The sheet reports the order of the columns it was given, which on a narrow
	 * screen is fewer than the reader has chosen. Merging rather than assigning is
	 * what keeps a drag on a phone from deleting the columns that phone could not
	 * show in the first place.
	 */
	function arranged(next: ColumnKey[]) {
		const whole = merge(columns, next);
		remembered = whole;
		remember(whole);
		replaceState(href({}, whole), page.state);
	}

	/** The one-value filters, each a select. Supplier and country are lists and
	    get their own panels below. */
	const FACETS = [
		{ key: 'family', label: 'Product type' },
		{ key: 'brand', label: 'Brand' },
		{ key: 'series', label: 'Product line' },
		{ key: 'colour', label: 'Colour' },
		{ key: 'surface', label: 'Surface' },
		{ key: 'firing', label: 'Firing' },
		{ key: 'application', label: 'Application' },
		{ key: 'form', label: 'Form' }
	] as const;

	const supplierBars = $derived(
		data.facets.supplier.slice(0, 10).map((row) => ({ label: row.label, value: row.products }))
	);

	/** Only the countries some supplier in the catalogue actually ships from. */
	const schengenHere = $derived(
		data.facets.country.map((row) => row.value).filter((code) => SCHENGEN.includes(code))
	);

	const inSchengen = $derived(
		schengenHere.length > 0 &&
			schengenHere.every((code) => data.filters.countries.includes(code)) &&
			data.filters.countries.length === schengenHere.length
	);

	/** One removable chip per active filter, whichever kind it is. */
	const active = $derived([
		...FACETS.map((facet) => ({
			label: facet.label,
			value: data.filters[facet.key] as string,
			href: href({ [facet.key]: null, page: null })
		})).filter((entry) => entry.value),
		...data.filters.suppliers.map((id) => ({
			label: 'Supplier',
			value: supplierName(id),
			href: href({ page: null }, columns, {
				suppliers: data.filters.suppliers.filter((other) => other !== id)
			})
		})),
		...data.filters.excluded.map((id) => ({
			label: 'Not supplier',
			value: supplierName(id),
			href: href({ page: null }, columns, {
				excluded: data.filters.excluded.filter((other) => other !== id)
			})
		})),
		...data.filters.countries.map((code) => ({
			label: 'Country',
			value: countryName(code),
			href: href({ page: null }, columns, {
				countries: data.filters.countries.filter((other) => other !== code)
			})
		}))
	]);

	function supplierName(id: string) {
		return data.facets.supplier.find((row) => row.value === id)?.label ?? id;
	}

	const pages = $derived(Math.max(1, Math.ceil(data.total / data.pageSize)));

	/**
	 * A facet's counts are computed with its own filter excluded, so the chosen
	 * value can be absent from its list once another filter rules it out. Keep
	 * it in the list — a select that renders blank looks broken.
	 */
	function options(key: (typeof FACETS)[number]['key']) {
		const list = data.facets[key];
		const chosen = data.filters[key];
		if (!chosen || list.some((entry) => entry.value === chosen)) return list;
		return [{ value: chosen, label: chosen, products: 0 }, ...list];
	}

	/**
	 * Filters and view state are both URL state, so every link carries both.
	 * A non-default column set rides along as repeated `col` params, so a link to
	 * a trimmed-down table is as shareable as a link to a filtered one.
	 */
	type Lists = { suppliers?: string[]; excluded?: string[]; countries?: string[] };

	function href(
		patch: Record<string, string | null>,
		cols: readonly ColumnKey[] = columns,
		lists: Lists = {}
	) {
		const { suppliers, excluded, countries, ...single } = data.filters;
		const current = {
			...single,
			view: data.view === 'grid' ? null : data.view,
			sort: sort.key === 'name' ? null : sort.key,
			dir: sort.dir === 'asc' ? null : sort.dir,
			page: data.page > 1 ? String(data.page) : null
		};
		const params = new URLSearchParams();
		for (const [key, value] of Object.entries({ ...current, ...patch })) {
			if (value) params.set(key, String(value));
		}
		// The list filters repeat their param rather than joining, so a value that
		// ever contains a comma cannot be split in half by the reader.
		for (const id of lists.suppliers ?? suppliers) params.append('supplier', id);
		for (const id of lists.excluded ?? excluded) params.append('no_supplier', id);
		for (const code of lists.countries ?? countries) params.append('country', code);
		if (!isDefaultColumns(cols)) for (const key of cols) params.append('col', key);
		const query = params.toString();
		return query ? `/explore?${query}` : '/explore';
	}

	/** Only used to say "8 of 15" on the column picker; the sheet reads the set. */
	const visible = $derived(columns.length);

	/**
	 * The viewport, so the sheet can drop the columns this screen cannot show and
	 * the filter row can fold itself away. Starts at a desktop width rather than
	 * zero: the first server-rendered paint has no window to measure, and guessing
	 * narrow would make every desktop load flash a two-column table.
	 */
	let viewport = $state(1280);

	/** What the sheet actually draws: the reader's arrangement, minus what does
	    not fit. The arrangement itself is untouched and comes back on rotation. */
	const shown = $derived(fit(columns, viewport));

	/** Said out loud, because a column silently missing reads as a bug. */
	const dropped = $derived(columns.length - shown.length);

	/**
	 * On a phone the eight filter controls are taller than the results they scope,
	 * so they fold behind one button. From `sm` up the row is always open and this
	 * has no effect.
	 */
	let filtersOpen = $state(false);

	/** For the fold's own label: everything currently narrowing the selection. */
	const activeCount = $derived(
		active.length + (data.filters.q ? 1 : 0) + (data.filters.stock ? 1 : 0)
	);
</script>

<!-- The page is a column: everything that scopes the selection sits at a fixed
     height at the top, and the selection itself takes whatever is left. In the
     table view that makes the sheet the full width and depth of the viewport,
     with the filters staying put while it scrolls. -->
<svelte:window bind:innerWidth={viewport} />

<div class="flex h-full flex-col">
<div
	class="shrink-0 px-3 sm:px-6"
	class:pt-2={data.view === 'table'}
	class:pb-2={data.view === 'table'}
	class:sm:pt-3={data.view === 'table'}
	class:sm:pb-3={data.view === 'table'}
	class:pt-5={data.view !== 'table'}
	class:sm:pt-6={data.view !== 'table'}
>
<!-- Two headers for two jobs. Over the cards there is room to say what the
     catalogue is and where the prices come from; over the sheet every line
     spent here is a row of data the reader does not get, so the same two
     caveats are compressed to one line and the rest goes on its hover. -->
{#if data.view === 'table'}
	<div class="flex flex-wrap items-baseline gap-x-3 gap-y-1">
		<h1 class="text-lg font-semibold" style="color: var(--text-primary)">Catalogue explorer</h1>
		<p
			class="text-xs"
			style="color: var(--text-muted)"
			title="Every facet is derived by the importer from what the storefront published, so coverage differs per supplier. A converted figure is indicative, not a transaction rate, and the listed currency stays beside it."
		>
			{count(data.total)} products match &middot; prices in EUR at the ECB rate{data.fx.date
				? ` of ${data.fx.date}`
				: ''}{data.fx.stale ? ' (last stored rates)' : ''}
		</p>
	</div>
{:else}
	<h1 class="text-xl font-semibold sm:text-2xl" style="color: var(--text-primary)">
		Catalogue explorer
	</h1>
	<p class="measure mt-1 text-sm" style="color: var(--text-secondary)">
		{count(data.total)} products match. Every facet is derived by the importer from what the storefront
		published, so coverage differs per supplier.
	</p>
	<p class="measure mt-1 text-xs" style="color: var(--text-muted)">
		Prices are shown in EUR at the ECB reference rate{data.fx.date
			? ` of ${data.fx.date}`
			: ''}{data.fx.stale ? ' (last stored rates - the ECB was unreachable)' : ''}. A converted
		figure is indicative, not a transaction rate, and the listed currency stays beside it.
	</p>
{/if}

<!-- One filter row, above everything it scopes. Plain GET form: every state of
     this page is a URL that can be shared or bookmarked.
     The search and the commit buttons stay on screen at every width; the eight
     facet controls fold behind one button below `lg`. The fold is about height,
     not about phones: at 700px those controls wrap to four rows and leave the
     sheet 200px to draw 67,000 products in. From `lg` up the row fits on one or
     two lines and is always open. -->
<form
	method="GET"
	class:mt-2={data.view === 'table'}
	class:lg:mt-3={data.view === 'table'}
	class:mt-4={data.view !== 'table'}
	class:lg:mt-6={data.view !== 'table'}
>
	<!-- Carried through the form so applying a filter keeps the current view. -->
	{#if data.view !== 'grid'}<input type="hidden" name="view" value={data.view} />{/if}
	{#if sort.key !== 'name'}<input type="hidden" name="sort" value={sort.key} />{/if}
	{#if sort.dir !== 'asc'}<input type="hidden" name="dir" value={sort.dir} />{/if}
	{#if !isDefaultColumns(columns)}
		{#each columns as key (key)}<input type="hidden" name="col" value={key} />{/each}
	{/if}

	<div class="flex items-end gap-2">
		<label
			class="flex min-w-0 flex-1 flex-col gap-1 text-xs lg:flex-none"
			style="color: var(--text-secondary)"
		>
			<span class="hidden lg:inline">Name contains</span>
			<input
				type="search"
				name="q"
				value={data.filters.q}
				placeholder="celadon, rutile, chamotte"
				autocomplete="off"
				class="w-full rounded-lg px-3 py-2 text-sm lg:w-56"
				style="background: var(--surface-1); color: var(--text-primary); border: 1px solid var(--hairline)"
				aria-label="Name contains"
			/>
		</label>

		<!-- The fold. It counts what is already narrowing the selection, so a reader
		     arriving on a shared link can see there are filters in force without
		     having to open anything. -->
		<button
			type="button"
			class="shrink-0 rounded-lg px-3 py-2 text-sm whitespace-nowrap lg:hidden"
			style="border: 1px solid var(--hairline); color: var(--text-secondary)"
			aria-expanded={filtersOpen}
			onclick={() => (filtersOpen = !filtersOpen)}
		>
			Filters{activeCount ? ` (${activeCount})` : ''}
			<span aria-hidden="true">{filtersOpen ? '▴' : '▾'}</span>
		</button>

		<button
			type="submit"
			class="shrink-0 rounded-lg px-4 py-2 text-sm font-medium lg:hidden"
			style="background: var(--primary); color: var(--primary-foreground)">Apply</button
		>
	</div>

	<div
		class="mt-3 grid-cols-2 items-end gap-3 sm:grid-cols-3 lg:flex lg:flex-wrap {filtersOpen
			? 'grid'
			: 'hidden'}"
	>
	{#each FACETS as facet (facet.key)}
		<label class="flex min-w-0 flex-col gap-1 text-xs" style="color: var(--text-secondary)">
			{facet.label}
			<select
				name={facet.key}
				class="w-full rounded-lg px-3 py-2 text-sm lg:max-w-56"
				style="background: var(--surface-1); color: var(--text-primary); border: 1px solid var(--hairline)"
				value={data.filters[facet.key] ?? ''}
				onchange={(event) => event.currentTarget.form?.requestSubmit()}
			>
				<option value="">any</option>
				{#each options(facet.key) as option (option.value)}
					<option value={option.value}>{option.label} ({count(option.products)})</option>
				{/each}
			</select>
		</label>
	{/each}

	<!-- Suppliers and countries are lists, not single choices, so they are
	     checkbox panels rather than selects. Both sit inside this form: one
	     Apply commits the whole row. An unticked box submits nothing, which is
	     what keeps the resulting URL down to what was actually chosen. -->
	<div class="col-span-2 flex flex-col gap-1 text-xs sm:col-span-1">
		<span style="color: var(--text-secondary)">Supplier</span>
		<!-- Named, so opening one panel closes the other rather than stacking two
		     overlapping sheets on top of the row. -->
		<details class="relative" name="filter-panel">
			<summary
				class="cursor-pointer list-none rounded-lg px-3 py-2 text-sm"
				style="background: var(--surface-1); color: var(--text-primary); border: 1px solid var(--hairline)"
			>
				{#if data.filters.suppliers.length || data.filters.excluded.length}
					{[
						data.filters.suppliers.length ? `${data.filters.suppliers.length} only` : '',
						data.filters.excluded.length ? `${data.filters.excluded.length} excluded` : ''
					]
						.filter(Boolean)
						.join(', ')}
				{:else}
					any
				{/if}
			</summary>
			<!-- Hung from the right edge and capped to the viewport: this row sits at
			     the right of the page, and a panel that spilled past it would make
			     the whole page scroll sideways. -->
			<div
				class="absolute right-0 z-10 mt-2 max-h-80 w-72 max-w-[calc(100vw-2rem)] overflow-y-auto rounded-xl p-3 shadow-lg"
				style="background: var(--surface-1); border: 1px solid var(--hairline)"
			>
				<p class="mb-2 text-xs" style="color: var(--text-muted)">
					Tick <strong>only</strong> to narrow to those shops, or <strong>drop</strong> to remove one.
					Leave a shop untouched to keep it.
				</p>
				<div class="grid grid-cols-[1fr_auto_auto] items-center gap-x-2 gap-y-1">
					<span class="text-xs" style="color: var(--text-muted)"></span>
					<span class="text-center text-xs" style="color: var(--text-muted)">only</span>
					<span class="text-center text-xs" style="color: var(--text-muted)">drop</span>
					{#each data.facets.supplier as row (row.value)}
						<span class="truncate text-xs" style="color: var(--text-secondary)" title={row.label}>
							{row.label}
							<span style="color: var(--text-muted)">({count(row.products)})</span>
						</span>
						<input
							type="checkbox"
							name="supplier"
							value={row.value}
							checked={data.filters.suppliers.includes(row.value)}
							aria-label="Only {row.label}"
						/>
						<input
							type="checkbox"
							name="no_supplier"
							value={row.value}
							checked={data.filters.excluded.includes(row.value)}
							aria-label="Exclude {row.label}"
						/>
					{/each}
				</div>
			</div>
		</details>
	</div>

	<div class="col-span-2 flex flex-col gap-1 text-xs sm:col-span-1">
		<span style="color: var(--text-secondary)">Country</span>
		<details class="relative" name="filter-panel">
			<summary
				class="cursor-pointer list-none rounded-lg px-3 py-2 text-sm"
				style="background: var(--surface-1); color: var(--text-primary); border: 1px solid var(--hairline)"
			>
				{#if inSchengen}
					Schengen
				{:else if data.filters.countries.length}
					{data.filters.countries.map(countryName).join(', ')}
				{:else}
					any
				{/if}
			</summary>
			<div
				class="absolute right-0 z-10 mt-2 max-h-80 w-64 max-w-[calc(100vw-2rem)] overflow-y-auto rounded-xl p-3 shadow-lg"
				style="background: var(--surface-1); border: 1px solid var(--hairline)"
			>
				<!-- Links, not a toggle: the quick pick writes the countries out in
				     full so the ticks below stay an honest account of the selection,
				     and so a shared URL keeps meaning what it meant when it was sent. -->
				<div class="mb-2 flex items-center gap-3 text-xs">
					<a href={href({ page: null }, columns, { countries: schengenHere })} style="color: var(--primary)">
						Schengen
					</a>
					<a href={href({ page: null }, columns, { countries: [] })} style="color: var(--text-secondary)">
						Clear
					</a>
				</div>
				{#each data.facets.country as row (row.value)}
					<label class="flex items-center gap-2 py-0.5 text-xs" style="color: var(--text-secondary)">
						<input
							type="checkbox"
							name="country"
							value={row.value}
							checked={data.filters.countries.includes(row.value)}
						/>
						{countryName(row.value)}
						<span style="color: var(--text-muted)">({count(row.products)})</span>
					</label>
				{/each}
			</div>
		</details>
	</div>

	<label
		class="col-span-2 flex items-center gap-2 py-1 text-xs sm:col-span-1 lg:py-0"
		style="color: var(--text-secondary)"
	>
		<input
			type="checkbox"
			name="stock"
			value="in"
			checked={data.filters.stock === 'in'}
			onchange={(event) => event.currentTarget.form?.requestSubmit()}
		/>
		In stock only
	</label>

	<!-- The phone has its own Apply up beside the search box, where it is reachable
	     without scrolling past eight controls first. -->
	<button
		type="submit"
		class="hidden rounded-lg px-4 py-2 text-sm font-medium lg:block"
		style="background: var(--primary); color: var(--primary-foreground)">Apply</button
	>
	{#if active.length || data.filters.q}
		<a
			href="/explore"
			class="col-span-2 px-2 py-2 text-sm sm:col-span-1"
			style="color: var(--text-secondary)">Clear all</a
		>
	{/if}
	</div>
</form>

{#if active.length}
	<div class="mt-3 flex flex-wrap gap-2">
		{#each active as entry (entry.label + entry.value)}
			<a
				href={entry.href}
				class="rounded-full px-3 py-1 text-xs"
				style="border: 1px solid var(--hairline); color: var(--text-secondary)"
			>
				{entry.label}: {entry.value} &times;
			</a>
		{/each}
	</div>
{/if}



<div
	class="flex flex-wrap items-center justify-between gap-x-3 gap-y-2"
	class:mt-2={data.view === 'table'}
	class:sm:mt-3={data.view === 'table'}
	class:mt-6={data.view !== 'table'}
>
	<div class="flex gap-1 rounded-lg p-1" style="border: 1px solid var(--hairline)">
		{#each [{ id: 'grid', label: 'Cards' }, { id: 'table', label: 'Table' }] as mode (mode.id)}
			<a
				href={href({ view: mode.id === 'grid' ? null : mode.id })}
				class="rounded px-3 py-1 text-xs"
				style="color: {data.view === mode.id
					? 'var(--text-primary)'
					: 'var(--text-secondary)'}; background: {data.view === mode.id
					? 'color-mix(in srgb, var(--primary) 14%, transparent)'
					: 'transparent'}">{mode.label}</a
			>
		{/each}
	</div>
	<div class="flex flex-wrap items-center gap-3">
		<!-- Only the table has columns to pick; the grid card is a fixed layout. -->
		{#if data.view === 'table'}
			<details class="relative">
				<summary
					class="cursor-pointer list-none rounded-lg px-3 py-1 text-xs"
					style="border: 1px solid var(--hairline); color: var(--text-secondary)"
				>
					Columns ({visible} of {COLUMNS.length})
				</summary>
				<!-- Its own GET form: the filters and the sort ride along as hidden
				     fields so applying a column set changes nothing else. The
				     submission is also where the arrangement is saved, because the
				     ticks are the reader saying what they want kept. -->
				<form
					method="GET"
					class="absolute right-0 z-10 mt-2 w-56 rounded-xl p-3 shadow-lg"
					style="background: var(--surface-1); border: 1px solid var(--hairline)"
					onsubmit={(event) => remember(new FormData(event.currentTarget).getAll('col') as string[])}
				>
					<input type="hidden" name="view" value="table" />
					{#if data.filters.q}<input type="hidden" name="q" value={data.filters.q} />{/if}
					{#each FACETS as facet (facet.key)}
						{#if data.filters[facet.key]}
							<input type="hidden" name={facet.key} value={data.filters[facet.key]} />
						{/if}
					{/each}
					{#if data.filters.stock}<input type="hidden" name="stock" value={data.filters.stock} />{/if}
				{#each data.filters.suppliers as id (id)}
					<input type="hidden" name="supplier" value={id} />
				{/each}
				{#each data.filters.excluded as id (id)}
					<input type="hidden" name="no_supplier" value={id} />
				{/each}
				{#each data.filters.countries as code (code)}
					<input type="hidden" name="country" value={code} />
				{/each}
					{#if sort.key !== 'name'}<input type="hidden" name="sort" value={sort.key} />{/if}
					{#if sort.dir !== 'asc'}<input type="hidden" name="dir" value={sort.dir} />{/if}
					{#if data.page > 1}<input type="hidden" name="page" value={data.page} />{/if}

					<!-- Shown columns first, in the order the sheet shows them, then the
					     hidden ones. A GET form submits checkboxes in document order, so
					     this listing is what makes ticking a column append it to the
					     arrangement instead of dropping it into the middle. -->
					{#each arrange(columns) as column (column.key)}
						<label class="flex items-center gap-2 py-0.5 text-xs" style="color: var(--text-secondary)">
							<input
								type="checkbox"
								name="col"
								value={column.key}
								checked={columns.includes(column.key)}
							/>
							{column.label}
						</label>
					{/each}

					<div class="mt-3 flex items-center justify-between gap-2">
						<button
							type="submit"
							class="rounded-lg px-3 py-1 text-xs font-medium"
							style="background: var(--primary); color: var(--primary-foreground)">Apply</button
						>
						<!-- Reset drops the saved arrangement as well as the one on screen.
						     Without that it would be undone by the next bare /explore. -->
						<button
							type="button"
							class="text-xs"
							style="color: var(--text-secondary)"
							onclick={() => {
								forget();
								goto(href({}, DEFAULT_COLUMNS));
							}}>Reset</button
						>
					</div>
				</form>
			</details>
			<!-- A column that is simply missing reads as a bug, so the sheet says how
			     many this screen could not take. The arrangement itself is untouched:
			     widening the window brings them straight back. -->
			{#if dropped > 0}
				<span
					class="text-xs"
					style="color: var(--text-muted)"
					title="Your column arrangement is kept. Widen the window, or turn the phone on its side, and these come back."
				>
					{dropped} too wide for this screen
				</span>
			{/if}
		{/if}
		<!-- The sheet says this in its own column header; only the card view,
		     which has no header to click, needs it spelled out. -->
		{#if data.view !== 'table'}
			<span class="text-xs" style="color: var(--text-muted)">
				sorted by {COLUMNS.find((column) => column.key === sort.key)?.label ?? sort.key}, {sort.dir ===
				'asc'
					? 'ascending'
					: 'descending'}
			</span>
		{/if}
	</div>
	</div>
</div><!-- end of the filter and toolbar chrome -->

{#if data.view === 'table'}
	<!-- Edge to edge and full height: no card, no rounding, no page margin. The
	     sheet owns the rest of the viewport and scrolls inside itself, so the
	     header and the filters above it never move. -->
	<div class="min-h-0 flex-1" style="border-top: 1px solid var(--gridline)">
		<ProductGrid
			columns={shown}
			width={viewport}
			query={pageQuery}
			total={data.total}
			{sort}
			onSort={sorted}
			onArrange={arranged}
			onOpen={(row) => goto(`/products/${row.id}`)}
		/>
	</div>
{:else}
	<!-- The card view keeps the page's own margins: cards want air around them,
	     which is exactly what the sheet does not want. -->
	<div class="min-h-0 flex-1 overflow-y-auto px-3 pb-8 sm:px-6">
	<!-- Only over the card view: in the table view the sheet is the point of the
	     page, and the same breakdown is on the overview page where it has room.
	     It scrolls with the cards rather than sitting in the fixed chrome, so it
	     costs the reader nothing once they have scrolled past it. -->
	{#if data.total > 0 && data.view !== 'table'}
		<div class="mt-6">
			<ChartCard
				title="Where these products come from"
				subtitle="Suppliers stocking the current selection"
			>
				{#snippet chart()}
					<BarChart items={supplierBars} format={count} labelWidth={160} />
				{/snippet}
				{#snippet table()}
					<table class="w-full text-xs" style="color: var(--text-secondary)">
						<thead>
							<tr class="text-left">
								<th class="py-1 pr-4">Supplier</th>
								<th class="py-1 text-right">Products</th>
							</tr>
						</thead>
						<tbody>
							{#each data.facets.supplier as row (row.value)}
								<tr>
									<td class="py-1 pr-4">{row.label}</td>
									<td class="py-1 text-right tabular-nums">{count(row.products)}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				{/snippet}
			</ChartCard>
		</div>
	{/if}
	<!-- The cards are a fixed size and the column count follows the window, so a
	     wide monitor shows more of the catalogue rather than four stretched cards.
	     Capped at five: past that the eye has to track too far to compare two. -->
	<div class="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 2xl:grid-cols-5">
	{#each data.rows as row (row.id)}
		<article class="viz-surface flex gap-3 rounded-xl p-3 text-left">
			{#if row.image_url}
				<img
					src={row.image_url}
					alt=""
					loading="lazy"
					referrerpolicy="no-referrer"
					class="h-16 w-16 shrink-0 rounded-lg object-cover"
					style="background: color-mix(in srgb, var(--recede) 40%, transparent)"
					onerror={(event) => ((event.currentTarget as HTMLImageElement).style.visibility = 'hidden')}
				/>
			{/if}
			<div class="min-w-0">
				<a
					href="/products/{row.id}"
					class="line-clamp-2 text-sm font-medium"
					style="color: var(--text-primary)">{row.name}</a
				>
				<div class="mt-1 flex flex-wrap items-center gap-1.5 text-xs">
					{#if row.code}
						<a
							href="/compare?q={encodeURIComponent(row.code)}"
							class="rounded px-1.5 py-0.5 tabular-nums"
							style="background: color-mix(in srgb, var(--primary) 14%, transparent); color: var(--text-primary)"
							>{row.code}</a
						>
					{/if}
					<span style="color: var(--text-muted)"
						>{row.supplier_label}{row.country ? ` (${row.country})` : ''}</span
					>
					{#if row.brand}
						<span style="color: var(--text-muted)">- {row.brand}</span>
					{/if}
				</div>
				<div class="mt-1 text-xs" style="color: var(--text-secondary)">
					{[
						row.family,
						row.colour,
						row.surface,
						firing(row),
						row.form === 'powder' ? 'dry mix' : row.form,
						row.application_methods?.join('/')
					]
						.filter(Boolean)
						.join(' - ')}
				</div>
				{#if row.price_eur != null}
					<div class="mt-1 text-xs tabular-nums" style="color: var(--text-primary)">
						{eur(row.price_eur)}
						{#if row.unit_price_eur}
							<span style="color: var(--text-muted)">
								({row.unit_price_eur.toFixed(2)} EUR/{row.unit_price_per})
							</span>
						{/if}
						{#if row.currency && row.currency !== 'EUR'}
							<span style="color: var(--text-muted)">- listed {price(row)}</span>
						{/if}
					</div>
				{/if}
				{#if stock(row)}
					<div
						class="mt-1 text-xs"
						style="color: {stock(row)!.inStock ? 'var(--good)' : 'var(--text-muted)'}"
					>
						<span aria-hidden="true">{stock(row)!.inStock ? '\u25CF' : '\u25CB'}</span>
						{stock(row)!.label}
					</div>
				{/if}
				<a href={row.url} target="_blank" rel="noreferrer noopener" class="mt-2 inline-block text-xs underline-offset-4 hover:underline" style="color: var(--primary)">Open storefront ↗</a>
			</div>
		</article>
	{/each}
	</div>

	{#if data.total === 0}
		<p class="mt-8 text-sm" style="color: var(--text-muted)">
			Nothing matches this combination. Facet counts above are computed against the other filters, so
			widening one of them will bring rows back.
		</p>
	{/if}

	<!-- Only the card view pages. The sheet is one continuous run of rows, so
	     there is no page to be on. -->
	{#if pages > 1}
		<nav class="mt-6 flex items-center gap-3 text-sm" style="color: var(--text-secondary)">
			{#if data.page > 1}
				<a href={href({ page: String(data.page - 1) })} class="rounded-lg px-3 py-1.5"
					style="border: 1px solid var(--hairline)">Previous</a
				>
			{/if}
			<span class="text-xs">page {data.page} of {count(pages)}</span>
			{#if data.page < pages}
				<a href={href({ page: String(data.page + 1) })} class="rounded-lg px-3 py-1.5"
					style="border: 1px solid var(--hairline)">Next</a
				>
			{/if}
		</nav>
	{/if}
	</div>
{/if}
</div>
