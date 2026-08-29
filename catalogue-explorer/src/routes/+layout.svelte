<script lang="ts">
	import '../app.css';
	// The chop rather than the plain weave: several MakersBrain surfaces are
	// usually open at once, and a tab is the one place the wordmark beside the
	// mark is too small to say which of them a tab belongs to.
	import favicon from '@makersbrain/ui/logo/chop.svg';
	import SurfaceLockup from '$lib/components/SurfaceLockup.svelte';
	import { page } from '$app/state';

	let { children, data } = $props();

	let theme: 'light' | 'dark' | null = $state(null);

	// Start from the reader's own choice if they made one, otherwise from the OS,
	// so the button always offers the other theme rather than guessing.
	$effect(() => {
		if (theme === null) {
			const saved = localStorage.getItem('theme');
			theme =
				saved === 'dark' || saved === 'light'
					? saved
					: matchMedia('(prefers-color-scheme: dark)').matches
						? 'dark'
						: 'light';
			document.documentElement.setAttribute('data-theme', theme);
		}
	});

	// The toggle has to beat the OS setting in both directions, so it stamps
	// data-theme on the root element rather than flipping a class.
	function toggle() {
		theme = theme === 'dark' ? 'light' : 'dark';
		localStorage.setItem('theme', theme);
		document.documentElement.setAttribute('data-theme', theme);
	}

	/** The one page that wants the whole window rather than a column of text. */
	const sheet = $derived(page.url.pathname.startsWith('/explore'));

	function tab(active: boolean) {
		return `color: ${active ? 'var(--text-primary)' : 'var(--text-secondary)'}; background: ${
			active ? 'color-mix(in srgb, var(--primary) 12%, transparent)' : 'transparent'
		}`;
	}

	/**
	 * The tabs, each with the short label a phone gets. The operations section has
	 * its own layout and its own live stream, so it is a link out of this shell
	 * rather than a tab inside it - but it belongs in the same row, because to the
	 * reader it is one of four places to be.
	 */
	const TABS = $derived([
		{ href: '/', label: 'Overview', short: 'Overview', exact: true },
		{ href: '/explore', label: 'Explore', short: 'Explore', exact: false },
		{ href: '/compare', label: 'Compare', short: 'Compare', exact: false },
		...(data.trendsEnabled
			? [{ href: '/trends', label: 'Trends', short: 'Trends', exact: false }]
			: []),
		{ href: '/ops', label: 'Operations', short: 'Ops', exact: false }
	]);

	const here = $derived((entry: (typeof TABS)[number]) =>
		entry.exact ? page.url.pathname === entry.href : page.url.pathname.startsWith(entry.href)
	);
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
	<title>Ceramics catalogue explorer</title>
</svelte:head>

<!--
	The shell is a fixed-height column: the nav keeps its place and everything
	below it scrolls inside `main`. That is what lets /explore hand its whole
	remaining height to the sheet, which has to know how tall it is before it can
	decide how many rows to draw.
-->
<div class="flex h-dvh flex-col">
	<header class="shrink-0 border-b" style="border-color: var(--hairline)">
		<!--
			One row at every width. It used to wrap, which on a phone cost three lines
			of a viewport that has about twelve - and the thing pushed down the screen
			was the table the reader came for. Now the title shortens, the tabs shorten,
			and if it still does not fit the tab strip scrolls sideways on its own
			rather than taking the page with it.
		-->
		<nav
			class="mx-auto flex w-full max-w-(--shell) items-center gap-3 px-3 py-2 sm:gap-6 sm:px-6 sm:py-3"
		>
			<!--
				The product word only needs its size bringing down to the nav's, so it
				sits with the tabs beside it rather than with the wordmark it follows.
				The colours are the brand's own and are no longer bridged: the page
				neutrals now resolve to `--mb-*` themselves, so restating them here
				once meant `--mb-text-muted: var(--text-secondary)` pointing at a
				`--text-secondary` that was already `var(--mb-text-muted)` -- a
				custom-property cycle, which CSS resolves by throwing both away.
			-->
			<SurfaceLockup product="Catalogue" size="1.25rem" style="--mb-text-body: 0.875rem" />
			<div class="-mx-1 flex min-w-0 flex-1 gap-1 overflow-x-auto px-1 [scrollbar-width:none]">
				{#each TABS as entry (entry.href)}
					<a
						href={entry.href}
						class="rounded-lg px-2.5 py-1.5 text-sm whitespace-nowrap sm:px-3"
						style={tab(here(entry))}
						aria-current={here(entry) ? 'page' : undefined}
					>
						<span class="hidden sm:inline">{entry.label}</span>
						<span class="sm:hidden">{entry.short}</span>
					</a>
				{/each}
			</div>
			<!-- A glyph on a phone, where the words would cost a tab. The accessible
			     name says which theme the press moves to either way. -->
			<button
				type="button"
				class="shrink-0 rounded-lg px-2.5 py-1.5 text-xs sm:px-3"
				style="color: var(--text-secondary); border: 1px solid var(--hairline)"
				onclick={toggle}
				aria-label={theme === 'dark' ? 'Switch to the light theme' : 'Switch to the dark theme'}
			>
				<span class="hidden sm:inline">{theme === 'dark' ? 'Light theme' : 'Dark theme'}</span>
				<span aria-hidden="true" class="sm:hidden">{theme === 'dark' ? '☀' : '☽'}</span>
			</button>
		</nav>
	</header>

	<!--
		The explore page is a spreadsheet, so it gets the viewport unpadded and
		unbounded and does its own spacing; the reading pages keep the measure that
		makes prose and charts legible.
	-->
	<main
		class="min-h-0 w-full flex-1 {sheet
			? 'overflow-hidden'
			: 'mx-auto max-w-(--shell) overflow-y-auto px-4 py-5 sm:px-6 sm:py-8'}"
	>
		{@render children()}
	</main>
</div>
