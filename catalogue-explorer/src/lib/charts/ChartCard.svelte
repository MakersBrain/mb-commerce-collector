<script lang="ts">
	import type { Snippet } from 'svelte';

	let {
		title,
		subtitle,
		note,
		chart,
		table,
		tableAlwaysVisible = false
	}: {
		title: string;
		subtitle?: string;
		note?: string;
		chart: Snippet;
		/** Every chart ships a table view, so no value is reachable only by hover. */
		table?: Snippet;
		/** Comparison screens can keep the table visible instead of using disclosure. */
		tableAlwaysVisible?: boolean;
	} = $props();
</script>

<section class="viz-surface rounded-xl p-4 sm:p-5">
	<header class="mb-4">
		<h2 class="text-base font-semibold" style="color: var(--text-primary)">{title}</h2>
		{#if subtitle}
			<p class="mt-0.5 text-sm" style="color: var(--text-secondary)">{subtitle}</p>
		{/if}
	</header>

	{@render chart()}

	{#if note}
		<p class="mt-4 text-xs leading-relaxed" style="color: var(--text-muted)">{note}</p>
	{/if}

	{#if table}
		{#if tableAlwaysVisible}
			<div class="mt-4 overflow-x-auto">
				{@render table()}
			</div>
		{:else}
			<details class="mt-3 text-sm">
				<summary class="cursor-pointer text-xs" style="color: var(--text-secondary)">
					Table view
				</summary>
				<div class="mt-2 overflow-x-auto">
					{@render table()}
				</div>
			</details>
		{/if}
	{/if}
</section>
