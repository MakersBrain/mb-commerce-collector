<script lang="ts">
	import type { Snippet } from 'svelte';
	import { Panel } from '@makersbrain/ui/svelte';

	/**
	 * A chart, what it is about, and the same numbers as a table.
	 *
	 * The chrome is the shared `Panel`: a hairline above and a title, rather than
	 * a bordered card. A page of these reads as one page with five parts instead
	 * of five objects to sort, and it stops a card from ever being nested inside
	 * another card.
	 *
	 * The plot itself keeps `.viz-surface`, and that is not an oversight. The
	 * chart palette was measured for colour-blind separation against that exact
	 * ground; the brand's card colour is a different, warmer one, and moving the
	 * marks onto it would quietly invalidate the measurement. So the panel is on
	 * the page and the plot is on its own island.
	 */
	let {
		title,
		subtitle,
		note,
		chart,
		table,
		tableAlwaysVisible = false,
		class: className = ''
	}: {
		title: string;
		subtitle?: string;
		note?: string;
		chart: Snippet;
		/** Every chart ships a table view, so no value is reachable only by hover. */
		table?: Snippet;
		/** Comparison screens can keep the table visible instead of using disclosure. */
		tableAlwaysVisible?: boolean;
		class?: string;
	} = $props();
</script>

<Panel {title} {subtitle} class={className}>
	<div class="viz-surface rounded-lg p-3 sm:p-4">
		{@render chart()}
	</div>

	{#if note}
		<p class="mb-panel-note">{note}</p>
	{/if}

	{#if table}
		{#if tableAlwaysVisible}
			<div class="mt-4 overflow-x-auto">
				{@render table()}
			</div>
		{:else}
			<details class="mt-3">
				<summary class="text-muted-foreground cursor-pointer text-xs">Table view</summary>
				<div class="mt-2 overflow-x-auto">
					{@render table()}
				</div>
			</details>
		{/if}
	{/if}
</Panel>
