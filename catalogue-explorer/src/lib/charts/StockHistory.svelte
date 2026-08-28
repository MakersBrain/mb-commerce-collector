<script lang="ts">
	import type { TrendObservation } from '$lib/trends';

	let { points, height = 96 }: { points: TrendObservation[]; height?: number } = $props();
	const width = 460;
	const pad = 5;
	const runs = $derived.by(() => {
		const found: { at: number; value: number }[][] = [];
		for (const point of points) {
			if (point.stock_quantity_kind !== 'exact' || point.stock_quantity === null) {
				if (found.at(-1)?.length) found.push([]);
				continue;
			}
			if (!found.length) found.push([]);
			found.at(-1)!.push({ at: new Date(point.observed_at).getTime(), value: point.stock_quantity });
		}
		return found.filter((run) => run.length);
	});
	const exact = $derived(runs.flat());
	const scale = $derived.by(() => {
		if (!exact.length) return null;
		const start = exact[0].at;
		const end = exact.at(-1)!.at;
		const high = Math.max(1, ...exact.map((point) => point.value));
		return {
			x: (at: number) => (end === start ? width / 2 : ((at - start) / (end - start)) * width),
			y: (value: number) => pad + (height - pad * 2) * (1 - value / high),
			high
		};
	});
	const path = $derived.by(() => {
		if (!scale || !exact.length) return '';
		let value = '';
		for (const run of runs) {
			value += ` M ${scale.x(run[0].at)} ${scale.y(run[0].value)}`;
			for (let index = 1; index < run.length; index += 1) {
				value += ` L ${scale.x(run[index].at)} ${scale.y(run[index - 1].value)}`;
				value += ` L ${scale.x(run[index].at)} ${scale.y(run[index].value)}`;
			}
		}
		return value;
	});
</script>

{#if scale && exact.length}
	<div class="flex items-baseline justify-between text-xs" style="color: var(--text-secondary)">
		<span>Exact inventory ({exact.length} reading{exact.length === 1 ? '' : 's'})</span>
		<span class="tabular-nums">latest {exact.at(-1)!.value} · peak {scale.high}</span>
	</div>
	<svg
		viewBox="0 0 {width} {height}"
		class="mt-1 w-full"
		style="height: {height}px"
		preserveAspectRatio="none"
		role="img"
		aria-label="Exact stock quantity step chart, latest {exact.at(-1)!.value}"
	>
		<line x1="0" x2={width} y1={height - pad} y2={height - pad} stroke="var(--hairline)" />
		<path d={path} fill="none" stroke="var(--primary)" stroke-width="2" vector-effect="non-scaling-stroke" />
	</svg>
{:else}
	<p class="text-muted-foreground text-sm">This provider does not publish exact inventory.</p>
{/if}
