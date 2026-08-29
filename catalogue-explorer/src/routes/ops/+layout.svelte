<script lang="ts">
	import { page } from '$app/state';
	import { invalidateAll } from '$app/navigation';
	import { onMount, setContext } from 'svelte';
	import { Tabs, StatusBadge } from '@makersbrain/ui/svelte';
	import { OpsStream } from '$lib/ops/stream.svelte';
	import ConnectionBadge from '$lib/ops/ConnectionBadge.svelte';

	let { children } = $props();

	// One subscription for the whole section. Every page reads from this store;
	// only /ops/runs/[id] opens a second, narrower one, so a browser never has
	// more than two streams open against the six-per-origin cap.
	const stream = new OpsStream('workers,notifications,runs,progress,proxies');
	setContext('ops-stream', stream);

	onMount(() => {
		stream.connect(() => invalidateAll());
		return () => stream.disconnect();
	});

	const tabs = [
		{ href: '/ops', label: 'Overview', exact: true },
		{ href: '/ops/runs', label: 'Runs' },
		{ href: '/ops/sources', label: 'Sources' },
		{ href: '/ops/notifications', label: 'Notifications' },
		{ href: '/ops/metrics', label: 'Metrics' },
		{ href: '/ops/proxies', label: 'Proxies' }
	];

	const unacknowledged = $derived(stream.unacknowledged.length);
	const busy = $derived(stream.workers.filter((w) => w.status === 'busy').length);
	const activeJobs = $derived(
		stream.workers.reduce((total, worker) => total + (worker.current_jobs?.length ?? 0), 0)
	);
</script>

<!--
	Operations used to declare a header of its own: a second brand row, a second
	tab idiom, and a sunken ground, so crossing the nav from the catalogue was
	crossing a seam between two applications. It is one application. What is left
	here is the second level of navigation - which view of operations - and the
	two live figures that only exist while the stream is connected.
-->
<div class="mb-6 flex flex-wrap items-center gap-x-4 gap-y-1">
	<Tabs items={tabs} current={page.url.pathname} label="Operations sections" class="flex-1">
		{#snippet badge(tab)}
			{#if tab.href === '/ops/notifications' && unacknowledged > 0}
				<StatusBadge tone="warn" class="tabular-nums">
					{unacknowledged}
					<span class="sr-only">unacknowledged</span>
				</StatusBadge>
			{/if}
		{/snippet}
	</Tabs>

	<div class="text-muted-foreground flex shrink-0 items-center gap-3 text-xs">
		<span class="hidden lg:inline">
			{busy}/{stream.workers.length} workers busy · {activeJobs} active jobs
		</span>
		<ConnectionBadge state={stream.connection} />
	</div>
</div>

{@render children()}
