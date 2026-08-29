<script lang="ts">
	import { getContext } from 'svelte';
	import { onMount } from 'svelte';
	import { enhance } from '$app/forms';
	import { DataList, Metric, Panel, SectionHeader, TableWrap } from '@makersbrain/ui/svelte';
	import type { OpsStream } from '$lib/ops/stream.svelte';
	import type { QueueStatus } from '$lib/ops/types';
	import WorkerCard from '$lib/ops/WorkerCard.svelte';
	import Unavailable from '$lib/ops/Unavailable.svelte';
	import { compact, count, relative, duration, stateTone } from '$lib/ops/format';
	import * as Table from '$lib/components/ui/table';
	import { StatusBadge } from '$lib/components/ui/status-badge';
	import { Button } from '$lib/components/ui/button';
	import { NativeSelect } from '$lib/components/ui/native-select';
	import { checkboxClass } from '$lib/components/ui/checkbox';

	let { data, form } = $props();
	const stream = getContext<OpsStream>('ops-stream');

	let picking = $state(false);
	let chosen = $state<string[]>([]);
	let polledQueueStats = $state<QueueStatus | undefined>();
	let queueRefreshError = $state<string | undefined>();
	const queueStats = $derived(polledQueueStats ?? data.queueStats);

	// The stream's roster is authoritative once it arrives; the loaded list is
	// only there so the first paint is not empty.
	const workers = $derived(stream.workers.length ? stream.workers : (data.workers ?? []));
	const queued = $derived(stream.queue.queued ?? 0);
	const running = $derived(stream.queue.running ?? 0);
	const brokerReady = $derived(
		totalSupported((queueStats?.broker?.routes ?? []).map((route) => route.ready.value))
	);
	const brokerInFlight = $derived(
		totalSupported((queueStats?.broker?.routes ?? []).map((route) => route.in_flight.value))
	);
	const providerLabels = {
		nats: 'NATS JetStream',
		cloudflare: 'Cloudflare Queues'
	} as const;
	const providerLabel = $derived(
		queueStats?.broker?.provider ? providerLabels[queueStats.broker.provider] : 'delivery provider'
	);
	const lastRun = $derived((data.runs ?? [])[0]);
	const nextFire = $derived(
		(data.schedules ?? [])
			.filter((s: any) => s.enabled && s.next_fire_at)
			.sort((a: any, b: any) => a.next_fire_at.localeCompare(b.next_fire_at))[0]
	);

	function bytes(value: number | null | undefined): string {
		if (value == null) return '—';
		if (value < 1024) return `${value} B`;
		if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KiB`;
		return `${(value / 1024 ** 2).toFixed(1)} MiB`;
	}

	function measured(value: { value?: number | null }): number | null | undefined {
		return value.value;
	}

	function totalSupported(values: (number | null | undefined)[]): number | undefined {
		const supported = values.filter((value): value is number => value != null);
		return supported.length ? supported.reduce((total, value) => total + value, 0) : undefined;
	}

	function quality(value: { accuracy: string }): string {
		return value.accuracy === 'best_effort' ? '≈' : '';
	}

	function age(value: { value?: number | null }): string {
		return value.value == null ? '—' : compact(value.value);
	}

	const jobFacts = $derived(
		queueStats
			? [
					{ term: 'Eligible', value: count(queueStats.eligible) },
					{ term: 'Leased', value: count(queueStats.jobs.leased ?? 0) },
					{ term: 'Running', value: count(queueStats.jobs.running ?? 0) },
					{ term: 'Oldest wait', value: compact(queueStats.oldest_queued_age_seconds ?? 0) }
				]
			: []
	);

	const outboxFacts = $derived(
		queueStats
			? [
					{ term: 'Ready', value: count(queueStats.outbox.ready) },
					{ term: 'Delayed', value: count(queueStats.outbox.delayed) },
					{ term: 'With errors', value: count(queueStats.outbox.errored) },
					{ term: 'Published 1h', value: count(queueStats.outbox.published_last_hour) }
				]
			: []
	);

	const brokerFacts = $derived.by(() => {
		const broker = queueStats?.broker;
		if (!broker) return [];
		const facts = [
			{
				term: 'Backlog',
				value: `${quality(broker.backlog_messages)}${count(measured(broker.backlog_messages))}`,
				title: broker.backlog_messages.accuracy
			},
			{ term: 'In flight', value: count(brokerInFlight) },
			{ term: 'Consumers', value: count(measured(broker.consumer_count)) },
			{
				term: 'Storage',
				value: `${quality(broker.backlog_bytes)}${bytes(measured(broker.backlog_bytes))}`,
				title: broker.backlog_bytes.accuracy
			}
		];
		if (broker.recovery_dlq) {
			facts.push({
				term: 'Recovery DLQ',
				value: `${quality(broker.recovery_dlq.backlog_messages)}${count(measured(broker.recovery_dlq.backlog_messages))}`,
				title: broker.recovery_dlq.backlog_messages.accuracy
			});
		}
		return facts;
	});

	onMount(() => {
		let live = true;
		const refresh = async () => {
			try {
				const response = await fetch('/ops/queue');
				if (!response.ok) throw new Error(`queue status returned ${response.status}`);
				const next = (await response.json()) as QueueStatus;
				if (live) {
					polledQueueStats = next;
					queueRefreshError = undefined;
				}
			} catch (error) {
				if (live) queueRefreshError = error instanceof Error ? error.message : String(error);
			}
		};
		const timer = window.setInterval(refresh, 5000);
		return () => {
			live = false;
			window.clearInterval(timer);
		};
	});
</script>

<svelte:head><title>Operations · catalogue</title></svelte:head>

{#if data.unavailable}
	<Unavailable reason={data.unavailable} />
{:else}
	<div class="grid gap-8">
		<!-- The state of the section in four numbers, ruled rather than boxed. -->
		<section class="mb-metrics mb-metrics-ruled">
			<Metric
				label="Queue"
				value={queued}
				detail={queueStats
					? `${count(brokerReady)} broker ready · ${queueStats.outbox.pending} outbox`
					: `${running} running`}
			/>

			<Metric
				label="Last run"
				value={lastRun ? lastRun.status : '—'}
				tone={lastRun ? undefined : 'quiet'}
				detail={lastRun
					? `${relative(lastRun.created_at)} · ${lastRun.succeeded}/${lastRun.jobs} ok`
					: undefined}
			/>

			<Metric
				label="Next scheduled"
				value={nextFire ? relative(nextFire.next_fire_at) : '—'}
				detail={nextFire?.id ?? 'no schedule enabled'}
			/>

			<!-- The one metric that is also a place to go: an unacknowledged count
			     is only useful beside the page that clears it. -->
			<Metric
				label="Unacknowledged"
				value={stream.unacknowledged.length}
				tone={stream.unacknowledged.length ? 'warn' : undefined}
				detail="notifications"
				href="/ops/notifications"
			/>
		</section>

		<!--
			One subject, three columns, and the per-route detail under them. It was a
			bordered card holding three hand-built definition grids, and the grids are
			now the shared `DataList` -- the same two columns, right-aligned and
			tabular, that the console builds by hand on its own status page.
		-->
		<Panel
			title="Queue delivery"
			subtitle="PostgreSQL authority → transactional outbox → {providerLabel}"
		>
			{#snippet actions()}
				<StatusBadge tone={queueStats?.broker?.available ? 'good' : 'bad'}>
					{queueStats?.broker?.available ? providerLabel : 'provider unavailable'}
				</StatusBadge>
			{/snippet}

			{#if queueStats}
				<div class="grid gap-6 sm:grid-cols-2 lg:grid-cols-3">
					<div>
						<h3 class="mb-eyebrow">Jobs</h3>
						<DataList items={jobFacts} />
					</div>

					<div>
						<h3 class="mb-eyebrow">Outbox</h3>
						<DataList items={outboxFacts} />
					</div>

					<div>
						<h3 class="mb-eyebrow">{providerLabel}</h3>
						{#if queueStats.broker}
							<DataList items={brokerFacts} />
							{#if queueStats.broker.error}
								<p class="text-destructive mt-2 text-xs">{queueStats.broker.error}</p>
							{:else if queueStats.broker.last_success_at}
								<p class="text-muted-foreground mt-2 text-xs">
									snapshot {relative(queueStats.broker.last_success_at)}
								</p>
							{/if}
						{:else}
							<p class="text-destructive text-sm">
								{queueStats.broker_error ?? 'No broker snapshot.'}
							</p>
						{/if}
					</div>
				</div>

				{#if queueStats.broker}
					<TableWrap class="mt-6">
						<Table.Root>
							<Table.Header>
								<Table.Row>
									<Table.Head>Route</Table.Head>
									<Table.Head class="text-right">Ready</Table.Head>
									<Table.Head class="text-right">In flight</Table.Head>
									<Table.Head class="text-right">Redelivered</Table.Head>
									<Table.Head class="text-right">Delivered</Table.Head>
									<Table.Head class="text-right">Oldest</Table.Head>
								</Table.Row>
							</Table.Header>
							<Table.Body>
								{#each queueStats.broker.routes as route (route.route)}
									<Table.Row>
										<Table.Cell><code>{route.route}</code></Table.Cell>
										<Table.Cell class="text-right tabular-nums" title={route.ready.accuracy}
											>{quality(route.ready)}{count(measured(route.ready))}</Table.Cell
										>
										<Table.Cell class="text-right tabular-nums" title={route.in_flight.accuracy}
											>{quality(route.in_flight)}{count(measured(route.in_flight))}</Table.Cell
										>
										<Table.Cell class="text-right tabular-nums" title={route.redelivered.accuracy}
											>{quality(route.redelivered)}{count(measured(route.redelivered))}</Table.Cell
										>
										<Table.Cell class="text-right tabular-nums" title={route.delivered.accuracy}
											>{quality(route.delivered)}{count(measured(route.delivered))}</Table.Cell
										>
										<Table.Cell
											class="text-right tabular-nums"
											title={route.oldest_age_seconds.accuracy}
											>{quality(route.oldest_age_seconds)}{age(route.oldest_age_seconds)}</Table.Cell
										>
									</Table.Row>
								{/each}
							</Table.Body>
						</Table.Root>
					</TableWrap>
				{/if}

				<p class="text-muted-foreground mt-3 text-xs">
					Updated {relative(queueStats.at)} · refreshes every 5s
					{#if queueRefreshError} · refresh failed: {queueRefreshError}{/if}
				</p>
			{:else}
				<p class="text-muted-foreground text-sm">Queue details are loading.</p>
			{/if}
		</Panel>

		<Panel title="Run now" subtitle="Start a collection across every source, or a chosen few">
			{#snippet actions()}
				<Button variant="ghost" size="xs" onclick={() => (picking = !picking)}>
					{picking ? 'all sources' : 'pick sources'}
				</Button>
			{/snippet}

			<form method="POST" action="?/run" use:enhance class="grid gap-3">
				{#if picking}
					<div
						class="border-border grid max-h-56 grid-cols-2 gap-1 overflow-y-auto rounded-md border p-2 sm:grid-cols-3 lg:grid-cols-4"
					>
						{#each data.sources ?? [] as source (source.source_id)}
							<label class="flex items-center gap-2 text-sm">
								<!-- A native input, not `<Checkbox>`: see `checkboxClass`. -->
								<input
									type="checkbox"
									class={checkboxClass}
									name="sources"
									value={source.source_id}
									bind:group={chosen}
								/>
								<span class="truncate" title={source.label}>{source.source_id}</span>
							</label>
						{/each}
					</div>
				{/if}

				<div class="flex flex-wrap items-center gap-3">
					<label class="flex items-center gap-2 text-sm">
						<span class="text-muted-foreground">cache</span>
						<NativeSelect name="cache_mode" class="h-8 text-xs" fit>
							<!-- refresh first and by default: a run under `auto` with a
							     stale max age replays yesterday's pages and reports
							     success while changing no prices. -->
							<option value="refresh">refresh (fetch everything)</option>
							<option value="auto">auto (use what is fresh)</option>
							<option value="replay">replay (offline, no network)</option>
						</NativeSelect>
					</label>
					<Button size="sm" type="submit">
						Run {picking && chosen.length ? `${chosen.length} sources` : 'all sources'}
					</Button>
					{#if form?.error}
						<span class="text-destructive text-sm">{form.error}</span>
					{:else if form?.run_id}
						<a
							class="text-success text-sm underline-offset-4 hover:underline"
							href="/ops/runs/{form.run_id}"
						>
							started {form.jobs} jobs
						</a>
					{/if}
				</div>
			</form>
		</Panel>

		<section>
			<SectionHeader title="Workers" description="{workers.length} registered" />
			{#if workers.length === 0}
				<p class="text-muted-foreground text-sm">
					No workers have registered. Start one with <code>catalogue-worker</code>.
				</p>
			{:else}
				<!-- Cards here, and only here: a worker is an object, not a section. -->
				<div class="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
					{#each workers as worker (worker.worker_id)}
						<WorkerCard {worker} {stream} />
					{/each}
				</div>
			{/if}
		</section>

		<section>
			<SectionHeader title="Recent runs">
				{#snippet actions()}
					<a class="text-sm underline-offset-4 hover:underline" href="/ops/runs">all runs</a>
				{/snippet}
			</SectionHeader>
			<TableWrap>
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.Head>Started</Table.Head>
							<Table.Head>Kind</Table.Head>
							<Table.Head>Status</Table.Head>
							<Table.Head>Sources</Table.Head>
							<Table.Head>Duration</Table.Head>
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each data.runs ?? [] as run (run.id)}
							<Table.Row>
								<Table.Cell>
									<a class="underline-offset-4 hover:underline" href="/ops/runs/{run.id}"
										>{relative(run.created_at)}</a
									>
								</Table.Cell>
								<Table.Cell>{run.kind}</Table.Cell>
								<Table.Cell
									><StatusBadge tone={stateTone(run.status)}>{run.status}</StatusBadge></Table.Cell
								>
								<Table.Cell>
									{run.succeeded}/{run.jobs}{run.failed ? ` · ${run.failed} failed` : ''}
								</Table.Cell>
								<Table.Cell>{duration(run.started_at, run.finished_at)}</Table.Cell>
							</Table.Row>
						{:else}
							<Table.Row>
								<Table.Cell colspan={5} class="text-muted-foreground">No runs yet.</Table.Cell>
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			</TableWrap>
		</section>
	</div>
{/if}
