<script lang="ts">
	import { page } from '$app/state';
	import Unavailable from '$lib/ops/Unavailable.svelte';
	import { relative, duration, count, stateTone } from '$lib/ops/format';
	import * as Table from '$lib/components/ui/table';
	import { Card, CardHeader, CardTitle, CardContent } from '$lib/components/ui/card';
	import { Metric } from '@makersbrain/ui/svelte';
	import { StatusBadge } from '$lib/components/ui/status-badge';
	import { Button } from '$lib/components/ui/button';
	import { Input } from '$lib/components/ui/input';
	import { NativeSelect } from '$lib/components/ui/native-select';
	import { Progress } from '$lib/components/ui/progress';

	let { data } = $props();
	let copied = $state<string | null>(null);

	const job = $derived(data.job);
	const runId = $derived(page.params.id ?? '');
	const coverage = $derived(Object.entries(job?.summary?.field_coverage ?? {}) as [string, number][]);
	const errors = $derived((job?.summary?.errors ?? []) as { url: string; error: string }[]);
	const changes = $derived(data.changes);
	const changeItems = $derived(changes?.items ?? []);

	const levelTone: Record<string, string> = {
		error: 'text-destructive',
		warning: 'text-warning',
		info: '',
		debug: 'text-muted-foreground/70'
	};

	function value(value: unknown): string {
		if (value == null) return '—';
		if (typeof value === 'string') return value;
		return JSON.stringify(value);
	}

	async function copy(label: string, value: string) {
		await navigator.clipboard.writeText(value);
		copied = label;
	}
</script>

<svelte:head><title>{job?.source_id ?? 'Job'} · operations</title></svelte:head>

{#if data.unavailable}
	<Unavailable reason={data.unavailable} />
{:else if job}
	<div class="mb-4 flex flex-wrap items-baseline gap-3">
		<a
			class="text-accent-foreground text-sm underline-offset-4 hover:underline"
			href="/ops/runs/{page.params.id}">← run</a
		>
		<h1 class="text-lg font-semibold">{job.source_id}</h1>
		<StatusBadge tone={stateTone(job.state)}>{job.state}</StatusBadge>
		<span class="text-muted-foreground text-sm">
			attempt {job.attempt}/{job.max_attempts} · {duration(job.started_at, job.finished_at)}
			{#if job.finished_at}· finished {relative(job.finished_at)}{/if}
		</span>
	</div>
	<div class="bg-muted/40 mb-4 flex flex-wrap items-center gap-x-4 gap-y-2 rounded-lg border px-3 py-2 text-xs">
		<span class="text-muted-foreground">Correlation</span>
		<span class="flex items-center gap-1">
			<span class="text-muted-foreground">job</span>
			<code>{job.id}</code>
			<Button variant="ghost" size="xs" type="button" onclick={() => copy('job', job.id)}>
				{copied === 'job' ? 'copied' : 'copy'}
			</Button>
		</span>
		<span class="flex items-center gap-1">
			<span class="text-muted-foreground">run</span>
			<code>{runId}</code>
			<Button variant="ghost" size="xs" type="button" onclick={() => copy('run', runId)}>
				{copied === 'run' ? 'copied' : 'copy'}
			</Button>
		</span>
		{#if job.trace_id}
			<span class="flex items-center gap-1">
				<span class="text-muted-foreground">trace</span>
				<code>{job.trace_id}</code>
				<Button variant="ghost" size="xs" type="button" onclick={() => job.trace_id && copy('trace', job.trace_id)}>
					{copied === 'trace' ? 'copied' : 'copy'}
				</Button>
			</span>
		{/if}
	</div>

	<div class="mb-6 grid gap-4 lg:grid-cols-3">
		<Card size="sm">
			<CardHeader><CardTitle class="mb-eyebrow">Collection</CardTitle></CardHeader>
			<CardContent class="grid gap-1 text-sm">
				<dl class="grid grid-cols-2 gap-x-3 gap-y-1">
					<dt class="text-muted-foreground">records</dt>
					<dd class="tabular-nums">{count(job.records ?? job.summary?.records)}</dd>
					<dt class="text-muted-foreground">requests</dt>
					<dd class="tabular-nums">{count(job.requests ?? job.summary?.requests)}</dd>
					<dt class="text-muted-foreground">rendered</dt>
					<dd class="tabular-nums">{count(job.rendered_pages ?? job.summary?.rendered_pages)}</dd>
					<dt class="text-muted-foreground">errors</dt>
					<dd class="tabular-nums">{count(job.error_count ?? job.summary?.error_count)}</dd>
					<dt class="text-muted-foreground">truncated</dt>
					<dd>{job.summary?.truncated ? 'yes' : 'no'}</dd>
				</dl>
			</CardContent>
		</Card>

		<Card size="sm">
			<CardHeader><CardTitle class="mb-eyebrow">Artifact</CardTitle></CardHeader>
			<CardContent class="grid gap-1 text-sm">
				{#if job.artifact_path}
					<p class="break-all font-mono text-xs">{job.artifact_path}</p>
					<p class="text-muted-foreground text-xs">
						{count(job.artifact_size)} bytes
					</p>
					<p class="text-muted-foreground/70 break-all font-mono text-xs" title="sha256">
						{job.artifact_sha256?.slice(0, 32)}…
					</p>
				{:else}
					<p class="text-muted-foreground">No artifact recorded for this attempt.</p>
				{/if}
			</CardContent>
		</Card>

		<Card size="sm">
			<CardHeader><CardTitle class="mb-eyebrow">In flight</CardTitle></CardHeader>
			<CardContent class="grid gap-1 text-sm">
				{#if (job.in_flight ?? []).length}
					<ul class="space-y-1">
						{#each job.in_flight as request (request.url)}
							<li class="truncate font-mono text-xs" title={request.url}>
								<span class="text-muted-foreground">{request.seconds}s</span>
								{request.url}
							</li>
						{/each}
					</ul>
				{:else}
					<p class="text-muted-foreground">Nothing in flight.</p>
				{/if}
			</CardContent>
		</Card>
	</div>

	<section class="mb-6">
		<div class="mb-2 flex flex-wrap items-baseline gap-2">
			<h2 class="mb-eyebrow">Changes since previous scrape</h2>
			{#if changes}
				<a
					class="text-accent-foreground text-xs underline-offset-4 hover:underline"
					href="/ops/runs/{changes.previous_run_id}/jobs/{changes.previous_job_id}"
				>
					previous artifact · {relative(changes.previous_finished_at)}
				</a>
			{/if}
		</div>

		{#if changes}
			<!-- Four metrics rather than daisyUI's joined `stats` strip: the tones
			     here are the validated status trio, and a joined strip put them in
			     one box where added-green and changed-violet sat edge to edge with
			     no rule between them. -->
			<div class="mb-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
				<Metric label="Added" value={count(changes.added)} tone="good" />
				<Metric label="Removed" value={count(changes.removed)} tone="bad" />
				<Metric label="Changed" value={count(changes.changed)} tone="warn" />
				<Metric label="Unchanged" value={count(changes.unchanged)} />
			</div>

			<form method="GET" class="mb-2 flex flex-wrap items-center gap-2">
				<NativeSelect name="change_kind" class="h-7 text-xs" fit value={data.changeKind ?? ''}>
					<option value="">all changes</option>
					<option value="added">added</option>
					<option value="removed">removed</option>
					<option value="changed">changed</option>
				</NativeSelect>
				<Input
					name="change_q"
					class="h-7 w-56 text-xs"
					placeholder="product name or id"
					value={data.changeSearch ?? ''}
				/>
				<Button variant="secondary" size="xs" type="submit">Filter</Button>
				<span class="text-muted-foreground text-xs">
					{count(changes.matched)} matching{(changes.matched ?? 0) > changeItems.length ? ` · showing first ${changeItems.length}` : ''}
				</span>
			</form>

			<div class="bg-card overflow-hidden rounded-lg border">
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.Head>Change</Table.Head>
							<Table.Head>Product</Table.Head>
							<Table.Head>Fields</Table.Head>
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each changeItems as change (change.kind + change.external_id)}
							{@const fields = change.fields ?? []}
							<Table.Row class="align-top">
								<Table.Cell class="align-top">
									<StatusBadge
										tone={change.kind === 'added'
											? 'good'
											: change.kind === 'removed'
												? 'bad'
												: 'warn'}
									>
										{change.kind}
									</StatusBadge>
								</Table.Cell>
								<Table.Cell class="align-top">
									<div>{change.name ?? 'unnamed record'}</div>
									<div class="text-muted-foreground/70 break-all font-mono text-xs">
										{change.external_id}
									</div>
								</Table.Cell>
								<Table.Cell class="min-w-80 align-top">
									{#if fields.length}
										<dl class="space-y-1 text-xs">
											{#each fields as field (field.field)}
												<div class="grid grid-cols-[8rem_1fr] gap-2">
													<dt class="font-medium">{field.field}</dt>
													<dd class="min-w-0 break-all">
														<span class="text-destructive line-through">{value(field.before)}</span>
														<span class="mx-1 opacity-40">→</span>
														<span class="text-success">{value(field.after)}</span>
													</dd>
												</div>
											{/each}
										</dl>
									{:else}
										<span class="text-muted-foreground/70 text-xs">whole record</span>
									{/if}
								</Table.Cell>
							</Table.Row>
						{:else}
							<Table.Row>
								<Table.Cell colspan={3} class="text-muted-foreground">
									No matching changes.
								</Table.Cell>
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			</div>
		{:else}
			<div class="bg-card text-muted-foreground rounded-lg border p-4 text-sm">
				{data.changesUnavailable ?? 'Comparison is not available yet.'}
			</div>
		{/if}
	</section>

	{#if coverage.length}
		<section class="mb-6">
			<h2 class="mb-eyebrow mb-2">
				Field coverage
				<span class="text-muted-foreground/70 ml-1 font-normal tracking-normal normal-case">
					— rows carrying each field, so a thin scraper is visible
				</span>
			</h2>
			<Card size="sm">
				<CardContent class="grid gap-1 sm:grid-cols-2 lg:grid-cols-3">
					{#each coverage.sort((a, b) => b[1] - a[1]) as [field, rows] (field)}
						{@const share = job.summary?.records ? (100 * rows) / job.summary.records : 0}
						<div class="flex items-center gap-2 text-xs">
							<span class="w-40 truncate">{field}</span>
							<Progress value={share} class="flex-1" label="{field} coverage" />
							<span class="text-muted-foreground w-10 text-right tabular-nums">
								{share.toFixed(0)}%
							</span>
						</div>
					{/each}
				</CardContent>
			</Card>
		</section>
	{/if}

	{#if errors.length}
		<section class="mb-6">
			<h2 class="mb-eyebrow mb-2">Errors</h2>
			<Card size="sm">
				<CardContent>
					<ul class="grid gap-2 text-xs">
					{#each errors as entry (entry.url + entry.error)}
						<li>
								<div class="text-muted-foreground truncate font-mono" title={entry.url}>
									{entry.url}
								</div>
								<div class="text-destructive">{entry.error}</div>
							</li>
						{/each}
					</ul>
				</CardContent>
			</Card>
		</section>
	{/if}

	<section>
		<form method="GET" class="mb-2 flex flex-wrap items-center gap-2">
			<h2 class="mb-eyebrow mr-2">Log</h2>
			<NativeSelect name="level" class="h-7 text-xs" fit value={data.level ?? ''}>
				<option value="">all levels</option>
				<option value="error">error</option>
				<option value="warning">warning</option>
				<option value="info">info</option>
				<option value="debug">debug</option>
			</NativeSelect>
			<Input name="q" class="h-7 w-48 text-xs" placeholder="search" value={data.search ?? ''} />
			<Button variant="secondary" size="xs" type="submit">Filter</Button>
		</form>

		<div class="bg-card max-h-[32rem] overflow-y-auto rounded-lg border p-3 font-mono text-xs">
			{#each data.lines ?? [] as line (line.id)}
				<div class="flex gap-2 {levelTone[line.level] ?? ''}">
					<span class="text-muted-foreground/70">
						{new Date(line.at).toLocaleTimeString('en-GB')}
					</span>
					<span class="text-muted-foreground w-16 shrink-0">{line.event ?? line.level}</span>
					<span class="break-all">{line.message}</span>
				</div>
			{:else}
				<p class="text-muted-foreground">
					No log lines. Lines are written at info and above; start the run with
					<code>log_level=debug</code> for more.
				</p>
			{/each}
		</div>
	</section>
{/if}
