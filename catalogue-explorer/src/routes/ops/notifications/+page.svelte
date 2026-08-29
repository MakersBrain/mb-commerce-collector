<script lang="ts">
	import { enhance } from '$app/forms';
	import Unavailable from '$lib/ops/Unavailable.svelte';
	import { relative, severityTone } from '$lib/ops/format';
	import * as Table from '$lib/components/ui/table';
	import { Card, CardContent } from '$lib/components/ui/card';
	import { StatusBadge } from '$lib/components/ui/status-badge';
	import { Button } from '$lib/components/ui/button';
	import { NativeSelect } from '$lib/components/ui/native-select';
	import { Notice } from '$lib/components/ui/notice';
	import { Checkbox, checkboxClass } from '$lib/components/ui/checkbox';

	let { data, form } = $props();
	let selected = $state<number[]>([]);

	// Unacknowledged first: this is a work list, and everything already dealt
	// with is history.
	const open = $derived(
		(data.notifications ?? []).filter((n: any) => !n.acknowledged_at && !n.resolved_at)
	);
	const closed = $derived(
		(data.notifications ?? []).filter((n: any) => n.acknowledged_at || n.resolved_at)
	);
	const allSelected = $derived(
		open.length > 0 && open.every((entry: any) => selected.includes(entry.id))
	);

	$effect(() => {
		const visible = new Set(open.map((entry: any) => entry.id));
		const remaining = selected.filter((id) => visible.has(id));
		if (remaining.length !== selected.length) selected = remaining;
	});

	function selectAll(checked: boolean): void {
		selected = checked ? open.map((entry: any) => entry.id) : [];
	}

	function link(entry: any): string | null {
		if (entry.run_id) return `/ops/runs/${entry.run_id}`;
		if (entry.source_id) return `/ops/sources`;
		return null;
	}
</script>

<svelte:head><title>Notifications · operations</title></svelte:head>

{#if data.unavailable}
	<Unavailable reason={data.unavailable} />
{:else}
	<div class="mb-4 flex flex-wrap items-center gap-3">
		<h1 class="text-lg font-semibold">Notifications</h1>
		<form method="GET" class="flex items-center gap-2">
			<NativeSelect name="severity" class="h-8 text-xs" fit value={data.severity ?? ''}>
				<option value="">all severities</option>
				<option value="critical">critical</option>
				<option value="warning">warning</option>
				<option value="info">info</option>
			</NativeSelect>
			<Button variant="secondary" size="sm" type="submit">Filter</Button>
		</form>
	</div>

	{#if form?.error}
		<Notice kind="error" class="mb-4">{form.error}</Notice>
	{/if}

	<section class="mb-8">
		<h2 class="mb-eyebrow mb-2">
			Needs attention ({open.length})
		</h2>
		{#if open.length === 0}
			<p class="text-muted-foreground text-sm">Nothing outstanding.</p>
		{:else}
			<div class="mb-2 flex flex-wrap items-center gap-3">
				<label class="flex cursor-pointer items-center gap-2 text-sm">
					<Checkbox
						checked={allSelected}
						indeterminate={selected.length > 0 && !allSelected}
						onchange={(event) => selectAll(event.currentTarget.checked)}
					/>
					<span>Select all visible</span>
				</label>
				<form id="bulk-ack" method="POST" action="?/bulkAck" use:enhance>
					<Button variant="secondary" size="sm" type="submit" disabled={selected.length === 0}>
						Acknowledge selected{selected.length ? ` (${selected.length})` : ''}
					</Button>
				</form>
			</div>
			<ul class="grid gap-2">
				{#each open as entry (entry.id)}
					<li>
						<Card class="gap-0 [--card-spacing:--spacing(4)]">
							<CardContent class="flex flex-row items-start gap-3">
								<input
									type="checkbox"
									class={checkboxClass}
									name="ids"
									value={entry.id}
									form="bulk-ack"
									aria-label={`Select ${entry.title}`}
									bind:group={selected}
								/>
								<StatusBadge tone={severityTone(entry.severity)}>{entry.severity}</StatusBadge>
								<div class="min-w-0 flex-1">
									<div class="font-medium">{entry.title}</div>
									{#if entry.body}
										<p class="text-muted-foreground mt-0.5 text-sm">{entry.body}</p>
									{/if}
									<div class="text-muted-foreground/70 mt-1 text-xs">
										{entry.kind} · {relative(entry.at)}
										{#if link(entry)}
											·
											<a
												class="text-accent-foreground underline-offset-4 hover:underline"
												href={link(entry)}>{entry.source_id ?? 'run'}</a
											>
										{/if}
									</div>
								</div>
								<form method="POST" action="?/ack" use:enhance>
									<input type="hidden" name="id" value={entry.id} />
									<Button variant="secondary" size="sm" type="submit">Acknowledge</Button>
								</form>
							</CardContent>
						</Card>
					</li>
				{/each}
			</ul>
		{/if}
	</section>

	<section>
		<h2 class="mb-eyebrow mb-2">
			Resolved and acknowledged
		</h2>
		<div class="bg-card overflow-hidden rounded-lg border">
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>When</Table.Head>
						<Table.Head>Severity</Table.Head>
						<Table.Head>Kind</Table.Head>
						<Table.Head>Title</Table.Head>
						<Table.Head>Closed</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each closed as entry (entry.id)}
						<Table.Row>
							<Table.Cell>{relative(entry.at)}</Table.Cell>
							<Table.Cell>
								<StatusBadge tone={severityTone(entry.severity)}>{entry.severity}</StatusBadge>
							</Table.Cell>
							<Table.Cell class="text-xs">{entry.kind}</Table.Cell>
							<Table.Cell class="max-w-96 truncate">{entry.title}</Table.Cell>
							<Table.Cell class="text-muted-foreground text-xs">
								{entry.resolved_at
									? 'resolved'
									: `acknowledged by ${entry.acknowledged_by ?? '—'}`}
							</Table.Cell>
						</Table.Row>
					{:else}
						<Table.Row>
							<Table.Cell colspan={5} class="text-muted-foreground">Nothing yet.</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		</div>
	</section>
{/if}
