<script lang="ts">
	import Unavailable from '$lib/ops/Unavailable.svelte';
	import { relative, stateTone } from '$lib/ops/format';
	import * as Table from '$lib/components/ui/table';
	import { Card, CardHeader, CardTitle, CardContent } from '$lib/components/ui/card';
	import { StatusBadge } from '$lib/components/ui/status-badge';
	import { Badge } from '$lib/components/ui/badge';
	import { Button } from '$lib/components/ui/button';
	import { Input } from '$lib/components/ui/input';
	import { NativeSelect } from '$lib/components/ui/native-select';
	import { Checkbox } from '$lib/components/ui/checkbox';
	import { Notice } from '$lib/components/ui/notice';
	import { Progress } from '$lib/components/ui/progress';

	let { data, form } = $props();
	const bytes = (value: number | null | undefined) =>
		value == null
			? '—'
			: `${(value / 1_000_000).toLocaleString('en-GB', { maximumFractionDigits: 1 })} MB`;
	const percent = (value: number, maximum: number) =>
		maximum ? Math.min(100, (100 * value) / maximum) : 0;
	const admin = $derived(data.operator?.role === 'admin');
	const operator = $derived(data.operator!);
	const overview = $derived(data.overview!);
	const profiles = $derived(data.profiles ?? []);
	const reservations = $derived(data.reservations ?? []);
	const cycle = $derived(data.overview?.cycle);
	const accounted = $derived(cycle?.accounted_bytes ?? 0);
	const discrepancy = $derived(
		(cycle?.provider_reported_bytes ?? 0) - (cycle?.application_bytes ?? 0)
	);

	/**
	 * Every confirmation field on this page is the same control, and it is the
	 * one place a form here can spend money or drop live leases. Sharing the
	 * class keeps them the same size, which is how they read as one mechanism
	 * rather than seven similar-looking text boxes.
	 */
	const confirm = 'h-7 text-xs font-mono';
</script>

<svelte:head><title>Proxies · operations</title></svelte:head>

{#if data.unavailable}
	<Unavailable reason={data.unavailable} />
{:else}
	<div class="mb-5 flex flex-wrap items-start justify-between gap-3">
		<div>
			<h1 class="text-lg font-semibold">Proxy manager</h1>
			<p class="text-muted-foreground text-sm">
				Decodo Residential · 3 GB provider ceiling · 2.4 GB operational ceiling
			</p>
		</div>
		<div class="flex flex-wrap gap-2">
			<StatusBadge tone={overview.deployment_enabled ? 'good' : 'bad'}>
				deployment {overview.deployment_enabled ? 'enabled' : 'disabled'}
			</StatusBadge>
			<StatusBadge tone={cycle?.kill_switch ? 'bad' : 'good'}>
				{cycle?.kill_switch ? 'new traffic stopped' : 'leases open'}
			</StatusBadge>
			<Badge variant="outline">{operator.role}</Badge>
		</div>
	</div>

	{#if form?.error}<Notice kind="error" class="mb-4">{form.error}</Notice>{/if}
	{#if form?.ok}<Notice kind="success" class="mb-4">Operation accepted.</Notice>{/if}

	<section class="grid gap-4 lg:grid-cols-3">
		<Card size="sm" class="lg:col-span-2">
			<CardHeader class="grid-cols-[1fr_auto] items-center">
				<CardTitle>Current cycle</CardTitle>
				<span class="text-muted-foreground text-xs">
					reconciled {relative(cycle?.reconciled_at)}
				</span>
			</CardHeader>
			<CardContent>
				{#if cycle}
					<Progress
						value={percent(accounted, cycle.purchased_bytes)}
						class="h-5 rounded"
						label="{bytes(accounted)} accounted"
					/>
					<div class="text-muted-foreground mt-1 flex justify-between text-xs">
						<span>{bytes(accounted)} accounted</span>
						<span>
							{bytes(cycle.operational_bytes)} operational / {bytes(cycle.purchased_bytes)} purchased
						</span>
					</div>
					<div class="mt-4 grid grid-cols-2 gap-3 text-sm sm:grid-cols-4">
						<div>
							<span class="mb-eyebrow block">Provider</span>{bytes(cycle.provider_reported_bytes)}
						</div>
						<div><span class="mb-eyebrow block">Application</span>{bytes(cycle.application_bytes)}</div>
						<div>
							<span class="mb-eyebrow block">Reserved</span>{bytes(cycle.active_reserved_bytes)}
						</div>
						<div>
							<span class="mb-eyebrow block">Headroom</span>{bytes(cycle.remaining_operational_bytes)}
						</div>
					</div>
					<p class="text-muted-foreground mt-2 text-xs">
						Today {bytes(cycle.daily_used_bytes)} · dynamic daily allowance {bytes(
							cycle.dynamic_daily_bytes
						)}
					</p>
					<p
						class="mt-3 text-xs {discrepancy > 10_000_000
							? 'text-warning'
							: 'text-muted-foreground'}"
					>
						Provider − application discrepancy: {bytes(discrepancy)}. Provider totals remain
						authoritative for headroom.
					</p>
				{:else}
					<p class="text-warning text-sm">No active billing cycle. Leases fail closed.</p>
				{/if}
			</CardContent>
		</Card>

		<Card size="sm">
			<CardHeader><CardTitle>Controls</CardTitle></CardHeader>
			<CardContent class="grid gap-2">
				{#if admin}
					<form method="POST" action="?/reconcile">
						<Button variant="secondary" size="sm" class="w-full" type="submit">Reconcile now</Button>
					</form>
					<form method="POST" action="?/kill">
						<input type="hidden" name="mode" value="activate" />
						<Button variant="destructive" size="sm" class="w-full" type="submit">
							Stop new paid traffic
						</Button>
					</form>
					<form method="POST" action="?/kill" class="grid gap-1">
						<input type="hidden" name="mode" value="clear" />
						<Input
							class={confirm}
							name="confirmation"
							placeholder="ENABLE PAID PROXY TRAFFIC"
							required
						/>
						<Button variant="secondary" size="sm" class="w-full" type="submit">Clear stop</Button>
					</form>
					<form method="POST" action="?/kill" class="grid gap-1">
						<input type="hidden" name="mode" value="revoke" />
						<Input
							class={confirm}
							name="confirmation"
							placeholder="REVOKE ACTIVE PROXY LEASES"
							required
						/>
						<Button variant="warning" size="sm" class="w-full" type="submit">
							Revoke active leases
						</Button>
					</form>
					<div class="grid grid-cols-2 items-end gap-2">
						<form method="POST" action="?/pilot" class="grid gap-1">
							<input type="hidden" name="mode" value="start" />
							<Input
								class={confirm}
								name="confirmation"
								placeholder="START PAID PROXY PILOT"
								required
							/>
							<Button variant="secondary" size="sm" class="w-full" type="submit">Start pilot</Button>
						</form>
						<form method="POST" action="?/pilot">
							<input type="hidden" name="mode" value="stop" />
							<Button variant="secondary" size="sm" class="w-full" type="submit">Stop pilot</Button>
						</form>
					</div>
				{:else}
					<p class="text-muted-foreground text-sm">Viewer access is read-only.</p>
				{/if}
			</CardContent>
		</Card>
	</section>

	<Card size="sm" class="mt-5">
		<CardHeader class="grid-cols-[1fr_auto] items-center">
			<CardTitle>Billing cycles</CardTitle>
			{#if admin}
				<form method="POST" action="?/proposeCycle">
					<Button variant="secondary" size="sm" type="submit">Propose from Decodo</Button>
				</form>
			{/if}
		</CardHeader>
		<CardContent>
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>Status</Table.Head>
						<Table.Head>UTC boundary</Table.Head>
						<Table.Head>Purchased</Table.Head>
						<Table.Head>Operational</Table.Head>
						<Table.Head><span class="sr-only">Cycle controls</span></Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each data.cycles as item (item.id)}
						<Table.Row>
							<Table.Cell>
								<StatusBadge tone={stateTone(item.lifecycle)}>{item.lifecycle}</StatusBadge>
							</Table.Cell>
							<Table.Cell>{item.cycle_start} → {item.cycle_end}</Table.Cell>
							<Table.Cell class="tabular-nums">{bytes(item.purchased_bytes)}</Table.Cell>
							<Table.Cell class="tabular-nums">{bytes(item.operational_bytes)}</Table.Cell>
							<Table.Cell>
								{#if admin && (item.lifecycle === 'proposed' || item.lifecycle === 'active')}
									<form method="POST" action="?/cycle" class="flex items-center gap-1">
										<input type="hidden" name="id" value={item.id} />
										<input
											type="hidden"
											name="mode"
											value={item.lifecycle === 'proposed' ? 'open' : 'close'}
										/>
										<input type="hidden" name="cycle" value={JSON.stringify(item)} />
										<Input
											class={confirm}
											name="confirmation"
											placeholder={item.lifecycle === 'proposed'
												? 'OPEN DECODO CYCLE'
												: 'CLOSE DECODO CYCLE'}
											required
										/>
										<Button variant="secondary" size="xs" type="submit">
											{item.lifecycle === 'proposed' ? 'Open confirmed' : 'Close expired'}
										</Button>
									</form>
								{/if}
							</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		</CardContent>
	</Card>

	<section class="mt-5 grid gap-4 xl:grid-cols-2">
		<Card size="sm">
			<CardHeader class="grid-cols-[1fr_auto] items-center">
				<CardTitle>Profiles / Decodo sub-users</CardTitle>
				{#if admin}
					<form method="POST" action="?/refreshProfiles">
						<Button variant="ghost" size="xs" type="submit">Refresh</Button>
					</form>
				{/if}
			</CardHeader>
			<CardContent>
				{#if admin}
					<form
						method="POST"
						action="?/createProfile"
						class="bg-muted grid grid-cols-2 gap-2 rounded-md p-3 text-sm"
					>
						<Input class="h-8 text-xs" name="logical_name" placeholder="logical_name" required />
						<Input class="h-8 text-xs" name="display_name" placeholder="Display name" required />
						<Input
							class="h-8 text-xs"
							name="allocated_mb"
							type="number"
							min="1"
							max="2400"
							value="100"
							required
						/>
						<Input
							class="h-8 text-xs"
							name="limit_mb"
							type="number"
							min="1"
							max="2400"
							value="100"
							required
						/>
						<Input
							class="col-span-2 {confirm}"
							name="confirmation"
							placeholder="CREATE logical_name"
							required
						/>
						<Button size="sm" class="col-span-2" type="submit">Create bounded sub-user</Button>
					</form>
				{/if}
				<div class="divide-border divide-y">
					{#each data.profiles as profile (profile.id)}
						<div class="py-3 text-sm">
							<div class="flex items-center justify-between gap-2">
								<strong>{profile.display_name}</strong>
								<StatusBadge tone={profile.enabled ? 'good' : 'neutral'}>
									{profile.lifecycle}
								</StatusBadge>
							</div>
							<p class="text-muted-foreground text-xs">
								{profile.logical_name} · {profile.username_mask ?? 'not installed'} · allocation {bytes(
									profile.allocated_bytes
								)} · limit {bytes(profile.provider_traffic_limit_bytes)} · generation {profile.secret_generation}
							</p>
							{#if admin}
								<div class="mt-2 grid gap-2">
									<form method="POST" action="?/profile" class="flex gap-1">
										<input type="hidden" name="id" value={profile.id} />
										<input type="hidden" name="logical_name" value={profile.logical_name} />
										<input type="hidden" name="mode" value="rotate" />
										<NativeSelect class="h-7 text-xs" wrapperClass="w-32" name="rotation_mode">
											<option value="drain">Drain first</option>
											<option value="blue-green">Blue-green</option>
										</NativeSelect>
										<Input
											class={confirm}
											name="confirmation"
											placeholder={`ROTATE ${profile.logical_name}`}
											required
										/>
										<Button variant="secondary" size="xs" type="submit">Rotate</Button>
									</form>
									<form method="POST" action="?/profile" class="flex gap-1">
										<input type="hidden" name="id" value={profile.id} />
										<input type="hidden" name="logical_name" value={profile.logical_name} />
										<input type="hidden" name="mode" value="disable" />
										<Input
											class={confirm}
											name="confirmation"
											placeholder={`DISABLE ${profile.logical_name}`}
											required
										/>
										<Button variant="warning" size="xs" type="submit">Disable</Button>
									</form>
									<form method="POST" action="?/profile" class="flex gap-1">
										<input type="hidden" name="id" value={profile.id} />
										<input type="hidden" name="logical_name" value={profile.logical_name} />
										<input type="hidden" name="mode" value="retire" />
										<Input
											class={confirm}
											name="confirmation"
											placeholder={`RETIRE ${profile.logical_name}`}
											required
										/>
										<Button variant="destructive" size="xs" type="submit">Retire</Button>
									</form>
								</div>
							{/if}
						</div>
					{/each}
				</div>
			</CardContent>
		</Card>

		<Card size="sm">
			<CardHeader><CardTitle>Routes and paid probe</CardTitle></CardHeader>
			<CardContent>
				<p class="text-muted-foreground text-xs">
					Saving a route spends nothing. A probe streams at most 1 MB of application data and
					reserves a 1.1 MB provider envelope. A new session does not guarantee a unique exit.
				</p>
				{#if admin && profiles.length}
					<form
						method="POST"
						action="?/createRoute"
						class="bg-muted mt-2 grid grid-cols-2 gap-2 rounded-md p-3 text-sm"
					>
						<Input class="h-8 text-xs" name="label" placeholder="Route label" required />
						<NativeSelect class="h-8 text-xs" name="profile_id">
							{#each profiles.filter((p: any) => p.enabled) as profile}
								<option value={profile.id}>{profile.logical_name}</option>
							{/each}
						</NativeSelect>
						<Input class="h-8 text-xs" name="country" maxlength={2} placeholder="Country, e.g. FR" />
						<NativeSelect class="h-8 text-xs" name="protocol">
							<option>http</option>
							<option>https</option>
							<option>socks5</option>
						</NativeSelect>
						<NativeSelect class="h-8 text-xs" name="session_mode">
							<option>random</option>
							<option>sticky</option>
						</NativeSelect>
						<Input
							class="h-8 text-xs"
							name="session_minutes"
							type="number"
							min="1"
							max="1440"
							value="30"
						/>
						<Input class="h-8 text-xs" name="max_mb" type="number" min="2" max="25" value="25" />
						<label class="flex items-center justify-start gap-2 text-xs">
							<Checkbox name="enabled" />
							enabled
						</label>
						<Button size="sm" class="col-span-2" type="submit">Save route — no traffic</Button>
					</form>
				{/if}
				<div class="divide-border divide-y">
					{#each data.routes as route (route.id)}
						<div class="flex items-center justify-between gap-2 py-3 text-sm">
							<div>
								<strong>{route.label}</strong>
								<p class="text-muted-foreground text-xs">
									{route.profile} · {route.protocol} · {route.country ?? 'any'} · {route.session_mode}
									· {bytes(route.max_bytes)}
								</p>
							</div>
							{#if admin}
								<div class="grid gap-1">
									<form method="POST" action="?/route" class="flex gap-1">
										<input type="hidden" name="id" value={route.id} />
										<input type="hidden" name="mode" value="probe" />
										<Input
											class={confirm}
											name="confirmation"
											placeholder="SPEND UP TO 1.1 MB"
											required
										/>
										<Button size="xs" type="submit" disabled={!overview.paid_probe_enabled}>
											Test session
										</Button>
									</form>
									<form method="POST" action="?/route">
										<input type="hidden" name="id" value={route.id} />
										<input type="hidden" name="mode" value="delete" />
										<Button variant="secondary" size="xs" type="submit">Retire route</Button>
									</form>
								</div>
							{/if}
						</div>
					{/each}
				</div>
			</CardContent>
		</Card>
	</section>

	<section class="mt-5 grid gap-4 xl:grid-cols-2">
		<Card size="sm">
			<CardHeader><CardTitle>Reservations</CardTitle></CardHeader>
			<CardContent>
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.Head>Consumer</Table.Head>
							<Table.Head>Profile</Table.Head>
							<Table.Head>Reserved / used</Table.Head>
							<Table.Head>State</Table.Head>
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each reservations.slice(0, 50) as row (row.id)}
							<Table.Row>
								<Table.Cell>{row.source_id ?? `probe ${row.probe_id?.slice(0, 8)}`}</Table.Cell>
								<Table.Cell>{row.profile}</Table.Cell>
								<Table.Cell class="tabular-nums">
									{bytes(row.reserved_bytes)} / {bytes(row.estimated_bytes)}
								</Table.Cell>
								<Table.Cell>
									<StatusBadge tone={stateTone(row.state)}>{row.state}</StatusBadge>
								</Table.Cell>
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			</CardContent>
		</Card>

		<Card size="sm">
			<CardHeader><CardTitle>Recent probes</CardTitle></CardHeader>
			<CardContent>
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.Head>When</Table.Head>
							<Table.Head>Exit</Table.Head>
							<Table.Head>Traffic</Table.Head>
							<Table.Head>Result</Table.Head>
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each data.probes as row (row.id)}
							<Table.Row>
								<Table.Cell>{relative(row.requested_at)}</Table.Cell>
								<Table.Cell>{row.exit_country ?? '—'} {row.exit_ip ?? ''}</Table.Cell>
								<Table.Cell class="tabular-nums">{bytes(row.estimated_bytes)}</Table.Cell>
								<Table.Cell>{row.state}</Table.Cell>
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			</CardContent>
		</Card>
	</section>

	<Card size="sm" class="mt-5">
		<CardHeader><CardTitle>Usage by day</CardTitle></CardHeader>
		<CardContent>
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>Day</Table.Head>
						<Table.Head>Download</Table.Head>
						<Table.Head>Upload</Table.Head>
						<Table.Head>Total</Table.Head>
						<Table.Head>Requests</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each data.usage as row}
						<Table.Row>
							<Table.Cell>{String(row.key)}</Table.Cell>
							<Table.Cell class="tabular-nums">{bytes(row.received_bytes)}</Table.Cell>
							<Table.Cell class="tabular-nums">{bytes(row.transmitted_bytes)}</Table.Cell>
							<Table.Cell class="tabular-nums">{bytes(row.total_bytes)}</Table.Cell>
							<Table.Cell class="tabular-nums">
								{(row.request_count ?? 0).toLocaleString()}
							</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		</CardContent>
	</Card>

	<Card size="sm" class="mt-5">
		<CardHeader><CardTitle>Source policy candidates</CardTitle></CardHeader>
		<CardContent>
			<p class="text-muted-foreground text-xs">
				Only sources explicitly marked proxy-eligible appear here. “Always” remains blocked until
				three successful proxy evidence runs promote the source.
			</p>
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>Source</Table.Head>
						<Table.Head>Failures / runs</Table.Head>
						<Table.Head>Evidence</Table.Head>
						<Table.Head>Policy</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each data.candidates as row (row.source_id)}
						<Table.Row>
							<Table.Cell>{row.source_id}</Table.Cell>
							<Table.Cell class="tabular-nums">{row.failures} / {row.runs}</Table.Cell>
							<Table.Cell>
								{row.evidence_state ?? 'unproven'} ({row.evidence_count ?? 0})
							</Table.Cell>
							<Table.Cell>
								{#if admin}
									<form method="POST" action="?/sourcePolicy" class="flex gap-1">
										<input type="hidden" name="source_id" value={row.source_id} />
										<NativeSelect class="h-7 text-xs" wrapperClass="w-24" name="policy">
											<option value="never" selected={row.policy === 'never'}>never</option>
											<option value="fallback" selected={row.policy === 'fallback'}>fallback</option>
											<option value="always" selected={row.policy === 'always'}>always</option>
										</NativeSelect>
										<NativeSelect class="h-7 text-xs" wrapperClass="w-32" name="route_id">
											<option value="">no route</option>
											{#each data.routes as route}
												<option value={route.id} selected={row.route_id === route.id}>
													{route.label}
												</option>
											{/each}
										</NativeSelect>
										<Input
											class="h-7 w-20 text-xs"
											name="max_megabytes"
											type="number"
											min="1"
											max="25"
											value="25"
										/>
										<Button variant="secondary" size="xs" type="submit">Apply</Button>
									</form>
								{:else}
									{row.policy ?? 'never'}
								{/if}
							</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		</CardContent>
	</Card>

	<Card size="sm" class="mt-5">
		<CardHeader><CardTitle>Audit</CardTitle></CardHeader>
		<CardContent>
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head>When</Table.Head>
						<Table.Head>Actor</Table.Head>
						<Table.Head>Action</Table.Head>
						<Table.Head>Resource</Table.Head>
						<Table.Head>Result</Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each data.audit as row (row.id)}
						<Table.Row>
							<Table.Cell>{relative(row.at)}</Table.Cell>
							<Table.Cell>{row.actor}</Table.Cell>
							<Table.Cell>{row.action}</Table.Cell>
							<Table.Cell>{row.resource_type} {row.resource_id ?? ''}</Table.Cell>
							<Table.Cell>{row.state}{row.error_code ? ` · ${row.error_code}` : ''}</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		</CardContent>
	</Card>
{/if}
