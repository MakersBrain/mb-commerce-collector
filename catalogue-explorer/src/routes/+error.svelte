<script lang="ts">
	import { page } from '$app/state';
	import { Button } from '$lib/components/ui/button';

	let copied = $state(false);
	const requestId = $derived(page.error?.requestId);

	async function copy() {
		if (!requestId) return;
		await navigator.clipboard.writeText(requestId);
		copied = true;
	}
</script>

<svelte:head><title>Request failed · Ceramics catalogue</title></svelte:head>

	<section class="mx-auto max-w-xl py-12">
	<p class="mb-eyebrow">Error {page.status}</p>
	<h1 class="mt-2 text-xl font-semibold">
		{page.error?.title ?? 'The request could not be completed'}
	</h1>
	{#if page.error?.detail ?? (!page.error?.title && page.error?.message)}
		<p class="text-muted-foreground mt-2">
			{page.error?.detail ?? page.error?.message}
		</p>
	{/if}
	{#if requestId}
		<div class="mt-5 flex items-center gap-2">
			<code class="bg-muted rounded px-2 py-1 text-xs">{requestId}</code>
			<Button variant="secondary" size="xs" type="button" onclick={copy}>
				{copied ? 'Copied' : 'Copy request ID'}
			</Button>
		</div>
	{/if}
</section>
