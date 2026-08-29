<script lang="ts">
	/**
	 * The MakersBrain lockup with a ceramics rappel on the mark.
	 *
	 * WHY THIS IS NOT `<BrandLockup>`. The brand component is deliberately closed
	 * to this: its own docstring says a surface is identified by the word beside
	 * the mark, and that "a second mark would say they were three companies".
	 * Putting a glyph on the weave is, by that definition, a second mark. This
	 * file exists so that departure is visible and in one place, rather than
	 * smuggled in as a style override on a component that argues against it.
	 *
	 * The mark and the wordmark themselves are still the brand's components, not
	 * copies. Nothing here redraws a path, and the weave's over-under is
	 * untouched - which is the rule the README calls load-bearing. The badge sits
	 * beside the mark's corner and overlaps it; it does not modify it. Delete
	 * this file and the header falls back to `<BrandLockup>` with nothing lost
	 * but the rappel.
	 *
	 * IF THIS PATTERN SPREADS, IT BELONGS UPSTREAM. The moment a second ceramics
	 * surface wants the same cue, this should become a cut in
	 * `MakersBrain/mb-ui` - where the generator can guarantee every derivation
	 * stays on the same geometry - rather than a second hand-rolled badge.
	 *
	 * The glyph is a ring, not a disc: a thrown vessel seen from above is the one
	 * ceramics figure that still reads at ten pixels, where a silhouette turns to
	 * mush. It is drawn on a disc of the page ground because the mark's clay
	 * strand occupies exactly this corner, and clay on clay is no cue at all.
	 */
	import { BrandMark, BrandWordmark } from '@makersbrain/ui/svelte';

	let {
		product = undefined,
		/** What the rappel says, for anyone who cannot see it. */
		surface = 'ceramics',
		size = '1.25rem',
		wordmarkHeight = '0.72rem',
		href = undefined,
		class: className = undefined,
		...rest
	}: {
		product?: string;
		surface?: string;
		size?: string;
		wordmarkHeight?: string;
		href?: string;
		class?: string;
		[key: string]: unknown;
	} = $props();

	const label = $derived(
		[product ? `MakersBrain ${product}` : 'MakersBrain', surface].filter(Boolean).join(' — ')
	);
</script>

<svelte:element
	this={href ? 'a' : 'span'}
	class="lockup {className ?? ''}"
	{href}
	title={label}
	{...rest}
>
	<span class="mark" style:--mark-size={size}>
		<BrandMark {size} />
		<!-- Decorative: the accessible name on the lockup already carries the
		     surface, so a reader hears it once rather than twice. -->
		<span class="rappel" aria-hidden="true">
			<svg viewBox="0 0 12 12" focusable="false">
				<circle cx="6" cy="6" r="3.5" fill="none" stroke="var(--mb-brand)" stroke-width="2.4" />
			</svg>
		</span>
	</span>
	<BrandWordmark height={wordmarkHeight} />
	{#if product}
		<span class="product">{product}</span>
	{/if}
</svelte:element>

<style>
	.lockup {
		display: flex;
		flex: none;
		align-items: center;
		gap: 0.5rem;
		color: inherit;
		text-decoration: none;
	}

	.mark {
		position: relative;
		display: inline-flex;
		flex: none;
	}

	/* Offset outward so the ring clears the strand's own edge rather than
	   sitting on the join, which at this size reads as a printing error. */
	.rappel {
		position: absolute;
		right: calc(var(--mark-size) * -0.14);
		bottom: calc(var(--mark-size) * -0.14);
		display: flex;
		padding: 1px;
		border-radius: 50%;
		background: var(--background);
	}

	.rappel svg {
		display: block;
		width: calc(var(--mark-size) * 0.46);
		height: calc(var(--mark-size) * 0.46);
	}

	/* Matches the brand component's own breakpoint: below this the words cost
	   more than they earn and the mark carries the lockup alone. The rappel
	   stays - it is the part that still fits. */
	.product {
		font-family: var(--mb-font-ui, inherit);
		font-size: var(--mb-text-body, 0.9375rem);
		color: var(--mb-text-muted, inherit);
		white-space: nowrap;
	}

	@media (max-width: 40rem) {
		.product {
			display: none;
		}
	}
</style>
