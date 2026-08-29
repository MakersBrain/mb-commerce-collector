/**
 * Turn a reviewed product label into its stable URL segment.
 *
 * @param {string} label
 */
export function productSlug(label) {
	return label
		.normalize('NFKD')
		.replace(/\p{Mark}/gu, '')
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, '-')
		.replace(/^-|-$/g, '');
}
