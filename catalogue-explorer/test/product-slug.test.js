import assert from 'node:assert/strict';
import test from 'node:test';

import { productSlug } from '../src/lib/product-slug.js';

test('creates a readable product URL segment', () => {
	assert.equal(productSlug('AMACO C-1 Obsidian'), 'amaco-c-1-obsidian');
});

test('normalizes punctuation, whitespace, and accents', () => {
	assert.equal(productSlug('  SIO-2  Émail / Blanc  '), 'sio-2-email-blanc');
});
