-- Reviewed identities required by the purchase-derived trends list.
--
-- Most purchased glaze references already resolve through manufacturer SKU
-- promotion. WC-108 and LUNA did not because several retailers put the maker's
-- code only in their local SKU or title. Keep these selectors explicit: this is
-- product curation, not a fuzzy-name matching rule.

insert into catalogue.canonical_products (
  id, brand, manufacturer_sku, manufacturer_id, sku_key,
  name, family, attributes, origin
) values (
  '31e265bf-7d12-50f0-9b3f-a806b1594996',
  'Laguna Clay Company', 'WC108', 'laguna', catalogue.sku_key('WC108'),
  'Power Turquoise', 'glaze',
  '{"curation":"Ceramics Purchase Overview.xlsx:GWC108LP"}'::jsonb, 'curated'
) on conflict (id) do update set
  name = excluded.name,
  family = excluded.family,
  attributes = catalogue.canonical_products.attributes || excluded.attributes,
  updated_at = now();

insert into catalogue.canonical_products (
  id, brand, manufacturer_sku, manufacturer_id, sku_key,
  name, family, attributes, origin
) values (
  '01080544-6570-51c8-ad41-c6358ba36252',
  'SIO-2', 'LUNA', 'sio-2', catalogue.sku_key('LUNA'),
  'LUNA speckled stoneware', 'clay_body',
  '{"curation":"explicitly requested purchase trend"}'::jsonb, 'curated'
) on conflict (id) do update set
  name = excluded.name,
  family = excluded.family,
  attributes = catalogue.canonical_products.attributes || excluded.attributes,
  updated_at = now();

update catalogue.source_products
   set canonical_product_id = '31e265bf-7d12-50f0-9b3f-a806b1594996'
 where family = 'glaze'
   and source_id = any(array[
     '1240-design', 'art4fun', 'corby-kilns', 'gwn-pottery',
     'keramikfryd', 'sounding-stone'
   ])
   and (
     sku ~* '^(G?WC-?108|WC108LP|WC108-10|Lag_WC108)$'
     or (name ~* 'power turquoise' and name !~* '\mdry\M')
   );

update catalogue.source_products
   set canonical_product_id = '01080544-6570-51c8-ad41-c6358ba36252'
 where family = 'clay_body'
   and source_id = any(array[
     'barro-ro', 'ceradel', 'ceramista-shop', 'esmalty-color',
     'kadar-ceramica', 'les-cousins', 'marphil', 'mestrebras',
     'paraceramica', 'peter-lavem', 'poterie-du-vieux-bac',
     'prodesco', 'ramfos', 'sio-2', 'the-makers-space'
   ])
   and (sku ~* 'luna' or name ~* '\mLUNA\M');
