# Sample seed data

This folder separates vendor-owned sample truth from customer-specific historical exemplar packages.

The tracked files in `seed_data/` are public synthetic fixtures for demos, tests, and first-time setup.
Do not replace them with real customer data in a clone you might later push.

For private customer corpora, use `seed_data/local/` with the same folder structure. That path is gitignored on purpose.

## Layout

- `historical_corpus_manifest.json`: historical corpus import manifest consumed by `python -m app.cli import-historical-corpus`
- `product_truth/`: vendor product-truth input consumed by `python -m app.cli import-product-truth`
- `historical_customers/nordtransit_logistik_ag/`: NordTransit PDF + workbook historical package
- `historical_customers/crownshield_insurance_services_ltd/`: CrownShield PDF + workbook historical package
- `historical_customers/asteron_industrial_components_bv/`: Asteron PDF + workbook historical package

## Contract

- `python -m app.cli import-historical-corpus` resolves `seed_data/historical_corpus_manifest.json` and reads the per-customer files via the relative paths declared there.
- `python -m app.cli import-product-truth` imports `seed_data/product_truth/product_truth.json`.
- vendor facts live in `seed_data/product_truth/product_truth.json`, not in `historical_corpus_manifest.json`.
- `historical_corpus_manifest.json` is intentionally limited to the customer package fields the importer actually consumes.
- Keep filenames stable unless you update the manifest, CLI defaults, tests, and docs together.
- the private equivalent layout under `seed_data/local/` is for real customer data you do not want tracked by git
- when replacing private product truth, prefer `python -m app.cli reimport-product-truth --path seed_data/local/product_truth/product_truth.json`

Unix/macOS convenience aliases remain available through `make import-historical-corpus` and `make import-product-truth`.
