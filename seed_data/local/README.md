# Private Local Corpus

Use this folder for real customer data that should stay local and untracked.

Keep the same layout as the public sample corpus:

- `seed_data/local/historical_corpus_manifest.json`
- `seed_data/local/product_truth/product_truth.json`
- `seed_data/local/historical_customers/<customer_slug>/...`

Import commands:

- historical corpus: `python -m app.cli import-historical-corpus --base-path seed_data/local`
- product truth replace: `python -m app.cli reimport-product-truth --path seed_data/local/product_truth/product_truth.json`

For Win11 after `bootstrap`, use:

- `.\.venv\Scripts\python.exe -m app.cli import-historical-corpus --base-path seed_data/local`
- `.\.venv\Scripts\python.exe -m app.cli reimport-product-truth --path seed_data/local/product_truth/product_truth.json`

Keep the tracked synthetic sample files in `seed_data/` unchanged for docs, tests, and first-run demos.
