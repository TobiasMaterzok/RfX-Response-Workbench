# Sample data: BluePeak RfX RAG dataset

## Purpose
This synthetic dataset is the canonical sample dataset for local development, sample runs, and end-to-end tests of RfX RAG Expert.

It represents one fictional vendor and three fictional client RfX packages.

Use it as:
- historical Q&A exemplar data,
- PDF-ingestion test input,
- multilingual fixture data,
- export and provenance test fixtures.

Do **not** treat it as product truth.

The sample dataset does not include canonical product-truth documents. Product truth is ingested separately from `seed_data/product_truth/product_truth.json` as described in `docs/product-truth-contract.md`.

## Repo location
Store these assets under:
- `seed_data/historical_corpus_manifest.json`
- `seed_data/product_truth/`
- `seed_data/historical_customers/<customer_slug>/`

Keep filenames stable unless an explicit migration updates the references and tests.

Important contract note:
- canonical vendor facts do **not** come from `historical_corpus_manifest.json`
- they come from `seed_data/product_truth/product_truth.json`
- the historical corpus manifest is intentionally minimal and only tells the importer which customer packages to ingest

## Vendor
**BluePeak Software GmbH**
- headquarters: Munich, Germany
- model: B2B software vendor
- products:
  - BluePeak Flow
  - BluePeak Vault
  - BluePeak Pulse

## Clients
1. **NordTransit Logistik AG**
   - industry: logistics and freight forwarding
   - location: Hamburg, Germany
   - primary language: German
2. **CrownShield Insurance Services Ltd**
   - industry: specialty insurance services
   - location: Leeds, United Kingdom
   - primary language: English
3. **Asteron Industrial Components B.V.**
   - industry: industrial manufacturing
   - location: Eindhoven, Netherlands
   - primary language: English

## Asset contract
Each client package contains:
- one PDF context brief,
- one XLSX workbook of historical Q&A examples.

Each workbook contains **30 rows** with these exact logical columns:
- `Context`
- `Question`
- `Answer`

Languages may be German or English depending on the client.

## How the system should use this dataset
- The PDF is ingested into a structured case profile.
- The XLSX rows are historical approved exemplars for retrieval.
- Each row must preserve source provenance.
- Retrieval can use row text, case-profile signals, and metadata.
- Historical answers may guide wording and structure, but must not override current case facts or product truth.
- The separate product-truth import provides canonical vendor facts and should be loaded alongside this dataset for the full local sample setup.

## Required provenance fields for ingested sample rows
At minimum, preserve:
- dataset ID,
- client name,
- source filename,
- worksheet name,
- row number,
- language,
- file hash,
- ingestion timestamp,
- schema version,
- approval status.

## Failure policy
Fail ingestion loudly when:
- an expected file is missing,
- workbook columns do not match the contract,
- row identifiers cannot be preserved,
- file hashes or provenance metadata cannot be computed,
- required text fields are missing or malformed.
