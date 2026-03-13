# Product truth contract

## Purpose
This document defines the canonical vendor-truth corpus that the system must consult separately from historical customer answers.

Historical answers are reusable exemplars. They are not authoritative evidence of product capability.

## Required product-truth coverage
The corpus should eventually cover at least:
- products and modules,
- deployment models,
- hosting and residency options,
- security controls,
- privacy/compliance claims that are actually supported,
- integrations and APIs,
- workflow/case-management features,
- analytics/reporting capabilities,
- implementation and delivery model,
- support and operating assumptions,
- known exclusions and non-supported scenarios.

## Recommended normalized record shape
- `truth_record_id`
- `product_area`
- `title`
- `body`
- `language`
- `source_file_name`
- `source_section`
- `effective_from`
- `effective_to` (nullable)
- `version`
- `approval_status`
- `file_hash`
- `ingested_at`

## Required rules
- Product-truth records must be separately retrievable from historical exemplars.
- Generation prompts must label product truth as canonical vendor truth.
- Unsupported or unknown capabilities must remain explicit unknowns.
- Compliance or security claims must not be invented from historical phrasing.

## Current version-handling behavior
- Retrieval only considers approved product-truth records.
- Retrieval excludes records whose `effective_to` is in the past.
- The current implementation does **not** automatically resolve overlapping active versions or conflicting approved records in `best_effort` mode.
- `strict_eval` forbids additive product-truth import once approved records already exist; use `reimport-product-truth` to replace the corpus cleanly.

## Failure policy
Fail loudly when:
- required provenance is missing,
- the source payload is malformed or required fields are missing,
- a product-truth body is empty,
- `strict_eval` attempts additive import after approved product-truth already exists,
- a historical exemplar is used as the only evidence for a product claim,
- expired records are retrieved as current despite `effective_to` being in the past.
