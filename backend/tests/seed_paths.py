from __future__ import annotations

from pathlib import Path


def seed_data_root(repo_root: Path) -> Path:
    return repo_root / "seed_data"


def product_truth_path(repo_root: Path) -> Path:
    return seed_data_root(repo_root) / "product_truth" / "product_truth.json"


def historical_customer_dir(repo_root: Path, slug: str) -> Path:
    return seed_data_root(repo_root) / "historical_customers" / slug
