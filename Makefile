PYTHON ?= python3
PIP ?= $(PYTHON) -m pip
NPM ?= npm

.PHONY: install install-backend install-frontend format lint typecheck test test-backend test-frontend e2e migrate ensure-local-identity import-historical-corpus import-product-truth eval-retrieval run-backend run-frontend run-bulk-fill-worker run-bulk-fill-worker-once

install: install-backend install-frontend

install-backend:
	$(PIP) install -e ./backend[dev]

install-frontend:
	$(NPM) --prefix frontend install

format:
	$(PYTHON) -m ruff format backend
	$(NPM) --prefix frontend run format

lint:
	$(PYTHON) -m ruff check backend
	$(NPM) --prefix frontend run lint

typecheck:
	$(PYTHON) -m mypy backend/app
	$(NPM) --prefix frontend run typecheck

test: test-backend test-frontend

test-backend:
	$(PYTHON) -m pytest backend/tests

eval-retrieval:
	$(PYTHON) -m pytest backend/tests/test_retrieval_eval.py

test-frontend:
	$(NPM) --prefix frontend run test -- --run

e2e:
	$(PYTHON) -m pytest backend/tests/test_api_e2e.py

migrate:
	$(PYTHON) -m alembic -c backend/alembic.ini upgrade head

ensure-local-identity:
	$(PYTHON) -m app.cli ensure-local-identity

import-historical-corpus:
	$(PYTHON) -m app.cli import-historical-corpus

import-product-truth:
	$(PYTHON) -m app.cli import-product-truth

run-backend:
	$(PYTHON) -m uvicorn app.main:app --reload --port 8000

run-frontend:
	$(NPM) --prefix frontend run dev

run-bulk-fill-worker:
	$(PYTHON) -m app.cli run-bulk-fill-worker

run-bulk-fill-worker-once:
	$(PYTHON) -m app.cli run-bulk-fill-worker --once
