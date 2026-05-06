# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-idmc-idu** collects internal displacement event data from the [IDMC IDU API](https://helix-tools-api.idmcdb.org/external-api/idus/last-180-days/) and publishes one HDX dataset per country. IDU (Internal Displacement Updates) provides provisional data on new displacements caused by conflicts and disasters over a 180-day rolling window, updated daily.

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python -m hdx.scraper.idmc.idu
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_idmc.py::TestIDMC::test_generate_dataset_and_showcase
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline flows through three stages in `__main__.py`:

1. **`get_idmc_territories`** — Loads a static CSV (`IDMC_territories.csv`) mapping IDMC territory codes to ISO3 country codes.

2. **`get_countriesdata`** — Calls the IDMC IDU API, filters and groups displacement events by country, and returns a list of country dicts for processing.

3. **`generate_dataset_and_showcase`** — Constructs an HDX `Dataset` object for a given ISO3 country, attaches resources, and optionally creates a `Showcase`. Returns `(dataset, showcase)`.

### Key design points

- **One dataset per country**: the scraper iterates over countries returned by `get_countriesdata` and creates/updates one HDX dataset each.
- **API auth via `IDMC_KEY`**: if the env var is set it is passed as `client_id` in extra params; otherwise `.extraparams.yaml` is used as fallback.
- **`Retrieve`** (`hdx-python-utilities`) abstracts HTTP downloads and supports save/replay via `save=True`/`use_saved=True` — used in tests to replay fixture data from `tests/fixtures/input/`.
- **Static config inside the package**: `config/` lives under `src/hdx/scraper/idmc/idu/config/` so it is installed with the package and located via `script_dir_plus_file`.

### Config files

- `src/hdx/scraper/idmc/idu/config/project_configuration.yaml` — API URL and dataset description template
- `src/hdx/scraper/idmc/idu/config/hdx_dataset_static.yaml` — Static HDX metadata applied to every dataset (license, methodology, source, etc.)
- `src/hdx/scraper/idmc/idu/config/IDMC_territories.csv` — Territory-to-ISO3 mapping

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `EXTRA_PARAMS`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-idmc-idu` entry.

Optionally requires `IDMC_KEY` env var (or `~/.extraparams.yaml` with an `hdx-scraper-idmc-idu` entry) for authenticated IDMC API access.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
