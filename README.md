# Qafila gross margin

Submitted by: Abdulmajeed Fahad Alshammari — academy: @SDAIAAcademy

The task, rules and grading are in [BRIEF.md](BRIEF.md).

```bash
uv sync
uv run python tools/make_data.py          # data/
uv run python run.py                      # data/ -> warehouse/ (DuckDB + Iceberg)
uv run python tools/check.py              # contract self-check
```

## Design

`app SQLite + supplier CSVs` → **Python + DuckDB** (`raw`, loaded as text, with file name) → **dbt** `staging` (parse, trim, Riyadh dates, reject reasons) → `silver` (SCD2 cost history) → `gold` (weekly margin) + `quality` (quarantine) → **PyIceberg** (3 tables, updated in place).

| Model | Layer | Grain (one row per ...) | Materialization |
|---|---|---|---|
| `stg_products`, `stg_stores`, `stg_orders`, `stg_order_items` | staging | product / store / order / order line | view |
| `stg_supplier_prices` | staging | supplier price row (file + row number), with `reject_reason` if unusable | view |
| `product_cost_history` | silver | SKU × cost version | table |
| `weekly_margin` | gold | ISO week × city × category (at least one counted line) | table |
| `price_quarantine` | quality | rejected supplier row | table |

**How the SCD2 history is built:** only rows that pass the quarantine rules are used. If the same SKU and `valid_from` appear in several files, the file whose name sorts last wins (so `..._W39_fix.csv` replaces `..._W39.csv`). Then, per SKU ordered by `valid_from`, a new version starts only when the rounded cost differs from the previous quote. A product missing from a weekly file therefore just keeps its cost, because a version lasts until the next *change*. `valid_to` is the next version's `valid_from` (exclusive), empty for the current one.

**How each order line finds its cost:** a join on the trimmed SKU with `valid_from <= order_date AND (valid_to IS NULL OR order_date < valid_to)`, where `order_date` is `created_at` (UTC) + 3 hours cut to a date. No match means the line is *uncosted*.

## Data problems I found

Counts are for the course dataset (seed 2026).

| Problem | Where | Rows affected | What I did |
|---|---|---|---|
| Decimal commas (`9,49`) | W38 file | 126 of 160 | `parse_cost` macro: replace `,` with `.` |
| Arabic-Indic digits (`٩٫٤٩`) | W38 file | 34 of 160 | `translate` the digits and `٫` to ASCII |
| Byte-order mark | W38 file header | 1 file | DuckDB's CSV reader strips it |
| Renamed column `unit_cost` → `cost_sar` | W39 file | 160 | `UNION ALL BY NAME`, then `coalesce(unit_cost, cost_sar)` |
| Zero / negative costs | W39 file | 4 | quarantined, reason `invalid_cost` |
| SKUs Qafila doesn't sell (`QF-NEW-…`) | W39 file | 3 | quarantined, reason `unknown_sku` |
| SKUs with trailing spaces | `products` (the macro also trims supplier SKUs) | 2 products | `clean_sku` macro on both sides before comparing |
| Same SKU + `valid_from` in two files | W39 + W39_fix (after the correction) | 5 | later file name wins |
| Orders outside the margin rules | app DB | 5,347 not delivered; 142 lines with quantity ≤ 0; 20,972 orders before 2026-08-31 | filtered in `weekly_margin` |
| Order lines with no cost yet | SKU not quoted before the order date | 20,626 of 82,404 counted lines | kept as `uncosted_lines`, excluded from cost columns |

## Tests

24 dbt tests, all passing (`dbt build` runs them before the gold table is built).

| Test | On | What it protects against |
|---|---|---|
| `unique`, `not_null` | `stg_products.sku`, `product_id`, `stg_stores`, `stg_orders` keys | Ambiguous SKU after trimming; duplicated keys that would double revenue in joins. The two checks that depend only on the incoming data (SKU unique, order date present) are `warn`, so a surprise in a new dataset is reported without stopping the pipeline |
| `unique_combination` | `stg_order_items` (order_id, line_no) | Duplicate order lines |
| `unique_combination` | `product_cost_history` (sku, valid_from) | Two versions starting the same day |
| `unique_combination` | `weekly_margin` (iso_week, city, category) | A broken grain |
| `positive`, `not_null` | `product_cost_history.unit_cost_sar`, `valid_from`, `sku` | Zero/negative or missing costs reaching the history |
| `accepted_values` | `price_quarantine.reason`, `stg_supplier_prices.reject_reason` | Reasons other than `unknown_sku` / `invalid_cost` |
| `assert_one_current_version_per_sku` | silver | A SKU with no or two current versions |
| `assert_versions_do_not_overlap_or_repeat` | silver | Gaps, overlaps, or a new version when the cost didn't change |
| `assert_margin_adds_up` | gold | margin ≠ costed revenue − cost; costed revenue > revenue; uncosted > lines |
| `assert_every_eligible_line_is_in_the_margin` | gold vs staging | Lines lost or duplicated by the joins (82,404 expected = 82,404 counted) |

## Re-runs and the correction

- **Safe re-run:** `ingest()` replaces the raw tables (never appends), and `publish()` compares the new data with the Iceberg table and writes nothing when they are equal. A second run printed `unchanged ... (no new snapshot)` for all three tables.
- **Correction:** after `tools/make_data.py --add-correction` and a new run, `silver.product_cost_history` went from 633 to 637 rows and `gold.weekly_margin` changed. Both were updated **in place** with one `overwrite` snapshot each (no drop/recreate).
- **Time travel on `gold.weekly_margin`:** total `margin_sar` before the correction is **801,016.88** in snapshot `4902815752965110282`, and after it **803,515.80** in snapshot `813038745504045603`. Reading the old snapshot (`table.scan(snapshot_id=4902815752965110282)`) still returns 801,016.88. Snapshot ids differ on every machine; the numbers should not.
- I also ran the whole sequence (empty warehouse → run → correction → run → run) on `--seed 11` and compared all three tables with an independent plain-Python implementation of the brief's rules: history, margin and quarantine matched exactly in every step.

## What I'd do next

With 100× more data, the first thing to hurt is that `publish()` reads the whole Iceberg table to compare it and rewrites all of it on a change; I'd partition `weekly_margin` by `iso_week` and rewrite only the weeks that changed, and make `weekly_margin` an incremental dbt model. With daily supplier files I'd stop reloading all files every run: load only new files (tracked by name and checksum), and keep the file-name-wins rule for corrections.

## AI assistance

Claude (Anthropic's AI assistant) wrote the pipeline code, the dbt models and tests, and this README from `BRIEF.md`. It checked the result with `tools/check.py`, with a separate plain-Python re-implementation of the rules, and on the seed 11 dataset, as described above.
