"""Bronze/Silver data-lake pipeline (Workshops 2-3).

bronze.py  - extraction, basic schema validation and raw JSON persistence
silver.py  - preprocessing of Bronze batches into typed, deduplicated Parquet
schema.py  - target Silver schema (single source of truth for code and docs)
paths.py   - data-lake folders and file nomenclature
reference.py - IBTrACS since1980 historical baseline for the analogs and outlier fences (Workshop 3)
"""
