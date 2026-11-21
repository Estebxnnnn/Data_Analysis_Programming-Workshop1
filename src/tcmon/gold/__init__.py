"""Gold layer (Workshop 3): PySpark aggregations over datalake_silver/.

spark.py         Spark session (local mode), Silver reader, single-file Parquet writer, geodesy columns
inventory.py     metadata of Bronze/Silver files and run logs (small, read with the standard library)
governance.py    data-governance KPIs  -> datalake_gold/governance_YYYYMMDD_HHMMSS.parquet
analogs.py       Storm Analog Similarity (SAS) in PySpark, plus a pandas twin used by the tests
storytelling.py  storytelling sections -> datalake_gold/storytelling_YYYYMMDD_HHMMSS.parquet

Both Gold tables are "tidy" (long) tables: one row per KPI value or per story fact,
so new KPIs or sections never change the file schema.
"""
