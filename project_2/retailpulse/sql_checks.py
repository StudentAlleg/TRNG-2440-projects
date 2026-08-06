"""Part D. Run the Spark SQL scripts and compare them to the PySpark Gold tables.

Every .sql file in sql/ is executed with `{catalog}` / `{schema}` substituted,
then the reconciliation queries in sql/checks/ must return zero rows.

    python -m retailpulse.sql_checks
"""

from __future__ import annotations

import logging
from pathlib import Path

from .config import PROJECT_ROOT, RunConfig, load_config
from .session import cli, get_spark

log = logging.getLogger("sql_checks")
SQL_DIR = PROJECT_ROOT / "sql"


def run_script(spark, cfg: RunConfig, path: Path):
    """Execute every statement in one file; return the last result set."""
    text = path.read_text(encoding="utf-8").format(catalog=cfg.catalog, schema=cfg.schema)
    result = None
    for statement in [s.strip() for s in text.split(";") if s.strip()]:
        result = spark.sql(statement)
    return result


def main(mode: str | None = None) -> None:
    cfg = load_config(mode)
    spark = get_spark(cfg)

    for path in sorted(SQL_DIR.glob("*.sql")):
        log.info("running %s", path.name)
        df = run_script(spark, cfg, path)
        if df is not None:
            df.show(20, truncate=False)

    failures = []
    for path in sorted((SQL_DIR / "checks").glob("*.sql")):
        df = run_script(spark, cfg, path)
        if df is None:
            continue
        rows = df.collect()
        log.info("check %-40s %s", path.name, "PASS" if not rows else "FAIL")
        if rows:
            df.show(20, truncate=False)
            failures.append(path.name)

    if failures:
        raise SystemExit(f"reconciliation failed: {', '.join(failures)}")
    log.info("all reconciliation checks passed")


if __name__ == "__main__":
    main(cli(__doc__))
