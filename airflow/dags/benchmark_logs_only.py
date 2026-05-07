from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import pendulum
from airflow.decorators import dag, task


TMP_DIR = Path(os.getenv("BENCHMARK_TMP_DIR", "/tmp/airflow_benchmark"))
ITEM_COUNT = int(os.getenv("BENCHMARK_ITEM_COUNT", "1000"))

logger = logging.getLogger(__name__)


def ensure_tmp_dir() -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)


def log_step(task_name: str, phase: str, extra: dict | None = None) -> None:
    payload = {
        "task": task_name,
        "phase": phase,
        "ts": time.time(),
    }

    if extra:
        payload.update(extra)

    logger.info(json.dumps(payload))


@dag(
    dag_id="benchmark_logs_only",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["benchmark"],
)
def benchmark_logs_only():
    @task
    def extract_data() -> str:
        ensure_tmp_dir()
        started_at = time.perf_counter()

        log_step("extract_data", "start", {"items_target": ITEM_COUNT})

        items = [
            {
                "id": i,
                "brand": "Honda" if i % 2 == 0 else "Toyota",
                "model": "Civic" if i % 2 == 0 else "Corolla",
                "year": 2020 + (i % 5),
                "price": 15_000 + (i * 7 % 40_000),
                "mileage": 5_000 + (i * 13 % 120_000),
            }
            for i in range(ITEM_COUNT)
        ]

        payload = {"items": items, "count": len(items)}

        raw_path = TMP_DIR / "raw.json"
        raw_path.write_text(json.dumps(payload), encoding="utf-8")

        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        log_step(
            "extract_data",
            "end",
            {
                "duration_ms": duration_ms,
                "output_path": str(raw_path),
                "count": len(items),
            },
        )

        return str(raw_path)

    @task
    def transform_data(raw_path: str) -> str:
        ensure_tmp_dir()
        started_at = time.perf_counter()

        log_step("transform_data", "start", {"input_path": raw_path})

        payload = json.loads(Path(raw_path).read_text(encoding="utf-8"))
        items = payload["items"]

        transformed_items = []
        for item in items:
            price = float(item["price"])
            year = int(item["year"])
            mileage = int(item["mileage"])

            price_bucket = (
                "cheap" if price < 20_000 else "mid" if price < 50_000 else "premium"
            )
            normalized_score = round((year * 1000) / (mileage + 1), 6)

            transformed_items.append(
                {
                    "source_id": item["id"],
                    "brand": item["brand"].lower(),
                    "model": item["model"].lower(),
                    "year": year,
                    "price": price,
                    "mileage": mileage,
                    "price_bucket": price_bucket,
                    "normalized_score": normalized_score,
                }
            )

        transformed_payload = {
            "items": transformed_items,
            "count": len(transformed_items),
        }

        transformed_path = TMP_DIR / "transformed.json"
        transformed_path.write_text(json.dumps(transformed_payload), encoding="utf-8")

        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        log_step(
            "transform_data",
            "end",
            {
                "duration_ms": duration_ms,
                "output_path": str(transformed_path),
                "count": len(transformed_items),
            },
        )

        return str(transformed_path)

    @task
    def load_data(transformed_path: str) -> dict:

        started_at = time.perf_counter()

        log_step("load_data", "start", {"input_path": transformed_path})

        payload = json.loads(Path(transformed_path).read_text(encoding="utf-8"))
        items = payload["items"]

        summary = {
            "count": len(items),
            "cheap": sum(1 for item in items if item["price_bucket"] == "cheap"),
            "mid": sum(1 for item in items if item["price_bucket"] == "mid"),
            "premium": sum(1 for item in items if item["price_bucket"] == "premium"),
        }

        summary_path = TMP_DIR / "summary.json"
        summary_path.write_text(json.dumps(summary), encoding="utf-8")

        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        log_step(
            "load_data",
            "end",
            {
                "duration_ms": duration_ms,
                "output_path": str(summary_path),
                **summary,
            },
        )

        return summary

    raw = extract_data()
    transformed = transform_data(raw)
    load_data(transformed)


benchmark_logs_only()
