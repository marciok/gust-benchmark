from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pendulum
import requests
from airflow.decorators import dag, task


TMP_DIR = Path(os.getenv("BENCHMARK_TMP_DIR", "/tmp/airflow_benchmark"))
MOCK_SERVER_URL = os.getenv("BENCHMARK_MOCK_SERVER_URL", "http://mock-server:8000")


def ensure_tmp_dir() -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)


@dag(
    dag_id="benchmark_mock_io",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    max_active_runs=100,
    catchup=False,
    tags=["benchmark", "mock-io"],
)
def benchmark_mock_io():
    @task
    def extract_data() -> str:
        ensure_tmp_dir()
        started_at = time.perf_counter()

        print(f"[extract_data] calling {MOCK_SERVER_URL}/extract")
        response = requests.get(f"{MOCK_SERVER_URL}/extract", timeout=30)
        response.raise_for_status()
        payload = response.json()

        raw_path = TMP_DIR / "raw.json"
        raw_path.write_text(json.dumps(payload), encoding="utf-8")

        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        print(
            f"[extract_data] done duration_ms={duration_ms} "
            f"count={payload.get('count', 0)} path={raw_path}"
        )

        return str(raw_path)

    @task
    def transform_data(raw_path: str) -> str:
        ensure_tmp_dir()
        started_at = time.perf_counter()

        print(f"[transform_data] reading {raw_path}")
        payload = json.loads(Path(raw_path).read_text(encoding="utf-8"))
        items = payload.get("items", [])

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
                    "brand": item["brand"].strip().lower(),
                    "model": item["model"].strip().lower(),
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
        print(
            f"[transform_data] done duration_ms={duration_ms} "
            f"count={len(transformed_items)} path={transformed_path}"
        )

        return str(transformed_path)

    @task
    def load_data(transformed_path: str) -> dict:
        started_at = time.perf_counter()

        print(f"[load_data] posting {transformed_path} to {MOCK_SERVER_URL}/load")
        payload = json.loads(Path(transformed_path).read_text(encoding="utf-8"))

        response = requests.post(
            f"{MOCK_SERVER_URL}/load",
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        result = response.json()

        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        print(
            f"[load_data] done duration_ms={duration_ms} "
            f"received={result.get('received')} ok={result.get('ok')}"
        )

        return result

    raw = extract_data()
    transformed = transform_data(raw)
    load_data(transformed)


benchmark_mock_io()
