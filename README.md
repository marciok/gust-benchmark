# Gust Benchmark

This repository compares **Gust** and **Apache Airflow** under the same workflow and Docker-based setup.

The benchmark uses an equivalent three-step pipeline in both systems:

1. Fetch data from a mock HTTP server
2. Transform the payload and write intermediate data to disk
3. Send the transformed payload back to the mock server

The goal is to measure how each orchestrator behaves at rest and under bursty parallel execution.

## Scenarios

The benchmark covers these scenarios:

- Idle system
- 50 parallel runs
- 80 parallel runs
- 100 parallel runs

In the environment used for the recorded results below, **Airflow did not complete the 100-run scenario**, while **Gust did**.

Each scenario was run independently and the Docker stack was restarted between runs.

## Repository Layout

- `gust/`: Gust Docker stack, DAG, and mock server
- `airflow/`: Airflow Docker stack, DAGs, and mock server
- `livebooks/gust_vs_airflow_benchmark.livemd`: Livebook used to trigger runs and collect `docker stats`

## Workflow Under Test

Both implementations run the same logical pipeline:

- `extract_data`: call `GET /extract` on the mock server
- `transform_data`: normalize and persist the returned data
- `load_data`: call `POST /load` with the transformed payload

The mock server is configured with:

- `MOCK_DELAY_SECONDS=2`
- `MOCK_ITEM_COUNT=1000`

## Running The Benchmark

### Gust

Start the stack:

```bash
cd gust
docker compose up --build
```

Trigger one run:

```bash
docker compose exec -T gust /app/bin/gust-cli trigger_run benchmark_mock_io
```

Gust Web is exposed at `http://localhost:4000`.

### Airflow

Start the stack:

```bash
cd airflow
docker compose up --build
```

Trigger one run:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger benchmark_mock_io
```

Airflow is exposed at `http://localhost:8080`.

### Automated Runs

The benchmark orchestration used for the recorded measurements lives in:

- [`livebooks/gust_vs_airflow_benchmark.livemd`](livebooks/gust_vs_airflow_benchmark.livemd)

That notebook triggers concurrent runs and samples `docker stats` during execution.

## Recorded Results

Versions used for the recorded results:

- Airflow 3.9
- Gust 0.1.31

### Peak Memory Usage

| Scenario | Airflow | Gust | Observation |
| --- | ---: | ---: | --- |
| Idle | 1,710 MiB | 387 MiB | Gust used about 4.4x less RAM |
| 50 parallel runs | 6,222 MiB | 3,064 MiB | Gust used about 2.0x less RAM |
| 80 parallel runs | 7,239 MiB | 5,508 MiB | Gust still used less RAM |
| 100 parallel runs | Not completed | 5,648 MiB | Gust completed this scenario in the recorded run |

### Peak CPU Usage

| Scenario | Airflow | Gust | Observation |
| --- | ---: | ---: | --- |
| Idle | 137% | ~5% | Gust stayed close to idle at rest |
| 50 parallel runs | 1,419% | 1,114% | Gust showed lower CPU pressure |
| 80 parallel runs | 1,313% | 1,198% | Gust remained slightly lower |

## Takeaways

- **Lower baseline cost:** Gust used substantially less memory in the idle scenario.
- **Lower burst overhead:** Gust showed lower CPU and memory pressure during 50- and 80-run bursts.
- **Better headroom in this setup:** Gust completed the 100-run scenario; Airflow did not in the recorded environment.

## Notes

- These numbers come from a local Docker benchmark and should be treated as comparative, not universal.
- The Airflow and Gust workflows are intended to be equivalent, but implementation details of each platform still matter.
- If you want to reproduce or extend the benchmark, start with the Livebook and the two Docker Compose stacks in this repo.
