## DuckDB-Powered Lightweight ETL Framework

![image](https://github.com/user-attachments/assets/02c932f6-66d2-496c-98ce-41da507c8c15)

This repository provides a lightweight ETL framework powered by **DuckDB** for configurable extract, transform, and load pipelines across local and cloud data sources.

## Attribution

This project was originally based on work from:
- https://github.com/soumilshah1995/duckdb-etl-framework

The framework in this repository has been adapted and extended for additional runtime and integration needs.

## Features

- Lightweight execution with DuckDB
- Config-driven ETL pipelines
- Support for file, S3, ABFS, and SQL Server sources
- SQL and Python transform steps
- Output export with partitioning/overwrite controls

## Getting Started

### 1) Install dependencies

This project is managed with Poetry:

```bash
poetry install
```

If you need AWS-backed reads/writes, export credentials before running:

```bash
export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_ACCESS_KEY"
export AWS_REGION="us-east-1"
```

### 2) Prepare a config file

Examples are available under [`examples/example-configs`](examples/example-configs).

### 3) Run the ETL

```bash
poetry run python src/framework/main.py --config examples/example-configs/csv.yaml
```

You can also use a template script:

```bash
poetry run python examples/template/template.py --config /path/to/config.yaml
```

## Docker

```bash
docker build -t duckdb-etl-framework .
```

```bash
docker run --rm duckdb-etl-framework python src/framework/main.py --config /app/examples/example-configs/csv.yaml
```

## Contributing

Contributions are welcome via issues and pull requests.
