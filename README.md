## DuckDB-Powered Lightweight ETL Framework

![image](https://github.com/user-attachments/assets/02c932f6-66d2-496c-98ce-41da507c8c15)

This repository provides a lightweight ETL framework powered by **DuckDB** for configurable extract, transform, and load pipelines across local and cloud data sources.

Maintainer: [Carlos Eloi Barros](https://github.com/celoibarros)

## Attribution

This project was originally based on work from:
- https://github.com/soumilshah1995/duckdb-etl-framework

The framework in this repository has been adapted and extended for additional runtime and integration needs.

## Features

- Lightweight execution with DuckDB
- Config-driven ETL pipelines
- Support for local files, Azure Blob Storage (ABFS), and SQL Server sources
- SQL and Python transform steps
- Output export with partitioning/overwrite controls

## Getting Started

### 1) Install dependencies

This project is managed with Poetry:

```bash
poetry install
```

For Azure-backed reads/writes, configure your Azure account and credentials in the ETL config (for example: account name, tenant ID, managed identity, service principal, or account key).

### 2) Prepare a config file

Examples are available under [`examples/example-configs`](examples/example-configs).

### 3) Run the ETL

```bash
poetry run python src/framework/main.py --config examples/example-configs/csv.yaml
```

## Optional Distributed Processing (smallpond)

You can run distributed SQL transforms with [`smallpond`](https://github.com/deepseek-ai/smallpond) by installing the optional extra:

```bash
poetry install --extras distributed
```

Then define a transform step with `type: smallpond_sql`:

```yaml
transform:
  steps:
    - type: smallpond_sql
      input_path: "abfs://container/input/*.parquet"
      output_path: "abfs://container/output/"
      sql: "SELECT key, COUNT(*) AS total FROM {0} GROUP BY key"
      repartition: 8
      hash_by: key
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
