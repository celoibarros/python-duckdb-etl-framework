FROM python:3.11-slim

WORKDIR /app

# pyodbc may require ODBC runtime libraries at execution time.
RUN apt-get update && \
    apt-get install -y --no-install-recommends unixodbc && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
COPY examples ./examples

RUN python -m pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

CMD ["python", "src/framework/main.py", "--config", "examples/example-configs/csv.yaml"]
