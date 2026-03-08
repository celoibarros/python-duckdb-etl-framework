import os
import time

import duckdb
import numpy as np
import pandas as pd

# ---------- Configuration ----------
ROWS = 10_000_000
THREAD_OPTIONS = [1, 2, 4, 8]
MEMORY_LIMITS = ["512MB", "1GB", "2GB", "4GB", "8GB"]
PARQUET_FILE = "benchmark_data.parquet"
USE_FASTPARQUET = False
REPEATS = 10


# ---------- Step 1: Generate Parquet ----------
def generate_parquet():
    if os.path.exists(PARQUET_FILE):
        print(f"[✔] Parquet file already exists: {PARQUET_FILE}")
        return

    print(f"[⏳] Generating dataset with {ROWS:,} rows...")
    categories = [f"Category_{i}" for i in range(20)]
    df = pd.DataFrame(
        {
            "id": np.arange(ROWS),
            "category": np.random.choice(categories, size=ROWS),
            "sales": np.random.uniform(10, 1000, size=ROWS),
            "quantity": np.random.randint(1, 10, size=ROWS),
            "date": pd.date_range("2022-01-01", periods=ROWS, freq="T"),
        }
    )

    engine = "fastparquet" if USE_FASTPARQUET else "pyarrow"
    df.to_parquet(PARQUET_FILE, engine=engine)
    print(f"[✔] Parquet saved: {PARQUET_FILE}")


# ---------- Step 2: Run Benchmark ----------
def run_benchmark(threads, memory_limit):
    query_names = [
        "Total Sales",
        "Group By Category",
        "Top 1000 Sales",
        "High Sales Filter",
        "Category + Date Grouping",
    ]
    query_sql = {
        "Total Sales": "SELECT SUM(sales) FROM benchmark",
        "Group By Category": "SELECT category, SUM(sales) FROM benchmark GROUP BY category",
        "Top 1000 Sales": "SELECT * FROM benchmark ORDER BY sales DESC LIMIT 1000",
        "High Sales Filter": "SELECT COUNT(*) FROM benchmark WHERE sales > 900",
        "Category + Date Grouping": "SELECT category, DATE_TRUNC('month', date) as month, SUM(sales) FROM benchmark GROUP BY category, month",
    }

    timings = {q: [] for q in query_names}

    print(f"\n[🔧] Benchmark: Threads={threads}, Memory={memory_limit}")
    for i in range(REPEATS):
        try:
            conn = duckdb.connect(database=":memory:")
            conn.execute(f"PRAGMA threads={threads}")
            conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
            conn.execute(
                f"CREATE TABLE benchmark AS SELECT * FROM read_parquet('{PARQUET_FILE}')"
            )

            for qname in query_names:
                try:
                    start = time.time()
                    conn.execute(query_sql[qname]).fetchall()
                    elapsed = time.time() - start
                    timings[qname].append(elapsed)
                except Exception as qe:
                    print(f"    ❌ Query failed: {qname} — {qe}")
                    timings[qname].append(None)

            conn.close()
        except Exception as e:
            print(f"    ❌ Failed iteration {i+1}: {e}")
            for q in query_names:
                timings[q].append(None)

    # Compute average
    averages = {}
    for q in query_names:
        valid_times = [t for t in timings[q] if t is not None]
        if valid_times:
            averages[q] = round(np.mean(valid_times), 3)
        else:
            averages[q] = "ERROR"

    return averages


# ---------- Step 3: Execute Benchmarks ----------
if __name__ == "__main__":
    generate_parquet()

    all_results = {}

    for memory in MEMORY_LIMITS:
        for threads in THREAD_OPTIONS:
            config_key = f"{threads}T/{memory}"
            result = run_benchmark(threads, memory)
            all_results[config_key] = result

    df_results = pd.DataFrame(all_results).T

    # ---------- Step 4: Highlight Best Configs ----------
    df_highlight = df_results.copy()

    for query in df_results.columns:
        try:
            best_time = pd.to_numeric(df_results[query], errors="coerce").min()
            df_highlight[query] = df_results[query].apply(
                lambda x: (
                    f"✅ {x}"
                    if x != "ERROR" and round(float(x), 3) == best_time
                    else str(x)
                )
            )
        except Exception:
            continue

    print("\n📊 Summary of Average Benchmark Times (sec) with Highlights:")
    print(df_highlight.to_string())
