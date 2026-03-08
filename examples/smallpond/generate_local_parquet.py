from pathlib import Path

import pandas as pd


if __name__ == "__main__":
    base = Path(__file__).resolve().parent
    input_dir = base / "input"
    output_dir = base / "output"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        {
            "customer_id": [1, 1, 2, 2, 3],
            "amount": [10.0, 15.5, 7.0, 8.5, 5.0],
            "event_date": [
                "2026-01-01",
                "2026-01-02",
                "2026-01-01",
                "2026-01-03",
                "2026-01-04",
            ],
        }
    )

    out_file = input_dir / "sales.parquet"
    df.to_parquet(out_file, index=False)
    print(f"Wrote sample parquet to: {out_file}")
