def transform(conn):
    # Calculate metrics using DuckDB
    conn.execute(
        """
        CREATE TEMP TABLE metrics AS
        SELECT
            customer_id,
            AVG(sale_amount) AS avg_sale,
            SUM(sale_amount) AS total_sales
        FROM cleaned
        GROUP BY customer_id
    """
    )

    # Pandas post-processing
    df = conn.execute("SELECT * FROM metrics").fetchdf()
    df["sales_tier"] = df["total_sales"].apply(
        lambda x: "high" if x > 10000 else "medium" if x > 5000 else "low"
    )

    # Write back to DuckDB
    conn.register("final_metrics", df)
    conn.execute(
        """
        CREATE TABLE final_output AS
        SELECT * FROM final_metrics
    """
    )
