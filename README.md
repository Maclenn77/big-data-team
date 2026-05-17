# big-data-team

PySpark pipeline to process New York taxi cab data from Kaggle and generate trend outputs.

## Requirements

- Python 3.9+
- Java 8/11 (required by Spark)
- `pyspark`

Install dependency:

```bash
pip install pyspark
```

## Input data

Download a Kaggle New York taxi CSV dataset (for example yellow taxi trips) and provide the CSV path as input.

## Run the pipeline

```bash
python nyc_taxi_pipeline.py \
  --input /path/to/nyc_taxi.csv \
  --output /path/to/output
```

## Output

The pipeline writes:

- `cleaned_trips/` (Parquet dataset with standardized timestamp columns)
- `trends/trips_by_hour/` (CSV: number of trips grouped by pickup hour)
- `trends/trips_by_day/` (CSV: number of trips grouped by pickup date)
- `trends/avg_passenger_by_hour/` (CSV: average passenger count by pickup hour, when available)
- `trends/revenue_by_payment_type/` (CSV: total fare by payment type, when available)
