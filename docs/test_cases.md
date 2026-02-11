



# Test Cases

| Test Scenario                                | Expected Outcome                                              | Actual Outcome                                               |
|---------------------------------------------|--------------------------------------------------------------|-------------------------------------------------------------|
| CSV files are generated correctly           | New CSV files created with correct schema and data           | CSV files generated with correct columns and valid sample data |
| Spark detects and processes new files       | Spark reads new CSVs and processes them                      | Spark successfully detected and processed new rows from CSVs |
| Data transformations are correct            | Transformed schema matches target table                      | Columns renamed and customer_name concatenated as expected |
| Data is written into PostgreSQL without error | Data visible in PostgreSQL event_log table                   | New rows appended to `event_log` table without errors |
| Performance metrics within expected limits  | Latency and throughput within acceptable thresholds          | Avg input rate = 22 rows/sec, processing rate = 18 rows/sec (within target) |
