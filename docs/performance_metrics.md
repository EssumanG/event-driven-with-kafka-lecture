# Spark Structured Streaming Metrics Overview

## Project Summary

This document provides an overview of key performance metrics for a Spark Structured Streaming job processing **e-commerce user events** (Product View & Product Purchase) using **FileStreamSource** and writing with **ForeachBatchSink**.


>*N.B.: The performance metrics below are derived from a single batch (batchId = 9). Values may vary across batches depending on data volume and system load.*
## Streaming Batch Metrics (Batch ID: 9)

| Metric                | Product View Events | Product Purchase Events | Combined Total |
|-----------------------|---------------------|-------------------------|----------------|
| **Batch ID**          | 9                   | 9                       | 9              |
| **Batch Timestamp**   | 2025-05-01T11:07:41Z| 2025-05-01T11:07:41Z    | —              |
| **Source Path**       | `/opt/real-time-spark/e-commerce-user-events/product-view-events` | `/opt/real-time-spark/e-commerce-user-events/product-purchase-events` | — |
| **Input Rows**        | 28                  | 39                      | **67 rows**    |
| **Input Rows/sec**    | 18.22 rows/sec      |   25.78 rows/sec          | **44.00 rows/sec** |
| **Processed Rows/sec**| 13.79 rows/sec      | 19.20 rows/sec          | **32.99 rows/sec** |
| **Start Offset (log)**| 8                   | 8                       | —              |
| **End Offset (log)**  | 9                   | 9                       | —              |
| **Sink Description**  | ForeachBatchSink    | ForeachBatchSink        | ForeachBatchSink |

## Processing Duration Breakdown (milliseconds)

| Stage            | Product View Events | Product Purchase Events |
|------------------|---------------------|-------------------------|
| **Trigger Execution** | 2031 | 2031 |
| **Get Batch**    | 85                  | 97                      |
| **Query Planning** | 49                | 54                      |
| **Add Batch**    | 807                 | 718                     |
| **Commit Offsets** | 133               | 131                     |
| **WAL Commit**   | 166                 | 182                     |
| **Latest Offset** | 786                | 837                     |

## Key Performance Insights

- **Total Data Processed**: 67 rows from both sources.
- **Input Throughput**: ~44 rows/sec.
- **Processing Throughput**: ~33 rows/sec.
- **Trigger Execution Time**: ~2.03 seconds for both sources.
- **Source Type**: FileStreamSource (directory-based input).
- **Sink Type**: ForeachBatchSink (custom batch processing logic).

## Observations & Considerations

- **Balanced Input Rates**: Both sources show steady input rates.
- **Processing Lag**: Processed rows/sec slightly lower than input rows/sec → monitor for lag buildup over time.
- **Batch Duration Stability**: Consistent ~2-second trigger execution suggests stable micro-batch processing.
- **No Stateful Operations**: `stateOperators` is empty → no aggregations with watermarking or joins.


