#  Performance Metrics Walkthrough (Grafana + Prometheus)

This document explains how to:

1. Verify Spark metrics in Prometheus  
2. Visualize them in Grafana  
3. Understand what each metric means  
4. Detect performance issues in Structured Streaming  

---

# Architecture Overview

Spark → Prometheus → Grafana

- **Spark** exposes metrics
- **Prometheus** scrapes and stores them
- **Grafana** visualizes them

---

#  Step 1: Verify Prometheus is Scraping Spark

Open Prometheus UI:
`http://localhost:9090`

Go to:

**Status → Targets**
You should see your Spark application target as: `UP`


If it is `DOWN`, Prometheus is not scraping Spark correctly.

---

##  Test a Metric in Prometheus

Go to:

**Graph → Expression**

Try querying: 
`spark_input_rows_per_second`
OR
`spark_processed_rows_per_second`

If data appears → metrics are flowing correctly.

---

# Step 2: Open Grafana

Open: `http://localhost:3000`


Navigate to:

**Dashboards → Your Imported Spark Dashboard**

Ensure the data source is set to: `Prometheus`

---

#  Key Streaming Metrics Explained

---

## 1. Input Rows per Second

Metric example: `spark_input_rows_per_second`

### What it means

- Number of records received per second
- Measures ingestion speed

### Interpretation

| Scenario | Meaning |
|----------|----------|
| Input rate stable | Healthy ingestion |
| Sudden spike | Traffic burst |
| Drops to zero | No incoming data |

---

##  2. Processed Rows per Second

Metric example:`spark_processed_rows_per_second`

### What it means

- Number of records processed per second
- Measures compute throughput

### Interpretation

| Scenario | Meaning |
|----------|----------|
| Processing ≥ Input | System healthy |
| Processing < Input | Falling behind |
| Processing fluctuates heavily | Resource pressure |

---

##  3. Batch Duration

Metric example: `spark_streaming_lastCompletedBatch_processingDelay`

### What it means

- Time taken to process one micro-batch

Compare this to your trigger interval.

Example:

If trigger = 10 seconds  
Batch duration = 12 seconds  

➡ Spark is falling behind.

---

##  4. Falling Behind Detection

You are falling behind when:
```
Processing Rate < Input Rate
```

This means:

- Backpressure building
- Latency increasing
- Potential memory pressure

---

#  How to Diagnose Issues Using Grafana

---

## Case 1: Input Rate Spikes

If green line spikes:

- Check Kafka lag
- Check producer burst traffic
- Scale Spark executors

---

## Case 2: Processing Rate Drops

Possible causes:

- CPU saturation
- Shuffle pressure
- Too many partitions
- State store growth

Check:

- Executor memory
- Shuffle partitions
- State store size

---

## Case 3: Increasing Batch Duration

Causes:

- Large state store
- Skewed partitions
- Expensive joins/windows
- Garbage collection

---

# Healthy Streaming Pattern

You want:

- Input rate ≈ Processing rate
- Stable batch duration
- No increasing trend in lag

Graph should look:

- Parallel green & orange lines
- Stable batch duration stat

---

# Advanced Monitoring Tips

### Monitor:

- JVM memory usage
- Executor CPU
- State store size
- Shuffle read/write size

### Watch for:

- Gradual performance degradation
- Sudden latency spikes
- Memory growth without drop

---

# Summary

| Metric | Purpose | Healthy Sign |
|--------|----------|--------------|
| Input Rate | Ingestion speed | Stable |
| Processing Rate | Throughput | ≥ Input |
| Batch Duration | Latency | < Trigger interval |
| Lag | Backpressure | Near zero |

---

Proper monitoring ensures:

- Low latency
- No backlog
- Scalable streaming architecture
- Production-ready performance