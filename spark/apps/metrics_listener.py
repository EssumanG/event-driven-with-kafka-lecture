from pyspark.sql.streaming import StreamingQueryListener
from prometheus_client import Gauge, start_http_server
import psutil  # Optional: for driver memory/CPU usage

# Start Prometheus metrics server on port 9095
start_http_server(9095)

# =========================
# Streaming metrics
# =========================
input_rows = Gauge("spark_input_rows_per_second", "Input rows/sec")
processed_rows = Gauge("spark_processed_rows_per_second", "Processed rows/sec")
batch_duration = Gauge("spark_batch_duration_ms", "Batch duration per batch in ms")

# Optional: track driver process memory and CPU
driver_memory = Gauge("python_driver_resident_memory_bytes", "Python driver resident memory bytes")
driver_cpu = Gauge("python_driver_cpu_percent", "Python driver CPU percent")

# Optional: track state store memory (if using stateful operations)
state_memory = Gauge("spark_state_store_memory_bytes", "State store memory usage in bytes")

# =========================
# Streaming Query Listener
# =========================
class MetricsListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Streaming query started: {event.id}")

    def onQueryProgress(self, event):
        progress = event.progress

        # Streaming metrics
        input_rows.set(progress.inputRowsPerSecond or 0)
        processed_rows.set(progress.processedRowsPerSecond or 0)
        batch_duration.set(progress.batchDuration or 0)

        # Optional: Python driver process metrics
        p = psutil.Process()
        driver_memory.set(p.memory_info().rss)  # resident memory
        driver_cpu.set(p.cpu_percent(interval=None))

        # Optional: state store memory
        if progress.stateOperators:
            total_state = sum(op.memoryUsedBytes for op in progress.stateOperators)
            state_memory.set(total_state)
        else:
            state_memory.set(0)

    def onQueryTerminated(self, event):
        print(f"Streaming query terminated: {event.id}")
