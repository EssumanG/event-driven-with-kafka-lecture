from prometheus_client import start_http_server, Gauge
import time

g = Gauge("test_metric", "Just a test")

start_http_server(9095)

i = 0
while True:
    g.set(i)
    i += 1
    time.sleep(2)
