# Load tests (Week 4)

k6 scripts that enqueue N jobs and measure throughput plus p50/p99 enqueue-to-start latency
at 1, 4 and 8 workers (`docker compose up --scale worker=N`). Results go here and in the
main README, with the hardware stated.
