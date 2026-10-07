"""Prometheus metrics. Each process (api, worker, reaper) exposes the ones it updates."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

_LATENCY_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
)  # fmt: skip

# API
JOBS_ENQUEUED = Counter("jobq_jobs_enqueued_total", "Jobs created via the API", ["priority"])
ENQUEUE_DUPLICATES = Counter(
    "jobq_enqueue_duplicates_total", "Enqueue requests that matched an existing Idempotency-Key"
)
ENQUEUE_REJECTED = Counter(
    "jobq_enqueue_rejected_total", "Enqueue requests rejected with 429 (backpressure)"
)
ENQUEUE_PUSH_FAILED = Counter(
    "jobq_enqueue_push_failed_total",
    "Jobs committed to Postgres whose Redis push failed (the reconciler re-pushes them)",
)

# Worker
JOBS_STARTED = Counter("jobq_jobs_started_total", "Attempts started", ["type"])
JOBS_FINISHED = Counter(
    "jobq_jobs_finished_total",
    "Attempts finished, by outcome: succeeded | retried | dead | released | fenced",
    ["type", "outcome"],
)
ENQUEUE_TO_START = Histogram(
    "jobq_enqueue_to_start_seconds",
    "Time from a job becoming runnable to a worker starting it",
    buckets=_LATENCY_BUCKETS,
)
JOB_DURATION = Histogram(
    "jobq_job_duration_seconds", "Handler run time", ["type"], buckets=_LATENCY_BUCKETS
)
LEASES_LOST = Counter("jobq_leases_lost_total", "Heartbeats that found the lease already gone")

# Reaper
LEASES_EXPIRED = Counter(
    "jobq_leases_expired_total", "Expired leases reclaimed by the reaper (crashed workers)"
)
RECONCILED = Counter(
    "jobq_reconciled_total", "Jobs re-pushed to Redis by the reconciler", ["status"]
)
SCHEDULED = Counter("jobq_scheduled_jobs_total", "Jobs created by cron schedules")
QUEUE_DEPTH = Gauge("jobq_queue_depth", "Jobs in the ready lists", ["priority"])
DELAYED = Gauge("jobq_delayed_jobs", "Jobs waiting for run_at (delayed or backing off)")
INFLIGHT = Gauge("jobq_inflight_jobs", "Jobs currently leased by a worker")
DLQ_SIZE = Gauge("jobq_dlq_size", "Jobs in the dead-letter queue (status = dead)")
JOBS_BY_STATUS = Gauge("jobq_jobs", "Jobs in Postgres by status", ["status"])


def serve(port: int) -> None:
    """Start the /metrics HTTP server in a background thread (no-op when port is 0)."""
    if port:
        start_http_server(port)
