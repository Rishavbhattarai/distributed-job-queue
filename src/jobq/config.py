"""Runtime settings, read from environment variables (12-factor style)."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field


def _default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def _parse_type_map(raw: str) -> dict[str, int]:
    """Parse ``"send_invoice=10,sleep=2"`` into ``{"send_invoice": 10, "sleep": 2}``."""
    out: dict[str, int] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        name, _, value = part.partition("=")
        if not name.strip() or not value.strip():
            raise ValueError(f"bad JOBQ_MAX_ATTEMPTS_BY_TYPE entry: {part!r}")
        out[name.strip()] = int(value)
    return out


@dataclass(frozen=True)
class Settings:
    database_url: str = "postgresql+asyncpg://jobq:jobq@localhost:5432/jobq"
    redis_url: str = "redis://localhost:6379/0"
    # All Redis keys live under this prefix, so tests (or several deployments)
    # can share one Redis without colliding.
    redis_prefix: str = "jobq"
    worker_id: str = field(default_factory=_default_worker_id)
    # How long run_once() keeps polling for work before giving up (tests, CLI loops).
    poll_timeout: float = 1.0
    # Sleep between empty lease attempts. Bounds idle enqueue-to-start latency.
    idle_sleep: float = 0.02
    # Extra modules the worker imports at startup so they can register handlers
    # (e.g. Project 3's billing handlers): JOBQ_HANDLER_MODULES="billing.jobs,foo.bar"
    handler_modules: tuple[str, ...] = ()
    log_level: str = "INFO"

    # Leases (see docs/adr/0002).
    lease_seconds: float = 30.0
    heartbeat_interval: float = 10.0

    # Retries (see docs/adr/0003). Full-jitter exponential backoff.
    default_max_attempts: int = 3
    max_attempts_by_type: dict[str, int] = field(default_factory=dict)
    backoff_base: float = 1.0
    backoff_cap: float = 300.0

    # Graceful shutdown: how long a worker waits for its in-flight job after SIGTERM
    # before cancelling it and releasing the job back to the queue.
    shutdown_grace: float = 25.0

    # Backpressure: POST /jobs returns 429 when ready-queue depth reaches this. 0 = off.
    max_queue_depth: int = 50_000

    # Reaper / reconciler / scheduler loop (see docs/adr/0004, 0005).
    promote_interval: float = 0.1
    reap_interval: float = 1.0
    schedule_interval: float = 1.0
    reconcile_interval: float = 30.0
    # A queued job must be at least this old before the reconciler treats it as missing
    # from Redis. Covers the gap between the Postgres commit and the Redis push.
    reconcile_grace: float = 30.0

    # Port for the Prometheus /metrics endpoint of the worker or reaper. 0 = disabled.
    metrics_port: int = 0

    def max_attempts_for(self, job_type: str) -> int:
        return self.max_attempts_by_type.get(job_type, self.default_max_attempts)

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ
        d = cls()

        def f(name: str, default: float) -> float:
            return float(env.get(name, default))

        def i(name: str, default: int) -> int:
            return int(env.get(name, default))

        modules = tuple(
            m.strip() for m in env.get("JOBQ_HANDLER_MODULES", "").split(",") if m.strip()
        )
        lease = f("JOBQ_LEASE_SECONDS", d.lease_seconds)
        return cls(
            database_url=env.get("JOBQ_DATABASE_URL", d.database_url),
            redis_url=env.get("JOBQ_REDIS_URL", d.redis_url),
            redis_prefix=env.get("JOBQ_REDIS_PREFIX", d.redis_prefix),
            worker_id=env.get("JOBQ_WORKER_ID") or _default_worker_id(),
            poll_timeout=f("JOBQ_POLL_TIMEOUT", d.poll_timeout),
            idle_sleep=f("JOBQ_IDLE_SLEEP", d.idle_sleep),
            handler_modules=modules,
            log_level=env.get("JOBQ_LOG_LEVEL", d.log_level),
            lease_seconds=lease,
            heartbeat_interval=f("JOBQ_HEARTBEAT_INTERVAL", lease / 3),
            default_max_attempts=i("JOBQ_DEFAULT_MAX_ATTEMPTS", d.default_max_attempts),
            max_attempts_by_type=_parse_type_map(env.get("JOBQ_MAX_ATTEMPTS_BY_TYPE", "")),
            backoff_base=f("JOBQ_BACKOFF_BASE", d.backoff_base),
            backoff_cap=f("JOBQ_BACKOFF_CAP", d.backoff_cap),
            shutdown_grace=f("JOBQ_SHUTDOWN_GRACE", d.shutdown_grace),
            max_queue_depth=i("JOBQ_MAX_QUEUE_DEPTH", d.max_queue_depth),
            promote_interval=f("JOBQ_PROMOTE_INTERVAL", d.promote_interval),
            reap_interval=f("JOBQ_REAP_INTERVAL", d.reap_interval),
            schedule_interval=f("JOBQ_SCHEDULE_INTERVAL", d.schedule_interval),
            reconcile_interval=f("JOBQ_RECONCILE_INTERVAL", d.reconcile_interval),
            reconcile_grace=f("JOBQ_RECONCILE_GRACE", d.reconcile_grace),
            metrics_port=i("JOBQ_METRICS_PORT", d.metrics_port),
        )
