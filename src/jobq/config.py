"""Runtime settings, read from environment variables (12-factor style)."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field


def _default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


@dataclass(frozen=True)
class Settings:
    database_url: str = "postgresql+asyncpg://jobq:jobq@localhost:5432/jobq"
    redis_url: str = "redis://localhost:6379/0"
    # All Redis keys live under this prefix, so tests (or several deployments)
    # can share one Redis without colliding.
    redis_prefix: str = "jobq"
    worker_id: str = field(default_factory=_default_worker_id)
    # How long a worker blocks on Redis waiting for work before looping again.
    poll_timeout: float = 1.0
    # Extra modules the worker imports at startup so they can register handlers
    # (e.g. Project 3's billing handlers): JOBQ_HANDLER_MODULES="billing.jobs,foo.bar"
    handler_modules: tuple[str, ...] = ()
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ
        modules = tuple(
            m.strip() for m in env.get("JOBQ_HANDLER_MODULES", "").split(",") if m.strip()
        )
        return cls(
            database_url=env.get("JOBQ_DATABASE_URL", cls.database_url),
            redis_url=env.get("JOBQ_REDIS_URL", cls.redis_url),
            redis_prefix=env.get("JOBQ_REDIS_PREFIX", cls.redis_prefix),
            worker_id=env.get("JOBQ_WORKER_ID") or _default_worker_id(),
            poll_timeout=float(env.get("JOBQ_POLL_TIMEOUT", cls.poll_timeout)),
            handler_modules=modules,
            log_level=env.get("JOBQ_LOG_LEVEL", cls.log_level),
        )
