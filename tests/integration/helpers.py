from __future__ import annotations

import time
import uuid

from jobq.client import Job, JobqClient
from jobq.reaper import Reaper
from jobq.worker import Worker


async def drain(
    worker: Worker, reaper: Reaper, client: JobqClient, job_id: uuid.UUID, timeout: float = 10
) -> Job:
    """Run worker + promotion until the job is terminal."""
    deadline = time.monotonic() + timeout
    while True:
        await reaper.promote_once()
        await worker.run_once(timeout=0.05)
        job = client.get(job_id)
        if job.is_terminal:
            return job
        if time.monotonic() > deadline:
            raise TimeoutError(f"job still {job.status}")
