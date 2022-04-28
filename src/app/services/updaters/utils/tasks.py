from typing import Optional

from arq.connections import ArqRedis
from arq.jobs import Job, JobStatus


async def is_jobs_complete(
    arq_pool: ArqRedis, job_ids: list[str], prefix: Optional[str] = None
) -> tuple[bool, int]:
    job_statuses = []

    for job_id in job_ids:
        _job_id = f"{prefix}_{job_id}" if prefix else job_id
        status = await Job(
            _job_id, arq_pool, arq_pool.default_queue_name, arq_pool.job_deserializer
        ).status()
        job_statuses.append(status.value)

    not_complete_count = 0
    for status in (
        JobStatus.not_found.value,
        JobStatus.deferred.value,
        JobStatus.in_progress.value,
        JobStatus.queued.value,
    ):
        if status in job_statuses:
            not_complete_count += 1

    return not_complete_count == 0, not_complete_count
