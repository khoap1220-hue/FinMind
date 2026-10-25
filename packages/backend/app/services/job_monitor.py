import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import uuid4

from ..observability import track_background_job

logger = logging.getLogger("finmind.jobs")


@dataclass
class JobRetryPolicy:
    max_retries: int = 3
    base_delay_seconds: float = 0.01
    backoff_factor: float = 2.0
    max_delay_seconds: float = 1.0

    def delay_for_attempt(self, attempt: int) -> float:
        if attempt <= 0:
            return 0.0
        raw = self.base_delay_seconds * (self.backoff_factor ** (attempt - 1))
        return float(min(raw, self.max_delay_seconds))


@dataclass
class JobExecutionRecord:
    job_id: str
    job_name: str
    status: str
    attempts: int
    retries: int
    duration_seconds: float
    created_at: str
    completed_at: str
    last_error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "status": self.status,
            "attempts": self.attempts,
            "retries": self.retries,
            "duration_seconds": round(self.duration_seconds, 6),
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "last_error": self.last_error,
            "metadata": self.metadata,
        }


class BackgroundJobMonitor:
    def __init__(self, max_history: int = 200) -> None:
        self.max_history = max_history
        self._history: list[JobExecutionRecord] = []
        self._dead_letter: dict[str, dict[str, Any]] = {}
        self._counters: dict[str, int] = {
            "total_runs": 0,
            "succeeded": 0,
            "retried": 0,
            "failed": 0,
        }

    def reset(self) -> None:
        self._history.clear()
        self._dead_letter.clear()
        for key in self._counters:
            self._counters[key] = 0

    def execute(
        self,
        job_name: str,
        func: Callable[..., Any],
        *args: Any,
        policy: JobRetryPolicy | None = None,
        metadata: dict[str, Any] | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        **kwargs: Any,
    ) -> tuple[bool, Any, JobExecutionRecord]:
        retry_policy = policy or JobRetryPolicy()
        job_id = uuid4().hex
        created_at = datetime.now(timezone.utc).isoformat()
        start_time = time.perf_counter()

        attempts = 0
        last_error: str | None = None
        result: Any = None
        succeeded = False
        max_attempts = max(1, retry_policy.max_retries + 1)

        while attempts < max_attempts:
            attempts += 1
            try:
                result = func(*args, **kwargs)
                if result is False:
                    raise RuntimeError(f"Job '{job_name}' returned False")
                succeeded = True
                last_error = None
                break
            except Exception as exc:
                last_error = str(exc) or exc.__class__.__name__
                logger.warning(
                    "Background job failed job=%s id=%s attempt=%s/%s error=%s",
                    job_name,
                    job_id,
                    attempts,
                    max_attempts,
                    last_error,
                )
                if attempts < max_attempts:
                    delay = retry_policy.delay_for_attempt(attempts)
                    if delay > 0:
                        sleep_fn(delay)

        duration = time.perf_counter() - start_time
        retries_used = max(0, attempts - 1)
        status = "succeeded" if succeeded else "failed"
        completed_at = datetime.now(timezone.utc).isoformat()

        record = JobExecutionRecord(
            job_id=job_id,
            job_name=job_name,
            status=status,
            attempts=attempts,
            retries=retries_used,
            duration_seconds=duration,
            created_at=created_at,
            completed_at=completed_at,
            last_error=last_error,
            metadata=dict(metadata or {}),
        )
        self._record_execution(
            record, func, args, kwargs, retry_policy, sleep_fn
        )
        track_background_job(
            job_name=job_name,
            status=status,
            retries=retries_used,
            duration_seconds=duration,
        )
        return succeeded, result, record

    def _record_execution(
        self,
        record: JobExecutionRecord,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        policy: JobRetryPolicy,
        sleep_fn: Callable[[float], None],
    ) -> None:
        self._counters["total_runs"] += 1
        if record.retries > 0:
            self._counters["retried"] += record.retries
        if record.status == "succeeded":
            self._counters["succeeded"] += 1
        else:
            self._counters["failed"] += 1
            self._dead_letter[record.job_id] = {
                "record": record,
                "func": func,
                "args": args,
                "kwargs": kwargs,
                "policy": policy,
                "sleep_fn": sleep_fn,
            }

        self._history.append(record)
        if len(self._history) > self.max_history:
            self._history = self._history[-self.max_history :]

    def retry_dead_letter(
        self, job_id: str
    ) -> tuple[bool, JobExecutionRecord | None]:
        entry = self._dead_letter.get(job_id)
        if not entry:
            return False, None

        old_record: JobExecutionRecord = entry["record"]
        ok, _res, new_record = self.execute(
            old_record.job_name,
            entry["func"],
            *entry["args"],
            policy=entry["policy"],
            metadata=old_record.metadata,
            sleep_fn=entry["sleep_fn"],
            **entry["kwargs"],
        )
        if ok:
            self._dead_letter.pop(job_id, None)
        return ok, new_record

    def summary(self) -> dict[str, Any]:
        return {
            "stats": {
                **self._counters,
                "dead_letter_count": len(self._dead_letter),
            },
            "recent_jobs": [
                r.to_dict() for r in reversed(self._history[-25:])
            ],
        }

    def list_dead_letters(self) -> list[dict[str, Any]]:
        return [
            entry["record"].to_dict() for entry in self._dead_letter.values()
        ]


job_monitor = BackgroundJobMonitor()
