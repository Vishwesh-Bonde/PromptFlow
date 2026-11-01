"""Generation orchestration and live instruction replacement.

Each task has one runtime object in this process. The runtime permits at most
one active generation worker and at most one pending restart/stop coordinator.
The short runtime lock protects only in-memory bookkeeping: database operations,
debounce waits, provider streaming, and cancellation cleanup all happen outside
it.

Version fencing is the durable safety net. A worker captures its generation
version and instruction sequence from SQLite; every checkpoint, commit, and
terminal write re-checks the fence. Replacement is therefore safe even if a
provider ignores cancellation for a while: the old worker cannot become current
again.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import aclosing
from dataclasses import dataclass, field
from time import monotonic

from app.config import Settings
from app.core.context_builder import build_generation_request
from app.core.errors import (
    InvalidTransitionError,
    StaleGenerationError,
    TaskStoppedError,
)
from app.core.event_bus import EventBus
from app.core.event_types import StreamEventType
from app.core.state import GenerationStatus, InstructionKind, TaskStatus
from app.database.repository import Repository
from app.services.ai_provider import AIProvider, ProviderError

logger = logging.getLogger(__name__)

DebounceWait = Callable[[float], Awaitable[None]]


@dataclass(slots=True)
class TaskRuntime:
    task_id: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    worker: asyncio.Task[None] | None = None
    worker_version: int | None = None
    desired_version: int | None = None
    instruction_seq: int = 0
    restart_task: asyncio.Task[None] | None = None
    stop_task: asyncio.Task[None] | None = None
    idle_event: asyncio.Event = field(default_factory=asyncio.Event)
    instruction_event: asyncio.Event = field(default_factory=asyncio.Event)
    debounce_started: asyncio.Event = field(default_factory=asyncio.Event)
    cancel_reason: str | None = None
    stopping: bool = False
    stop_requested: bool = False
    stop_finalized_version: int | None = None
    shutdown_requested: bool = False

    def __post_init__(self) -> None:
        self.idle_event.set()


class GenerationCoordinator:
    def __init__(
        self,
        *,
        repository: Repository,
        event_bus: EventBus,
        provider: AIProvider,
        settings: Settings,
        debounce_wait: DebounceWait | None = None,
    ) -> None:
        self._repository = repository
        self._event_bus = event_bus
        self._provider = provider
        self._settings = settings
        self._debounce_wait = debounce_wait or asyncio.sleep
        self._runtimes: dict[str, TaskRuntime] = {}
        self._closing = False

    # ------------------------------------------------------------------
    # Public scheduling API
    # ------------------------------------------------------------------
    async def schedule_initial(self, task_id: str, version: int) -> None:
        """Start the first generation without waiting for provider output."""
        if self._closing:
            raise RuntimeError("The generation coordinator is shutting down")

        runtime = self._runtime(task_id)
        async with runtime.lock:
            if runtime.worker is not None and not runtime.worker.done():
                raise RuntimeError(f"Task {task_id} already has an active worker")
            if runtime.desired_version is not None and runtime.desired_version != version:
                # An instruction won the race before the initial worker was
                # scheduled. The replacement supervisor will use the newer
                # version; starting v1 would be intentionally stale.
                self._ensure_restart_locked(runtime)
                return
            runtime.desired_version = version
            runtime.instruction_seq = 0
            runtime.stopping = False
            runtime.stop_requested = False
            runtime.stop_finalized_version = None
            runtime.shutdown_requested = False
            runtime.cancel_reason = None
            runtime.idle_event.clear()
            runtime.worker_version = version
            runtime.worker = asyncio.create_task(
                self._run_worker(runtime, version),
                name=f"generation-{task_id}-v{version}",
            )

    async def accept_instruction(
        self,
        *,
        task_id: str,
        new_version: int,
        instruction_seq: int,
        sequence_no: int,
        kind: InstructionKind,
        instruction_id: str,
    ) -> None:
        """Acknowledge a persisted instruction and request replacement.

        The durable instruction transaction has already committed. This method
        only updates process-local invalidation state, publishes live events,
        and schedules a single debounce/restart supervisor. It never waits for
        the old provider or the replacement generation.
        """
        if self._closing:
            raise RuntimeError("The generation coordinator is shutting down")

        runtime = self._runtime(task_id)
        async with runtime.lock:
            # A stop or a newer instruction may have been accepted locally
            # while this call was waiting for the runtime lock, even if this
            # instruction's database transaction committed first. Never let a
            # delayed acceptance revive a stopped task, roll the target
            # backwards, or clear a stop supervisor.
            superseded = (
                self._closing
                or runtime.shutdown_requested
                or runtime.stop_requested
                or (
                    runtime.desired_version is not None
                    and runtime.desired_version > new_version
                )
            )
            already_current = (
                runtime.desired_version == new_version
                and runtime.instruction_seq == instruction_seq
            )
            worker: asyncio.Task[None] | None = None
            if not superseded:
                if already_current:
                    # A restart supervisor may have refreshed this target from
                    # SQLite before this coordinator handoff ran. Never cancel
                    # the worker it already started, schedule a duplicate
                    # replacement, or awaken a target its own timeout already
                    # parked. A newer instruction or recovery handles that case.
                    pass
                else:
                    runtime.desired_version = new_version
                    runtime.instruction_seq = instruction_seq
                    runtime.stopping = False
                    runtime.stop_requested = False
                    runtime.instruction_event.set()
                    self._ensure_restart_locked(runtime)
                    worker = runtime.worker
                    runtime.idle_event.clear()

        self._publish(
            task_id,
            StreamEventType.INSTRUCTION_ACCEPTED,
            new_version,
            {
                "instruction_id": instruction_id,
                "sequence_no": sequence_no,
                "kind": kind.value,
            },
        )
        if superseded:
            return
        old_version = new_version - 1
        self._publish(
            task_id,
            StreamEventType.GENERATION_INVALIDATED,
            old_version,
            {"new_generation_version": new_version},
        )

        if worker is not None and not worker.done():
            async with runtime.lock:
                self._record_cancel_reason(runtime, "replaced")
            if worker.cancelling() == 0:
                worker.cancel()

    async def accept_stop(
        self,
        *,
        task_id: str,
        stop_version: int,
        invalidated_version: int | None,
    ) -> None:
        """Cancel work and schedule exactly one stop cleanup coordinator."""
        runtime = self._runtime(task_id)
        async with runtime.lock:
            runtime.stopping = True
            runtime.stop_requested = True
            runtime.desired_version = stop_version
            runtime.idle_event.clear()

            if runtime.restart_task is not None and not runtime.restart_task.done():
                runtime.restart_task.cancel()

            worker = runtime.worker
            if worker is not None and not worker.done():
                self._record_cancel_reason(runtime, "stopped")
                if worker.cancelling() == 0:
                    worker.cancel()

            if runtime.stop_task is None or runtime.stop_task.done():
                runtime.stop_task = asyncio.create_task(
                    self._stop_supervisor(
                        runtime,
                        stop_version=stop_version,
                        invalidated_version=invalidated_version,
                    ),
                    name=f"stop-{task_id}-v{stop_version}",
                )

        if invalidated_version is not None:
            self._publish(
                task_id,
                StreamEventType.GENERATION_INVALIDATED,
                invalidated_version,
                {
                    "new_generation_version": stop_version,
                    "reason": "manual_stop",
                },
            )

    async def wait_for_idle(
        self, task_id: str, *, timeout: float | None = None
    ) -> None:
        """Wait until no worker or coordinator is active for this task."""

        async def _wait() -> None:
            while True:
                runtime = self._runtimes.get(task_id)
                if runtime is None:
                    return
                async with runtime.lock:
                    if (
                        runtime.worker is None
                        and runtime.restart_task is None
                        and runtime.stop_task is None
                    ):
                        return
                    event = runtime.idle_event
                await event.wait()

        if timeout is None:
            await _wait()
        else:
            async with asyncio.timeout(timeout):
                await _wait()

    async def wait_until_debouncing(
        self, task_id: str, *, timeout: float | None = None
    ) -> None:
        """Test/observability hook: wait until the restart supervisor is waiting."""
        runtime = self._runtimes.get(task_id)
        if runtime is None:
            raise TimeoutError(f"Task {task_id} has no runtime")
        if timeout is None:
            await runtime.debounce_started.wait()
        else:
            await asyncio.wait_for(runtime.debounce_started.wait(), timeout=timeout)

    async def shutdown(self) -> None:
        """Cancel local workers, persist truthful state, and await cleanup."""
        if self._closing:
            return
        self._closing = True

        runtimes = tuple(self._runtimes.values())
        coordinators: list[asyncio.Task[None]] = []
        workers: list[asyncio.Task[None]] = []
        for runtime in runtimes:
            async with runtime.lock:
                runtime.shutdown_requested = True
                for task in (runtime.restart_task, runtime.stop_task):
                    if task is not None and not task.done():
                        task.cancel()
                        coordinators.append(task)
                worker = runtime.worker
                if worker is not None and not worker.done():
                    if runtime.cancel_reason is None:
                        runtime.cancel_reason = "shutdown"
                    if worker.cancelling() == 0:
                        worker.cancel()
                    workers.append(worker)

        pending = coordinators + workers
        if pending:
            try:
                async with asyncio.timeout(
                    self._settings.cancellation_timeout_seconds
                ):
                    await asyncio.gather(*pending, return_exceptions=True)
            except TimeoutError:
                logger.error(
                    "Timed out waiting for %d coordinator/worker task(s) to stop",
                    len(pending),
                )

        await self._reconcile_after_shutdown(runtimes)
        self._runtimes.clear()

    # ------------------------------------------------------------------
    # Runtime state
    # ------------------------------------------------------------------
    def _runtime(self, task_id: str) -> TaskRuntime:
        runtime = self._runtimes.get(task_id)
        if runtime is None:
            runtime = TaskRuntime(task_id=task_id)
            self._runtimes[task_id] = runtime
        return runtime

    def _ensure_restart_locked(self, runtime: TaskRuntime) -> None:
        if runtime.restart_task is not None and not runtime.restart_task.done():
            return
        runtime.restart_task = asyncio.create_task(
            self._restart_supervisor(runtime),
            name=f"restart-{runtime.task_id}",
        )

    def _set_idle_if_idle_locked(self, runtime: TaskRuntime) -> None:
        if (
            runtime.worker is None
            and runtime.restart_task is None
            and runtime.stop_task is None
        ):
            runtime.idle_event.set()

    def _ensure_current(self, runtime: TaskRuntime, version: int) -> None:
        """Raise the correct control-flow exception for an invalid attempt."""
        if runtime.shutdown_requested:
            raise asyncio.CancelledError
        if runtime.stop_requested or runtime.stopping:
            raise asyncio.CancelledError
        if runtime.worker_version != version:
            raise StaleGenerationError(runtime.task_id, version)
        if (
            runtime.desired_version is not None
            and runtime.desired_version != version
        ):
            raise StaleGenerationError(runtime.task_id, version)

    @staticmethod
    def _record_cancel_reason(runtime: TaskRuntime, reason: str) -> None:
        """Keep the strongest cleanup reason for terminal classification."""
        if runtime.cancel_reason == "shutdown":
            return
        if reason == "stopped":
            runtime.cancel_reason = "stopped"
        elif runtime.cancel_reason is None:
            runtime.cancel_reason = reason

    def _publish(
        self,
        task_id: str,
        event_type: StreamEventType,
        version: int,
        data: dict[str, object] | None = None,
    ) -> None:
        self._event_bus.publish(
            task_id,
            event_type,
            generation_version=version,
            data=data,
        )

    # ------------------------------------------------------------------
    # Replacement debounce / cleanup
    # ------------------------------------------------------------------
    async def _wait_until_quiet(self, runtime: TaskRuntime) -> bool:
        """Return True when the debounce window expired with no newer input."""
        while True:
            async with runtime.lock:
                if (
                    self._closing
                    or runtime.shutdown_requested
                    or runtime.stopping
                    or runtime.stop_requested
                ):
                    return False
                target = (runtime.desired_version, runtime.instruction_seq)
                runtime.instruction_event.clear()
                runtime.debounce_started.set()
                runtime.idle_event.clear()

            interrupted = await self._wait_debounce_or_instruction(runtime)

            async with runtime.lock:
                if (
                    self._closing
                    or runtime.shutdown_requested
                    or runtime.stopping
                    or runtime.stop_requested
                ):
                    return False
                current = (runtime.desired_version, runtime.instruction_seq)

            if interrupted or current != target:
                continue
            return True

    async def _wait_debounce_or_instruction(self, runtime: TaskRuntime) -> bool:
        seconds = self._settings.instruction_debounce_ms / 1_000
        if seconds <= 0:
            # Still yield once so accepted instructions queued by the event loop
            # can be observed before the replacement transaction begins.
            await asyncio.sleep(0)
            return runtime.instruction_event.is_set()

        debounce_task = asyncio.create_task(self._debounce_wait(seconds))
        instruction_task = asyncio.create_task(runtime.instruction_event.wait())
        try:
            done, _ = await asyncio.wait(
                {debounce_task, instruction_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if debounce_task in done and not debounce_task.cancelled():
                error = debounce_task.exception()
                if error is not None:
                    raise error
            return instruction_task in done
        finally:
            for task in (debounce_task, instruction_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                debounce_task, instruction_task, return_exceptions=True
            )

    async def _restart_supervisor(self, runtime: TaskRuntime) -> None:
        last_target: tuple[int, int] | None = None
        try:
            while True:
                if not await self._wait_until_quiet(runtime):
                    return

                async with runtime.lock:
                    if (
                        runtime.shutdown_requested
                        or runtime.stopping
                        or runtime.stop_requested
                    ):
                        return
                    target_version = runtime.desired_version
                    target_seq = runtime.instruction_seq
                    worker = runtime.worker
                if target_version is None:
                    return
                last_target = (target_version, target_seq)

                if worker is not None and not worker.done():
                    if not await self._wait_for_worker(
                        runtime, worker, reason="replaced"
                    ):
                        await self._mark_cancellation_timeout(
                            runtime, target_version
                        )
                        return
                elif worker is not None:
                    # Retrieve the result so a completed worker cannot leave an
                    # unobserved exception behind.
                    self._consume_worker_result(worker)

                state = await self._repository.get_task_state(
                    runtime.task_id, event_limit=1
                )
                if state is None:
                    return
                if (
                    state.task.stop_requested
                    or state.task.status in (TaskStatus.STOPPED, TaskStatus.CANCELLING)
                ):
                    return

                if (
                    state.task.generation_version != target_version
                    or state.task.instruction_seq != target_seq
                ):
                    async with runtime.lock:
                        runtime.desired_version = state.task.generation_version
                        runtime.instruction_seq = state.task.instruction_seq
                    continue

                await self._finish_latest_generating(
                    runtime.task_id,
                    below_version=target_version,
                    error_code="replaced_by_instruction",
                    error_message="A newer instruction replaced this attempt.",
                )

                previous = state.previous_generation
                try:
                    await self._repository.create_revision_generation(
                        runtime.task_id,
                        target_version,
                        base_instruction_seq=target_seq,
                    )
                except (
                    InvalidTransitionError,
                    StaleGenerationError,
                    TaskStoppedError,
                    ValueError,
                ) as exc:
                    logger.debug(
                        "Replacement for task %s v%s could not start yet: %s",
                        runtime.task_id,
                        target_version,
                        exc,
                    )
                    refreshed = await self._repository.get_task_state(
                        runtime.task_id, event_limit=1
                    )
                    if refreshed is None:
                        return
                    if refreshed.task.stop_requested or refreshed.task.status in (
                        TaskStatus.STOPPED,
                        TaskStatus.CANCELLING,
                    ):
                        return
                    if (
                        refreshed.task.generation_version == target_version
                        and refreshed.task.instruction_seq == target_seq
                    ):
                        # The database is in an unexpected state for this exact
                        # target. Leave it truthful instead of spinning.
                        await self._mark_cancellation_timeout(
                            runtime, target_version
                        )
                        return
                    async with runtime.lock:
                        runtime.desired_version = (
                            refreshed.task.generation_version
                        )
                        runtime.instruction_seq = refreshed.task.instruction_seq
                    last_target = (
                        refreshed.task.generation_version,
                        refreshed.task.instruction_seq,
                    )
                    continue

                self._publish(
                    runtime.task_id,
                    StreamEventType.CONTEXT_UPDATED,
                    target_version,
                    {
                        "instruction_seq": target_seq,
                        "instruction_count": target_seq,
                        "previous_generation_version": (
                            previous.version if previous is not None else None
                        ),
                        "previous_draft_chars": (
                            len(previous.draft_text) if previous is not None else 0
                        ),
                    },
                )
                self._publish(
                    runtime.task_id,
                    StreamEventType.GENERATION_RESTARTED,
                    target_version,
                    {
                        "base_instruction_seq": target_seq,
                        "previous_generation_version": (
                            previous.version if previous is not None else None
                        ),
                    },
                )

                if not await self._start_worker(runtime, target_version):
                    await self._finish_generation_if_generating(
                        runtime.task_id,
                        target_version,
                        error_code="replaced_before_start",
                        error_message="The replacement was superseded before it started.",
                    )
                    async with runtime.lock:
                        if runtime.stopping or runtime.shutdown_requested:
                            return
                    continue
                last_target = (target_version, target_seq)
                return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Replacement supervisor failed for task %s", runtime.task_id
            )
            await self._mark_cancellation_timeout(runtime, None)
        finally:
            async with runtime.lock:
                if runtime.restart_task is asyncio.current_task():
                    runtime.restart_task = None
                    # An instruction may have advanced the target after this
                    # supervisor captured one but before it was marked done -
                    # including during a cancellation timeout. accept_instruction
                    # saw a live supervisor and therefore did not create another
                    # one, so hand the unhandled target to a fresh supervisor.
                    if (
                        not self._closing
                        and not runtime.shutdown_requested
                        and not runtime.stopping
                        and not runtime.stop_requested
                        and runtime.desired_version is not None
                        and (
                            last_target is None
                            or runtime.desired_version != last_target[0]
                            or runtime.instruction_seq != last_target[1]
                        )
                    ):
                        self._ensure_restart_locked(runtime)
                    self._set_idle_if_idle_locked(runtime)

    async def _finalize_stop_once(
        self,
        runtime: TaskRuntime,
        *,
        stop_version: int,
        invalidated_version: int | None,
    ) -> None:
        """Claim STOPPED and publish its events at most once per stop version.

        Both the stop supervisor and a worker that finishes late after a
        cancellation timeout may race to finalize. SQLite makes the transition
        itself idempotent; the in-memory marker prevents duplicate terminal
        events reaching subscribers.
        """
        task = await self._repository.finalize_stop(
            runtime.task_id, stop_version
        )
        if task.status is not TaskStatus.STOPPED:
            return
        async with runtime.lock:
            if runtime.stop_finalized_version == stop_version:
                return
            runtime.stop_finalized_version = stop_version

        self._publish(
            runtime.task_id,
            StreamEventType.TASK_STOPPED,
            stop_version,
            {
                "status": TaskStatus.STOPPED.value,
                "stop_version": stop_version,
                "invalidated_generation_version": invalidated_version,
            },
        )
        self._publish(
            runtime.task_id,
            StreamEventType.STATUS,
            stop_version,
            {"status": TaskStatus.STOPPED.value},
        )

    async def _stop_supervisor(
        self,
        runtime: TaskRuntime,
        *,
        stop_version: int,
        invalidated_version: int | None,
    ) -> None:
        try:
            async with runtime.lock:
                worker = runtime.worker
            cleaned = True
            if worker is not None and not worker.done():
                cleaned = await self._wait_for_worker(
                    runtime, worker, reason="stopped"
                )
            elif worker is not None:
                self._consume_worker_result(worker)

            if not cleaned:
                self._publish(
                    runtime.task_id,
                    StreamEventType.ERROR,
                    stop_version,
                    {
                        "code": "cancellation_timeout",
                        "message": (
                            "The active generation did not stop before the "
                            "cancellation timeout; the task remains CANCELLING."
                        ),
                    },
                )
                return

            await self._finish_latest_generating(
                runtime.task_id,
                below_version=stop_version,
                error_code="manual_stop",
                error_message="The user stopped this generation.",
            )
            await self._finalize_stop_once(
                runtime,
                stop_version=stop_version,
                invalidated_version=invalidated_version,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Stop supervisor failed for task %s", runtime.task_id)
        finally:
            async with runtime.lock:
                if runtime.stop_task is asyncio.current_task():
                    runtime.stop_task = None
                    self._set_idle_if_idle_locked(runtime)

    async def _wait_for_worker(
        self,
        runtime: TaskRuntime,
        worker: asyncio.Task[None],
        *,
        reason: str,
    ) -> bool:
        async with runtime.lock:
            self._record_cancel_reason(runtime, reason)
        if not worker.done() and worker.cancelling() == 0:
            worker.cancel()

        done, _ = await asyncio.wait(
            {worker},
            timeout=self._settings.cancellation_timeout_seconds,
        )
        if worker not in done:
            return False
        self._consume_worker_result(worker)
        return True

    @staticmethod
    def _consume_worker_result(worker: asyncio.Task[None]) -> None:
        try:
            worker.result()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Generation worker exited with an error")

    async def _start_worker(self, runtime: TaskRuntime, version: int) -> bool:
        async with runtime.lock:
            if (
                self._closing
                or runtime.shutdown_requested
                or runtime.stopping
                or runtime.stop_requested
            ):
                return False
            if (
                runtime.desired_version is not None
                and runtime.desired_version != version
            ):
                return False
            if runtime.worker is not None and not runtime.worker.done():
                return False
            if runtime.worker is not None:
                self._consume_worker_result(runtime.worker)

            runtime.cancel_reason = None
            runtime.worker_version = version
            runtime.idle_event.clear()
            runtime.worker = asyncio.create_task(
                self._run_worker(runtime, version),
                name=f"generation-{runtime.task_id}-v{version}",
            )
            return True

    async def _mark_cancellation_timeout(
        self, runtime: TaskRuntime, version: int | None
    ) -> None:
        async with runtime.lock:
            runtime.stopping = True
        try:
            await self._repository.set_task_status(
                runtime.task_id, TaskStatus.CANCELLING
            )
        except Exception:
            logger.exception(
                "Could not mark task %s as CANCELLING after cleanup timeout",
                runtime.task_id,
            )
        event_version = version or runtime.worker_version or 1
        self._publish(
            runtime.task_id,
            StreamEventType.ERROR,
            event_version,
            {
                "code": "cancellation_timeout",
                "message": (
                    "The previous generation did not finish cleanup; no "
                    "replacement worker will be started."
                ),
            },
        )

    async def _finish_latest_generating(
        self,
        task_id: str,
        *,
        below_version: int,
        error_code: str,
        error_message: str,
    ) -> bool:
        generation = await self._repository.get_current_generation(task_id)
        if (
            generation is None
            or generation.version >= below_version
            or generation.status is not GenerationStatus.GENERATING
        ):
            return False
        return await self._finish_generation_if_generating(
            task_id,
            generation.version,
            error_code=error_code,
            error_message=error_message,
        )

    async def _finish_generation_if_generating(
        self,
        task_id: str,
        version: int,
        *,
        error_code: str,
        error_message: str,
    ) -> bool:
        generation = await self._repository.get_generation(task_id, version)
        if generation is None or generation.status is not GenerationStatus.GENERATING:
            return False
        try:
            updated = await self._repository.finish_generation(
                task_id,
                version,
                GenerationStatus.CANCELLED,
                draft_text=generation.draft_text,
                error_code=error_code,
                error_message=error_message,
            )
        except Exception:
            logger.exception(
                "Could not finish cancelled generation %s v%s", task_id, version
            )
            return False
        if updated:
            self._publish(
                task_id,
                StreamEventType.GENERATION_CANCELLED,
                version,
                {
                    "status": GenerationStatus.CANCELLED.value,
                    "error_code": error_code,
                    "draft_chars": len(generation.draft_text),
                },
            )
            if generation.draft_text:
                self._publish(
                    task_id,
                    StreamEventType.DRAFT_PRESERVED,
                    version,
                    {"draft_chars": len(generation.draft_text)},
                )
        return updated

    # ------------------------------------------------------------------
    # Worker
    # ------------------------------------------------------------------
    async def _run_worker(self, runtime: TaskRuntime, version: int) -> None:
        task_id = runtime.task_id
        buffer: list[str] = []
        chars_received = 0
        last_checkpoint = monotonic()

        try:
            state = await self._repository.get_task_state(task_id, event_limit=1)
            if state is None:
                return
            task = state.task
            if (
                task.generation_version != version
                or task.status not in (TaskStatus.GENERATING, TaskStatus.REVISING)
                or task.stop_requested
            ):
                return
            generation = state.generation
            if (
                generation is None
                or generation.version != version
                or generation.status is not GenerationStatus.GENERATING
            ):
                return

            self._publish(
                task_id,
                StreamEventType.GENERATION_STARTED,
                version,
                {
                    "status": generation.status.value,
                    "base_instruction_seq": generation.base_instruction_seq,
                },
            )

            instructions = tuple(
                instruction
                for instruction in state.instructions
                if instruction.sequence_no <= generation.base_instruction_seq
            )
            request = build_generation_request(
                task=task,
                instructions=instructions,
                previous_generation=state.previous_generation,
            )

            async with asyncio.timeout(self._settings.provider_timeout_seconds):
                async with aclosing(self._provider.stream(request)) as stream:
                    async for chunk in stream:
                        if not chunk:
                            continue
                        self._ensure_current(runtime, version)

                        buffer.append(chunk)
                        offset = chars_received
                        chars_received += len(chunk)
                        self._publish(
                            task_id,
                            StreamEventType.TOKEN_DELTA,
                            version,
                            {
                                "text": chunk,
                                "offset": offset,
                                "chars": len(chunk),
                            },
                        )

                        now = monotonic()
                        elapsed_ms = (now - last_checkpoint) * 1_000
                        if elapsed_ms >= self._settings.checkpoint_interval_ms:
                            draft = "".join(buffer)
                            saved = await self._repository.save_generation_draft(
                                task_id, version, draft
                            )
                            if not saved:
                                raise StaleGenerationError(task_id, version)
                            self._publish(
                                task_id,
                                StreamEventType.CHECKPOINT,
                                version,
                                {
                                    "draft_chars": len(draft),
                                    "checkpointed_at": now,
                                },
                            )
                            last_checkpoint = now

            answer = "".join(buffer)
            if not answer.strip():
                raise ProviderError(
                    "empty_output",
                    "The provider finished without producing an answer.",
                )
            self._ensure_current(runtime, version)

            committed = await self._repository.commit_generation(
                task_id, version, answer
            )
            if not committed:
                raise StaleGenerationError(task_id, version)

            self._publish(
                task_id,
                StreamEventType.GENERATION_COMPLETED,
                version,
                {"answer_chars": len(answer)},
            )
            self._publish(
                task_id,
                StreamEventType.STATUS,
                version,
                {"status": TaskStatus.COMPLETED.value},
            )
        except asyncio.CancelledError:
            await self._finish_cancelled(runtime, version, "".join(buffer))
            raise
        except TimeoutError:
            await self._fail(
                task_id,
                version,
                "".join(buffer),
                "provider_timeout",
                "The AI provider timed out.",
            )
        except ProviderError as exc:
            await self._fail(
                task_id,
                version,
                "".join(buffer),
                exc.code,
                exc.message,
            )
        except StaleGenerationError:
            await self._mark_stale(task_id, version, "".join(buffer))
        except Exception:
            logger.exception("Generation worker failed for task %s", task_id)
            await self._fail(
                task_id,
                version,
                "".join(buffer),
                "internal_error",
                "The generation failed unexpectedly.",
            )
        finally:
            pending_stop_version: int | None = None
            async with runtime.lock:
                if runtime.worker is asyncio.current_task():
                    runtime.worker = None
                    runtime.worker_version = None
                    runtime.cancel_reason = None
                    if runtime.stop_requested and runtime.desired_version is not None:
                        pending_stop_version = runtime.desired_version
                    else:
                        self._set_idle_if_idle_locked(runtime)
            # A stop supervisor may already have reported a cancellation timeout
            # and returned. Any terminal worker path (cancelled, failed, stale,
            # or interrupted) still finalizes the pending stop after its provider
            # cleanup and draft write are complete. Idle is not signalled until
            # that durable transition has been attempted.
            if pending_stop_version is not None:
                try:
                    await self._finalize_stop_once(
                        runtime,
                        stop_version=pending_stop_version,
                        invalidated_version=version,
                    )
                except Exception:
                    logger.exception(
                        "Could not finalize late stop for task %s", runtime.task_id
                    )
                finally:
                    async with runtime.lock:
                        self._set_idle_if_idle_locked(runtime)

    async def _fail(
        self,
        task_id: str,
        version: int,
        draft: str,
        error_code: str,
        error_message: str,
    ) -> None:
        try:
            updated = await self._repository.finish_generation(
                task_id,
                version,
                GenerationStatus.FAILED,
                draft_text=draft,
                error_code=error_code,
                error_message=error_message,
                task_status=TaskStatus.FAILED,
            )
        except Exception:
            logger.exception("Could not persist failure for task %s", task_id)
            return
        if updated:
            self._publish(
                task_id,
                StreamEventType.GENERATION_FAILED,
                version,
                {
                    "status": GenerationStatus.FAILED.value,
                    "error_code": error_code,
                    "error_message": error_message,
                    "draft_chars": len(draft),
                },
            )
            # A provider may raise after an instruction has already advanced
            # the desired version. In that case the generation row is marked
            # failed, but the task remains UPDATE_PENDING/REVISING for the
            # replacement. Do not publish a misleading current task status.
            try:
                task = await self._repository.get_task(task_id)
            except Exception:
                logger.exception("Could not read task %s after failure", task_id)
                task = None
            if (
                task is not None
                and task.generation_version == version
                and task.status is TaskStatus.FAILED
            ):
                self._publish(
                    task_id,
                    StreamEventType.STATUS,
                    version,
                    {"status": TaskStatus.FAILED.value},
                )

    async def _finish_cancelled(
        self, runtime: TaskRuntime, version: int, draft: str
    ) -> None:
        """Persist the latest partial draft and a truthful cancellation status."""
        reason = runtime.cancel_reason or "cancelled"
        if reason == "shutdown":
            status = GenerationStatus.INTERRUPTED
            task_status = TaskStatus.INTERRUPTED
            error_code = "server_shutdown"
            error_message = "The server stopped before this generation finished."
        else:
            status = GenerationStatus.CANCELLED
            task_status = None
            if reason == "stopped":
                error_code = "manual_stop"
                error_message = "The user stopped this generation."
            else:
                error_code = "replaced_by_instruction"
                error_message = "A newer instruction replaced this attempt."

        try:
            updated = await self._repository.finish_generation(
                runtime.task_id,
                version,
                status,
                draft_text=draft,
                error_code=error_code,
                error_message=error_message,
                task_status=task_status,
            )
        except Exception:
            logger.exception(
                "Could not persist cancellation for task %s v%s",
                runtime.task_id,
                version,
            )
            return
        if updated:
            self._publish(
                runtime.task_id,
                StreamEventType.GENERATION_CANCELLED,
                version,
                {
                    "status": status.value,
                    "error_code": error_code,
                    "error_message": error_message,
                    "draft_chars": len(draft),
                },
            )
            if draft:
                self._publish(
                    runtime.task_id,
                    StreamEventType.DRAFT_PRESERVED,
                    version,
                    {"draft_chars": len(draft)},
                )
            if reason == "shutdown":
                try:
                    task = await self._repository.get_task(runtime.task_id)
                except Exception:
                    logger.exception(
                        "Could not read task %s after shutdown cancellation",
                        runtime.task_id,
                    )
                    task = None
                if (
                    task is not None
                    and task.generation_version == version
                    and task.status is TaskStatus.INTERRUPTED
                ):
                    self._publish(
                        runtime.task_id,
                        StreamEventType.STATUS,
                        version,
                        {"status": TaskStatus.INTERRUPTED.value},
                    )

    async def _mark_stale(self, task_id: str, version: int, draft: str) -> None:
        try:
            updated = await self._repository.finish_generation(
                task_id,
                version,
                GenerationStatus.STALE,
                draft_text=draft,
                error_code="stale_generation",
                error_message="A newer generation replaced this attempt.",
            )
        except Exception:
            logger.exception("Could not persist stale state for task %s", task_id)
            return
        if updated:
            self._publish(
                task_id,
                StreamEventType.GENERATION_CANCELLED,
                version,
                {
                    "status": GenerationStatus.STALE.value,
                    "error_code": "stale_generation",
                    "error_message": "A newer generation replaced this attempt.",
                    "draft_chars": len(draft),
                },
            )
            if draft:
                self._publish(
                    task_id,
                    StreamEventType.DRAFT_PRESERVED,
                    version,
                    {"draft_chars": len(draft)},
                )

    # ------------------------------------------------------------------
    # Shutdown reconciliation
    # ------------------------------------------------------------------
    async def _reconcile_after_shutdown(
        self, runtimes: tuple[TaskRuntime, ...]
    ) -> None:
        for runtime in runtimes:
            if runtime.worker is not None and not runtime.worker.done():
                logger.warning(
                    "Task %s still has a running worker during shutdown",
                    runtime.task_id,
                )
                continue
            try:
                task = await self._repository.get_task(runtime.task_id)
                generation = await self._repository.get_current_generation(
                    runtime.task_id
                )
                if task is None or generation is None:
                    continue
                if generation.status is not GenerationStatus.GENERATING:
                    continue

                if task.stop_requested:
                    await self._repository.finish_generation(
                        runtime.task_id,
                        generation.version,
                        GenerationStatus.CANCELLED,
                        draft_text=generation.draft_text,
                        error_code="server_shutdown",
                        error_message=(
                            "The server stopped before this generation finished."
                        ),
                    )
                    await self._repository.finalize_stop(
                        runtime.task_id, task.generation_version
                    )
                else:
                    await self._repository.finish_generation(
                        runtime.task_id,
                        generation.version,
                        GenerationStatus.INTERRUPTED,
                        draft_text=generation.draft_text,
                        error_code="server_shutdown",
                        error_message=(
                            "The server stopped before this generation finished."
                        ),
                        task_status=(
                            TaskStatus.INTERRUPTED
                            if task.generation_version == generation.version
                            else None
                        ),
                    )
            except Exception:
                logger.exception(
                    "Could not reconcile shutdown state for task %s",
                    runtime.task_id,
                )
