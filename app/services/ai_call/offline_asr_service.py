from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallDialogueSegmentModel, AiCallRecordingTrackModel
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.oss.crud import OssCRUD
from app.core.logger import log
from app.services.ai_call.dialogue_merge import (
    CUSTOMER_SPEAKER_TYPE,
    OFFLINE_ASR_SOURCE,
    QWEN_REALTIME_SOURCE,
    is_duplicate_dialogue_segment,
)

HUMAN_AGENT_SPEAKER_TYPE = "human_agent"
OFFLINE_ASR_TRACK_ROLES = frozenset({CUSTOMER_SPEAKER_TYPE, HUMAN_AGENT_SPEAKER_TYPE})


@dataclass(frozen=True, slots=True)
class OfflineAsrSegment:
    text: str
    begin_time_ms: int | None = None
    end_time_ms: int | None = None


@dataclass(frozen=True, slots=True)
class OfflineAsrResult:
    task_id: str | None
    transcription_url: str | None
    segments: list[OfflineAsrSegment]


class OfflineAsrProviderProtocol(Protocol):
    provider_name: str
    model_name: str

    async def transcribe(self, *, audio_url: str) -> OfflineAsrResult: ...


class DashScopeParaformerAsrProvider:
    """DashScope Paraformer 录音文件识别适配器。"""

    provider_name = "dashscope_paraformer"
    submit_url = "https://dashscope.aliyuncs.com/api/v1/services/audio/asr/transcription"
    task_url_template = "https://dashscope.aliyuncs.com/api/v1/tasks/{task_id}"
    success_statuses = frozenset({"SUCCEEDED", "SUCCESS", "COMPLETED"})
    failed_statuses = frozenset({"FAILED", "CANCELED", "CANCELLED"})

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        language_hints: Sequence[str] | None = None,
        timeout_seconds: float = 300.0,
        poll_interval_seconds: float = 2.0,
    ) -> None:
        self.api_key = api_key
        self.model_name = model.strip() or "paraformer-v2"
        self.language_hints = [hint for hint in (language_hints or []) if hint]
        self.timeout_seconds = max(5.0, timeout_seconds)
        self.poll_interval_seconds = max(0.5, poll_interval_seconds)

    async def transcribe(self, *, audio_url: str) -> OfflineAsrResult:
        if not self.api_key:
            raise ValueError("ASR API key 未配置")
        if not audio_url:
            raise ValueError("ASR 输入音频 URL 为空")
        task_id = await self._submit(audio_url)
        task_data = await self._wait_for_task(task_id)
        transcription_url = self._transcription_url(task_data)
        if not transcription_url:
            raise ValueError("ASR 任务完成但未返回 transcription_url")
        result_data = await self._fetch_transcription_result(transcription_url)
        return OfflineAsrResult(
            task_id=task_id,
            transcription_url=transcription_url,
            segments=self._parse_segments(result_data),
        )

    async def _submit(self, audio_url: str) -> str:
        async with httpx.AsyncClient(timeout=min(30.0, self.timeout_seconds)) as client:
            response = await client.post(
                self.submit_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "X-DashScope-Async": "enable",
                },
                json=self._submit_payload(audio_url),
            )
        response.raise_for_status()
        data = response.json()
        task_id = self._task_id(data)
        if not task_id:
            raise ValueError("ASR 提交响应缺少 task_id")
        return task_id

    def _submit_payload(self, audio_url: str) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": self.model_name,
            "input": {"file_urls": [audio_url]},
        }
        if self.language_hints:
            payload["parameters"] = {"language_hints": self.language_hints}
        return payload

    async def _wait_for_task(self, task_id: str) -> dict[str, Any]:
        deadline = time.monotonic() + self.timeout_seconds
        last_data: dict[str, Any] = {}
        while time.monotonic() < deadline:
            async with httpx.AsyncClient(timeout=min(30.0, self.timeout_seconds)) as client:
                response = await client.get(
                    self.task_url_template.format(task_id=task_id),
                    headers={"Authorization": f"Bearer {self.api_key}"},
                )
            response.raise_for_status()
            data = response.json()
            last_data = data if isinstance(data, dict) else {}
            status = self._task_status(last_data)
            if status in self.success_statuses:
                return last_data
            if status in self.failed_statuses:
                raise RuntimeError(self._failure_message(last_data) or f"ASR 任务失败: {status}")
            await asyncio.sleep(self.poll_interval_seconds)
        status = self._task_status(last_data) or "timeout"
        raise TimeoutError(f"ASR 任务轮询超时: taskId={task_id}, status={status}")

    async def _fetch_transcription_result(self, transcription_url: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=min(30.0, self.timeout_seconds)) as client:
            response = await client.get(transcription_url)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, dict) else {}

    @classmethod
    def _task_id(cls, data: dict[str, Any]) -> str | None:
        output = cls._output(data)
        value = output.get("task_id") or data.get("task_id")
        return str(value) if value else None

    @classmethod
    def _task_status(cls, data: dict[str, Any]) -> str | None:
        output = cls._output(data)
        value = output.get("task_status") or data.get("task_status")
        return str(value).upper() if value else None

    @classmethod
    def _transcription_url(cls, data: dict[str, Any]) -> str | None:
        output = cls._output(data)
        result = output.get("result")
        if isinstance(result, dict):
            value = result.get("transcription_url")
            if value:
                return str(value)
        results = output.get("results")
        if isinstance(results, list):
            for item in results:
                if not isinstance(item, dict):
                    continue
                value = item.get("transcription_url") or item.get("url")
                if value:
                    return str(value)
        value = output.get("transcription_url") or data.get("transcription_url")
        return str(value) if value else None

    @classmethod
    def _failure_message(cls, data: dict[str, Any]) -> str | None:
        output = cls._output(data)
        value = (
            output.get("message") or output.get("code") or data.get("message") or data.get("code")
        )
        return str(value) if value else None

    @staticmethod
    def _output(data: dict[str, Any]) -> dict[str, Any]:
        output = data.get("output")
        return output if isinstance(output, dict) else {}

    @classmethod
    def _parse_segments(cls, data: dict[str, Any]) -> list[OfflineAsrSegment]:
        segments: list[OfflineAsrSegment] = []
        for container in cls._transcript_containers(data):
            raw_sentences = container.get("sentences")
            if not isinstance(raw_sentences, list):
                continue
            for sentence in raw_sentences:
                if not isinstance(sentence, dict):
                    continue
                text = cls._text_value(sentence)
                if not text:
                    continue
                segments.append(
                    OfflineAsrSegment(
                        text=text,
                        begin_time_ms=cls._millis_value(
                            sentence.get("begin_time")
                            or sentence.get("start_time")
                            or sentence.get("beginTime")
                            or sentence.get("startTime")
                        ),
                        end_time_ms=cls._millis_value(
                            sentence.get("end_time") or sentence.get("endTime")
                        ),
                    )
                )
        if segments:
            return segments
        text = cls._text_value(data)
        if text:
            return [OfflineAsrSegment(text=text)]
        for container in cls._transcript_containers(data):
            text = cls._text_value(container)
            if text:
                return [OfflineAsrSegment(text=text)]
        return []

    @staticmethod
    def _transcript_containers(data: dict[str, Any]) -> list[dict[str, Any]]:
        containers = [data]
        for key in ("transcripts", "results", "channels"):
            value = data.get(key)
            if isinstance(value, list):
                containers.extend(item for item in value if isinstance(item, dict))
        return containers

    @staticmethod
    def _text_value(data: dict[str, Any]) -> str:
        value = data.get("text") or data.get("transcript") or data.get("content")
        return str(value).strip() if value else ""

    @staticmethod
    def _millis_value(value: Any) -> int | None:
        if value is None or value == "":
            return None
        try:
            return max(0, int(float(value)))
        except (TypeError, ValueError):
            return None


class DashScopeQwenFileTranscriptionAsrProvider(DashScopeParaformerAsrProvider):
    """DashScope Qwen3 录音文件识别适配器。"""

    provider_name = "dashscope_qwen_filetrans"

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        language_hints: Sequence[str] | None = None,
        timeout_seconds: float = 300.0,
        poll_interval_seconds: float = 2.0,
    ) -> None:
        super().__init__(
            api_key=api_key,
            model=model.strip() or "qwen3-asr-flash-filetrans",
            language_hints=language_hints,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )

    def _submit_payload(self, audio_url: str) -> dict[str, Any]:
        parameters: dict[str, Any] = {
            "channel_id": [0],
            "enable_itn": True,
            "enable_words": True,
        }
        if self.language_hints:
            parameters["language"] = self.language_hints[0]
        return {
            "model": self.model_name,
            "input": {"file_url": audio_url},
            "parameters": parameters,
        }


def build_dashscope_offline_asr_provider(
    *,
    provider_name: str,
    api_key: str,
    model: str,
    language_hints: Sequence[str] | None = None,
    timeout_seconds: float = 300.0,
    poll_interval_seconds: float = 2.0,
) -> OfflineAsrProviderProtocol:
    provider_types = {
        DashScopeParaformerAsrProvider.provider_name: DashScopeParaformerAsrProvider,
        DashScopeQwenFileTranscriptionAsrProvider.provider_name: (
            DashScopeQwenFileTranscriptionAsrProvider
        ),
    }
    try:
        provider_type = provider_types[provider_name]
    except KeyError as exc:
        raise ValueError(f"不支持的离线 ASR provider: {provider_name}") from exc
    return provider_type(
        api_key=api_key,
        model=model,
        language_hints=language_hints,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )


@dataclass(slots=True)
class _PreparedAsrSegment:
    job_id: int
    track_id: int
    track_role: str
    participant_identity: str
    track_started_at: datetime | None
    index: int
    segment: OfflineAsrSegment


class AiCallOfflineAsrService:
    """把分轨录音异步转写为 ai_call_dialogue_segment。"""

    def __init__(
        self,
        repository: AiCallRecordRepository,
        *,
        provider: OfflineAsrProviderProtocol,
        enabled: bool = True,
        commit_between_steps: bool = False,
    ) -> None:
        self.repository = repository
        self.provider = provider
        self.enabled = enabled
        self.commit_between_steps = commit_between_steps

    async def process_call(self, call_id: str) -> dict[str, int]:
        if not self.enabled:
            return {"jobs": 0, "segments": 0, "skipped": 0, "failed": 0}
        tracks = await self.repository.list_recording_tracks(call_id)
        prepared_segments: list[_PreparedAsrSegment] = []
        stats = {"jobs": 0, "segments": 0, "skipped": 0, "failed": 0}
        completed_job_ids: set[int] = set()
        for track in tracks:
            result = await self._transcribe_track(track)
            if result is None:
                stats["skipped"] += 1
                continue
            job_id, segments = result
            stats["jobs"] += 1
            completed_job_ids.add(job_id)
            prepared_segments.extend(segments)
        counts = await self._persist_segments(call_id, prepared_segments)
        for job_id in completed_job_ids:
            segment_count = counts.get(job_id, 0)
            await self.repository.update_asr_job(
                job_id,
                status="completed",
                completed_at=self._utc_now(),
                segment_count=segment_count,
                failure_stage=None,
                failure_message=None,
            )
            stats["segments"] += segment_count
        await self._checkpoint()
        return stats

    async def _transcribe_track(
        self,
        track: AiCallRecordingTrackModel,
    ) -> tuple[int, list[_PreparedAsrSegment]] | None:
        if track.track_role not in OFFLINE_ASR_TRACK_ROLES:
            return None
        if track.status != "completed" or track.oss_id is None:
            return None
        source_url = await self._play_url(track.oss_id)
        job = await self.repository.create_asr_job(
            call_id=track.call_id,
            track_id=track.id,
            track_role=track.track_role,
            participant_identity=track.participant_identity,
            provider=self.provider.provider_name,
            model=self.provider.model_name,
            status="pending",
            source_url=source_url,
            submitted_at=None,
        )
        if job.status == "completed":
            return None
        if not source_url:
            await self.repository.update_asr_job(
                job.id,
                status="failed",
                completed_at=self._utc_now(),
                failure_stage="oss_url",
                failure_message="分轨录音未生成可访问播放URL",
            )
            await self._checkpoint()
            return None
        try:
            submitted_at = self._utc_now()
            await self.repository.update_asr_job(
                job.id,
                status="running",
                source_url=source_url,
                submitted_at=submitted_at,
                completed_at=None,
                failure_stage=None,
                failure_message=None,
            )
            await self._checkpoint()
            result = await self.provider.transcribe(audio_url=source_url)
        except Exception as exc:
            log.warning(
                "AI Call 离线 ASR 转写失败: callId={}, trackRole={}, trackId={}, "
                "errorType={}, message={}",
                track.call_id,
                track.track_role,
                track.id,
                type(exc).__name__,
                str(exc),
            )
            await self.repository.update_asr_job(
                job.id,
                status="failed",
                completed_at=self._utc_now(),
                failure_stage="asr_transcribe",
                failure_message=self._short_message(exc),
            )
            await self._checkpoint()
            return None
        await self.repository.update_asr_job(
            job.id,
            status="running",
            task_id=result.task_id,
            transcription_url=result.transcription_url,
        )
        await self._checkpoint()
        prepared = [
            _PreparedAsrSegment(
                job_id=job.id,
                track_id=track.id,
                track_role=track.track_role,
                participant_identity=track.participant_identity,
                track_started_at=track.started_at,
                index=index,
                segment=segment,
            )
            for index, segment in enumerate(result.segments, start=1)
            if segment.text.strip()
        ]
        return job.id, prepared

    async def _persist_segments(
        self,
        call_id: str,
        segments: list[_PreparedAsrSegment],
    ) -> dict[int, int]:
        counts: dict[int, int] = {}
        next_segment_no = await self.repository.next_dialogue_segment_no(call_id)
        existing_segments = await self.repository.list_dialogue_segments(call_id)
        for prepared in sorted(segments, key=self._segment_sort_key):
            segment = prepared.segment
            text = segment.text.strip()
            if not text:
                continue
            started_at = self._absolute_time(prepared.track_started_at, segment.begin_time_ms)
            ended_at = self._absolute_time(prepared.track_started_at, segment.end_time_ms)
            if self._matches_existing_realtime_segment(
                existing_segments,
                speaker_type=prepared.track_role,
                text=text,
                started_at=started_at,
                ended_at=ended_at,
            ):
                continue
            persisted = await self.repository.upsert_dialogue_segment(
                call_id=call_id,
                segment_no=next_segment_no,
                speaker_type=prepared.track_role,
                speaker_identity=prepared.participant_identity,
                source=OFFLINE_ASR_SOURCE,
                source_segment_id=f"track_{prepared.track_id}_{prepared.index}",
                segment_text=text,
                segment_status="final",
                started_at=started_at,
                ended_at=ended_at,
                duration_ms=self._duration_ms(segment.begin_time_ms, segment.end_time_ms),
                audio_start_ms=segment.begin_time_ms,
                audio_end_ms=segment.end_time_ms,
            )
            existing_segments.append(persisted)
            counts[prepared.job_id] = counts.get(prepared.job_id, 0) + 1
            next_segment_no += 1
        return counts

    @staticmethod
    def _matches_existing_realtime_segment(
        existing_segments: list[AiCallDialogueSegmentModel],
        *,
        speaker_type: str,
        text: str,
        started_at: datetime | None,
        ended_at: datetime | None,
    ) -> bool:
        for existing in existing_segments:
            if existing.source != QWEN_REALTIME_SOURCE:
                continue
            if is_duplicate_dialogue_segment(
                speaker_type=speaker_type,
                text=text,
                started_at=started_at,
                ended_at=ended_at,
                candidate_speaker_type=existing.speaker_type,
                candidate_text=existing.segment_text,
                candidate_started_at=existing.started_at,
                candidate_ended_at=existing.ended_at,
            ):
                return True
        return False

    async def _play_url(self, oss_id: int) -> str | None:
        auth = AuthSchema(user=None, check_data_scope=False, db=self.repository.db)
        row = await OssCRUD(auth).get_url_by_oss_id_crud(oss_id=oss_id)
        if not row:
            return None
        url = row.get("url")
        return str(url) if url else None

    async def _checkpoint(self) -> None:
        if self.commit_between_steps:
            await self.repository.db.commit()
            return
        await self.repository.db.flush()

    @staticmethod
    def _segment_sort_key(prepared: _PreparedAsrSegment) -> tuple[datetime, int, int]:
        absolute = AiCallOfflineAsrService._absolute_time(
            prepared.track_started_at,
            prepared.segment.begin_time_ms,
        )
        return (
            absolute or datetime.max.replace(tzinfo=timezone.utc),
            prepared.track_id,
            prepared.index,
        )

    @staticmethod
    def _absolute_time(started_at: datetime | None, offset_ms: int | None) -> datetime | None:
        if started_at is None or offset_ms is None:
            return None
        base = started_at if started_at.tzinfo else started_at.replace(tzinfo=timezone.utc)
        return base + timedelta(milliseconds=offset_ms)

    @staticmethod
    def _duration_ms(start_ms: int | None, end_ms: int | None) -> int | None:
        if start_ms is None or end_ms is None:
            return None
        return max(0, end_ms - start_ms)

    @staticmethod
    def _short_message(exc: Exception) -> str:
        message = str(exc) or type(exc).__name__
        return message[:500]

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)


class AiCallOfflineAsrWorker:
    """录音停止后异步执行离线 ASR。"""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        provider: OfflineAsrProviderProtocol,
        enabled: bool = True,
        queue_max_size: int = 1000,
        on_call_ready_for_semantic_analysis: Callable[[str], None] | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.provider = provider
        self.enabled = enabled
        self.on_call_ready_for_semantic_analysis = on_call_ready_for_semantic_analysis
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=max(1, queue_max_size))
        self.dropped_count = 0
        self.processed_count = 0
        self.failed_count = 0
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._queued_or_running: set[str] = set()

    async def start(self) -> None:
        if not self.enabled:
            return
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="ai-call-offline-asr-worker")

    async def stop(self, timeout_seconds: float = 3.0) -> None:
        self._stopping.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self.queue.join(), timeout=timeout_seconds)
        except TimeoutError:
            log.warning("AI Call 离线 ASR 队列关闭超时，仍有任务未完成")
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    def enqueue(self, call_id: str) -> None:
        if not self.enabled or not call_id:
            return
        if call_id in self._queued_or_running:
            return
        try:
            self.queue.put_nowait(call_id)
            self._queued_or_running.add(call_id)
        except asyncio.QueueFull:
            self.dropped_count += 1
            log.warning("AI Call 离线 ASR 队列已满，丢弃任务: callId={}", call_id)

    async def flush_pending(self) -> None:
        await self.queue.join()

    async def _run(self) -> None:
        while not self._stopping.is_set() or not self.queue.empty():
            try:
                call_id = await asyncio.wait_for(self.queue.get(), timeout=0.2)
            except TimeoutError:
                continue
            try:
                await self._process_call(call_id)
                self.processed_count += 1
            except Exception as exc:
                self.failed_count += 1
                log.warning(
                    "AI Call 离线 ASR 任务处理失败: callId={}, errorType={}, message={}",
                    call_id,
                    type(exc).__name__,
                    str(exc),
                )
            finally:
                self._queued_or_running.discard(call_id)
                self.queue.task_done()

    async def _process_call(self, call_id: str) -> None:
        async with self.session_factory() as db:
            repository = AiCallRecordRepository(db)
            service = AiCallOfflineAsrService(
                repository,
                provider=self.provider,
                enabled=self.enabled,
                commit_between_steps=True,
            )
            await service.process_call(call_id)
            await db.commit()
        self._notify_semantic_analysis_ready(call_id)

    def _notify_semantic_analysis_ready(self, call_id: str) -> None:
        if self.on_call_ready_for_semantic_analysis is None:
            return
        try:
            self.on_call_ready_for_semantic_analysis(call_id)
        except Exception as exc:
            log.warning(
                "AI Call 离线 ASR 已完成但语义分析入队失败: callId={}, errorType={}, message={}",
                call_id,
                type(exc).__name__,
                str(exc),
            )


def parse_language_hints(value: str | Sequence[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return [str(item).strip() for item in value if str(item).strip()]
