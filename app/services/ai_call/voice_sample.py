import base64
import hashlib
import io
import json
import subprocess
import tempfile
import wave
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from mutagen import File as MutagenFile

from app.utils.minio_util import MinioUtil

_MAX_SIZE_BYTES = 10 * 1024 * 1024
_MIN_DURATION_SECONDS = 3
_MAX_DURATION_SECONDS = 60
_MIN_SAMPLE_RATE = 24000
_FORMAT_MISMATCH_MESSAGE = "文件扩展名、音频格式与 Content-Type 不一致"


@dataclass(frozen=True)
class _AudioFormatRule:
    canonical_content_type: str
    accepted_content_types: frozenset[str]
    container_mime_types: frozenset[str]


_AUDIO_FORMATS = {
    ".wav": _AudioFormatRule(
        canonical_content_type="audio/wav",
        accepted_content_types=frozenset({
            "audio/wav",
            "audio/x-wav",
            "audio/wave",
            "audio/vnd.wave",
        }),
        container_mime_types=frozenset({"audio/wav", "audio/x-wav", "audio/wave"}),
    ),
    ".mp3": _AudioFormatRule(
        canonical_content_type="audio/mpeg",
        accepted_content_types=frozenset({
            "audio/mpeg",
            "audio/mp3",
            "audio/x-mp3",
            "audio/mpeg3",
            "audio/x-mpeg-3",
        }),
        container_mime_types=frozenset({
            "audio/mpeg",
            "audio/mp3",
            "audio/x-mp3",
            "audio/mpeg3",
            "audio/x-mpeg-3",
        }),
    ),
    ".m4a": _AudioFormatRule(
        canonical_content_type="audio/mp4",
        accepted_content_types=frozenset({"audio/mp4", "audio/x-m4a", "audio/m4a"}),
        container_mime_types=frozenset({"audio/mp4", "audio/x-m4a", "audio/m4a"}),
    ),
}


class VoiceSampleValidationError(ValueError):
    """声音样本不符合音色复刻约束。"""


@dataclass(frozen=True)
class VoiceSampleMetadata:
    filename: str
    content_type: str
    size_bytes: int
    duration_seconds: float
    sample_rate: int
    channels: int
    sha256: str


class VoiceSampleStorage(Protocol):
    async def put(
        self,
        *,
        object_key: str,
        data: bytes,
        content_type: str,
    ) -> None:
        raise NotImplementedError

    async def get(self, object_key: str) -> bytes:
        raise NotImplementedError

    async def delete(self, object_key: str) -> None:
        raise NotImplementedError


class MinioVoiceSampleStorage:
    def __init__(self, config: dict) -> None:
        self._config = dict(config)

    async def put(
        self,
        *,
        object_key: str,
        data: bytes,
        content_type: str,
    ) -> None:
        await MinioUtil.put_object(
            self._config,
            object_key,
            data,
            content_type,
        )

    async def get(self, object_key: str) -> bytes:
        return await MinioUtil.get_object(self._config, object_key)

    async def delete(self, object_key: str) -> None:
        await MinioUtil.delete_object(self._config, object_key)


def inspect_sample(
    data: bytes,
    *,
    filename: str,
    content_type: str,
) -> VoiceSampleMetadata:
    extension = Path(filename).suffix.lower()
    format_rule = _AUDIO_FORMATS.get(extension)
    if format_rule is None:
        raise VoiceSampleValidationError("仅支持 WAV、MP3、M4A 格式")
    normalized_content_type = content_type.partition(";")[0].strip().lower()
    if normalized_content_type not in format_rule.accepted_content_types:
        raise VoiceSampleValidationError(_FORMAT_MISMATCH_MESSAGE)
    if not data:
        raise VoiceSampleValidationError("声音样本不能为空")
    if len(data) >= _MAX_SIZE_BYTES:
        raise VoiceSampleValidationError("声音样本必须小于 10 MB")

    try:
        if extension == ".wav":
            duration_seconds, sample_rate, channels, sample_width = _inspect_wav(data)
        else:
            duration_seconds, sample_rate, channels = _inspect_compressed(
                data,
                extension=extension,
                expected_mime_types=format_rule.container_mime_types,
            )
            sample_width = None
    except VoiceSampleValidationError:
        raise
    except Exception:
        raise VoiceSampleValidationError("声音样本文件损坏或格式无法识别") from None

    if not _MIN_DURATION_SECONDS <= duration_seconds <= _MAX_DURATION_SECONDS:
        raise VoiceSampleValidationError("声音样本时长必须在 3～60 秒之间")
    if sample_rate < _MIN_SAMPLE_RATE:
        raise VoiceSampleValidationError("声音样本采样率不得低于 24000 Hz")
    if channels != 1:
        raise VoiceSampleValidationError("声音样本必须为单声道")
    if extension == ".wav" and sample_width != 2:
        raise VoiceSampleValidationError("WAV 声音样本必须为 16 位")

    return VoiceSampleMetadata(
        filename=filename,
        content_type=format_rule.canonical_content_type,
        size_bytes=len(data),
        duration_seconds=duration_seconds,
        sample_rate=sample_rate,
        channels=channels,
        sha256=hashlib.sha256(data).hexdigest(),
    )


def to_data_url(data: bytes, content_type: str) -> str:
    encoded = base64.b64encode(data).decode("ascii")
    return f"data:{content_type};base64,{encoded}"


def _inspect_wav(data: bytes) -> tuple[float, int, int, int]:
    with wave.open(io.BytesIO(data), "rb") as wav_file:
        sample_rate = wav_file.getframerate()
        frame_count = wav_file.getnframes()
        channels = wav_file.getnchannels()
        sample_width = wav_file.getsampwidth()
        if sample_rate <= 0:
            raise VoiceSampleValidationError("声音样本文件损坏或格式无法识别")
        frames = wav_file.readframes(frame_count)
        if len(frames) != frame_count * channels * sample_width:
            raise VoiceSampleValidationError("声音样本文件损坏或格式无法识别")
        return (
            frame_count / sample_rate,
            sample_rate,
            channels,
            sample_width,
        )


def _inspect_compressed(
    data: bytes,
    *,
    extension: str,
    expected_mime_types: frozenset[str],
) -> tuple[float, int, int]:
    audio = MutagenFile(io.BytesIO(data))
    info = getattr(audio, "info", None)
    if info is None:
        raise VoiceSampleValidationError("声音样本文件损坏或格式无法识别")
    container_mime_types = getattr(audio, "mime", ())
    if isinstance(container_mime_types, str):
        container_mime_types = (container_mime_types,)
    normalized_container_mime_types = {
        str(value).partition(";")[0].strip().lower() for value in container_mime_types
    }
    if not normalized_container_mime_types.intersection(expected_mime_types):
        raise VoiceSampleValidationError(_FORMAT_MISMATCH_MESSAGE)
    channels = int(info.channels)
    if extension == ".m4a" and channels != 1:
        channels = _ffprobe_channels(data, extension) or channels
    return (float(info.length), int(info.sample_rate), channels)


def _ffprobe_channels(data: bytes, extension: str) -> int | None:
    try:
        with tempfile.NamedTemporaryFile(suffix=extension) as sample_file:
            sample_file.write(data)
            sample_file.flush()
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "a:0",
                    "-show_entries",
                    "stream=channels",
                    "-of",
                    "json",
                    sample_file.name,
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        if result.returncode != 0:
            return None
        streams = json.loads(result.stdout or "{}").get("streams") or []
    except Exception:
        return None
    channels = streams[0].get("channels")
    return int(channels) if channels else None
