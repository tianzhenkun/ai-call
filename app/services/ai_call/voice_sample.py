import asyncio
import base64
import hashlib
import io
import wave
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from mutagen import File as MutagenFile

from app.utils.minio_util import MinioUtil

_SUPPORTED_EXTENSIONS = {".wav", ".mp3", ".m4a"}
_MAX_SIZE_BYTES = 10 * 1024 * 1024
_MIN_DURATION_SECONDS = 3
_MAX_DURATION_SECONDS = 60
_MIN_SAMPLE_RATE = 24000
_VOICE_SAMPLE_PREFIX = "ai-call/voice-samples"


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
    async def put(self, *, data: bytes, filename: str, content_type: str) -> str:
        raise NotImplementedError

    async def get(self, object_key: str) -> bytes:
        raise NotImplementedError

    async def delete(self, object_key: str) -> None:
        raise NotImplementedError


class MinioVoiceSampleStorage:
    def __init__(self, config: dict) -> None:
        self._config = dict(config)

    async def put(self, *, data: bytes, filename: str, content_type: str) -> str:
        config = {**self._config, "prefix": _VOICE_SAMPLE_PREFIX}
        _, object_key = await asyncio.to_thread(
            MinioUtil.upload,
            config,
            data,
            filename,
            content_type,
        )
        return object_key

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
    if extension not in _SUPPORTED_EXTENSIONS:
        raise VoiceSampleValidationError("仅支持 WAV、MP3、M4A 格式")
    if not data:
        raise VoiceSampleValidationError("声音样本不能为空")
    if len(data) >= _MAX_SIZE_BYTES:
        raise VoiceSampleValidationError("声音样本必须小于 10 MB")

    try:
        if extension == ".wav":
            duration_seconds, sample_rate, channels, sample_width = _inspect_wav(data)
        else:
            duration_seconds, sample_rate, channels = _inspect_compressed(data)
            sample_width = None
    except VoiceSampleValidationError:
        raise
    except Exception:
        raise VoiceSampleValidationError(
            "声音样本文件损坏或格式无法识别"
        ) from None

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
        content_type=content_type,
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
        if sample_rate <= 0:
            raise VoiceSampleValidationError("声音样本文件损坏或格式无法识别")
        return (
            frame_count / sample_rate,
            sample_rate,
            wav_file.getnchannels(),
            wav_file.getsampwidth(),
        )


def _inspect_compressed(data: bytes) -> tuple[float, int, int]:
    audio = MutagenFile(io.BytesIO(data))
    info = getattr(audio, "info", None)
    if info is None:
        raise VoiceSampleValidationError("声音样本文件损坏或格式无法识别")
    return (
        float(info.length),
        int(info.sample_rate),
        int(info.channels),
    )
