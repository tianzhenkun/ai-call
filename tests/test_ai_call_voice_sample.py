import base64
import hashlib
import io
import wave
from types import SimpleNamespace

import pytest

from app.services.ai_call.voice_sample import (
    MinioVoiceSampleStorage,
    VoiceSampleValidationError,
    inspect_sample,
    to_data_url,
)


def _wav_bytes(
    *,
    seconds: float = 3,
    sample_rate: int = 24000,
    channels: int = 1,
    sample_width: int = 2,
) -> bytes:
    frame_count = round(seconds * sample_rate)
    output = io.BytesIO()
    with wave.open(output, "wb") as wav_file:
        wav_file.setnchannels(channels)
        wav_file.setsampwidth(sample_width)
        wav_file.setframerate(sample_rate)
        wav_file.writeframes(b"\0" * frame_count * channels * sample_width)
    return output.getvalue()


def test_inspect_sample_accepts_valid_wav_and_returns_metadata() -> None:
    data = _wav_bytes(seconds=10)

    metadata = inspect_sample(data, filename="voice.wav", content_type="audio/wav")

    assert metadata.filename == "voice.wav"
    assert metadata.content_type == "audio/wav"
    assert metadata.size_bytes == len(data)
    assert metadata.duration_seconds == pytest.approx(10)
    assert metadata.sample_rate == 24000
    assert metadata.channels == 1
    assert metadata.sha256 == hashlib.sha256(data).hexdigest()


@pytest.mark.parametrize("filename", ["voice.ogg", "voice", "voice.WMA"])
def test_inspect_sample_rejects_unsupported_extension(filename: str) -> None:
    with pytest.raises(VoiceSampleValidationError, match="仅支持"):
        inspect_sample(
            _wav_bytes(),
            filename=filename,
            content_type="application/octet-stream",
        )


def test_inspect_sample_rejects_empty_file() -> None:
    with pytest.raises(VoiceSampleValidationError, match="不能为空"):
        inspect_sample(b"", filename="voice.wav", content_type="audio/wav")


@pytest.mark.parametrize("size_bytes", [10 * 1024 * 1024, 10 * 1024 * 1024 + 1])
def test_inspect_sample_requires_size_strictly_less_than_10_mb(size_bytes: int) -> None:
    with pytest.raises(VoiceSampleValidationError, match="小于 10 MB"):
        inspect_sample(
            b"x" * size_bytes,
            filename="voice.mp3",
            content_type="audio/mpeg",
        )


@pytest.mark.parametrize("seconds", [2.99, 60.01])
def test_inspect_sample_rejects_duration_outside_allowed_range(seconds: float) -> None:
    with pytest.raises(VoiceSampleValidationError, match="3～60 秒"):
        inspect_sample(
            _wav_bytes(seconds=seconds),
            filename="voice.wav",
            content_type="audio/wav",
        )


@pytest.mark.parametrize("seconds", [3, 60])
def test_inspect_sample_accepts_duration_boundaries(seconds: float) -> None:
    metadata = inspect_sample(
        _wav_bytes(seconds=seconds),
        filename="voice.wav",
        content_type="audio/wav",
    )

    assert metadata.duration_seconds == pytest.approx(seconds)


def test_inspect_sample_rejects_sample_rate_below_24khz() -> None:
    with pytest.raises(VoiceSampleValidationError, match="24000 Hz"):
        inspect_sample(
            _wav_bytes(sample_rate=16000),
            filename="voice.wav",
            content_type="audio/wav",
        )


def test_inspect_sample_rejects_multiple_channels() -> None:
    with pytest.raises(VoiceSampleValidationError, match="单声道"):
        inspect_sample(
            _wav_bytes(channels=2),
            filename="voice.wav",
            content_type="audio/wav",
        )


def test_inspect_sample_rejects_non_16_bit_wav() -> None:
    with pytest.raises(VoiceSampleValidationError, match="16 位"):
        inspect_sample(
            _wav_bytes(sample_width=1),
            filename="voice.wav",
            content_type="audio/wav",
        )


def test_inspect_sample_hides_media_parser_error() -> None:
    with pytest.raises(
        VoiceSampleValidationError,
        match="声音样本文件损坏或格式无法识别",
    ) as error:
        inspect_sample(
            b"secret-corrupt-content",
            filename="voice.wav",
            content_type="audio/wav",
        )

    assert "secret-corrupt-content" not in str(error.value)
    assert error.value.__cause__ is None


@pytest.mark.parametrize(
    ("filename", "content_type"),
    [("voice.mp3", "audio/mpeg"), ("voice.m4a", "audio/mp4")],
)
def test_inspect_sample_uses_mutagen_metadata_for_compressed_formats(
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    content_type: str,
) -> None:
    seen_data: list[bytes] = []

    def fake_mutagen_file(file_object):
        seen_data.append(file_object.read())
        return SimpleNamespace(
            info=SimpleNamespace(length=8.5, sample_rate=48000, channels=1)
        )

    monkeypatch.setattr(
        "app.services.ai_call.voice_sample.MutagenFile",
        fake_mutagen_file,
    )

    metadata = inspect_sample(
        b"compressed-audio",
        filename=filename,
        content_type=content_type,
    )

    assert seen_data == [b"compressed-audio"]
    assert metadata.duration_seconds == pytest.approx(8.5)
    assert metadata.sample_rate == 48000
    assert metadata.channels == 1


def test_inspect_sample_rejects_compressed_file_without_audio_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "app.services.ai_call.voice_sample.MutagenFile",
        lambda _: None,
    )

    with pytest.raises(
        VoiceSampleValidationError,
        match="声音样本文件损坏或格式无法识别",
    ):
        inspect_sample(
            b"not-an-mp3",
            filename="voice.mp3",
            content_type="audio/mpeg",
        )


def test_to_data_url_base64_encodes_audio() -> None:
    data = b"\x00voice\xff"

    result = to_data_url(data, "audio/mpeg")

    assert result == f"data:audio/mpeg;base64,{base64.b64encode(data).decode('ascii')}"


@pytest.mark.anyio
async def test_minio_storage_uses_private_prefix_and_returns_only_object_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = {
        "endpoint": "minio.example.com",
        "bucket_name": "recov",
        "access_key": "access-key",
        "secret_key": "secret-key",
        "prefix": "must-not-be-used",
    }
    seen: dict[str, object] = {}

    def fake_upload(
        received_config: dict,
        data: bytes,
        filename: str,
        content_type: str,
    ) -> tuple[str, str]:
        seen["put"] = (received_config, data, filename, content_type)
        return (
            "https://public.example.com/recov/ai-call/voice-samples/object.wav",
            "ai-call/voice-samples/object.wav",
        )

    async def fake_get(received_config: dict, object_key: str, **_: object) -> bytes:
        seen["get"] = (received_config, object_key)
        return b"stored-audio"

    async def fake_delete(received_config: dict, object_key: str, **_: object) -> None:
        seen["delete"] = (received_config, object_key)

    monkeypatch.setattr("app.utils.minio_util.MinioUtil.upload", fake_upload)
    monkeypatch.setattr("app.utils.minio_util.MinioUtil.get_object", fake_get)
    monkeypatch.setattr("app.utils.minio_util.MinioUtil.delete_object", fake_delete)
    storage = MinioVoiceSampleStorage(config)

    object_key = await storage.put(
        data=b"audio",
        filename="sample.wav",
        content_type="audio/wav",
    )
    stored = await storage.get(object_key)
    await storage.delete(object_key)

    assert object_key == "ai-call/voice-samples/object.wav"
    assert stored == b"stored-audio"
    upload_config, *upload_args = seen["put"]
    assert upload_config["prefix"] == "ai-call/voice-samples"
    assert upload_args == [b"audio", "sample.wav", "audio/wav"]
    assert seen["get"][1] == object_key
    assert seen["delete"][1] == object_key
