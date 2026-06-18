import pytest

from app.core.redis_crud import RedisCURD


class FakeScanRedis:
    async def scan_iter(self, match: str, count: int):
        assert match == "system_dict:*"
        assert count == 1000
        for key in [b"system_dict:a", b"system_dict:b"]:
            yield key


@pytest.mark.anyio
async def test_get_keys_uses_scan_iter() -> None:
    keys = await RedisCURD(FakeScanRedis()).get_keys("system_dict:*")

    assert keys == [b"system_dict:a", b"system_dict:b"]
