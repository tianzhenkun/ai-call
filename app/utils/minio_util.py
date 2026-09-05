import hashlib
import hmac
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlencode, urlparse

import httpx

from app.common.constant import RET
from app.core.exceptions import CustomException


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(secret_key: str, date_stamp: str, region: str) -> bytes:
    k = _hmac_sha256(f"AWS4{secret_key}".encode(), date_stamp)
    k = _hmac_sha256(k, region)
    k = _hmac_sha256(k, "s3")
    return _hmac_sha256(k, "aws4_request")


class MinioUtil:
    """MinIO 文件上传工具类（httpx + AWS Signature V4，无需 minio 包）"""

    @staticmethod
    def _build_object_name(original_filename: str, prefix: str = "") -> str:
        """生成 MinIO object key：{prefix}/{yyyy/MM/dd}/{uuid}.{ext}"""
        ext = Path(original_filename).suffix.lower()
        date_path = datetime.now().strftime("%Y/%m/%d")
        unique_name = f"{uuid.uuid4().hex}{ext}"
        parts = [p for p in [prefix.strip(), date_path, unique_name] if p]
        return "/".join(parts)

    @staticmethod
    def _build_url(config: dict, object_name: str) -> str:
        protocol = "https" if config.get("is_https", "N") == "Y" else "http"
        domain = (config.get("domain") or "").strip()
        bucket = str(config["bucket_name"]).strip("/")
        object_path = object_name.lstrip("/")
        if domain:
            base_url = domain.rstrip("/")
            if not base_url.startswith(("http://", "https://")):
                base_url = f"{protocol}://{base_url}"
            if bucket and not base_url.endswith(f"/{bucket}"):
                base_url = f"{base_url}/{bucket}"
            return f"{base_url}/{object_path}"
        endpoint = str(config["endpoint"]).strip().rstrip("/")
        return f"{protocol}://{endpoint}/{bucket}/{object_path}"

    @staticmethod
    def _endpoint_base_and_host(
        config: dict, *, use_public_domain: bool = True
    ) -> tuple[str, str]:
        protocol = "https" if config.get("is_https", "N") == "Y" else "http"
        endpoint = str(
            (config.get("domain") or config["endpoint"])
            if use_public_domain
            else config["endpoint"]
        ).strip().rstrip("/")
        if endpoint.startswith(("http://", "https://")):
            parsed = urlparse(endpoint)
            return endpoint, parsed.netloc
        return f"{protocol}://{endpoint}", endpoint

    @classmethod
    def presigned_get_url(
        cls,
        config: dict,
        object_name: str,
        *,
        expires_seconds: int = 900,
        now: datetime | None = None,
    ) -> str:
        """生成私有对象的短时 GET 地址，不向调用方暴露 MinIO 密钥。"""
        if not 1 <= expires_seconds <= 604800:
            raise ValueError("expires_seconds must be between 1 and 604800")

        endpoint_base, host = cls._endpoint_base_and_host(config)
        bucket = str(config["bucket_name"]).strip("/")
        object_path = object_name.lstrip("/")
        access_key = config["access_key"]
        secret_key = config["secret_key"]
        region = (config.get("region") or "").strip() or "us-east-1"

        signed_at = now or datetime.now(timezone.utc)
        date_stamp = signed_at.strftime("%Y%m%d")
        amz_date = signed_at.strftime("%Y%m%dT%H%M%SZ")
        credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
        canonical_uri = f"/{bucket}/{quote(object_path, safe='/')}"
        query_params = {
            "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
            "X-Amz-Credential": f"{access_key}/{credential_scope}",
            "X-Amz-Date": amz_date,
            "X-Amz-Expires": str(expires_seconds),
            "X-Amz-SignedHeaders": "host",
        }
        canonical_query = urlencode(
            sorted(query_params.items()),
            quote_via=quote,
            safe="-_.~",
        )
        canonical_request = "\n".join(
            [
                "GET",
                canonical_uri,
                canonical_query,
                f"host:{host}\n",
                "host",
                "UNSIGNED-PAYLOAD",
            ]
        )
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signature = hmac.new(
            _signing_key(secret_key, date_stamp, region),
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return (
            f"{endpoint_base}{canonical_uri}?{canonical_query}"
            f"&X-Amz-Signature={signature}"
        )

    @classmethod
    def upload(
        cls, config: dict, data: bytes, original_filename: str, content_type: str
    ) -> tuple[str, str]:
        """
        通过 S3 API（AWS Signature V4）上传文件到 MinIO。

        参数:
        - config: sys_oss_config 记录（dict）
        - data: 文件二进制内容
        - original_filename: 原始文件名
        - content_type: MIME 类型

        返回:
        - tuple[str, str]: (访问 url, object_name)
        """
        try:
            endpoint = config["endpoint"]
            bucket = config["bucket_name"]
            access_key = config["access_key"]
            secret_key = config["secret_key"]
            region = (config.get("region") or "").strip() or "us-east-1"
            secure = config.get("is_https", "N") == "Y"
            protocol = "https" if secure else "http"

            object_name = cls._build_object_name(original_filename, config.get("prefix") or "")

            now = datetime.now(timezone.utc)
            date_stamp = now.strftime("%Y%m%d")
            amz_date = now.strftime("%Y%m%dT%H%M%SZ")
            payload_hash = hashlib.sha256(data).hexdigest()

            # 需要签名的 headers（按字母序）
            signed_hdrs = {
                "content-type": content_type,
                "host": endpoint,
                "x-amz-content-sha256": payload_hash,
                "x-amz-date": amz_date,
            }
            canonical_headers = "".join(f"{k}:{v}\n" for k, v in sorted(signed_hdrs.items()))
            signed_headers_str = ";".join(sorted(signed_hdrs.keys()))

            canonical_uri = f"/{bucket}/{quote(object_name, safe='/')}"
            canonical_request = "\n".join([
                "PUT",
                canonical_uri,
                "",
                canonical_headers,
                signed_headers_str,
                payload_hash,
            ])

            credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
            string_to_sign = "\n".join([
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ])

            sig_key = _signing_key(secret_key, date_stamp, region)
            signature = hmac.new(
                sig_key, string_to_sign.encode("utf-8"), hashlib.sha256
            ).hexdigest()

            authorization = (
                f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
                f"SignedHeaders={signed_headers_str}, Signature={signature}"
            )

            request_headers = {
                "Authorization": authorization,
                "Content-Type": content_type,
                "x-amz-content-sha256": payload_hash,
                "x-amz-date": amz_date,
            }

            put_url = f"{protocol}://{endpoint}/{bucket}/{quote(object_name, safe='/')}"
            with httpx.Client(timeout=30) as client:
                resp = client.put(put_url, content=data, headers=request_headers)
                resp.raise_for_status()

            return cls._build_url(config, object_name), object_name

        except CustomException:
            raise
        except Exception as e:
            raise CustomException(msg=f"MinIO上传失败: {e}", code=RET.SERVERERR.code)

    @classmethod
    async def head_object_size(
        cls, config: dict, object_name: str, timeout: float = 5.0
    ) -> int | None:
        """通过 S3 HEAD 查询对象大小，用于登记外部组件已写入的文件。"""
        endpoint_base, host = cls._endpoint_base_and_host(
            config, use_public_domain=False
        )
        bucket = str(config["bucket_name"]).strip("/")
        access_key = config["access_key"]
        secret_key = config["secret_key"]
        region = (config.get("region") or "").strip() or "us-east-1"
        object_path = object_name.lstrip("/")

        now = datetime.now(timezone.utc)
        date_stamp = now.strftime("%Y%m%d")
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        payload_hash = hashlib.sha256(b"").hexdigest()

        signed_hdrs = {
            "host": host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        canonical_headers = "".join(f"{k}:{v}\n" for k, v in sorted(signed_hdrs.items()))
        signed_headers_str = ";".join(sorted(signed_hdrs.keys()))
        canonical_uri = f"/{bucket}/{quote(object_path, safe='/')}"
        canonical_request = "\n".join([
            "HEAD",
            canonical_uri,
            "",
            canonical_headers,
            signed_headers_str,
            payload_hash,
        ])

        credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ])
        sig_key = _signing_key(secret_key, date_stamp, region)
        signature = hmac.new(sig_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        authorization = (
            f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers_str}, Signature={signature}"
        )

        url = f"{endpoint_base}/{bucket}/{quote(object_path, safe='/')}"
        headers = {
            "Authorization": authorization,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.head(url, headers=headers)
            resp.raise_for_status()

        content_length = resp.headers.get("content-length")
        if not content_length:
            return None
        return int(content_length)

    @classmethod
    async def get_object(
        cls,
        config: dict,
        object_name: str,
        *,
        byte_range: tuple[int, int] | None = None,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
        now: datetime | None = None,
    ) -> bytes:
        """读取私有对象，只向 MinIO 发送服务端 SigV4 凭据。"""
        if byte_range is not None:
            start, end = byte_range
            if start < 0 or end < start:
                raise ValueError("对象读取区间不合法")
        try:
            response = await cls._signed_empty_body_request(
                "GET",
                config,
                object_name,
                request_headers=(
                    {"Range": f"bytes={start}-{end}"}
                    if byte_range is not None
                    else None
                ),
                timeout=timeout,
                transport=transport,
                now=now,
            )
        except httpx.HTTPStatusError as error:
            failure_message = f"MinIO读取对象失败: HTTP {error.response.status_code}"
        except httpx.HTTPError:
            failure_message = "MinIO读取对象失败: 网络请求异常"
        else:
            return response.content
        raise CustomException(msg=failure_message, code=RET.SERVERERR.code)

    @classmethod
    async def put_object(
        cls,
        config: dict,
        object_name: str,
        data: bytes,
        content_type: str,
        *,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
        now: datetime | None = None,
    ) -> None:
        """使用调用方给定的精确对象键上传私有对象。"""
        try:
            await cls._signed_request(
                "PUT",
                config,
                object_name,
                data=data,
                content_type=content_type,
                timeout=timeout,
                transport=transport,
                now=now,
            )
        except httpx.HTTPStatusError as error:
            failure_message = f"MinIO上传对象失败: HTTP {error.response.status_code}"
        except httpx.HTTPError:
            failure_message = "MinIO上传对象失败: 网络请求异常"
        else:
            return
        raise CustomException(msg=failure_message, code=RET.SERVERERR.code)

    @classmethod
    async def delete_object(
        cls,
        config: dict,
        object_name: str,
        *,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
        now: datetime | None = None,
    ) -> None:
        """删除私有对象。MinIO 的任意 2xx 响应都视为成功。"""
        try:
            await cls._signed_empty_body_request(
                "DELETE",
                config,
                object_name,
                timeout=timeout,
                transport=transport,
                now=now,
            )
        except httpx.HTTPStatusError as error:
            failure_message = f"MinIO删除对象失败: HTTP {error.response.status_code}"
        except httpx.HTTPError:
            failure_message = "MinIO删除对象失败: 网络请求异常"
        else:
            return
        raise CustomException(msg=failure_message, code=RET.SERVERERR.code)

    @classmethod
    async def _signed_empty_body_request(
        cls,
        method: str,
        config: dict,
        object_name: str,
        *,
        request_headers: dict[str, str] | None = None,
        timeout: float,
        transport: httpx.AsyncBaseTransport | None,
        now: datetime | None,
    ) -> httpx.Response:
        return await cls._signed_request(
            method,
            config,
            object_name,
            data=b"",
            content_type=None,
            request_headers=request_headers,
            timeout=timeout,
            transport=transport,
            now=now,
        )

    @classmethod
    async def _signed_request(
        cls,
        method: str,
        config: dict,
        object_name: str,
        *,
        data: bytes,
        content_type: str | None,
        request_headers: dict[str, str] | None = None,
        timeout: float,
        transport: httpx.AsyncBaseTransport | None,
        now: datetime | None,
    ) -> httpx.Response:
        endpoint_base, _ = cls._endpoint_base_and_host(
            config, use_public_domain=False
        )
        bucket = str(config["bucket_name"]).strip("/")
        object_path = object_name.lstrip("/")
        access_key = config["access_key"]
        secret_key = config["secret_key"]
        region = (config.get("region") or "").strip() or "us-east-1"
        object_url = (
            f"{endpoint_base}/{bucket}/{quote(object_path, safe='/')}"
        )
        request = httpx.Request(
            method,
            object_url,
            content=data,
            headers=request_headers,
        )
        host = request.headers["host"]
        canonical_uri = request.url.raw_path.decode("ascii")

        signed_at = now or datetime.now(timezone.utc)
        date_stamp = signed_at.strftime("%Y%m%d")
        amz_date = signed_at.strftime("%Y%m%dT%H%M%SZ")
        payload_hash = hashlib.sha256(data).hexdigest()
        signed_headers = {
            "host": host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        if content_type is not None:
            signed_headers["content-type"] = content_type
        canonical_headers = "".join(
            f"{name}:{value}\n" for name, value in sorted(signed_headers.items())
        )
        signed_header_names = ";".join(sorted(signed_headers))
        canonical_request = "\n".join(
            [
                method,
                canonical_uri,
                "",
                canonical_headers,
                signed_header_names,
                payload_hash,
            ]
        )
        credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signature = hmac.new(
            _signing_key(secret_key, date_stamp, region),
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        authorization = (
            f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
            f"SignedHeaders={signed_header_names}, Signature={signature}"
        )
        request_headers = {
            "Authorization": authorization,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        if content_type is not None:
            request_headers["Content-Type"] = content_type
        request.headers.update(request_headers)
        async with httpx.AsyncClient(timeout=timeout, transport=transport) as client:
            response = await client.send(request)
            response.raise_for_status()
        return response
