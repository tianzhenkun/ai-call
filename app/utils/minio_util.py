import hashlib
import hmac
import io
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import httpx

from app.common.constant import RET
from app.core.exceptions import CustomException


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(secret_key: str, date_stamp: str, region: str) -> bytes:
    k = _hmac_sha256(f"AWS4{secret_key}".encode("utf-8"), date_stamp)
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

    @classmethod
    def upload(cls, config: dict, data: bytes, original_filename: str, content_type: str) -> tuple[str, str]:
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
            signature = hmac.new(sig_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

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
