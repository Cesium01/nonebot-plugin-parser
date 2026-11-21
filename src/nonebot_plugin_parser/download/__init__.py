import re
import math
import random
import asyncio
from pathlib import Path
from datetime import datetime, timezone
from functools import partial
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin

import httpx
import aiofiles
from nonebot import logger, get_driver
from tenacity import AsyncRetrying, RetryCallState, retry_if_exception, stop_after_attempt
from aiofiles.tempfile import NamedTemporaryFile
from aiofiles.threadpool.binary import AsyncBufferedIOBase

from .task import auto_task
from ..utils import merge_av, safe_unlink, generate_file_name, is_module_available
from ..config import pconfig
from ..constants import COMMON_HEADER, DOWNLOAD_TIMEOUT
from ..exception import IgnoreException, DownloadException

_RATE_LIMIT_STATUSES = {429, 567}
_RETRYABLE_STATUSES = _RATE_LIMIT_STATUSES | {408, 500, 502, 503, 504}


def _is_retryable(error: BaseException) -> bool:
    if isinstance(error, httpx.HTTPStatusError):
        return error.response.status_code in _RETRYABLE_STATUSES
    return isinstance(error, (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError))


def _http_date(value: str) -> datetime | None:
    try:
        date = parsedate_to_datetime(value)
    except (ValueError, OverflowError):
        return None
    return date if date.tzinfo is not None else None


def _retry_after(response: httpx.Response) -> float:
    value = response.headers.get("Retry-After", "").strip()
    if value.isascii() and value.isdecimal():
        delay = float(value)
        return delay if math.isfinite(delay) else 0.0
    if date := _http_date(value):
        return max(0.0, (date - datetime.now(timezone.utc)).total_seconds())
    return 0.0


class StreamDownloader:
    def __init__(self):
        self.headers: dict[str, str] = COMMON_HEADER.copy()
        self.cache_dir: Path = pconfig.cache_dir
        self.client: httpx.AsyncClient = httpx.AsyncClient(
            timeout=DOWNLOAD_TIMEOUT, verify=False, proxy=pconfig.cnproxy or None
        )
        self._cooldown_until: dict[str, float] = {}

    async def aclose(self):
        await self.client.aclose()

    async def _wait_for_cooldown(self, host: str) -> None:
        loop = asyncio.get_running_loop()
        # 其他请求可能在等待期间延长冷却时间。
        while (delay := self._cooldown_until.get(host, 0.0) - loop.time()) > 0:  # noqa: ASYNC110
            await asyncio.sleep(delay)
        self._cooldown_until.pop(host, None)

    def _retry_wait(self, host: str, state: RetryCallState) -> float:
        backoff = 2 ** (state.attempt_number - 1)
        jitter = random.uniform(0, 1)
        error = state.outcome.exception() if state.outcome else None
        if isinstance(error, httpx.HTTPStatusError):
            response = error.response
            delay = _retry_after(response)
            if response.status_code in _RATE_LIMIT_STATUSES:
                delay = max(delay, 5 * backoff + jitter)
            if delay > 0:
                now = asyncio.get_running_loop().time()
                self._cooldown_until = {key: until for key, until in self._cooldown_until.items() if until > now}
                response_host = response.url.host
                for key in {host, response_host}:
                    self._cooldown_until[key] = max(self._cooldown_until.get(key, 0.0), now + delay)
                logger.warning(f"下载主机 {response_host} 返回 HTTP {response.status_code}，等待 {delay:.1f} 秒")
                return 0.0  # 发出下一次请求前统一等待，其他同主机的下载也会等待。
        return backoff + jitter

    @staticmethod
    def _validate_size(response: httpx.Response, size: int) -> None:
        if size <= 0:
            logger.warning(f"媒体 url: {response.url}, 大小为 0, 取消下载")
            raise IgnoreException

        if (file_size := size / 1024 / 1024) > pconfig.max_size:
            logger.warning(f"媒体 url: {response.url} 大小 {file_size:.2f} MB, 超过 {pconfig.max_size} MB, 取消下载")
            raise IgnoreException

    @staticmethod
    def _resume_validator(response: httpx.Response) -> tuple[str, str] | None:
        etag = response.headers.get("ETag")
        if etag is not None:
            if len(etag) >= 2 and etag.startswith('"') and etag.endswith('"'):
                return "ETag", etag
            return None

        modified = response.headers.get("Last-Modified", "")
        date = _http_date(response.headers.get("Date", ""))
        last_modified = _http_date(modified)
        if date and last_modified and (date - last_modified).total_seconds() >= 60:
            return "Last-Modified", modified
        return None

    @staticmethod
    def _range_bounds(response: httpx.Response, offset: int, total_size: int | None) -> tuple[int, int]:
        content_range = response.headers.get("Content-Range", "")
        match = re.fullmatch(r"bytes ([0-9]+)-([0-9]+)/([0-9]+)", content_range, re.IGNORECASE)
        if match:
            try:
                start, end, size = map(int, match.groups())
                length = response.headers.get("Content-Length")
                if (
                    start == offset
                    and start <= end < size
                    and (total_size is None or total_size == size)
                    and (length is None or int(length) == end - start + 1)
                ):
                    return end + 1, size
            except ValueError as error:
                raise httpx.RemoteProtocolError("Invalid range response length", request=response.request) from error
        raise httpx.RemoteProtocolError("Invalid Content-Range for download resume", request=response.request)

    async def _download_to_file(self, url: str, headers: dict[str, str], file: AsyncBufferedIOBase) -> None:
        host = httpx.URL(url).host
        base_headers = httpx.Headers(headers)
        base_headers["Accept-Encoding"] = "identity"
        validator: tuple[str, str] | None = None
        total_size: int | None = None
        resource_url: httpx.URL | None = None

        async def receive() -> bool:
            nonlocal validator, total_size, resource_url
            offset = await file.tell() if validator else 0
            if offset == total_size:
                offset = 0
            request_headers = base_headers.copy()
            if offset and validator:
                request_headers["Range"] = f"bytes={offset}-"
                request_headers["If-Range"] = validator[1]

            await self._wait_for_cooldown(host)
            async with self.client.stream("GET", url, headers=request_headers, follow_redirects=True) as response:
                if offset and response.status_code == 416:
                    validator = None
                    raise httpx.RemoteProtocolError("Server rejected download resume", request=response.request)
                response.raise_for_status()

                identity = response.headers.get("Content-Encoding", "identity").strip().lower() == "identity"
                if response.status_code == 200:
                    await file.seek(0)
                    await file.truncate()
                    offset = 0
                    length = response.headers.get("Content-Length")
                    total_size = int(length) if length is not None else None
                    expected_end = total_size if identity else None
                    validator = self._resume_validator(response) if identity else None
                    resource_url = response.url
                elif response.status_code == 206 and offset and validator:
                    try:
                        name, value = validator
                        if not identity or response.url != resource_url or response.headers.get(name, value) != value:
                            raise httpx.RemoteProtocolError("Resource changed during resume", request=response.request)
                        expected_end, total_size = self._range_bounds(response, offset, total_size)
                    except httpx.RemoteProtocolError:
                        validator = None
                        raise
                else:
                    raise DownloadException(f"下载收到意外的 HTTP {response.status_code} 响应")

                if total_size is not None:
                    self._validate_size(response, total_size)
                async for chunk in response.aiter_bytes():
                    offset += len(chunk)
                    self._validate_size(response, offset)
                    if expected_end is not None and offset > expected_end:
                        raise DownloadException("下载数据超出响应声明的长度")
                    await file.write(chunk)

                if expected_end is not None and offset != expected_end:
                    raise httpx.RemoteProtocolError("Incomplete download response", request=response.request)
                self._validate_size(response, offset)
                return response.status_code == 200 or offset == total_size

        async for attempt in AsyncRetrying(
            retry=retry_if_exception(_is_retryable),
            wait=partial(self._retry_wait, host),
            stop=stop_after_attempt(5),
            reraise=True,
        ):
            with attempt:
                while not await receive():
                    continue
                return

    async def _download_file_with_httpx(
        self,
        url: str,
        *,
        file_path: Path,
        headers: dict[str, str],
    ) -> Path:
        part_path = None
        try:
            async with NamedTemporaryFile(dir=file_path.parent, suffix=".part", delete=False) as file:
                part_path = Path(file.name)
                await self._download_to_file(url, headers, file)
            return part_path.replace(file_path)
        finally:
            if part_path is not None:
                await safe_unlink(part_path)

    async def _download_file(
        self,
        url: str,
        *,
        file_name: str | None = None,
        ext_headers: dict[str, str] | None = None,
    ) -> Path:
        """download file by url with fallback"""
        if not file_name:
            file_name = generate_file_name(url)
        file_path = self.cache_dir / file_name
        headers = {**self.headers, **(ext_headers or {})}

        try:
            path = await self._download_file_with_httpx(url, file_path=file_path, headers=headers)
        except Exception:
            logger.opt(exception=True).warning(f"下载失败(httpx) | url: {url}")
            raise DownloadException("媒体下载失败")

        return path

    @auto_task
    async def download_video(
        self,
        url: str,
        *,
        video_name: str | None = None,
        ext_headers: dict[str, str] | None = None,
    ) -> Path:
        """download video file by url with stream"""
        if video_name is None:
            video_name = generate_file_name(url, ".mp4")

        return await self._download_file(
            url,
            file_name=video_name,
            ext_headers=ext_headers,
        )

    @auto_task
    async def download_audio(
        self,
        url: str,
        *,
        audio_name: str | None = None,
        ext_headers: dict[str, str] | None = None,
    ) -> Path:
        """download audio file by url with stream"""
        if audio_name is None:
            audio_name = generate_file_name(url, ".mp3")

        return await self._download_file(
            url,
            file_name=audio_name,
            ext_headers=ext_headers,
        )

    @auto_task
    async def download_img(
        self,
        url: str,
        *,
        img_name: str | None = None,
        ext_headers: dict[str, str] | None = None,
    ) -> Path:
        """download image file by url with stream"""
        if img_name is None:
            img_name = generate_file_name(url, ".jpg")

        return await self._download_file(
            url,
            file_name=img_name,
            ext_headers=ext_headers,
        )

    @auto_task
    async def download_av_and_merge(
        self,
        v_url: str,
        a_url: str,
        *,
        output_path: Path,
        ext_headers: dict[str, str] | None = None,
    ) -> Path:
        """download video and audio file by url with stream and merge"""
        v_path, a_path = await asyncio.gather(
            self._download_file(v_url, ext_headers=ext_headers),
            self._download_file(a_url, ext_headers=ext_headers),
        )
        await merge_av(v_path=v_path, a_path=a_path, output_path=output_path)
        return output_path

    @auto_task
    async def download_m3u8(
        self,
        m3u8_url: str,
        *,
        video_name: str | None = None,
        ext_headers: dict[str, str] | None = None,
    ) -> Path:
        """download m3u8 file by url with stream"""
        if video_name is None:
            video_name = generate_file_name(m3u8_url, ".mp4")

        video_path = pconfig.cache_dir / video_name

        try:
            async with aiofiles.open(video_path, "wb") as f:
                for url in await self._get_m3u8_slices(m3u8_url):
                    async with self.client.stream("GET", url, headers=ext_headers) as response:
                        async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                            await f.write(chunk)
        except httpx.HTTPError:
            await safe_unlink(video_path)
            logger.exception("m3u8 视频下载失败")
            raise DownloadException("m3u8 视频下载失败")

        return video_path

    async def _get_m3u8_slices(self, m3u8_url: str):
        """获取 m3u8 分片"""

        response = await self.client.get(m3u8_url)
        response.raise_for_status()

        slices_text = response.text
        slices: list[str] = []

        for line in slices_text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            slices.append(urljoin(m3u8_url, line))

        return slices


downloader: StreamDownloader = StreamDownloader()
"""全局下载器实例，提供下载功能"""
yt_dlp_downloader = None
"""yt-dlp 下载器实例，提供下载视频功能，若 yt-dlp 未安装则为 None"""

if is_module_available("yt_dlp"):
    from .ytdlp import YtdlpDownloader

    yt_dlp_downloader = YtdlpDownloader()


@get_driver().on_shutdown
async def close_download_client():
    logger.debug("正在关闭下载器...")
    await downloader.aclose()
