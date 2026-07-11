import httpx
import pytest
from nonebot import logger


def test_generate_file_name():
    import random

    from nonebot_plugin_parser.utils import generate_file_name

    suffix_lst = [
        ".jpg",
        ".png",
        ".gif",
        ".webp",
        ".jpeg",
        ".bmp",
        ".tiff",
        ".ico",
        ".svg",
        ".heic",
        ".heif",
    ]
    # 测试 100 个链接
    for i in range(20):
        url = f"https://www.google.com/test{i}{random.choice(suffix_lst)}"
        file_name = generate_file_name(url)
        new_file_name = generate_file_name(url)
        assert file_name == new_file_name
        logger.info(f"{url}: {file_name}")


@pytest.mark.asyncio
async def test_httpx_download_resume(tmp_path, monkeypatch):
    from nonebot_plugin_parser.download import downloader

    file_path = tmp_path / "resume_test.bin"
    file_path.write_bytes(b"1234567890")
    url = "https://example.com/file.bin"

    async def handler(request: httpx.Request):
        assert request.headers["Range"] == "bytes=10-"
        headers = {
            "Content-Length": "5",
            "Content-Range": "bytes 10-14/15",
        }
        return httpx.Response(206, headers=headers, content=b"ABCDE")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(downloader, "client", client)

    def dummy_progress(desc: str, total: int | None = None):
        class DummyProgress:
            def __enter__(self):
                return lambda *args, **kwargs: None

            def __exit__(self, exc_type, exc, tb):
                return False

        return DummyProgress()

    monkeypatch.setattr(
        downloader.__class__,
        "rich_progress",
        staticmethod(dummy_progress),
    )

    path = await downloader._download_file_with_httpx(
        url,
        file_path=file_path,
        headers={"Accept": "*/*"},
        chunk_size=2,
    )

    assert path == file_path
    assert file_path.read_bytes() == b"1234567890ABCDE"


def test_limited_size_dict():
    from nonebot_plugin_parser.download.ytdlp import LimitedSizeDict

    limited_size_dict = LimitedSizeDict()
    for i in range(20):
        limited_size_dict[f"test{i}"] = f"test{i}"
    assert len(limited_size_dict) == 20
    for i in range(20):
        assert limited_size_dict[f"test{i}"] == f"test{i}"
    for i in range(20, 30):
        limited_size_dict[f"test{i}"] = f"test{i}"
    assert len(limited_size_dict) == 20
