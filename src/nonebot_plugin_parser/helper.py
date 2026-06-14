import base64
from typing import Any, AsyncGenerator
from pathlib import Path
from collections.abc import Sequence
import uuid

from nonebot.matcher import current_bot
from nonebot_plugin_alconna.uniseg import (
    File,
    Text,
    Image,
    Video,
    Voice,
    Segment,
    Reference,
    CustomNode,
    UniMessage,
)

from .config import pconfig

# from .exception import TipException

ForwardNodeInner = str | Segment | UniMessage
"""转发消息节点内部允许的类型"""


class UniHelper:
    @staticmethod
    def construct_forward_message(
        segments: Sequence[ForwardNodeInner],
        user_id: str | None = None,
    ) -> Reference:
        """构造转发消息"""
        if user_id is None:
            user_id = current_bot.get().self_id

        nodes: list[CustomNode] = []
        for seg in segments:
            if isinstance(seg, str):
                content = UniMessage([Text(seg)])
            elif isinstance(seg, Segment):
                content = UniMessage([seg])
            else:
                content = seg
            node = CustomNode(uid=user_id, name=pconfig.nickname, content=content)
            nodes.append(node)

        return Reference(nodes=nodes)

    @staticmethod
    def img_seg(
        file: Path | bytes,
    ) -> Image:
        """图片 Seg"""
        if isinstance(file, bytes):
            return Image(raw=file)

        if pconfig.use_base64:
            return Image(raw=file.read_bytes())
        else:
            return Image(path=file)

    @staticmethod
    def record_seg(audio_path: Path) -> Voice:
        """语音 Seg"""
        if pconfig.use_base64:
            return Voice(raw=audio_path.read_bytes())
        else:
            return Voice(path=audio_path)

    @classmethod
    def video_seg(
        cls,
        video_path: Path,
        thumbnail: Path | None = None,
    ) -> Video | File | Text:
        """视频 Seg"""
        # 检测文件大小
        file_size_byte_count = int(video_path.stat().st_size)
        if file_size_byte_count == 0:
            return Text("视频为空文件")
        elif file_size_byte_count > 100 * 1024 * 1024:
            # 转为文件 Seg
            return cls.file_seg(video_path, display_name=video_path.name)
        else:
            if pconfig.use_base64:
                video = Video(raw=video_path.read_bytes())
                if thumbnail and thumbnail.stat().st_size > 0:
                    video.thumbnail = cls.img_seg(thumbnail.read_bytes())
                return video
            else:
                video = Video(path=video_path)
                if thumbnail and thumbnail.stat().st_size > 0:
                    video.thumbnail = cls.img_seg(thumbnail)
                return video

    @staticmethod
    def file_seg(
        file: Path,
        display_name: str | None = None,
    ) -> File:
        """文件 Seg"""
        if not display_name:
            display_name = file.name

        if pconfig.use_base64:
            return File(raw=file.read_bytes(), name=display_name)
        else:
            return File(path=file, name=display_name)
        
    @staticmethod
    def get_chunk_info(
        seg: Video,
        chunk_size: int = 1024 * 1024
    ) -> dict[str, Any]:
        if seg.raw:
            data = seg.raw if isinstance(seg.raw, bytes) else seg.raw.read()
        elif seg.path:
            with open(seg.path, "rb") as f:
                data = f.read()
        else:
            raise ValueError("empty video data")
        return {
            "data": data,
            "file_size": len(data),
            "total_chunks": len(data)+chunk_size-1 // chunk_size 
        }
        
    @staticmethod
    async def get_video_seg_stream(
        data: bytes,
        chunk_size: int = 1024 * 1024
    ) -> AsyncGenerator[bytes, None]:
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    @staticmethod
    async def upload_video_via_stream(
        msg: UniMessage[Video],
        chunk_size: int = 1024 * 1024
    ) -> str:
        """流式上传视频"""
        bot = current_bot.get()
        stream_id = str(uuid.uuid4())
        chunk_index = 0
        file_id = ""
        chunk_info = UniHelper.get_chunk_info(msg[0], chunk_size)
        async for chunk in UniHelper.get_video_seg_stream(chunk_info["data"], chunk_size):
            b64_data = base64.b64encode(chunk).decode("utf-8")
            res = await bot.call_api(
                "upload_file_stream",
                stream_id=stream_id,
                chunk_data=b64_data,
                chunk_index=chunk_index,
                total_chunks=chunk_info["total_chunks"],
                file_size=chunk_info["file_size"]
            )
            if res:
                file_id = res.get("file") or res.get("file_id", "")
            chunk_index += 1
        return file_id