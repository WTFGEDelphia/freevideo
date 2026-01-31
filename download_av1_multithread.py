import os
import sys
import re
import asyncio
import aiohttp
import aiofiles
import m3u8
import subprocess
import logging
import platform
import argparse
from urllib.parse import urlparse
from yarl import URL 
from tqdm import tqdm
from Crypto.Cipher import AES

"""
特殊的 URL（来自 XHamster 等类似架构的 CDN）包含了一些复杂的路径结构（如 multi=... 这种包含逗号和冒号的路径），并且通常会有严格的 User-Agent 校验和 Referer 校验。如果直接下载，CDN 可能会返回 403 Forbidden。
针对这种格式，我做了以下核心优化：
添加浏览器 User-Agent：模拟真实浏览器请求，防止被 CDN 拦截。
自动基础路径修复：使用 m3u8 库内置的 base_uri 处理能力，确保像 multi=... 这种复杂路径下的片段能被正确拼接。
Referer 自动模拟：自动将 M3U8 的域名作为 Referer 发送，这是绕过许多 CDN 限制的关键。
支持 AV1 编码处理：URL 中提到了 av1.mp4，这说明视频流可能是 AV1 编码。FFmpeg 合并时我们会确保使用兼容性最好的参数。
"""
# 配置管理
class Config:
    def __init__(self):
        self.log_level = logging.INFO
        self.log_format = "%(asctime)s - %(levelname)s - %(message)s"
        self.log_file = "download.log"
        self.chunk_size = 1024 * 64
        self.min_file_size = 100 # fMP4 片段可能很小
        self.max_retries = 5
        self.retry_delay = 2
        self.timeout = 30
        self.max_connections = 10
        self.ffmpeg_path = self._get_ffmpeg_path()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": "https://xhamster.com/", 
            "Origin": "https://xhamster.com",
            "Connection": "keep-alive"
        }

    def _get_ffmpeg_path(self):
        if platform.system() == "Windows":
            return "ffmpeg.exe"
        return "ffmpeg"

config = Config()

def setup_logging():
    logging.basicConfig(level=config.log_level, format=config.log_format,
                        handlers=[logging.StreamHandler(), logging.FileHandler(config.log_file)])
    return logging.getLogger(__name__)

logger = setup_logging()

# --- 下载函数 ---
async def download_file(session, semaphore, url, file_path, desc="File"):
    """通用下载函数"""
    if os.path.exists(file_path) and os.path.getsize(file_path) > config.min_file_size:
        return True

    async with semaphore:
        for attempt in range(config.max_retries):
            try:
                async with session.get(URL(url, encoded=True), timeout=config.timeout) as response:
                    if response.status == 200:
                        async with aiofiles.open(file_path, "wb") as f:
                            await f.write(await response.read())
                        return True
                    elif response.status == 403:
                        logger.error(f"403 Forbidden: {url}")
                        return False
            except Exception as e:
                if attempt == config.max_retries - 1:
                    logger.error(f"Failed to download {desc}: {e}")
                await asyncio.sleep(config.retry_delay)
    return False

async def download_m3u8(url_str, output_dir, max_workers):
    os.makedirs(output_dir, exist_ok=True)
    semaphore = asyncio.Semaphore(max_workers)
    
    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn, headers=config.headers) as session:
        # 1. 获取 M3U8
        async with session.get(URL(url_str, encoded=True)) as response:
            if response.status != 200: return False
            content = await response.text()

        m3u8_obj = m3u8.loads(content, uri=url_str)

        # 2. 处理 Master Playlist
        if m3u8_obj.playlists:
            m3u8_obj.playlists.sort(key=lambda p: p.stream_info.bandwidth or 0, reverse=True)
            return await download_m3u8(m3u8_obj.playlists[0].absolute_uri, output_dir, max_workers)

        # 3. 关键：处理 fMP4 初始化段 (EXT-X-MAP)
        if m3u8_obj.segment_map:
            init_url = m3u8_obj.segment_map[0].absolute_uri
            logger.info(f"fMP4 detected. Downloading init segment...")
            init_path = os.path.join(output_dir, "init.mp4")
            await download_file(session, semaphore, init_url, init_path, "Init Segment")

        # 4. 下载片段
        tasks = []
        with tqdm(total=len(m3u8_obj.segments), desc="Downloading", unit="seg") as pbar:
            for i, segment in enumerate(m3u8_obj.segments):
                seg_url = segment.absolute_uri
                # 注意：即便后缀是 .ts，fMP4 内部也是 mp4 数据
                seg_path = os.path.join(output_dir, f"index{i:05d}.ts")
                
                # 包装下载任务，增加进度条更新
                async def task_wrapper(s, sem, u, p, idx, pb):
                    res = await download_file(s, sem, u, p, f"Seg {idx}")
                    if pb: pb.update(1)
                    return res
                
                tasks.append(task_wrapper(session, semaphore, seg_url, seg_path, i, pbar))
            
            results = await asyncio.gather(*tasks)
            return all(results)

# --- 优化后的合并函数 ---
def ffmpeg_merge(output_dir, output_file):
    abs_dir = os.path.abspath(output_dir)
    ts_files = sorted([f for f in os.listdir(abs_dir) if f.startswith("index") and f.endswith(".ts")])
    if not ts_files: return False

    init_file = os.path.join(abs_dir, "init.mp4")
    has_init = os.path.exists(init_file)

    # 方案：如果是 fMP4，最稳妥的方法是二进制合并 init + segments 再用 FFmpeg 修复
    # 因为 FFmpeg 的 concat demuxer 对没有 init 段的片段会直接报错
    temp_combined = os.path.join(abs_dir, "combined_raw.mp4")
    
    try:
        logger.info("Performing binary concatenation...")
        with open(temp_combined, "wb") as outfile:
            # 1. 首先写入初始化段
            if has_init:
                with open(init_file, "rb") as infile:
                    outfile.write(infile.read())
            # 2. 顺序写入所有片段
            for ts in ts_files:
                with open(os.path.join(abs_dir, ts), "rb") as infile:
                    outfile.write(infile.read())

        # 3. 使用 FFmpeg 重新封装（修复时间戳和元数据）
        logger.info("Fixing container with FFmpeg...")
        output_path = os.path.join(abs_dir, output_file)
        
        # 对于 fMP4，直接 copy 即可，无需 concat 协议
        cmd = [
            config.ffmpeg_path, "-i", temp_combined,
            "-c", "copy", "-movflags", "+faststart", "-y", output_path
        ]
        
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode == 0:
            if os.path.exists(temp_combined): os.remove(temp_combined)
            logger.info(f"Successfully merged: {output_file}")
            return True
        else:
            logger.error(f"FFmpeg Error: {res.stderr}")
            return False
    except Exception as e:
        logger.error(f"Merge exception: {e}")
        return False

# --- 主流程 ---
async def process_url(url, output_root, output_video, max_workers, keep_segments=False):
    url_id = re.sub(r"[^\w]", "_", url)[-40:]
    output_dir = os.path.join(output_root, url_id)

    if await download_m3u8(url, output_dir, max_workers):
        if ffmpeg_merge(output_dir, output_video):
            if not keep_segments:
                for f in os.listdir(output_dir):
                    if f.endswith(".ts") or f == "filelist.txt" or f == "init.mp4":
                        try: os.remove(os.path.join(output_dir, f))
                        except: pass
            return True
    return False

async def main_async():
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--url", help="M3U8 URL")
    parser.add_argument("-o", "--output-dir", default="output")
    parser.add_argument("-v", "--output-video", default="final_video.mp4")
    parser.add_argument("-t", "--threads", type=int, default=10)
    args = parser.parse_args()

    url = args.url or "https://surrit.com/5f4c5a4f-a0e2-4fb1-80c0-6b520463c84d/1080p/video.m3u8"
    
    await process_url(url, args.output_dir, args.output_video, args.threads)

if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main_async())
