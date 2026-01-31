import os
import sys
import re
import time
import argparse
import asyncio
import aiohttp
import aiofiles
import m3u8
import subprocess
import logging
import platform
from urllib.parse import urljoin, urlparse
from tqdm import tqdm
from Crypto.Cipher import AES

# 配置类
class Config:
    def __init__(self):
        self.log_level = logging.INFO
        self.log_format = "%(asctime)s - %(levelname)s - %(message)s"
        self.log_file = "download.log"
        self.chunk_size = 1024 * 16
        self.min_file_size = 512  # 只要大于512字节就认为是有效片段
        self.max_retries = 3
        self.retry_delay = 1
        self.timeout = 30
        self.max_connections = 10 # 默认并发数
        self.ffmpeg_path = self._get_ffmpeg_path()

    def _get_ffmpeg_path(self):
        if platform.system() == "Windows":
            for path in os.environ["PATH"].split(os.pathsep):
                ffmpeg_path = os.path.join(path, "ffmpeg.exe")
                if os.path.isfile(ffmpeg_path): return ffmpeg_path
            return "ffmpeg.exe"
        else:
            for path in ["/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg"]:
                if os.path.isfile(path): return path
            return "ffmpeg"

config = Config()

def setup_logging():
    logging.basicConfig(
        level=config.log_level,
        format=config.log_format,
        handlers=[logging.StreamHandler(), logging.FileHandler(config.log_file)],
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# --- 解密相关逻辑 ---
async def get_decrypt_key(session, key_url):
    """异步获取解密密钥"""
    try:
        async with session.get(key_url, timeout=10) as response:
            if response.status == 200:
                return await response.read()
    except Exception as e:
        logger.error(f"Failed to fetch AES key from {key_url}: {e}")
    return None

def decrypt_segment(data, key, iv):
    """使用 AES-128-CBC 解密数据"""
    cipher = AES.new(key, AES.MODE_CBC, iv=iv)
    return cipher.decrypt(data)

# --- 核心下载逻辑 ---
async def download_segment(session, semaphore, segment_url, segment_file_path, index, key_info=None, pbar=None):
    """下载并（如果需要）解密单个视频片段"""
    if os.path.exists(segment_file_path) and os.path.getsize(segment_file_path) > config.min_file_size:
        if pbar: pbar.update(1)
        return True

    async with semaphore:
        for attempt in range(config.max_retries):
            try:
                async with session.get(segment_url, timeout=config.timeout) as response:
                    if response.status == 200:
                        data = await response.read()
                        
                        # 如果有加密信息则进行解密
                        if key_info and key_info.get("key"):
                            # 如果 M3U8 没提供 IV，通常使用片段索引作为 IV
                            iv = key_info.get("iv") or index.to_bytes(16, 'big')
                            data = decrypt_segment(data, key_info["key"], iv)
                        
                        async with aiofiles.open(segment_file_path, "wb") as f:
                            await f.write(data)
                        
                        if pbar: pbar.update(1)
                        return True
            except Exception as e:
                if attempt == config.max_retries - 1:
                    logger.error(f"Error downloading {segment_url}: {e}")
                await asyncio.sleep(config.retry_delay)
    return False

async def download_m3u8(url, output_dir, max_workers):
    """解析并下载 M3U8"""
    os.makedirs(output_dir, exist_ok=True)
    semaphore = asyncio.Semaphore(max_workers)
    
    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        # 获取 M3U8
        async with session.get(url, timeout=config.timeout) as response:
            if response.status != 200:
                logger.error(f"Failed to fetch M3U8: {response.status}")
                return False
            content = await response.text()
        
        m3u8_obj = m3u8.loads(content)
        base_url = urljoin(url, ".")

        # 处理 Master Playlist
        if m3u8_obj.playlists:
            logger.info("Master playlist detected, selecting highest quality...")
            m3u8_obj.playlists.sort(key=lambda p: p.stream_info.bandwidth or 0, reverse=True)
            sub_url = urljoin(base_url, m3u8_obj.playlists[0].uri)
            return await download_m3u8(sub_url, output_dir, max_workers)

        # 处理加密密钥 (Key)
        key_info = None
        if m3u8_obj.keys and m3u8_obj.keys[0]:
            key_uri = urljoin(base_url, m3u8_obj.keys[0].uri)
            key_bytes = await get_decrypt_key(session, key_uri)
            if key_bytes:
                iv = None
                if m3u8_obj.keys[0].iv:
                    iv = bytes.fromhex(m3u8_obj.keys[0].iv.replace("0x", ""))
                key_info = {"key": key_bytes, "iv": iv}
                logger.info("AES Decryption key loaded.")

        if not m3u8_obj.segments:
            logger.error("No segments found in m3u8!")
            return False

        # 任务分发
        tasks = []
        with tqdm(total=len(m3u8_obj.segments), desc="Downloading", unit="seg") as pbar:
            for i, segment in enumerate(m3u8_obj.segments):
                seg_url = urljoin(base_url, segment.uri)
                seg_path = os.path.join(output_dir, f"index{i:05d}.ts") # 使用 05d 保证文件名排序自然正确
                tasks.append(download_segment(session, semaphore, seg_url, seg_path, i, key_info, pbar))
            
            results = await asyncio.gather(*tasks)
            return all(results)

# --- 合并逻辑 ---
def ffmpeg_merge_ts_files(output_dir, output_file):
    """调用 FFmpeg 合并片段"""
    abs_dir = os.path.abspath(output_dir)
    # 找到所有 TS 片段并按文件名排序
    ts_files = sorted([f for f in os.listdir(abs_dir) if f.endswith(".ts")])
    if not ts_files: return False

    filelist_path = os.path.join(abs_dir, "filelist.txt")
    with open(filelist_path, "w", encoding="utf-8") as f:
        for ts in ts_files:
            # 处理 Windows 路径兼容性
            safe_path = os.path.join(abs_dir, ts).replace("\\", "/")
            f.write(f"file '{safe_path}'\n")

    output_path = os.path.join(abs_dir, output_file)
    cmd = [
        config.ffmpeg_path, "-f", "concat", "-safe", "0", "-i", filelist_path,
        "-c", "copy", "-bsf:a", "aac_adtstoasc", "-movflags", "+faststart", "-y", output_path
    ]

    try:
        logger.info(f"Running FFmpeg merge...")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"Merge successful: {output_path}")
            return True
        else:
            logger.error(f"FFmpeg failed: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"FFmpeg error: {e}")
        return False

# --- 原始架构的主流程 ---
async def process_url(url, output_dir, output_file, max_workers, use_ffmpeg=True, keep_segments=False):
    # 为每个 URL 创建独立目录名
    url_hash = re.sub(r"[^\w]", "_", url)[-20:]
    url_output_dir = os.path.join(output_dir, url_hash)

    logger.info(f"Processing: {url}")
    if await download_m3u8(url, url_output_dir, max_workers):
        merge_success = ffmpeg_merge_ts_files(url_output_dir, output_file)
        
        if merge_success and not keep_segments:
            for f in os.listdir(url_output_dir):
                if f.endswith(".ts") or f == "filelist.txt":
                    os.remove(os.path.join(url_output_dir, f))
            logger.info("Segments cleaned up.")
        return merge_success
    return False

# --- 参数解析部分 (完全保留) ---
def parse_arguments():
    parser = argparse.ArgumentParser(description="Download and merge M3U8 video segments")
    parser.add_argument("-u", "--url", help="M3U8 URL to download")
    parser.add_argument("-f", "--file", help="File containing M3U8 URLs (one per line)")
    parser.add_argument("-o", "--output-dir", default="output", help="Directory to save segments")
    parser.add_argument("-v", "--output-video", default="output_video.mp4", help="Output video filename")
    parser.add_argument("-t", "--threads", type=int, default=10, help="Max concurrent downloads")
    parser.add_argument("--ffmpeg", help="Path to FFmpeg executable")
    parser.add_argument("--keep-segments", action="store_true", help="Keep TS files")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()

async def main_async():
    args = parse_arguments()
    logger.setLevel(getattr(logging, args.log_level.upper()))
    if args.ffmpeg: config.ffmpeg_path = args.ffmpeg

    urls = []
    if args.url: urls.append(args.url)
    elif args.file:
        with open(args.file, "r") as f:
            urls = [line.strip() for line in f if line.strip()]
    else:
        # 你提到的测试 URL
        urls = ["https://video-cf.xhcdn.com/vAcKOfcGS10P5OMcpJXE2crvPZKguLI25r0SNAEsv4s%3D/104/1769886000/media=hls4/multi=256x144:144p:,426x240:240p:,854x480:480p:,1280x720:720p:,1920x1080:1080p:/026/042/423/1080p.av1.mp4.m3u8"]

    os.makedirs(args.output_dir, exist_ok=True)
    
    tasks = []
    for i, url in enumerate(urls):
        out_name = args.output_video if len(urls) == 1 else f"video_{i}.mp4"
        tasks.append(process_url(url, args.output_dir, out_name, args.threads, keep_segments=args.keep_segments))
    
    results = await asyncio.gather(*tasks)
    logger.info(f"Total: {len(urls)}, Succeeded: {results.count(True)}, Failed: {results.count(False)}")

def main():
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()