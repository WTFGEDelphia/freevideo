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
from urllib.parse import urljoin, urlparse
from tqdm import tqdm
from Crypto.Cipher import AES

# 配置类
class Config:
    def __init__(self):
        self.log_level = logging.INFO
        self.log_format = "%(asctime)s - %(levelname)s - %(message)s"
        self.log_file = "download.log"
        self.chunk_size = 1024 * 16  # 16KB
        self.min_file_size = 512     # 降低门槛，只要大于512字节就认为可能是有效片段
        self.max_retries = 5
        self.retry_delay = 2
        self.timeout = 60
        self.max_connections = 10    # 同时下载的最大连接数
        self.ffmpeg_path = self._get_ffmpeg_path()

    def _get_ffmpeg_path(self):
        if platform.system() == "Windows":
            for path in os.environ["PATH"].split(os.pathsep):
                ffmpeg_path = os.path.join(path, "ffmpeg.exe")
                if os.path.isfile(ffmpeg_path): return ffmpeg_path
            return "ffmpeg.exe"
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

# --- 新增：解密工具函数 ---
async def get_decryptor(session, key_info, base_url):
    """获取解密器"""
    if not key_info or key_info.method == "NONE":
        return None
    
    key_url = urljoin(base_url, key_info.uri)
    logger.info(f"Fetching decryption key from: {key_url}")
    try:
        async with session.get(key_url, timeout=10) as response:
            if response.status == 200:
                key_bytes = await response.read()
                # IV 默认使用 key_info.iv，如果没有则在下载片段时由序号生成
                iv = bytes.fromhex(key_info.iv.replace("0x", "")) if key_info.iv else None
                return {"key": key_bytes, "iv": iv, "method": key_info.method}
    except Exception as e:
        logger.error(f"Failed to fetch key: {e}")
    return None

def decrypt_data(data, key_dict, index):
    """AES-128 解密逻辑"""
    if not key_dict:
        return data
    
    # 如果 M3U8 没有提供 IV，则使用片段索引作为 IV (16字节)
    iv = key_dict["iv"] if key_dict["iv"] else index.to_bytes(16, 'big')
    cipher = AES.new(key_dict["key"], AES.MODE_CBC, iv=iv)
    return cipher.decrypt(data)

# --- 核心下载函数 ---
async def download_segment(session, semaphore, segment_url, segment_file_path, index, key_dict=None, pbar=None):
    """带解密功能的下载"""
    if os.path.exists(segment_file_path) and os.path.getsize(segment_file_path) > config.min_file_size:
        if pbar: pbar.update(1)
        return True

    async with semaphore:
        for attempt in range(config.max_retries):
            try:
                async with session.get(segment_url, timeout=config.timeout) as response:
                    if response.status == 200:
                        content = await response.read()
                        
                        # 如果需要解密
                        if key_dict:
                            content = decrypt_data(content, key_dict, index)
                        
                        async with aiofiles.open(segment_file_path, "wb") as f:
                            await f.write(content)
                        
                        if pbar: pbar.update(1)
                        return True
                    elif response.status == 404:
                        logger.error(f"Segment not found (404): {segment_url}")
                        return False
            except Exception as e:
                if attempt == config.max_retries - 1:
                    logger.error(f"Failed to download {segment_url} after {config.max_retries} attempts: {e}")
                await asyncio.sleep(config.retry_delay)
    return False

async def download_m3u8(url, output_dir):
    """主下载流程"""
    os.makedirs(output_dir, exist_ok=True)
    conn = aiohttp.TCPConnector(ssl=False)
    semaphore = asyncio.Semaphore(config.max_connections)

    async with aiohttp.ClientSession(connector=conn) as session:
        # 1. 获取 M3U8 内容
        async with session.get(url, timeout=config.timeout) as response:
            if response.status != 200: return False
            m3u8_content = await response.text()
        
        m3u8_obj = m3u8.loads(m3u8_content)
        base_url = urljoin(url, ".")

        # 2. 处理 Master Playlist (选取最高质量)
        if m3u8_obj.playlists:
            selected = sorted(m3u8_obj.playlists, key=lambda p: p.stream_info.bandwidth or 0, reverse=True)[0]
            return await download_m3u8(urljoin(base_url, selected.uri), output_dir)

        # 3. 处理加密 Key
        key_dict = None
        if m3u8_obj.keys:
            # 简单起见，取第一个 key。实际中可能多个片段对应不同 key，此处可扩展。
            key_dict = await get_decryptor(session, m3u8_obj.keys[0], base_url)

        if not m3u8_obj.segments:
            logger.error("No segments found!")
            return False

        # 4. 创建任务
        segment_count = len(m3u8_obj.segments)
        logger.info(f"Found {segment_count} segments. Decryption: {'ON' if key_dict else 'OFF'}")
        
        with tqdm(total=segment_count, desc="Downloading", unit="seg") as pbar:
            tasks = []
            for i, segment in enumerate(m3u8_obj.segments):
                seg_url = urljoin(base_url, segment.uri)
                file_path = os.path.join(output_dir, f"index{i}.ts")
                tasks.append(download_segment(session, semaphore, seg_url, file_path, i, key_dict, pbar))
            
            results = await asyncio.gather(*tasks)
            return all(results)

# --- 合并函数 ---
def ffmpeg_merge(output_dir, output_file):
    """使用 FFmpeg 合并 TS 片段"""
    abs_dir = os.path.abspath(output_dir)
    ts_files = sorted(
        [f for f in os.listdir(abs_dir) if f.endswith(".ts")],
        key=lambda x: int(re.findall(r'\d+', x)[0])
    )

    if not ts_files: return False

    filelist_path = os.path.join(abs_dir, "filelist.txt")
    with open(filelist_path, "w", encoding="utf-8") as f:
        for ts in ts_files:
            f.write(f"file '{os.path.join(abs_dir, ts)}'\n")

    output_path = os.path.join(abs_dir, output_file)
    # 关键点：使用 -i filelist 时，-c copy 是最快的
    cmd = [
        config.ffmpeg_path, "-f", "concat", "-safe", "0", "-i", filelist_path,
        "-c", "copy", "-bsf:a", "aac_adtstoasc", "-movflags", "+faststart", "-y", output_path
    ]

    try:
        logger.info(f"Merging with FFmpeg...")
        process = subprocess.run(cmd, capture_output=True, text=True)
        if process.returncode == 0:
            logger.info(f"Success: {output_path}")
            return True
        else:
            logger.error(f"FFmpeg Error: {process.stderr}")
            return False
    except Exception as e:
        logger.error(f"Merge exception: {e}")
        return False

# --- 主逻辑 ---
async def process_url(url, output_root):
    # 用 URL 的 MD5 或部分字符创建唯一文件夹
    folder_name = re.sub(r'[^\w]', '_', url)[-30:]
    output_dir = os.path.join(output_root, folder_name)
    
    if await download_m3u8(url, output_dir):
        if ffmpeg_merge(output_dir, "final_video.mp4"):
            # 清理 TS 文件
            for f in os.listdir(output_dir):
                if f.endswith(".ts") or f == "filelist.txt":
                    os.remove(os.path.join(output_dir, f))
            return True
    return False

async def main():
    # 示例 URL
    target_url = "https://vodcnd09.rsfcxq.com/20241220/KjSIbsFH/2091kb/hls/index.m3u8"
    await process_url(target_url, "output")

if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
