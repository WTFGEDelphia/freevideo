import os
import re
import asyncio
import aiofiles
import m3u8
import subprocess
import logging
import platform
import argparse
from urllib.parse import urlparse
from curl_cffi import AsyncSession
from tqdm import tqdm

# missav.ws 链路：页面 -> surrit.com/playlist.m3u8 (master, 诱饵 video*.jpeg) ->
# edge-hls.saawsedge.com/.../master/{id}_{q}.m3u8 (master, 含 #EXT-X-MOUFLON pkey) ->
# media-hls.saawsedge.com/.../{id}_{q}.m3u8 (media, fMP4 with EXT-X-MAP init segment)
# 实测：分片 / init / media playlist 均不需要 pkey，但需要 Referer。
# surrit.com 在 Cloudflare 后，需用 curl_cffi 模拟 Chrome TLS 指纹绕过 JS Challenge。
DEFAULT_REFERER = "https://creative.mavrtracktor.com/"
DEFAULT_IMPERSONATE = "chrome124"


class Config:
    def __init__(self):
        self.log_level = logging.INFO
        self.log_format = "%(asctime)s - %(levelname)s - %(message)s"
        self.log_file = "download.log"
        self.chunk_size = 1024 * 64
        self.min_file_size = 100  # fMP4 片段可能很小
        self.max_retries = 5
        self.retry_delay = 2
        self.timeout = 60
        self.max_connections = 10
        self.ffmpeg_path = self._get_ffmpeg_path()
        self.referer = DEFAULT_REFERER
        self.impersonate = DEFAULT_IMPERSONATE
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": self.referer,
            "Connection": "keep-alive",
            "sec-ch-ua-platform": '"macOS"',
        }

    def _get_ffmpeg_path(self):
        if platform.system() == "Windows":
            return "ffmpeg.exe"
        return "ffmpeg"


config = Config()

# 匹配 missav/saawsedge master playlist 里的自定义签名标签：#EXT-X-MOUFLON:PSCH:v2:{pkey}
MOUFLON_RE = re.compile(r"#EXT-X-MOUFLON:PSCH:v2:([A-Za-z0-9]+)")


def extract_mouflon_pkeys(content):
    """从 master playlist 文本中提取所有 #EXT-X-MOUFLON pkey 候选。"""
    return MOUFLON_RE.findall(content)


def apply_pkey(url, pkey):
    """若 URL 无 query 且提供了 pkey，拼接 ?psch=v2&pkey=...（兼容性处理，当前 CDN 不强制）。"""
    if not pkey or not url:
        return url
    if urlparse(url).query:
        return url
    return f"{url}?psch=v2&pkey={pkey}"


def setup_logging():
    logging.basicConfig(
        level=config.log_level,
        format=config.log_format,
        handlers=[logging.StreamHandler(), logging.FileHandler(config.log_file)],
    )
    return logging.getLogger(__name__)


logger = setup_logging()


async def download_file(session, semaphore, url, file_path, desc="File"):
    """通用下载函数。404/403 视为永久失败（live 滑窗中片段已滑出或 CF 拦截），不再重试。"""
    if os.path.exists(file_path) and os.path.getsize(file_path) > config.min_file_size:
        return True

    async with semaphore:
        for attempt in range(config.max_retries):
            try:
                response = await session.get(url, headers=config.headers, timeout=config.timeout)
                if response.status_code == 200:
                    async with aiofiles.open(file_path, "wb") as f:
                        await f.write(response.content)
                    return True
                if response.status_code in (403, 404):
                    logger.error(f"{response.status_code} on {desc}: {url}")
                    return False
            except Exception as e:
                if attempt == config.max_retries - 1:
                    logger.error(f"Failed to download {desc}: {e}")
                await asyncio.sleep(config.retry_delay)
    return False


async def download_m3u8(url_str, output_dir, max_workers, quality=None):
    os.makedirs(output_dir, exist_ok=True)
    semaphore = asyncio.Semaphore(max_workers)

    async with AsyncSession(impersonate=config.impersonate) as session:
        # 1. 获取 M3U8
        response = await session.get(url_str, headers=config.headers, timeout=config.timeout)
        if response.status_code != 200:
            logger.error(f"Failed to fetch M3U8: HTTP {response.status_code} for {url_str}")
            return False
        content = response.text

        # missav master 含 #EXT-X-MOUFLON pkey，提取以备子 playlist 拼接
        pkeys = extract_mouflon_pkeys(content)
        pkey = pkeys[0] if pkeys else None
        if pkey:
            logger.info(f"Extracted MOUFLON pkey: {pkey}")

        m3u8_obj = m3u8.loads(content, uri=url_str)

        # 2. 处理 Master Playlist
        if m3u8_obj.playlists:
            chosen = choose_playlist(m3u8_obj.playlists, quality)
            sub_url = chosen.absolute_uri
            logger.info(f"Master playlist -> selecting {chosen.stream_info.resolution or 'auto'} (bw={chosen.stream_info.bandwidth})")
            return await download_m3u8(sub_url, output_dir, max_workers, quality=None)

        # 3. 关键：处理 fMP4 初始化段 (EXT-X-MAP)
        if m3u8_obj.segment_map:
            init_url = m3u8_obj.segment_map[0].absolute_uri
            logger.info(f"fMP4 detected. Downloading init segment...")
            init_path = os.path.join(output_dir, "init.mp4")
            ok = await download_file(session, semaphore, init_url, init_path, "Init Segment")
            if not ok:
                logger.error("Failed to download init segment; aborting.")
                return False

        if not m3u8_obj.segments:
            logger.error("No segments found in m3u8!")
            return False

        # 4. 下载片段
        tasks = []
        with tqdm(total=len(m3u8_obj.segments), desc="Downloading", unit="seg") as pbar:
            for i, segment in enumerate(m3u8_obj.segments):
                seg_url = segment.absolute_uri
                # 注意：即便后缀是 .ts，fMP4 内部也是 mp4 数据
                seg_path = os.path.join(output_dir, f"index{i:05d}.ts")

                async def task_wrapper(s, sem, u, p, idx, pb):
                    res = await download_file(s, sem, u, p, f"Seg {idx}")
                    if pb:
                        pb.update(1)
                    return res

                tasks.append(task_wrapper(session, semaphore, seg_url, seg_path, i, pbar))

            results = await asyncio.gather(*tasks)
            return all(results)


def choose_playlist(playlists, quality):
    """根据 quality（如 '720p'）选择子流，否则取最高带宽。"""
    if quality:
        q = quality.lower().strip()
        for p in playlists:
            name = (getattr(p.stream_info, "name", None) or "").lower()
            res = p.stream_info.resolution
            res_str = f"{res[1]}p" if res else ""
            if q in name or q == res_str:
                return p
        logger.warning(f"Quality '{quality}' not found, falling back to highest bandwidth.")
    playlists.sort(key=lambda p: p.stream_info.bandwidth or 0, reverse=True)
    return playlists[0]


# --- 合并函数 ---
def ffmpeg_merge(output_dir, output_file):
    abs_dir = os.path.abspath(output_dir)
    ts_files = sorted([f for f in os.listdir(abs_dir) if f.startswith("index") and f.endswith(".ts")])
    if not ts_files:
        return False

    init_file = os.path.join(abs_dir, "init.mp4")
    has_init = os.path.exists(init_file)

    # fMP4：二进制拼接 init + segments，再用 FFmpeg 重封修复时间戳/元数据
    temp_combined = os.path.join(abs_dir, "combined_raw.mp4")

    try:
        logger.info("Performing binary concatenation...")
        with open(temp_combined, "wb") as outfile:
            if has_init:
                with open(init_file, "rb") as infile:
                    outfile.write(infile.read())
            for ts in ts_files:
                with open(os.path.join(abs_dir, ts), "rb") as infile:
                    outfile.write(infile.read())

        logger.info("Fixing container with FFmpeg...")
        output_path = os.path.join(abs_dir, output_file)

        cmd = [
            config.ffmpeg_path, "-i", temp_combined,
            "-c", "copy", "-movflags", "+faststart", "-y", output_path
        ]

        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode == 0:
            if os.path.exists(temp_combined):
                os.remove(temp_combined)
            logger.info(f"Successfully merged: {output_file}")
            return True
        else:
            logger.error(f"FFmpeg Error: {res.stderr}")
            return False
    except Exception as e:
        logger.error(f"Merge exception: {e}")
        return False


async def process_url(url, output_root, output_video, max_workers, keep_segments=False):
    url_id = re.sub(r"[^\w]", "_", url)[-40:]
    output_dir = os.path.join(output_root, url_id)

    if await download_m3u8(url, output_dir, max_workers):
        if ffmpeg_merge(output_dir, output_video):
            if not keep_segments:
                for f in os.listdir(output_dir):
                    if f.endswith(".ts") or f == "filelist.txt" or f == "init.mp4" or f == "combined_raw.mp4":
                        try:
                            os.remove(os.path.join(output_dir, f))
                        except OSError:
                            pass
            return True
    return False


async def main_async():
    parser = argparse.ArgumentParser(description="fMP4 M3U8 downloader (missav/saawsedge/surrit compatible)")
    parser.add_argument("-u", "--url", help="M3U8 URL (master or media)")
    parser.add_argument("-o", "--output-dir", default="output")
    parser.add_argument("-v", "--output-video", default="final_video.mp4")
    parser.add_argument("-t", "--threads", type=int, default=10)
    parser.add_argument("-q", "--quality", help="Quality hint, e.g. 720p / 480p / 240p (master playlist only)")
    parser.add_argument("--referer", default=DEFAULT_REFERER, help=f"Referer header (default: {DEFAULT_REFERER})")
    parser.add_argument("--impersonate", default=DEFAULT_IMPERSONATE,
                        help=f"curl_cffi TLS fingerprint to bypass Cloudflare (default: {DEFAULT_IMPERSONATE}; e.g. chrome120, chrome116, chrome110)")
    parser.add_argument("--keep-segments", action="store_true")
    args = parser.parse_args()

    if args.referer:
        config.headers["Referer"] = args.referer
    config.impersonate = args.impersonate

    url = args.url or "https://edge-hls.saawsedge.com/hls/243097153/master/243097153_480p.m3u8"

    await process_url(url, args.output_dir, args.output_video, args.threads, keep_segments=args.keep_segments)


if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main_async())
