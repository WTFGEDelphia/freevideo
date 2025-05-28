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
import tempfile
import shutil
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


# 配置类，集中管理所有配置参数
class Config:
    def __init__(self):
        self.log_level = logging.DEBUG
        self.log_format = "%(asctime)s - %(levelname)s - %(message)s"
        self.log_file = "download.log"
        self.chunk_size = 8192
        self.min_file_size = 100 * 1024  # 100KB
        self.max_retries = 3
        self.retry_delay = 1  # 秒
        self.timeout = 30  # 秒
        self.max_connections = 10
        self.ffmpeg_path = self._get_ffmpeg_path()

    def _get_ffmpeg_path(self):
        """根据操作系统获取FFmpeg路径"""
        if platform.system() == "Windows":
            # 尝试在PATH中查找ffmpeg
            for path in os.environ["PATH"].split(os.pathsep):
                ffmpeg_path = os.path.join(path, "ffmpeg.exe")
                if os.path.isfile(ffmpeg_path):
                    return ffmpeg_path
            # 如果PATH中没有，返回默认值，用户可以通过命令行参数覆盖
            return "ffmpeg.exe"
        else:
            # 在类Unix系统中，通常在/usr/bin或/usr/local/bin
            for path in ["/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg"]:
                if os.path.isfile(path):
                    return path
            return "ffmpeg"  # 默认假设ffmpeg在PATH中


# 初始化配置
config = Config()


# 设置日志
def setup_logging():
    logging.basicConfig(
        level=config.log_level,
        format=config.log_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(config.log_file),
        ],
    )
    return logging.getLogger(__name__)


logger = setup_logging()


# URL处理函数
async def check_url(session, url):
    """检查URL是否可访问"""
    try:
        async with session.head(
            url, allow_redirects=True, timeout=config.timeout
        ) as response:
            return response.status == 200
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(f"URL {url} is not accessible: {e}")
        return False


def get_base_url(url):
    """获取基础URL，用于处理相对路径"""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{os.path.dirname(parsed.path)}/"


def get_bandwidth(playlist):
    """获取播放列表的带宽信息"""
    if playlist.stream_info is None:
        return -1
    return playlist.stream_info.bandwidth or -1


# 下载函数
async def download_segment(session, segment_url, segment_file_path, pbar=None):
    """下载单个视频片段，带有重试机制"""
    # 检查文件是否已存在且大小合适
    if (
        os.path.exists(segment_file_path)
        and os.path.getsize(segment_file_path) > config.min_file_size
    ):
        logger.debug(f"Segment {segment_file_path} already exists. Skipping download.")
        if pbar:
            pbar.update(1)
        return True

    # 重试机制
    for attempt in range(config.max_retries):
        try:
            # 检查URL可访问性
            if not await check_url(session, segment_url):
                logger.error(f"Segment URL not accessible: {segment_url}")
                return False

            # 下载片段
            async with session.get(segment_url, timeout=config.timeout) as response:
                if response.status == 200:
                    async with aiofiles.open(segment_file_path, "wb") as segment_file:
                        async for chunk in response.content.iter_chunked(
                            config.chunk_size
                        ):
                            await segment_file.write(chunk)
                    logger.info(f"Downloaded {segment_file_path}")
                    if pbar:
                        pbar.update(1)
                    return True
                else:
                    logger.error(
                        f"Failed to download segment {segment_url}: HTTP {response.status}"
                    )

            # 如果下载失败但不是因为异常，等待后重试
            time.sleep(config.retry_delay)

        except (aiohttp.ClientError, asyncio.TimeoutError, IOError) as e:
            logger.error(
                f"Error downloading {segment_file_path} (attempt {attempt+1}/{config.max_retries}): {e}"
            )
            # 如果文件已部分下载但出错，删除它
            if os.path.exists(segment_file_path):
                os.remove(segment_file_path)
            # 最后一次尝试失败
            if attempt == config.max_retries - 1:
                return False
            # 否则等待后重试
            time.sleep(config.retry_delay)

    return False


async def download_m3u8(url, output_dir, max_workers=None):
    """下载M3U8播放列表及其所有片段"""
    if max_workers is None:
        max_workers = config.max_connections

    logger.info("\n=== Processing Master Playlist ===")
    logger.info(f"Master playlist URL: {url}")

    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    # 创建HTTP会话 禁用SSL验证
    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        # 检查URL可访问性
        if not await check_url(session, url):
            logger.error(f"M3U8 URL not accessible: {url}")
            return False

        base_url = get_base_url(url)
        logger.info(f"Base URL: {base_url}")

        # 获取主播放列表内容
        try:
            async with session.get(url, timeout=config.timeout) as response:
                if response.status != 200:
                    logger.error(
                        f"Failed to fetch M3U8 URL {url}: HTTP {response.status}"
                    )
                    return False
                content = await response.text()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Error fetching M3U8 URL {url}: {e}")
            return False

        # 打印原始内容
        logger.debug("\nMaster playlist content:")
        logger.debug(content)

        # 解析M3U8内容
        m3u8_obj = m3u8.loads(content)

        # 打印解析后的内容
        logger.debug("\nParsed m3u8 object:")
        logger.debug(f"Segments: {m3u8_obj.segments}")
        logger.debug(f"Playlists: {m3u8_obj.playlists}")

        # 处理主播放列表
        if not m3u8_obj.is_endlist and m3u8_obj.playlists:
            logger.info("This is a master playlist")
            logger.debug("\n=== Available Quality Streams ===")

            # 显示所有可用的质量流
            for i, playlist in enumerate(m3u8_obj.playlists):
                bandwidth = (
                    playlist.stream_info.bandwidth if playlist.stream_info else 0
                )
                resolution = (
                    playlist.stream_info.resolution
                    if playlist.stream_info
                    else "unknown"
                )
                logger.debug(
                    f"Stream {i}: Bandwidth={bandwidth}, Resolution={resolution}, URI={playlist.uri}"
                )

            # 获取质量最高的流
            selected_playlist = sorted(
                m3u8_obj.playlists, key=get_bandwidth, reverse=True
            )[0]
            logger.debug(f"\nSelected highest quality stream: {selected_playlist.uri}")
            logger.debug(f"Bandwidth: {selected_playlist.stream_info.bandwidth}")
            logger.debug(f"Resolution: {selected_playlist.stream_info.resolution}")

            # 构建子播放列表的完整URL
            sub_playlist_url = urljoin(base_url, selected_playlist.uri)
            logger.info(f"\n=== Processing Media Playlist ===")
            logger.info(f"Media playlist URL: {sub_playlist_url}")

            # 获取子播放列表内容
            try:
                async with session.get(
                    sub_playlist_url, timeout=config.timeout
                ) as sub_response:
                    if sub_response.status != 200:
                        logger.error(
                            f"Failed to fetch sub-playlist: HTTP {sub_response.status}"
                        )
                        return False
                    sub_content = await sub_response.text()

                    # 打印子播放列表内容
                    logger.debug("\nMedia playlist content:")
                    logger.debug(sub_content)

                    # 更新base_url为子播放列表的URL
                    base_url = get_base_url(sub_playlist_url)
                    m3u8_obj = m3u8.loads(sub_content)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f"Error fetching sub-playlist {sub_playlist_url}: {e}")
                return False
        else:
            logger.info("This is a media playlist")

        # 确保我们有片段可以下载
        if not m3u8_obj.segments:
            logger.error("No segments found in the m3u8 file!")
            return False

        # 下载所有片段
        segment_count = len(m3u8_obj.segments)
        logger.info(f"Found {segment_count} segments to download")

        # 创建进度条
        with tqdm(
            total=segment_count, desc="Downloading segments", unit="segment"
        ) as pbar:
            # 准备下载任务
            tasks = []
            for i, segment in enumerate(m3u8_obj.segments):
                segment_url = segment.uri
                if segment_url is None:
                    logger.error(f"Segment {i} has no URI")
                    continue

                # 处理相对URL
                if not segment_url.startswith("http"):
                    segment_url = urljoin(base_url, segment_url)

                segment_file_path = os.path.join(output_dir, f"index{i}.ts")
                tasks.append(
                    download_segment(session, segment_url, segment_file_path, pbar)
                )

            # 并发下载所有片段
            results = await asyncio.gather(*tasks)

            # 检查是否所有片段都下载成功
            if all(results):
                logger.info(f"Successfully downloaded all {segment_count} segments")
                return True
            else:
                failed_count = results.count(False)
                logger.error(
                    f"Failed to download {failed_count} out of {segment_count} segments"
                )
                return False


# 合并函数
def merge_ts_files(output_dir, output_file, chunk_size=1024 * 1024):
    """使用分块读写合并TS文件，减少内存占用"""
    # 获取并排序所有TS文件
    ts_files = sorted(
        [
            os.path.join(output_dir, f)
            for f in os.listdir(output_dir)
            if f.endswith(".ts")
        ],
        key=lambda x: int(os.path.splitext(os.path.basename(x))[0].split("index")[1]),
    )

    if not ts_files:
        logger.error(f"No .ts files found in {output_dir}")
        return False

    # 创建输出文件的完整路径
    output_path = (
        os.path.join(output_dir, output_file)
        if not os.path.isabs(output_file)
        else output_file
    )

    # 使用进度条显示合并进度
    total_size = sum(os.path.getsize(f) for f in ts_files)
    with tqdm(
        total=total_size, desc="Merging files", unit="B", unit_scale=True
    ) as pbar:
        try:
            with open(output_path, "wb") as merged_file:
                for ts_file in ts_files:
                    file_size = os.path.getsize(ts_file)
                    with open(ts_file, "rb") as segment_file:
                        # 分块读写，减少内存占用
                        bytes_read = 0
                        while bytes_read < file_size:
                            chunk = segment_file.read(chunk_size)
                            if not chunk:
                                break
                            merged_file.write(chunk)
                            bytes_read += len(chunk)
                            pbar.update(len(chunk))

            logger.info(f"Merged {len(ts_files)} files into {output_path}")
            return True
        except IOError as e:
            logger.error(f"Error merging files: {e}")
            # 如果合并失败，删除可能已部分写入的输出文件
            if os.path.exists(output_path):
                os.remove(output_path)
            return False


def ffmpeg_merge_ts_files(output_dir, output_file):
    """使用FFmpeg合并TS文件，支持跨平台"""
    # 获取输出目录的绝对路径
    absolute_path = os.path.abspath(output_dir)

    # 获取并排序所有TS文件
    ts_files = sorted(
        [
            os.path.join(absolute_path, f)
            for f in os.listdir(absolute_path)
            if f.endswith(".ts")
        ],
        key=lambda x: int(os.path.splitext(os.path.basename(x))[0].split("index")[1]),
    )

    if not ts_files:
        logger.error(f"No .ts files found in {absolute_path}")
        return False

    # 创建临时文件列表
    temp_dir = tempfile.mkdtemp()
    filelist_path = os.path.join(temp_dir, "filelist.txt")

    try:
        # 写入文件列表
        with open(filelist_path, "w", encoding="utf-8") as filelist:
            for ts_file in ts_files:
                # 使用正确的路径格式，处理Windows路径中的反斜杠
                safe_path = (
                    ts_file.replace("\\", "/")
                    if platform.system() == "Windows"
                    else ts_file
                )
                filelist.write(f"file '{safe_path}'\n")

        # with open("filelist.txt", "w") as filelist:
        #     for ts_file in ts_files:
        #         filelist.write(f"file '{ts_file}'\n")

        # 构建输出文件的完整路径
        output_path = (
            os.path.join(absolute_path, output_file)
            if not os.path.isabs(output_file)
            else output_file
        )

        # 构建FFmpeg命令
        cmd = [
            config.ffmpeg_path,
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            filelist_path,
            "-c",
            "copy",
            "-bsf:a",
            "aac_adtstoasc",
            "-movflags",
            "+faststart",
            "-y",
            output_path,
        ]

        # 执行FFmpeg命令
        logger.info(f"Running FFmpeg command: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)

        if result.returncode == 0:
            logger.info(
                f"Successfully merged {len(ts_files)} files into {output_path} using FFmpeg"
            )
            return True
        else:
            logger.error(f"FFmpeg error: {result.stderr}")
            return False

    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg process error: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Error during FFmpeg merge: {e}")
        return False
    finally:
        # 清理临时文件
        try:
            # os.remove(filelist_path)
            # os.rmdir(temp_dir)
            logger.error(f"OK")
        except OSError:
            pass


# 命令行参数解析
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Download and merge M3U8 video segments"
    )
    parser.add_argument("-u", "--url", help="M3U8 URL to download")
    parser.add_argument("-f", "--file", help="File containing M3U8 URLs (one per line)")
    parser.add_argument(
        "-o",
        "--output-dir",
        default="output_segments",
        help="Directory to save segments",
    )
    parser.add_argument(
        "-v", "--output-video", default="output_video.mp4", help="Output video filename"
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        default=5,
        help="Maximum number of concurrent downloads",
    )
    parser.add_argument("--ffmpeg", help="Path to FFmpeg executable")
    parser.add_argument("--no-merge", action="store_true", help="Skip merging step")
    parser.add_argument(
        "--no-ffmpeg", action="store_true", help="Use basic merging instead of FFmpeg"
    )
    parser.add_argument(
        "--keep-segments",
        action="store_true",
        help="Keep downloaded segments after merging",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level",
    )

    return parser.parse_args()


# 主函数
async def process_url(
    url, output_dir, output_file, max_workers, use_ffmpeg=True, keep_segments=False
):
    """处理单个M3U8 URL的完整流程"""
    # 为每个URL创建单独的输出目录
    url_hash = re.sub(r"[^\w]", "_", url)[-20:]  # 使用URL的一部分作为目录名
    url_output_dir = os.path.join(output_dir, url_hash)

    # 下载片段
    logger.info(f"Processing URL: {url}")
    download_success = await download_m3u8(url, url_output_dir, max_workers)

    if not download_success:
        logger.error(f"Failed to download segments for {url}")
        return False

    # 合并片段
    if use_ffmpeg:
        merge_success = ffmpeg_merge_ts_files(url_output_dir, output_file)
    else:
        merge_success = merge_ts_files(url_output_dir, output_file)

    if not merge_success:
        logger.error(f"Failed to merge segments for {url}")
        return False

    # 清理片段文件
    if not keep_segments:
        try:
            for file in os.listdir(url_output_dir):
                if file.endswith(".ts"):
                    os.remove(os.path.join(url_output_dir, file))
            logger.info(f"Cleaned up segment files for {url}")
        except OSError as e:
            logger.warning(f"Error cleaning up segment files: {e}")

    logger.info(f"Successfully processed {url}")
    return True


async def main_async():
    # 解析命令行参数
    args = parse_arguments()

    # 设置日志级别
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if isinstance(numeric_level, int):
        logger.setLevel(numeric_level)

    # 设置FFmpeg路径
    if args.ffmpeg:
        config.ffmpeg_path = args.ffmpeg

    # 获取要处理的URL列表
    urls = []
    if args.url:
        urls.append(args.url)
    elif args.file:
        try:
            with open(args.file, "r") as f:
                urls = [line.strip() for line in f if line.strip()]
        except IOError as e:
            logger.error(f"Error reading URL file: {e}")
            return 1
    else:
        # 使用示例URL
        urls = [
            # "https://v.cdnlz22.com/20250403/15049_29e89b20/2000k/hls/mixed.m3u8",
            "https://tenxun.aly.k6yakjux.cc/videos/202005/30/5ec2937d97e29f27c9b859ab/ec12gg/index.m3u8",
            # 其他示例URL可以在这里添加
        ]
        logger.warning("No URL or file specified, using example URL")

    if not urls:
        logger.error("No URLs to process")
        return 1

    # 创建主输出目录
    os.makedirs(args.output_dir, exist_ok=True)

    # 处理所有URL
    tasks = []
    for i, url in enumerate(urls):
        # 为每个URL生成唯一的输出文件名
        if len(urls) > 1:
            output_file = f"output_video_{i+1}.mp4"
        else:
            output_file = args.output_video

        tasks.append(
            process_url(
                url,
                args.output_dir,
                output_file,
                args.threads,
                use_ffmpeg=not args.no_ffmpeg,
                keep_segments=args.keep_segments,
            )
        )

    # 并发处理所有URL
    results = await asyncio.gather(*tasks)

    # 统计结果
    success_count = results.count(True)
    logger.info(
        f"Processed {len(urls)} URLs: {success_count} succeeded, {len(urls) - success_count} failed"
    )

    return 0 if all(results) else 1


def main():
    # 在Windows上需要使用特定的事件循环策略
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        return 130  # 标准的SIGINT退出码
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
