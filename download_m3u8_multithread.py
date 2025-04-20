import os
import requests
import m3u8
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

import logging
from urllib.parse import urljoin, urlparse

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # 输出到控制台
        logging.FileHandler("download.log"),  # 同时写入文件
    ],
)

logger = logging.getLogger(__name__)


def check_url(url):
    try:
        response = requests.head(url, allow_redirects=True)
        return response.status_code == 200
    except requests.RequestException as e:
        logger.error(f"URL {url} is not accessible: {e}")
        return False


def download_segment(segment_url, segment_file_path):
    if os.path.exists(segment_file_path) and os.path.getsize(segment_file_path) > 0:
        logger.warn(f"Segment {segment_file_path} already exists. Skipping download.")
        return

    if not check_url(segment_url):
        logger.error(f"Segment URL not accessible: {segment_url}")
        return

    try:
        segment_response = requests.get(segment_url, stream=True)
        if segment_response.status_code == 200:
            with open(segment_file_path, "wb") as segment_file:
                for chunk in segment_response.iter_content(chunk_size=8192):
                    if chunk:
                        segment_file.write(chunk)
            logger.info(f"Downloaded {segment_file_path}")
        else:
            logger.error(
                f"Failed to download segment {segment_url}: HTTP {segment_response.status_code}"
            )
    except Exception as e:
        logger.error(f"Error downloading {segment_file_path}: {e}")
        if os.path.exists(segment_file_path):
            os.remove(segment_file_path)
        raise


def get_base_url(url):
    """获取基础URL，用于处理相对路径"""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{os.path.dirname(parsed.path)}/"


def get_bandwidth(playlist):
    if playlist.stream_info is None:
        return -1
    return playlist.stream_info.bandwidth or -1


def download_m3u8(url, output_dir, max_workers=5):
    logger.info("\n=== Processing Master Playlist ===")
    logger.info(f"Master playlist URL: {url}")

    if not check_url(url):
        logger.error(f"M3U8 URL not accessible: {url}")
        return

    base_url = get_base_url(url)
    logger.info(f"Base URL: {base_url}")

    # 获取主播放列表内容
    response = requests.get(url)
    if response.status_code != 200:
        logger.error(f"Failed to fetch M3U8 URL {url}: HTTP {response.status_code}")
        return

    # 打印原始内容
    logger.debug("\nMaster playlist content:")
    logger.debug(response.text)

    m3u8_obj = m3u8.loads(response.text)

    # 打印解析后的内容
    logger.debug("\nParsed m3u8 object:")
    logger.debug(f"Segments: {m3u8_obj.segments}")
    logger.debug(f"Playlists: {m3u8_obj.playlists}")

    # 如果是主播放列表（Master Playlist）
    if m3u8_obj.is_endlist:
        logger.info("This is a media playlist")
    else:
        logger.info("This might be a master playlist")
        if m3u8_obj.playlists:
            logger.debug("\n=== Available Quality Streams ===")
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
                m3u8_obj.playlists,
                key=get_bandwidth,
                reverse=True,
            )[0]
            logger.debug(f"\nSelected highest quality stream: {selected_playlist.uri}")
            logger.debug(f"Bandwidth: {selected_playlist.stream_info.bandwidth}")
            logger.debug(f"Resolution: {selected_playlist.stream_info.resolution}")

            # 构建子播放列表的完整URL
            sub_playlist_url = urljoin(base_url, selected_playlist.uri)
            logger.info(f"\n=== Processing Media Playlist ===")
            logger.info(f"Media playlist URL: {sub_playlist_url}")
            sub_response = requests.get(sub_playlist_url)
            if sub_response.status_code == 200:
                logger.debug("\nMedia playlist content:")
                logger.debug(sub_response.text)
                # 更新base_url为子播放列表的URL
                base_url = get_base_url(sub_playlist_url)
                m3u8_obj = m3u8.loads(sub_response.text)
            else:
                logger.error(
                    f"Failed to fetch sub-playlist: HTTP {sub_response.status_code}"
                )
                return

    # 创建存放ts片段的目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 确保我们有片段可以下载
    if not m3u8_obj.segments:
        logger.error("No segments found in the m3u8 file!")
        return

    # 创建存放ts片段的目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 下载每个片段
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, segment in enumerate(m3u8_obj.segments):
            segment_url = segment.uri
            if segment_url is None:
                logger.error(f"Segment {i} has no URI")
                continue
            if not segment_url.startswith("http"):
                segment_url = os.path.dirname(base_url) + "/" + segment_url
            segment_file_path = os.path.join(output_dir, f"index{i}.ts")
            futures.append(
                executor.submit(download_segment, segment_url, segment_file_path)
            )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error occurred: {e}")


def merge_ts_files(output_dir, output_file):
    ts_files = sorted(
        [
            os.path.join(output_dir, f)
            for f in os.listdir(output_dir)
            if f.endswith(".ts")
        ],
        key=lambda x: int(os.path.splitext(os.path.basename(x))[0].split("index")[1]),
    )

    with open(output_file, "wb") as merged_file:
        for ts_file in ts_files:
            with open(ts_file, "rb") as segment_file:
                merged_file.write(segment_file.read())
    logger.info(f"Merged {len(ts_files)} files into {output_file}")


def ffmpeg_merge_ts_files(output_dir, output_file):
    absolute_path = os.path.abspath(output_dir)
    ts_files = sorted(
        [
            os.path.join(absolute_path, f)
            for f in os.listdir(absolute_path)
            if f.endswith(".ts")
        ],
        key=lambda x: int(os.path.splitext(os.path.basename(x))[0].split("index")[1]),
    )

    with open("filelist.txt", "w") as filelist:
        for ts_file in ts_files:
            filelist.write(f"file '{ts_file}'\n")
    # 使用 ffmpeg 合并 .ts 文件
    cmd = [
        "D:\\ProgramData\\ffmpeg-6.1.1-full_build\\bin\\ffmpeg.exe",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        "filelist.txt",
        "-c",
        "copy",
        os.path.join(absolute_path, output_file),
    ]
    subprocess.run(cmd, check=False)


def main():
    # 使用示例
    # m3u8_url = "https://tenxun.aly.k6yakjux.cc/videos/202005/30/5ec2937d97e29f27c9b859ab/ec12gg/index.m3u8"
    m3u8_url = "https://v.cdnlz3.com/20241015/27930_52f4b5be/index.m3u8"
    # m3u8_url = "https://v.cdnlz3.com/20241015/27930_52f4b5be/2000k/hls/mixed.m3u8"
    output_directory = "output_segments"
    output_file = "output_video.mp4"
    max_threads = 1  # 最大线程数

    download_m3u8(m3u8_url, output_directory, max_workers=max_threads)
    # merge_ts_files(output_directory, output_file)
    # ffmpeg_merge_ts_files(output_directory, output_file)


if __name__ == "__main__":
    main()
