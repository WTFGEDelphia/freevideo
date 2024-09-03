import os
import requests
import m3u8
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def check_url(url):
    try:
        response = requests.head(url, allow_redirects=True)
        return response.status_code == 200
    except requests.RequestException as e:
        print(f"URL {url} is not accessible: {e}")
        return False

def download_segment(segment_url, segment_file_path):
    if os.path.exists(segment_file_path) and os.path.getsize(segment_file_path) > 0:
        print(f"Segment {segment_file_path} already exists. Skipping download.")
        return

    if not check_url(segment_url):
        print(f"Segment URL not accessible: {segment_url}")
        return

    try:
        segment_response = requests.get(segment_url, stream=True)
        if segment_response.status_code == 200:
            with open(segment_file_path, "wb") as segment_file:
                for chunk in segment_response.iter_content(chunk_size=8192):
                    if chunk:
                        segment_file.write(chunk)
            print(f"Downloaded {segment_file_path}")
        else:
            print(
                f"Failed to download segment {segment_url}: HTTP {segment_response.status_code}"
            )
    except Exception as e:
        print(f"Error downloading {segment_file_path}: {e}")
        if os.path.exists(segment_file_path):
            os.remove(segment_file_path)
        raise

def download_m3u8(url, output_dir, max_workers=10):
    if not check_url(url):
        print(f"M3U8 URL not accessible: {url}")
        return

    # 获取m3u8文件内容
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch M3U8 URL {url}: HTTP {response.status_code}")
        return

    m3u8_obj = m3u8.loads(response.text)

    # 创建存放ts片段的目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 下载每个片段
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, segment in enumerate(m3u8_obj.segments):
            segment_url = segment.uri
            if not segment_url.startswith("http"):
                segment_url = os.path.dirname(url) + "/" + segment_url
            segment_file_path = os.path.join(output_dir, f"index{i}.ts")
            futures.append(
                executor.submit(download_segment, segment_url, segment_file_path)
            )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred: {e}")


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
    print(f"Merged {len(ts_files)} files into {output_file}")


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


def process_m3u8_url(m3u8_url, output_dir, output_file):
    print(f"Processing URL: {m3u8_url}")
    # download_m3u8(m3u8_url, output_dir)
    merge_ts_files(output_dir, output_file)
    print(f"Finished processing URL: {m3u8_url}")


def process_m3u8_urls(m3u8_urls, output_dirs, output_files, max_threads=1):
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for m3u8_url, output_dir, output_file in zip(
            m3u8_urls, output_dirs, output_files
        ):
            futures.append(
                executor.submit(process_m3u8_url, m3u8_url, output_dir, output_file)
            )

        for future in as_completed(futures):
            future.result()


def main():
    # 示例数据
    # m3u8_urls = [
    #     "https://tenxun.aly.k6yakjux.cc/videos/202004/28/5e7e1f2a9966ef7c73b12237/186beb/index.m3u8",
    # ]

    m3u8_urls = [
        "https://tenxun.aly.k6yakjux.cc/videos/202209/27/633012e2f4c8b66b990224a9/920b27/index.m3u8",
    ]
    output_dirs = [f"output_segments_{i}" for i in range(len(m3u8_urls))]
    output_files = [f"output_video_{i}.mp4" for i in range(len(m3u8_urls))]
    max_threads = len(m3u8_urls)  # 最大线程数

    # 处理多个 M3U8 URL
    process_m3u8_urls(m3u8_urls, output_dirs, output_files, max_threads)


if __name__ == "__main__":
    main()
