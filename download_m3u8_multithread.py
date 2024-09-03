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


def download_m3u8(url, output_dir, max_workers=5):
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


def main():
    # 使用示例
    m3u8_url = "https://tenxun.aly.k6yakjux.cc/videos/202005/30/5ec2937d97e29f27c9b859ab/ec12gg/index.m3u8"
    output_directory = "output_segments"
    output_file = "output_video.mp4"
    max_threads = 10  # 最大线程数

    download_m3u8(m3u8_url, output_directory, max_workers=max_threads)
    merge_ts_files(output_directory, output_file)
    ffmpeg_merge_ts_files(output_directory, output_file)


if __name__ == "__main__":
    main()
