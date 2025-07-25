import asyncio
import logging
import os
import platform
import argparse
from tqdm import tqdm
from config import config
import downloader
import merger

# Setup logging
logger = logging.getLogger(__name__)

def setup_logging(log_level):
    """Sets up logging and ensures old log file is removed."""
    # Remove old log file if it exists
    if os.path.exists(config.log_file):
        try:
            os.remove(config.log_file)
        except OSError as e:
            print(f"Warning: Could not remove old log file: {e}")

    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(config.log_file)],
    )

async def process_single_url(url, args):
    """Orchestrates the download and merge process for a single URL."""
    logger.info(f"Processing URL: {url}")

    # 1. Get segment count for the progress bar
    total_segments = await downloader.get_segment_count(url)
    if total_segments == 0:
        logger.error(f"Could not retrieve segments for {url}. Skipping.")
        return False

    # Create a unique output directory for this URL's segments
    url_hash = "".join(filter(str.isalnum, url))[-20:]
    segment_dir = os.path.join(args.output_dir, url_hash)
    os.makedirs(segment_dir, exist_ok=True)

    # 2. Download with progress bar
    download_success = False
    with tqdm(total=total_segments, desc=f"Downloading {url_hash}", unit="seg") as pbar:
        download_success = await downloader.download_m3u8(
            url,
            segment_dir,
            args.threads,
            progress_callback=pbar.update,
        )

    if not download_success:
        logger.error(f"Download failed for {url}.")
        return False

    # 3. Merge files
    logger.info(f"Download complete. Starting merge process for {url_hash}...")
    if not args.no_merge:
        output_filename = os.path.join(args.output_dir, args.output_video)
        if args.no_ffmpeg:
            merge_success = merger.merge_ts_files(segment_dir, output_filename)
        else:
            merge_success = merger.ffmpeg_merge_ts_files(segment_dir, output_filename)
        
        if not merge_success:
            logger.error(f"Merging failed for {url}.")
            return False

    # 4. Cleanup
    if not args.keep_segments and not args.no_merge:
        logger.info(f"Cleaning up segment files in {segment_dir}")
        try:
            for file in os.listdir(segment_dir):
                os.remove(os.path.join(segment_dir, file))
            os.rmdir(segment_dir)
        except OSError as e:
            logger.error(f"Error during cleanup: {e}")

    logger.info(f"Successfully processed {url}")
    return True

def parse_arguments():
    parser = argparse.ArgumentParser(description="M3U8 Downloader - CLI")
    parser.add_argument("-u", "--url", help="M3U8 URL to download")
    parser.add_argument("-f", "--file", help="File with M3U8 URLs (one per line)")
    parser.add_argument("-o", "--output-dir", default="output", help="Directory for final videos and segments")
    parser.add_argument("-v", "--output-video", default="output.mp4", help="Output video filename")
    parser.add_argument("-t", "--threads", type=int, default=10, help="Number of download threads")
    parser.add_argument("--ffmpeg", help="Custom path to FFmpeg executable")
    parser.add_argument("--no-merge", action="store_true", help="Download segments only, do not merge")
    parser.add_argument("--no-ffmpeg", action="store_true", help="Use basic binary merge instead of FFmpeg")
    parser.add_argument("--keep-segments", action="store_true", help="Do not delete segment files after merging")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()

async def main():
    args = parse_arguments()
    setup_logging(args.log_level)

    if args.ffmpeg:
        config.ffmpeg_path = args.ffmpeg

    urls = []
    if args.url:
        urls.append(args.url)
    elif args.file:
        try:
            with open(args.file, "r") as f:
                urls = [line.strip() for line in f if line.strip()]
        except IOError as e:
            logger.error(f"Cannot read file {args.file}: {e}")
            return 1
    
    if not urls:
        logger.error("No URL provided. Use -u or -f to specify a URL.")
        return 1

    os.makedirs(args.output_dir, exist_ok=True)
    
    results = []
    for url in urls:
        # Note: This processes URLs sequentially. For concurrent processing of multiple URLs,
        # you would wrap process_single_url calls in asyncio.gather.
        success = await process_single_url(url, args)
        results.append(success)

    successful_downloads = results.count(True)
    logger.info(f"All tasks finished. {successful_downloads}/{len(urls)} URLs processed successfully.")
    
    return 0 if all(results) else 1

if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        exit_code = asyncio.run(main())
        exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
        exit(130)