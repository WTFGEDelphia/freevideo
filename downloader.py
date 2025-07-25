import os
import aiohttp
import aiofiles
import asyncio
import logging
from urllib.parse import urljoin, urlparse
import m3u8
from config import config

logger = logging.getLogger(__name__)

async def _get_m3u8_obj_from_url(session, url):
    """Helper function to fetch and parse an M3U8 playlist."""
    base_url = get_base_url(url)
    try:
        async with session.get(url, timeout=config.timeout) as response:
            if response.status != 200:
                logger.error(f"Failed to fetch M3U8 URL {url}: HTTP {response.status}")
                return None, None
            content = await response.text()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(f"Error fetching M3U8 URL {url}: {e}")
        return None, None

    m3u8_obj = m3u8.loads(content)

    # If it's a master playlist, find the highest bandwidth stream
    if m3u8_obj.is_variant:
        logger.info("Master playlist found. Selecting highest bandwidth stream.")
        # Sort by bandwidth (highest first)
        sorted_playlists = sorted(m3u8_obj.playlists, key=lambda p: p.stream_info.bandwidth, reverse=True)
        best_playlist = sorted_playlists[0]
        
        sub_playlist_url = urljoin(base_url, best_playlist.uri)
        logger.info(f"Processing media playlist: {sub_playlist_url}")
        return await _get_m3u8_obj_from_url(session, sub_playlist_url)

    return m3u8_obj, base_url


async def get_segment_count(url):
    """
    Fetches an M3U8 playlist and returns the total number of segments.
    Returns 0 if the playlist cannot be fetched or parsed.
    """
    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        m3u8_obj, _ = await _get_m3u8_obj_from_url(session, url)
        if m3u8_obj and m3u8_obj.segments:
            return len(m3u8_obj.segments)
        else:
            logger.error(f"Could not find any segments in URL: {url}")
            return 0


async def download_segment(session, segment_url, segment_file_path, progress_callback=None):
    """Downloads a single video segment with retries."""
    if os.path.exists(segment_file_path) and os.path.getsize(segment_file_path) > 0:
        logger.debug(f"Segment {segment_file_path} already exists. Skipping.")
        if progress_callback:
            progress_callback()
        return True

    for attempt in range(config.max_retries):
        try:
            async with session.get(segment_url, timeout=config.timeout) as response:
                if response.status == 200:
                    async with aiofiles.open(segment_file_path, "wb") as f:
                        await f.write(await response.read())
                    logger.debug(f"Downloaded {segment_file_path}")
                    if progress_callback:
                        progress_callback()
                    return True
                else:
                    logger.warning(f"Failed to download {segment_url}: HTTP {response.status}")
            await asyncio.sleep(config.retry_delay)
        except (aiohttp.ClientError, asyncio.TimeoutError, IOError) as e:
            logger.warning(f"Error on {segment_url} (attempt {attempt + 1}): {e}")
            if os.path.exists(segment_file_path):
                os.remove(segment_file_path)
            if attempt == config.max_retries - 1:
                return False
            await asyncio.sleep(config.retry_delay)
    return False


async def download_m3u8(url, output_dir, max_workers=None, progress_callback=None):
    """
    Downloads all segments from an M3U8 playlist.
    Returns True on success, False on failure.
    """
    if max_workers is None:
        max_workers = config.max_connections
    
    os.makedirs(output_dir, exist_ok=True)
    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        m3u8_obj, base_url = await _get_m3u8_obj_from_url(session, url)

        if not m3u8_obj or not m3u8_obj.segments:
            logger.error("Failed to parse M3U8 or find segments.")
            return False

        segment_count = len(m3u8_obj.segments)
        logger.info(f"Found {segment_count} segments to download.")

        tasks = []
        for i, segment in enumerate(m3u8_obj.segments):
            segment_url = segment.uri
            if not segment_url.startswith("http"):
                segment_url = urljoin(base_url, segment_url)
            
            segment_file_path = os.path.join(output_dir, f"index{i}.ts")
            tasks.append(
                download_segment(session, segment_url, segment_file_path, progress_callback)
            )

        results = await asyncio.gather(*tasks)

        if all(results):
            logger.info(f"Successfully downloaded all {segment_count} segments.")
            return True
        else:
            failed_count = results.count(False)
            logger.error(f"Failed to download {failed_count} out of {segment_count} segments.")
            return False

def get_base_url(url):
    """Helper to get the base URL for resolving relative paths."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{os.path.dirname(parsed.path)}/"