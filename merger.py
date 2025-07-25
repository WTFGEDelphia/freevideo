import os
import tempfile
import platform
import subprocess
import logging
from tqdm import tqdm
from config import config


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


def merge_ts_files(output_dir, output_file, chunk_size=1024 * 1024):
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
    output_path = (
        os.path.join(output_dir, output_file)
        if not os.path.isabs(output_file)
        else output_file
    )
    total_size = sum(os.path.getsize(f) for f in ts_files)
    with tqdm(
        total=total_size, desc="Merging files", unit="B", unit_scale=True
    ) as pbar:
        try:
            with open(output_path, "wb") as merged_file:
                for ts_file in ts_files:
                    file_size = os.path.getsize(ts_file)
                    with open(ts_file, "rb") as segment_file:
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
            if os.path.exists(output_path):
                os.remove(output_path)
            return False


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
    if not ts_files:
        logger.error(f"No .ts files found in {absolute_path}")
        return False
    temp_dir = tempfile.mkdtemp()
    filelist_path = os.path.join(temp_dir, "filelist.txt")
    try:
        with open(filelist_path, "w", encoding="utf-8") as filelist:
            for ts_file in ts_files:
                safe_path = (
                    ts_file.replace("\\", "/")
                    if platform.system() == "Windows"
                    else ts_file
                )
                filelist.write(f"file '{safe_path}'\n")
        output_path = (
            os.path.join(absolute_path, output_file)
            if not os.path.isabs(output_file)
            else output_file
        )
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
        pass
