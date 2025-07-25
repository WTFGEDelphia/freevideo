import os
import platform
import logging


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
        if platform.system() == "Windows":
            for path in os.environ["PATH"].split(os.pathsep):
                ffmpeg_path = os.path.join(path, "ffmpeg.exe")
                if os.path.isfile(ffmpeg_path):
                    return ffmpeg_path
            return "ffmpeg.exe"
        else:
            for path in ["/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg"]:
                if os.path.isfile(path):
                    return path
            return "ffmpeg"


config = Config()
