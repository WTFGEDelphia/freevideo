import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import asyncio
import queue
import logging
import os
import re

# Import the refactored backend modules
import downloader
import merger
from config import config

class QueueLogHandler(logging.Handler):
    """Redirects log records to a queue to be processed by the GUI."""
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(self.format(record))

class Application(tk.Frame):
    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.master.title("M3U8 视频下载器 (GUI)")
        self.master.geometry("800x600")
        self.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self.log_queue = queue.Queue()
        self.create_widgets()
        self.setup_logging()
        self.poll_log_queue() # Start listening for log messages
        self.master.protocol("WM_DELETE_WINDOW", self.on_closing) # Bind closing event

    def on_closing(self):
        """Handles window closing event to clean up log file."""
        try:
            if os.path.exists(config.log_file):
                # To prevent file lock issues, we need to release the logger first
                root_logger = logging.getLogger()
                for handler in root_logger.handlers[:]:
                    if isinstance(handler, logging.FileHandler):
                        handler.close()
                        root_logger.removeHandler(handler)
                os.remove(config.log_file)
                print(f"Log file '{config.log_file}' removed.")
        except Exception as e:
            print(f"Error removing log file: {e}")
        finally:
            self.master.destroy()

    def create_widgets(self):
        # --- Input Frame ---
        input_frame = ttk.LabelFrame(self, text="输入")
        input_frame.pack(fill=tk.X, pady=5)
        ttk.Label(input_frame, text="M3U8 URL:").pack(side=tk.LEFT, padx=5, pady=5)
        self.url_entry = ttk.Entry(input_frame)
        self.url_entry.pack(fill=tk.X, expand=True, padx=5, pady=5)
        self.url_entry.insert(0, "https://v1.qrssv.com/202308/19/E5vLJaiE5q2/video/2000k_1080/hls/index.m3u8")

        # --- Options Frame ---
        opts_frame = ttk.LabelFrame(self, text="选项")
        opts_frame.pack(fill=tk.X, pady=5)
        
        # Output Directory
        dir_frame = ttk.Frame(opts_frame)
        dir_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Label(dir_frame, text="输出目录:").pack(side=tk.LEFT)
        self.output_dir = tk.StringVar(value=os.path.abspath("output"))
        ttk.Entry(dir_frame, textvariable=self.output_dir).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(dir_frame, text="浏览...", command=self.browse_dir).pack(side=tk.LEFT)

        # Threads & Checkboxes
        misc_frame = ttk.Frame(opts_frame)
        misc_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Label(misc_frame, text="线程数:").pack(side=tk.LEFT)
        self.threads = tk.IntVar(value=10)
        ttk.Spinbox(misc_frame, from_=1, to=50, textvariable=self.threads, width=5).pack(side=tk.LEFT, padx=5)
        
        self.use_ffmpeg = tk.BooleanVar(value=True)
        ttk.Checkbutton(misc_frame, text="使用 FFmpeg", variable=self.use_ffmpeg).pack(side=tk.LEFT, padx=10)
        self.keep_segments = tk.BooleanVar(value=False)
        ttk.Checkbutton(misc_frame, text="保留分片", variable=self.keep_segments).pack(side=tk.LEFT, padx=10)

        # --- Control & Progress Frame ---
        prog_frame = ttk.LabelFrame(self, text="控制")
        prog_frame.pack(fill=tk.X, pady=5)
        self.start_button = ttk.Button(prog_frame, text="开始下载", command=self.start_download_thread)
        self.start_button.pack(side=tk.LEFT, padx=10, pady=5)
        self.progress = ttk.Progressbar(prog_frame, orient="horizontal", mode="determinate")
        self.progress.pack(fill=tk.X, expand=True, padx=10, pady=5)

        # --- Log Frame ---
        log_frame = ttk.LabelFrame(self, text="日志")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        self.log_text = tk.Text(log_frame, state="disabled", wrap=tk.WORD)
        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        self.log_text.config(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    def browse_dir(self):
        path = filedialog.askdirectory(initialdir=self.output_dir.get())
        if path:
            self.output_dir.set(path)

    def setup_logging(self):
        """Configure the root logger to use our queue handler."""
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        handler = QueueLogHandler(self.log_queue)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

    def poll_log_queue(self):
        """Periodically check the queue for new log messages."""
        while not self.log_queue.empty():
            record = self.log_queue.get()
            self.log_text.config(state="normal")
            self.log_text.insert(tk.END, record + "\n")
            self.log_text.config(state="disabled")
            self.log_text.see(tk.END)
        self.master.after(100, self.poll_log_queue)

    def start_download_thread(self):
        """Starts the download process in a separate thread to keep the GUI responsive."""
        url = self.url_entry.get().strip()
        if not url:
            messagebox.showerror("错误", "请输入 M3U8 URL。")
            return
        
        self.start_button.config(state="disabled")
        self.progress["value"] = 0
        
        thread = threading.Thread(target=self.run_async_download, args=(url,), daemon=True)
        thread.start()

    def run_async_download(self, url):
        """The target for the download thread. Runs the asyncio event loop."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.download_and_merge(url))
        finally:
            loop.close()
            self.master.after(0, lambda: self.start_button.config(state="normal"))

    async def download_and_merge(self, url):
        """The main async task for downloading and merging."""
        try:
            # --- Preparation ---
            output_dir = self.output_dir.get()
            os.makedirs(output_dir, exist_ok=True)
            
            total_segments = await downloader.get_segment_count(url)
            if total_segments == 0:
                messagebox.showerror("错误", "无法获取视频片段，请检查 URL 和网络。")
                return

            self.master.after(0, lambda: self.progress.config(maximum=total_segments))
            
            def progress_callback():
                self.master.after(0, lambda: self.progress.step(1))

            # --- Download ---
            url_hash = "".join(filter(str.isalnum, url))[-20:]
            segment_dir = os.path.join(output_dir, url_hash)
            
            download_success = await downloader.download_m3u8(
                url, segment_dir, self.threads.get(), progress_callback
            )

            if not download_success:
                messagebox.showerror("失败", "下载失败，请查看日志。")
                return

            # --- Merge ---
            output_filename = os.path.join(output_dir, f"video_{url_hash}.mp4")
            logging.info(f"开始合并文件 -> {output_filename}")

            if self.use_ffmpeg.get():
                merge_success = merger.ffmpeg_merge_ts_files(segment_dir, output_filename)
            else:
                merge_success = merger.merge_ts_files(segment_dir, output_filename)

            if not merge_success:
                messagebox.showerror("失败", "合并文件失败，请查看日志。")
                return

            # --- Cleanup ---
            if not self.keep_segments.get():
                logging.info(f"清理分片文件: {segment_dir}")
                try:
                    for file in os.listdir(segment_dir):
                        os.remove(os.path.join(segment_dir, file))
                    os.rmdir(segment_dir)
                except OSError as e:
                    logging.error(f"清理失败: {e}")
            
            messagebox.showinfo("成功", f"任务完成！视频已保存到:\n{output_filename}")

        except Exception as e:
            logging.error(f"发生严重错误: {e}", exc_info=True)
            messagebox.showerror("严重错误", f"发生严重错误，详情请查看日志。\n{e}")

if __name__ == "__main__":
    root = tk.Tk()
    app = Application(master=root)
    app.mainloop()