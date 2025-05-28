# M3U8视频下载器

一个功能强大的Python工具，用于下载和处理M3U8格式的视频流。

## 功能特点

- **多线程异步下载**：利用`ThreadPoolExecutor`和`asyncio`实现高效并发下载
- **智能流选择**：自动选择最高带宽的视频流质量
- **断点续传**：支持从中断处继续下载
- **多种合并选项**：提供基本合并和FFmpeg高级合并两种方式
- **跨平台兼容**：自动检测并适配不同操作系统的FFmpeg路径
- **命令行界面**：支持通过命令行参数控制下载行为
- **批量处理**：支持同时处理多个M3U8 URL
- **进度显示**：实时显示下载和处理进度
- **健壮性**：包含重试机制和资源清理，提高下载成功率

## 系统要求

- Python 3.7+
- FFmpeg（用于高质量视频合并）

## 依赖库

- aiofiles
- aiohttp
- argparse
- asyncio
- m3u8
- requests
- tqdm

## 安装

1. 克隆仓库：

```bash
git clone https://github.com/yourusername/freevideo.git
cd freevideo
```
2. 安装依赖：
```bash
# pipreqs ./freevideo --encoding=utf-8 --force
# pip install -r requirements.txt --no-cache-dir --no-compil
pip install -r requirements.txt
```
3. 确保FFmpeg已安装并添加到系统PATH中。
```bash
python download_m3u8_multithread.py -u "https://example.com/video.m3u8" -o "output_video.mp4"
```
## 命令行参数
```plaintext
-u, --url          M3U8 URL (可以指定多个)
-o, --output       输出文件名
-d, --dir          下载目录
-t, --threads      下载线程数 (默认: 10)
-r, --retries      重试次数 (默认: 3)
-f, --ffmpeg       使用FFmpeg合并 (默认: True)
-k, --keep         保留临时文件 (默认: False)
-v, --verbose      详细输出模式
```
## 示例
1. 下载单个视频：
```bash
python download_m3u8_multithread.py -u "https://example.com/video.m3u8" -o "movie.mp4"
```
2. 下载多个视频：
```bash
python download_m3u8_multithread.py -u "https://example.com/video1.m3u8" "https://example.com/video2.m3u8" -d "downloads"
```
3. 自定义下载目录和线程数：
```bash
python download_m3u8_multithread.py -u "https://example.com/video.m3u8" -t 20 -r 5
```
## 代码结构
### 配置模块
- Config类 ：集中管理下载参数和配置
- 命令行参数解析 ：处理用户输入的参数
### URL处理模块
- check_url ：验证URL可访问性
- get_base_url ：处理相对路径
- get_bandwidth ：选择最高质量的视频流
### 下载模块
- download_segment ：下载单个视频片段
- download_m3u8 ：处理M3U8播放列表并下载所有片段
- 异步下载 ：使用aiohttp和asyncio实现高效下载
### 合并模块
- merge_ts_files ：基本的TS文件合并
- ffmpeg_merge_ts_files ：使用FFmpeg进行高质量合并
### 主控模块
- process_m3u8_url ：处理单个M3U8 URL的完整流程
- main ：程序入口点，处理命令行参数并启动下载
## 工作流程
1. 解析命令行参数和配置
2. 检查M3U8 URL的可访问性
3. 解析M3U8播放列表（处理主播放列表和媒体播放列表）
4. 创建输出目录
5. 并发下载所有视频片段
6. 合并下载的片段（基本合并或FFmpeg合并）
7. 清理临时文件（可选）
## 高级特性
### 内存优化
程序使用分块读写来处理大文件，避免将整个文件加载到内存中。

### 错误处理
包含重试机制和异常处理，确保在网络不稳定的情况下也能完成下载。

### 跨平台兼容性
自动检测操作系统并使用适当的FFmpeg路径和命令格式。