# M3U8视频下载器

一个功能强大的Python工具，用于下载和处理M3U8格式的视频流。支持多线程异步下载、智能流选择、断点续传等功能。

## 功能特点

- **多线程异步下载**：利用`ThreadPoolExecutor`和`asyncio`实现高效并发下载
- **智能流选择**：自动选择最高带宽的视频流质量
- **断点续传**：支持从中断处继续下载，避免重复下载
- **多种合并选项**：提供基础合并和FFmpeg高质量合并两种方式
- **跨平台兼容**：自动检测并适配不同操作系统的FFmpeg路径
- **命令行界面**：支持通过命令行参数控制下载行为
- **批量处理**：支持单个URL或批量文件下载
- **进度显示**：实时显示下载和合并进度
- **健壮性**：包含重试机制和资源清理，提高下载成功率
- **内存优化**：使用分块读写处理大文件，避免内存溢出

## 系统要求

- Python 3.7+
- FFmpeg（用于高质量视频合并，推荐安装并加入系统PATH）

### Python安装指导

#### Windows
1. **下载Python**：
   - 访问 [Python官网](https://www.python.org/downloads/)
   - 下载最新版本的Python（推荐Python 3.11或更高版本）
   - 选择适合你系统的版本（32位或64位）

2. **安装方法**：
   ```bash
   # 方法1：使用Microsoft Store（推荐）
   # 在Microsoft Store中搜索"Python"并安装

   # 方法2：使用Chocolatey包管理器
   choco install python

   # 方法3：使用Scoop包管理器
   scoop install python

   # 方法4：手动安装
   # 1. 运行下载的安装程序
   # 2. 勾选"Add Python to PATH"选项
   # 3. 选择"Install Now"或"Customize installation"
   ```

3. **验证安装**：
   ```bash
   python --version
   # 或
   python3 --version
   ```

#### macOS
1. **使用Homebrew（推荐）**：
   ```bash
   # 安装Homebrew（如果未安装）
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

   # 安装Python
   brew install python
   ```

2. **使用官方安装包**：
   - 访问 [Python官网](https://www.python.org/downloads/)
   - 下载macOS版本的安装包
   - 运行安装程序

3. **使用pyenv管理多个Python版本**：
   ```bash
   # 安装pyenv
   brew install pyenv

   # 安装特定版本的Python
   pyenv install 3.11.0
   pyenv global 3.11.0
   ```

4. **验证安装**：
   ```bash
   python3 --version
   ```

#### Linux (Ubuntu/Debian)
1. **使用apt包管理器**：
   ```bash
   # 更新包列表
   sudo apt update

   # 安装Python 3
   sudo apt install python3 python3-pip python3-venv

   # 设置python3为默认python命令
   sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 1
   ```

2. **使用pyenv管理多个Python版本**：
   ```bash
   # 安装依赖
   sudo apt install build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python3-openssl git

   # 安装pyenv
   curl https://pyenv.run | bash

   # 添加到shell配置
   echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
   echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
   echo 'eval "$(pyenv init -)"' >> ~/.bashrc

   # 重新加载配置
   source ~/.bashrc

   # 安装Python
   pyenv install 3.11.0
   pyenv global 3.11.0
   ```

3. **验证安装**：
   ```bash
   python3 --version
   pip3 --version
   ```

#### Linux (CentOS/RHEL/Fedora)
1. **使用yum/dnf包管理器**：
   ```bash
   # CentOS/RHEL 7
   sudo yum install python3 python3-pip

   # CentOS/RHEL 8/9 或 Fedora
   sudo dnf install python3 python3-pip
   ```

2. **使用pyenv管理多个Python版本**：
   ```bash
   # 安装依赖
   sudo dnf groupinstall "Development Tools"
   sudo dnf install openssl-devel bzip2-devel libffi-devel

   # 安装pyenv
   curl https://pyenv.run | bash

   # 添加到shell配置
   echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
   echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
   echo 'eval "$(pyenv init -)"' >> ~/.bashrc

   # 重新加载配置
   source ~/.bashrc

   # 安装Python
   pyenv install 3.11.0
   pyenv global 3.11.0
   ```

3. **验证安装**：
   ```bash
   python3 --version
   pip3 --version
   ```

#### Linux (Arch Linux)
1. **使用pacman包管理器**：
   ```bash
   sudo pacman -S python python-pip
   ```

2. **验证安装**：
   ```bash
   python --version
   pip --version
   ```

### Python版本要求说明

- **最低版本**：Python 3.7
- **推荐版本**：Python 3.11 或更高版本
- **原因**：本项目使用了异步编程特性，需要较新的Python版本支持

### 常见问题

**Q: 如何检查当前Python版本？**
A: 在终端中运行：
```bash
python --version
# 或
python3 --version
```

**Q: 如何升级pip？**
A: 
```bash
# Windows
python -m pip install --upgrade pip

# Linux/macOS
pip3 install --upgrade pip
# 或
python3 -m pip install --upgrade pip
```

**Q: 如何安装特定版本的Python？**
A: 推荐使用pyenv工具管理多个Python版本：
```bash
# 安装pyenv后
pyenv install 3.11.0
pyenv global 3.11.0
```

**Q: 系统同时安装了Python 2和Python 3怎么办？**
A: 
- 使用 `python3` 命令明确指定Python 3
- 使用pyenv管理不同版本
- 在Windows上可以修改PATH优先级

## 依赖库

- aiofiles - 异步文件操作
- aiohttp - 异步HTTP客户端
- m3u8 - M3U8播放列表解析
- tqdm - 进度条显示

## 安装

1. 克隆仓库：
```bash
git clone https://github.com/WTFGEDelphia/freevideo.git
cd freevideo
```

2. 创建并激活虚拟环境（推荐）：
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/macOS
python3 -m venv venv
source venv/bin/activate
```

3. 安装依赖：
```bash
pip install -r requirements.txt
```

4. 确保FFmpeg已安装并添加到系统PATH中

### FFmpeg安装指导

#### Windows
1. **下载安装包**：
   - 访问 [FFmpeg官网](https://ffmpeg.org/download.html)
   - 下载Windows版本（推荐选择"Windows builds from gyan.dev"）
   - 选择适合你系统的版本（32位或64位）

2. **安装方法**：
   ```bash
   # 方法1：使用Chocolatey包管理器
   choco install ffmpeg

   # 方法2：使用Scoop包管理器
   scoop install ffmpeg

   # 方法3：手动安装
   # 1. 解压下载的zip文件到 C:\ffmpeg
   # 2. 将 C:\ffmpeg\bin 添加到系统PATH环境变量
   ```

3. **验证安装**：
   ```bash
   ffmpeg -version
   ```

#### macOS
1. **使用Homebrew（推荐）**：
   ```bash
   # 安装Homebrew（如果未安装）
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

   # 安装FFmpeg
   brew install ffmpeg
   ```

2. **使用MacPorts**：
   ```bash
   sudo port install ffmpeg
   ```

3. **验证安装**：
   ```bash
   ffmpeg -version
   ```

#### Linux (Ubuntu/Debian)
1. **使用apt包管理器**：
   ```bash
   # 更新包列表
   sudo apt update

   # 安装FFmpeg
   sudo apt install ffmpeg
   ```

2. **验证安装**：
   ```bash
   ffmpeg -version
   ```

#### Linux (CentOS/RHEL/Fedora)
1. **使用yum/dnf包管理器**：
   ```bash
   # CentOS/RHEL 7
   sudo yum install epel-release
   sudo yum install ffmpeg

   # CentOS/RHEL 8/9 或 Fedora
   sudo dnf install ffmpeg
   ```

2. **验证安装**：
   ```bash
   ffmpeg -version
   ```

#### Linux (Arch Linux)
1. **使用pacman包管理器**：
   ```bash
   sudo pacman -S ffmpeg
   ```

2. **验证安装**：
   ```bash
   ffmpeg -version
   ```

### 常见问题

**Q: 安装后仍然提示"FFmpeg未找到"？**
A: 请检查以下几点：
- 确保FFmpeg已添加到系统PATH环境变量
- 重启终端/命令提示符
- 使用 `which ffmpeg` (Linux/macOS) 或 `where ffmpeg` (Windows) 检查路径

**Q: 如何手动添加PATH环境变量？**
A: 
- **Windows**: 系统属性 → 环境变量 → 编辑PATH → 添加FFmpeg的bin目录路径
- **Linux/macOS**: 在 `~/.bashrc` 或 `~/.zshrc` 中添加 `export PATH="/path/to/ffmpeg:$PATH"`

**Q: 可以使用项目内置的FFmpeg吗？**
A: 可以，使用 `--ffmpeg` 参数指定FFmpeg可执行文件的完整路径：
```bash
python download_m3u8_multithread.py -u "URL" --ffmpeg "/path/to/ffmpeg"
```

## 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-u, --url` | M3U8地址（单个URL） | - |
| `-f, --file` | 包含多个M3U8地址的文件（每行一个） | - |
| `-o, --output-dir` | 下载输出目录 | `output_segments` |
| `-v, --output-video` | 输出视频文件名 | `output_video.mp4` |
| `-t, --threads` | 下载线程数 | `5` |
| `--ffmpeg` | 指定FFmpeg可执行文件路径 | 自动检测 |
| `--no-merge` | 只下载不合并 | `False` |
| `--no-ffmpeg` | 不用FFmpeg合并，使用基础合并 | `False` |
| `--keep-segments` | 合并后保留分片文件 | `False` |
| `--log-level` | 日志级别 | `INFO` |

## 使用示例

### 1. 下载单个视频
```bash
python download_m3u8_multithread.py -u "https://example.com/video.m3u8" -v "movie.mp4"
```

### 2. 批量下载（从文件读取URL）
```bash
# 创建包含多个URL的文件 urls.txt
echo "https://example.com/video1.m3u8" > urls.txt
echo "https://example.com/video2.m3u8" >> urls.txt

# 批量下载
python download_m3u8_multithread.py -f urls.txt -o "downloads"
```

### 3. 自定义下载参数
```bash
# 使用20个线程，5次重试，保留分片文件
python download_m3u8_multithread.py -u "https://example.com/video.m3u8" -t 20 --keep-segments
```

### 4. 使用基础合并（不使用FFmpeg）
```bash
python download_m3u8_multithread.py -u "https://example.com/video.m3u8" --no-ffmpeg
```

### 5. 只下载不合并
```bash
python download_m3u8_multithread.py -u "https://example.com/video.m3u8" --no-merge
```

### 6. 自定义FFmpeg路径
```bash
python download_m3u8_multithread.py -u "https://example.com/video.m3u8" --ffmpeg "C:\ffmpeg\bin\ffmpeg.exe"
```

## 代码结构

### 配置模块
- **Config类**：集中管理下载参数和配置
- **命令行参数解析**：处理用户输入的参数
- **日志系统**：支持多级别日志输出

### URL处理模块
- **check_url**：验证URL可访问性
- **get_base_url**：处理相对路径
- **get_bandwidth**：选择最高质量的视频流

### 下载模块
- **download_segment**：下载单个视频片段，支持重试机制
- **download_m3u8**：处理M3U8播放列表并下载所有片段
- **异步下载**：使用aiohttp和asyncio实现高效下载

### 合并模块
- **merge_ts_files**：基础的TS文件合并，使用分块读写
- **ffmpeg_merge_ts_files**：使用FFmpeg进行高质量合并

### 主控模块
- **process_url**：处理单个M3U8 URL的完整流程
- **main_async**：异步主函数，支持批量处理
- **main**：程序入口点，处理命令行参数并启动下载

## 工作流程

1. **参数解析**：解析命令行参数和配置
2. **URL验证**：检查M3U8 URL的可访问性
3. **播放列表解析**：解析主播放列表和媒体播放列表
4. **流选择**：自动选择最高质量的视频流
5. **目录创建**：创建输出目录
6. **并发下载**：使用多线程异步下载所有视频片段
7. **文件合并**：合并下载的片段（基础合并或FFmpeg合并）
8. **清理资源**：清理临时文件（可选）

## 高级特性

### 内存优化
- 使用分块读写来处理大文件，避免将整个文件加载到内存中
- 支持大文件的高效处理

### 错误处理
- 包含重试机制和异常处理
- 确保在网络不稳定的情况下也能完成下载
- 自动清理失败的下载文件

### 跨平台兼容性
- 自动检测操作系统并使用适当的FFmpeg路径
- 支持Windows、Linux、macOS等系统
- 处理不同系统的路径格式

### 智能流选择
- 自动解析主播放列表中的多个质量流
- 选择带宽最高的流进行下载
- 支持自适应码率流

## 常见问题

### Q: FFmpeg未找到怎么办？
A: 请确保已安装FFmpeg并添加到系统PATH中，或使用`--ffmpeg`参数指定FFmpeg路径。

### Q: 下载速度慢怎么办？
A: 可以增加线程数（`-t`参数），但要注意不要超过服务器限制。

### Q: 合并失败怎么办？
A: 尝试使用`--no-ffmpeg`参数使用基础合并，或检查FFmpeg是否正确安装。

### Q: 如何保留分片文件？
A: 使用`--keep-segments`参数可以在合并后保留分片文件。

### Q: 支持哪些日志级别？
A: 支持DEBUG、INFO、WARNING、ERROR、CRITICAL级别，使用`--log-level`参数设置。

## 注意事项

1. 确保有足够的磁盘空间存储下载的视频
2. 某些M3U8链接可能有访问限制或过期时间
3. 建议在网络稳定的环境下使用
4. 大量并发下载可能被服务器限制，请合理设置线程数

## 许可证

本项目采用MIT许可证，详见LICENSE文件。

## 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## 更新日志

- 支持多线程异步下载
- 添加智能流选择功能
- 支持批量下载
- 优化内存使用
- 增强错误处理机制