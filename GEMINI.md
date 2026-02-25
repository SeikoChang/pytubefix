# Pytubefix Task Manager Project

`pytubefix` is a robust Python library for downloading YouTube videos, forked from `pytube` to address stability issues and add modern features. This project extends it with a persistent Task Manager and automated workflow.

## 🚀 Core Components

### 1. `run.py` (Unified Downloader)
The main driver script that handles:
- **Single Video**: via URL.
- **Playlists/Channels**: via URL.
- **Search**: via keyword query.
- **Adaptive Streaming**: Downloads separate video/audio streams and merges them using `moviepy`.

### 2. `YouTubeTaskManager` (Persistence Layer)
An SQLite-backed manager (`youtube_tasks.db`) that ensures:
- **Resumability**: Tracks `pending`, `in_progress`, `completed`, and `failed` status.
- **Duplicate Prevention**: Checks both database records and physical file existence before downloading.
- **Filename Integrity**: Handles sanitization and unique filename generation.

## 📊 Process Flow

![System Architecture](https://kroki.io/mermaid/svg/eJxtkc1uwjAQhO99ij310tLckdoK8gMBQhGhlZDFYUlWiYWxI8ehINR3r2un9FKfrJ1vdsfrSmNTwya6A3tGLDeozRB0J5-ayw4GgxcYXzfYHiBDiRVpSCU3X44eOzlkYU3FAd7XiyF88JJUsBJ4Ebw1QVijlCSCnFAX9c65QueKrvHZaCwMbFW36fYEaeS7Rk6P_dD4bNu0wCVE41evx1aHLbWPEKpjI8hQ6RwJyw-8gUh9SqGw3P3BS5uIZMllFSTIRc9PWEKmqG_zMzJYokFvnDhmykZlGbw3tk7wE2gIdkGma59TCSutKk1t6w1TZ0jZ73y_CriHUVdyZW2a8NizqWNn1zUVSp5ImyAjXRHEEvc2Xv_QWf9QB89Zpk6cVpcheNa3f_Dtd3-GpXL8guV4IsipQW3Di4tH5k7MWMIlCkjsMmCFprYxp9jWEKIoOoGGK-n5hefdPXP3JftvHbev8LalQ99YLPtC4gvfawm3BQ==)

## 🛠 Usage Example

```python
from run import YouTubeDownloader

# Initialize the downloader with custom configuration
downloader = YouTubeDownloader()
downloader.video_destination_directory = "downloads/videos"
downloader.audio_destination_directory = "downloads/audio"
downloader.download_video = True
downloader.download_audio = True

# Download a specific video (automatically tracked in DB)
downloader._download_youtube_video("https://www.youtube.com/watch?v=Abc12345678")

# Process a list of videos
video_urls = ["url1", "url2", "url3"]
downloader._download_videos_from_list(video_urls)
```

## ⚠️ Important Implementation Details
- **YouTube ID**: Strictly 11 characters (verified via regex).
- **Mocks in Testing**: When writing tests, ensure `os.path.exists` side effects account for the multiple stages (DB check, extraction check, merge check).
- **MoviePy Integration**: The logger must be correctly handled to avoid `RootLogger` callable errors during the `write_videofile` phase.

## 💡 Best Practices

### 1. Persistence & Integrity
- **Database First**: Check `YouTubeTaskManager` before any download to prevent redundant API calls and disk I/O.
- **Atomic Moves**: Download to a temporary location and use `shutil.move` only after successful processing/merging.
- **Hash Verification**: Use `_calculate_file_hash` to ensure file integrity after moves or conversions.

### 2. Testing Strategy
- **Path-Aware Mocks**: When mocking `os.path.exists`, use a `side_effect` list that mirrors the real execution flow (e.g., `[False (initial), True (after download)]`).
- **Mocking Instance Methods**: Use `patch.object(downloader, '_download_youtube_video')` to mock methods wrapped in decorators.
- **Isolated DBs**: Use the `temp_db` fixture to ensure each test runs against a clean, in-memory or temporary SQLite instance.

### 3. Media Processing
- **Stream Filtering**: Always specify `progressive=False` for high-definition video to get the best quality DASH streams.
- **Clip Management**: Explicitly call `.close()` on all `moviepy` clips in a `finally` block to release system resources.
- **Sanitization**: Use `helpers.safe_filename` and additional character stripping to ensure cross-platform filename compatibility.

## 📝 Maintenance
- **`TODOs.md`**: Must be updated automatically for every new task and completion.
- **`Pipfile`**: Use `.venv/bin/pytest` for all verification runs to ensure isolated environment consistency.
