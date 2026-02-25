import pytest
import os
import shutil
import sqlite3
from unittest.mock import MagicMock, patch, call
from run import YouTubeTaskManager, YouTubeDownloader, on_progress, VideoUnavailable

# --- Fixtures ---

@pytest.fixture
def temp_dir(tmp_path):
    """Provides a temporary directory for file operations."""
    video_dir = tmp_path / "video"
    audio_dir = tmp_path / "audio"
    video_dir.mkdir()
    audio_dir.mkdir()
    return {"video": str(video_dir), "audio": str(audio_dir), "root": str(tmp_path)}

@pytest.fixture
def manager(temp_dir):
    """Provides a YouTubeTaskManager instance with a temporary database."""
    db_path = os.path.join(temp_dir["root"], "test_tasks.db")
    manager = YouTubeTaskManager(
        db_name=db_path,
        video_dst_dir=temp_dir["video"],
        audio_dst_dir=temp_dir["audio"]
    )
    yield manager
    manager.close()

@pytest.fixture
def downloader(manager, temp_dir):
    """Provides a YouTubeDownloader instance with a configured task manager."""
    dl = YouTubeDownloader()
    # Close the manager created in __init__ to avoid double connections
    if hasattr(dl, 'task_manager') and dl.task_manager:
        dl.task_manager.close()
    
    dl.task_manager = manager
    dl.video_destination_directory = temp_dir["video"]
    dl.audio_destination_directory = temp_dir["audio"]
    dl.download_video = True
    dl.download_audio = True
    dl.reconvert_media = True
    dl.logger = MagicMock()
    return dl

# --- YouTubeTaskManager Tests ---

def test_manager_init(manager):
    """Tests if the database and table are initialized correctly."""
    assert manager.conn is not None
    manager.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tasks'")
    assert manager.cursor.fetchone() is not None

def test_extract_youtube_id(manager):
    """Tests extraction of 11-character YouTube IDs from various URL formats."""
    urls = [
        "https://www.youtube.com/watch?v=ABC12345678",
        "https://youtu.be/ABC12345678",
        "https://www.youtube.com/embed/ABC12345678",
        "https://www.youtube.com/watch?v=ABC12345678&feature=shared"
    ]
    for url in urls:
        assert manager._extract_youtube_id(url) == "ABC12345678"
    
    assert manager._extract_youtube_id("invalid_url") == ""

def test_add_and_get_task(manager):
    """Tests adding a task and retrieving it."""
    url = "https://www.youtube.com/watch?v=id123456789"
    title = "Test Video"
    task = manager.add_task(url, title, max_file_length=100)
    
    assert task["youtube_id"] == "id123456789"
    assert task["status"] == "pending"
    
    retrieved = manager.get_task("id123456789")
    assert retrieved["video_url"] == url
    assert retrieved["suggested_filename_base"] == "Test Video"

def test_update_task(manager):
    """Tests updating task metadata."""
    manager.add_task("https://www.youtube.com/watch?v=id123456789", "Title", 60)
    manager.update_task("id123456789", {"status": "completed", "video_filepath": "/tmp/v.mp4"})
    
    task = manager.get_task("id123456789")
    assert task["status"] == "completed"
    assert task["video_filepath"] == "/tmp/v.mp4"

# --- YouTubeDownloader Tests ---

@patch("run.YouTube")
@patch("run.VideoFileClip")
@patch("run.AudioFileClip")
@patch("os.path.exists")
@patch("shutil.move")
@patch("os.remove")
@patch("run.on_progress")
def test_download_video_success(
    mock_on_progress, mock_remove, mock_move, mock_exists,
    mock_audio_clip, mock_video_clip, mock_yt_class,
    downloader, temp_dir
):
    """Tests a full successful download and merge flow."""
    video_id = "SUCCESS1234"
    url = f"https://www.youtube.com/watch?v={video_id}"
    
    # Mock YouTube object
    mock_yt = MagicMock()
    mock_yt.title = "Video"
    mock_yt.length = 100
    mock_yt_class.return_value = mock_yt
    
    # Mock Streams
    mock_stream = MagicMock()
    mock_yt.streams.filter.return_value.order_by.return_value.desc.return_value.last.return_value = mock_stream
    mock_yt.streams.filter.return_value.asc.return_value.first.return_value = mock_stream
    
    # Mock moviepy clips
    mock_vfc = MagicMock()
    mock_afc = MagicMock()
    mock_video_clip.return_value = mock_vfc
    mock_audio_clip.return_value = mock_afc
    
    # Trace-based side_effect:
    # 1-2: add_task collision checks (Video.mp4, Video.mp3) -> False
    # 3-4: _download_youtube_video initial checks (Video.mp4, Video.mp3) -> False
    # 5: extract audio check (Video.mp4) -> False
    # 6: temp audio file check (./Video.mp4) -> False
    # 7-8: merge block reconvert check (Video.mp4, Video.mp3) -> True
    # 9: already merged check -> False
    # 10: original video removal check -> True
    mock_exists.side_effect = [False, False, False, False, False, False, True, True, False, True] + [True]*10

    result = downloader._download_youtube_video(url)
    
    assert result is True
    assert mock_yt_class.called
    assert mock_stream.download.call_count == 2
    assert mock_video_clip.called
    assert mock_afc.write_audiofile.called
    assert mock_vfc.write_videofile.called
    
    # Verify task status
    task = downloader.task_manager.get_task(video_id)
    assert task["status"] == "completed"

@patch("run.YouTube")
@patch("time.sleep") # Speed up tests by skipping delay
def test_download_video_unavailable(mock_sleep, mock_yt_class, downloader):
    """Tests error handling when a video is unavailable."""
    video_id = "UNAVAIL1234"
    url = f"https://www.youtube.com/watch?v={video_id}"
    
    # Add task manually first so it exists in DB for update_task to find it
    downloader.task_manager.add_task(url, "Unavail Video", 60)
    
    mock_yt = MagicMock()
    mock_yt.check_availability.side_effect = VideoUnavailable(video_id)
    mock_yt_class.return_value = mock_yt
    
    # The retry decorator wraps the error into a generic Exception
    with pytest.raises(Exception) as excinfo:
        downloader._download_youtube_video(url)
    
    assert "All retries failed" in str(excinfo.value)
    assert video_id in str(excinfo.value)
    
    # Task should be marked as failed
    task = downloader.task_manager.get_task(video_id)
    assert task is not None
    assert task["status"] == "failed"
    assert "is unavailable" in task["error_message"]

def test_download_videos_from_list(manager, downloader):
    """Tests batch downloading from a list of URLs."""
    urls = ["https://www.youtube.com/watch?v=url11111111", 
            "https://www.youtube.com/watch?v=url22222222", 
            "https://www.youtube.com/watch?v=url33333333"]
    
    with patch("run.YouTube") as mock_yt, \
         patch("os.path.exists", return_value=False), \
         patch.object(downloader, "_download_youtube_video") as mock_download_single:
        
        mock_download_single.return_value = True
        
        # Mock YouTube object for each URL
        mock_yt_inst = MagicMock()
        mock_yt_inst.title = "Mock Title"
        mock_yt.return_value = mock_yt_inst
        
        downloader._preprocess_videos_from_list(urls)
        
        assert mock_download_single.call_count == 3
        mock_download_single.assert_has_calls([call(urls[0]), call(urls[1]), call(urls[2])])

def test_filename_collision_logic(manager):
    """Tests the unique filename generation logic in YouTubeTaskManager."""
    url1 = "https://www.youtube.com/watch?v=vid11111111"
    url2 = "https://www.youtube.com/watch?v=vid22222222"
    title = "Collision"
    
    # Add first task
    manager.add_task(url1, title, 60)
    task1 = manager.get_task("vid11111111")
    assert task1["final_video_filename"] == "Collision.mp4"
    
    # Add second task with same title
    manager.add_task(url2, title, 60)
    task2 = manager.get_task("vid22222222")
    assert task2["final_video_filename"] == "Collision_1.mp4"
