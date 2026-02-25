import pytest
import os
import sqlite3
from unittest.mock import MagicMock, patch

# Assuming run.py is in the parent directory
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from run import YouTubeTaskManager, YouTubeDownloader, on_progress


@pytest.fixture
def temp_db(tmp_path):
    """Provides an in-memory SQLite database for testing YouTubeTaskManager."""
    db_path = tmp_path / "test_youtube_tasks.db"
    manager = YouTubeTaskManager(
        db_name=str(db_path), video_dst_dir="dummy_video", audio_dst_dir="dummy_audio"
    )
    yield manager
    manager.close()
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture
def downloader_instance(temp_db):
    """Provides a YouTubeDownloader instance with a mocked task manager."""
    downloader = YouTubeDownloader()
    downloader.task_manager = temp_db  # Use the in-memory task manager
    downloader.video_destination_directory = "dummy_video_dir"
    downloader.audio_destination_directory = "dummy_audio_dir"
    downloader.download_video = True
    downloader.download_audio = True
    downloader.reconvert_media = True  # Enable merging for some tests
    downloader.logger = (
        MagicMock()
    )  # Mock logger to prevent actual logging during tests
    return downloader


# --- Tests for YouTubeTaskManager ---


def test_task_manager_initialization(temp_db):
    assert temp_db.conn is not None
    assert temp_db.cursor is not None
    temp_db.cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='tasks'"
    )
    assert temp_db.cursor.fetchone() is not None


def test_add_task_new_task(temp_db):
    video_url = "https://www.youtube.com/watch?v=test_id_001"
    video_title = "Test Video Title 1"
    task = temp_db.add_task(video_url, video_title, max_file_length=60)
    assert task is not None
    assert task["youtube_id"] == "test_id_001"
    assert task["status"] == "pending"
    assert "Test Video Title 1.mp4" in task["final_video_filename"]
    assert "Test Video Title 1.mp3" in task["final_audio_filename"]


def test_add_task_existing_task(temp_db):
    video_url = "https://www.youtube.com/watch?v=test_id_002"
    video_title = "Test Video Title 2"
    temp_db.add_task(video_url, video_title, max_file_length=60)
    # Try to add the same task again
    task = temp_db.add_task(video_url, video_title, max_file_length=60)
    assert task is not None
    assert task["youtube_id"] == "test_id_002"
    # Ensure only one entry exists in the database
    temp_db.cursor.execute(
        "SELECT COUNT(*) FROM tasks WHERE youtube_id = ?", ("test_id_002",)
    )
    assert temp_db.cursor.fetchone()[0] == 1


def test_get_task(temp_db):
    video_url = "https://www.youtube.com/watch?v=test_id_003"
    video_title = "Test Video Title 3"
    temp_db.add_task(video_url, video_title, max_file_length=60)
    task = temp_db.get_task("test_id_003")
    assert task is not None
    assert task["youtube_id"] == "test_id_003"
    assert task["video_url"] == video_url


def test_update_task(temp_db):
    video_url = "https://www.youtube.com/watch?v=test_id_004"
    video_title = "Test Video Title 4"
    temp_db.add_task(video_url, video_title, max_file_length=60)
    temp_db.update_task(
        "test_id_004", {"status": "completed", "video_filepath": "/path/to/video.mp4"}
    )
    updated_task = temp_db.get_task("test_id_004")
    assert updated_task["status"] == "completed"
    assert updated_task["video_filepath"] == "/path/to/video.mp4"


@patch("os.path.exists", return_value=False)
def test_filename_collision_exists_no_collision(mock_exists, temp_db):
    assert not temp_db._filename_collision_exists("non_existent_file")
    mock_exists.assert_any_call(os.path.join("dummy_video", "non_existent_file.mp4"))
    mock_exists.assert_any_call(os.path.join("dummy_audio", "non_existent_file.mp3"))


@patch("os.path.exists", side_effect=[True, False])  # Video exists, audio doesn't
def test_filename_collision_exists_on_disk_video(mock_exists, temp_db):
    assert temp_db._filename_collision_exists("existent_video_file")
    mock_exists.assert_any_call(os.path.join("dummy_video", "existent_video_file.mp4"))


@patch("os.path.exists", side_effect=[False, True])  # Video doesn't, audio exists
def test_filename_collision_exists_on_disk_audio(mock_exists, temp_db):
    assert temp_db._filename_collision_exists("existent_audio_file")
    mock_exists.assert_any_call(os.path.join("dummy_video", "existent_audio_file.mp4"))
    mock_exists.assert_any_call(os.path.join("dummy_audio", "existent_audio_file.mp3"))


def test_filename_collision_exists_in_db(temp_db):
    video_url = "https://www.youtube.com/watch?v=db_collisio"
    video_title = "DB Collision"
    temp_db.add_task(video_url, video_title, max_file_length=60)
    # Now check collision for the base filename of this task
    assert temp_db._filename_collision_exists("DB Collision")


# --- Tests for YouTubeDownloader ---


def test_downloader_initialization(downloader_instance):
    assert downloader_instance.task_manager is not None
    assert downloader_instance.download_video is True
    assert downloader_instance.download_audio is True
    assert downloader_instance.video_destination_directory == "dummy_video_dir"
    assert downloader_instance.audio_destination_directory == "dummy_audio_dir"


@patch("run.YouTube")
@patch("pytubefix.streams.Stream.download")
@patch("run.VideoFileClip")
@patch("run.AudioFileClip")
@patch("os.path.exists")
@patch("shutil.move")
@patch("os.remove")
@patch("run.on_progress")  # ADDED THIS PATCH
def test_download_youtube_video_success(
    mock_on_progress,
    mock_os_remove,
    mock_shutil_move,
    mock_os_path_exists,
    mock_audio_file_clip,
    mock_video_file_clip,
    mock_stream_download,
    mock_youtube,
    downloader_instance,
):
    # Setup mocks
    youtube_id = "Abc12345678"
    video_url = f"https://www.youtube.com/watch?v={youtube_id}"
    video_title = "Success Video"
    final_video_filename = "Success Video.mp4"
    final_audio_filename = "Success Video.mp3"

    # Mock YouTube object behavior
    mock_yt_instance = MagicMock()
    mock_yt_instance.title = video_title
    mock_yt_instance.length = 120
    mock_yt_instance.captions = {}
    mock_yt_instance.channel_url = (
        None  # Prevent VideoUnavailable from check_availability
    )
    mock_yt_instance.check_availability.return_value = (
        None  # Ensure availability check passes
    )
    mock_youtube.return_value = mock_yt_instance

    # Mock streams object directly
    mock_streams = MagicMock()
    mock_yt_instance.streams = (
        mock_streams  # Assign mock_streams to mock_yt_instance.streams
    )

    # Mock stream objects
    mock_video_stream = MagicMock()
    mock_video_stream.itag = 137
    mock_video_stream.resolution = "1080p"
    mock_video_stream.video_codec = "avc1"
    mock_video_stream.abr = None
    mock_video_stream.audio_codec = None
    mock_streams.filter.return_value.order_by.return_value.desc.return_value.last.return_value = (
        mock_video_stream
    )
    mock_streams.get_highest_resolution.return_value = mock_video_stream  # Fallback

    mock_audio_stream = MagicMock()
    mock_audio_stream.itag = 251
    mock_audio_stream.abr = "128kbps"
    mock_streams.filter.return_value.asc.return_value.first.return_value = (
        mock_audio_stream
    )
    mock_streams.get_audio_only.return_value = mock_audio_stream  # Fallback

    # Mock moviepy clips
    mock_vfc_instance = MagicMock()
    mock_vfc_instance.audio = MagicMock()
    mock_vfc_instance.audio.write_audiofile.return_value = None
    mock_video_file_clip.return_value = mock_vfc_instance

    mock_afc_instance = MagicMock()
    mock_audio_file_clip.return_value = mock_afc_instance

    # Mock os.path.exists
    # 1-2: add_task, 3-4: initial checks in download, 5: extract audio check,
    # 6: temp audio file check, 7-8: merge checks, 9: final merged check,
    # 10: original video removal check
    mock_os_path_exists.side_effect = [False, False, False, False, False, False, True, True, False, True] + [True] * 10

    # Add task to the task manager
    downloader_instance.task_manager.add_task(
        video_url, video_title, max_file_length=60
    )

    # Execute the method under test
    result = downloader_instance._download_youtube_video(video_url)

    # Assertions
    assert result is True
    mock_youtube.assert_called_once_with(
        url=video_url,
        use_oauth=False,
        allow_oauth_cache=False,
        on_progress_callback=mock_on_progress,  # Use the mock
    )
    mock_yt_instance.check_availability.assert_called_once()
    mock_yt_instance.streams.filter.assert_any_call(
        progressive=False, mime_type="video/mp4", res="1080p"
    )
    mock_video_stream.download.assert_called_once_with(
        output_path=".", filename=final_video_filename
    )
    mock_shutil_move.assert_any_call(
        os.path.join(".", final_video_filename),
        os.path.join(
            downloader_instance.video_destination_directory, final_video_filename
        ),
    )

    mock_yt_instance.streams.filter.assert_any_call(
        mime_type="audio/mp4", abr="128kbps"
    )
    mock_audio_stream.download.assert_called_once_with(
        output_path=".", filename=f"{os.path.splitext(final_audio_filename)[0]}.mp4"
    )  # original audio filename is mp4 before conversion to mp3

    # AudioFileClip is called twice: once for conversion, once for merging
    assert mock_audio_file_clip.call_count == 2
    mock_audio_file_clip.assert_any_call(
        os.path.join(".", f"{os.path.splitext(final_audio_filename)[0]}.mp4")
    )
    mock_audio_file_clip.assert_any_call(
        os.path.join(
            downloader_instance.audio_destination_directory, final_audio_filename
        )
    )

    mock_afc_instance.write_audiofile.assert_called_once_with(
        filename=os.path.join(
            downloader_instance.audio_destination_directory, final_audio_filename
        ),
        codec=None,
    )

    # Verify moviepy merge calls
    # VideoFileClip is called once for merging (and possibly once more if we didn't mock exists correctly, but here once)
    mock_video_file_clip.assert_any_call(
        os.path.join(
            downloader_instance.video_destination_directory, final_video_filename
        )
    )
    
    mock_vfc_instance.write_videofile.assert_called_once_with(
        filename=f"{os.path.splitext(final_audio_filename)[0]}_merged.mp4",
        codec=None,
        audio_codec="aac",
        temp_audiofile=os.path.join(".", "_temp_audio.m4a"),
        remove_temp=True,
    )
    mock_shutil_move.assert_any_call(
        f"{os.path.splitext(final_audio_filename)[0]}_merged.mp4",
        os.path.join(
            downloader_instance.video_destination_directory, final_video_filename
        ),
    )

    # Verify task status update
    updated_task = downloader_instance.task_manager.get_task(youtube_id)
    assert updated_task["status"] == "completed"
    assert updated_task["video_filepath"] == os.path.join(
        downloader_instance.video_destination_directory, final_video_filename
    )
    assert updated_task["audio_filepath"] == os.path.join(
        downloader_instance.audio_destination_directory, final_audio_filename
    )
