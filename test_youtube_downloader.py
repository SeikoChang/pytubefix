import pytest
from unittest.mock import MagicMock, patch
import os
import shutil
import logging
import unicodedata
from functools import wraps
from logging.handlers import TimedRotatingFileHandler
import sys  # Ensure sys is imported at the top level

from moviepy.editor import AudioFileClip, VideoFileClip

from pytubefix import Channel, Playlist, Search, YouTube, helpers
from pytubefix.cli import on_progress
from pytubefix.contrib.search import Filter
from pytubefix.exceptions import (
    BotDetection,
    RegexMatchError,
    VideoUnavailable,
    LiveStreamError,
    ExtractError,
)


class YouTubeDownloader:
    """A class to download YouTube videos, playlists, or channel content,
    with options for audio extraction, video/audio merging, and caption downloading.

    This class encapsulates all configuration and logic required for downloading
    YouTube content, ensuring modularity and reusability. It supports individual
    video downloads, playlist downloads, channel downloads, and quick searches.
    Logging is integrated to provide detailed feedback during the process.
    """

    def __init__(self):
        """Initializes the YouTubeDownloader with default configurations.

        Configures logging, file paths, download preferences (video, audio,
        captions), and lists for URLs/queries. Creates destination directories
        if they don't exist.
        """
        # --- Search Filters for pytubefix.contrib.search.Filter --- #
        self.relevance_filter = Filter.SortBy.RELEVANCE
        self.upload_date_filter = Filter.SortBy.UPLOAD_DATE
        self.view_count_filter = Filter.SortBy.VIEW_COUNT
        self.rating_filter = Filter.SortBy.RATING

        # --- Logging Configuration --- #
        self.log_date_format = "%Y-%m-%d %H:%M:%S"
        self.log_format = (
            "%(asctime)s | %(levelname)s : %(message)s"  # Corrected log_format
        )
        self.log_level = logging.INFO

        # Configure base logging to display INFO level and above to stdout
        logging.basicConfig(
            level=self.log_level,
            format=self.log_format,
            datefmt=self.log_date_format,
            stream=sys.stdout,
            force=True,  # Force Colab to use this config, overriding defaults
        )
        self.logger: logging.Logger = logging.getLogger()

        # Add a TimedRotatingFileHandler to save logs to a file, rotating daily
        self.file_handler = TimedRotatingFileHandler(
            filename="pytub.log",
            when="midnight",  # Rotate at midnight every day
            interval=1,  # Every 1 day
            backupCount=7,  # Keep 7 backup log files (for a week)
        )
        self.file_handler.suffix = "_%Y%m%d.log"  # Suffix for rotated log files
        self.formatter = logging.Formatter(
            fmt=self.log_format, datefmt=self.log_date_format
        )
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)

        # --- Environment Settings --- #
        self.env = os.getenv("ENV", "DEV")

        # --- File and Directory Settings --- #
        self.max_file_length = 63  # Maximum length for filenames
        self.dry_run = False  # If True, no actual downloads will occur
        self.download_all_streams = (
            False  # If True, download all available streams (not used extensively)
        )
        self.base_path = "./drive/MyDrive/"  # Base path for saving files
        self.video_destination_directory = os.path.join(
            self.base_path, "日本演歌"
        )  # Default video destination
        self.audio_destination_directory = os.path.join(
            self.base_path, "日本演歌-Audio"
        )  # Default audio destination

        # --- Download Preferences --- #
        self.download_captions = True  # Download captions if available

        self.download_video = True  # Enable video download
        self.video_extension = "mp4"  # Desired video file extension
        self.video_mime_type = "mp4"  # Video MIME type filter
        self.video_resolution = "1080p"  # Desired video resolution
        self.video_codec = "av1"  # Desired video codec (not strictly enforced)
        self.keep_original_video = (
            False  # Keep original video file after conversion/merging
        )
        self.progressive_streams = (
            False  # Filter for progressive streams (video and audio combined)
        )
        self.stream_order_by = "itag"  # Stream sorting order

        self.download_audio = True  # Enable audio download
        self.audio_extension = "mp3"  # Desired audio file extension
        self.audio_mime_type = "mp4"  # Audio MIME type filter (e.g., 'mp4' for m4a)
        self.audio_bitrate = "128kbps"  # Desired audio bitrate
        self.audio_codec = "abr"  # Desired audio codec (not strictly enforced)
        self.keep_original_audio = False  # Keep original audio file after conversion

        self.reconvert_media = True  # Reconvert video/audio to merge or re-encode
        self.convert_video_codec = (
            None  # Codec for video re-encoding (moviepy) - None for auto
        )
        self.convert_audio_codec = (
            "aac"  # Codec for audio re-encoding (moviepy) - None for auto
        )

        # --- Modes of Operation --- #
        self.enable_playlist_download = True  # Enable playlist downloads
        self.enable_channel_download = False  # Enable channel downloads
        self.enable_quick_search_download = False  # Enable quick search downloads

        # Create destination directories if they don't exist
        os.makedirs(self.video_destination_directory, exist_ok=True)
        os.makedirs(self.audio_destination_directory, exist_ok=True)

        # --- Lists of URLs for individual videos, playlists, channels,
        # and search queries --- #
        self.video_urls = [
            # Example video URLs (commented out)
            # "https://www.youtube.com/watch?v=zb3nAoJJGYo",
        ]

        self.playlist_urls = [
            # Example playlist URLs
            "https://youtube.com/playlist?list=PLhkqiApN_VYay4opZamqmnHIeKQtR9l-T&si=KYV2DqljMbF0W4mQ",  # 日本演歌
        ]

        self.channel_urls = [
            # Example channel URLs
        ]

        self.search_queries = [
            # Example search queries: (query string, filter, top N results)
            # ("learn english", self.relevance_filter, 3),
        ]

    def _retry_function(self, retries: int = 1, delay: int = 1):
        """A decorator to retry a function multiple times with a delay
        between retries.

        Args:
            retries (int): The number of times to retry the function.
            delay (int): The delay in seconds between retries.

        Returns:
            Callable: A decorator function that wraps the target function.
        """

        def outer_decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                err = ""
                for i in range(1, retries + 1):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        self.logger.error(
                            f"Retry [{func.__module__}.{func.__name__}] "
                            f"[{i}/{retries}] delay [{delay}] secs, reason: {e}"
                        )
                        time.sleep(delay)
                        err = str(e)
                # If all retries fail, re-raise the last exception
                raise Exception(
                    f"[{func.__module__}.{func.__name__}] All retries failed: {err}"
                )

            return wrapper

        return outer_decorator

    @staticmethod
    def _remove_characters(filename: str) -> str:
        """Removes illegal characters from a filename string.

        Args:
            filename (str): The original filename string.

        Returns:
            str: The cleaned filename string, safe for file system operations.
        """
        cleaned_filename = ""
        # List of characters typically illegal in Windows/Unix filenames.
        # Using a set for efficient lookup.
        illegal_chars_set = {
            "｜",
            ",",
            "/",
            "\\",
            ":",
            "*",
            "?",
            "<",
            ">",
            '"',
        }  # Corrected illegal_chars_set
        for char in filename:
            if char not in illegal_chars_set:
                cleaned_filename += char
        return cleaned_filename

    def _get_comparable_name(self, original_string: str) -> str:
        """Normalizes a string for consistent comparison, especially for filenames.

        This involves Unicode normalization, standardizing spaces, and applying
        `helpers.safe_filename` to sanitize for filename compatibility and length.

        Args:
            original_string (str): The input string (e.g., YouTube video title or
                                   existing filename).

        Returns:
            str: The normalized and safe string for comparison.
        """
        if not isinstance(original_string, str):
            self.logger.warning(
                f"_get_comparable_name received non-string input: "
                f"{type(original_string)}. Returning original input."
            )
            return original_string
        # 1. Unicode Normalization (NFKC for compatibility, e.g., 'シん゙' to 'ジ')
        normalized_string = unicodedata.normalize("NFKC", original_string)
        # 2. Replace ideographic space (U+3000) with standard space (U+0020)
        normalized_string = normalized_string.replace("\u3000", " ")
        # 3. Apply helpers.safe_filename for compatibility and length
        return helpers.safe_filename(
            s=normalized_string, max_length=self.max_file_length
        )

    def _download_youtube_video(self, url: str) -> bool:
        """Downloads a YouTube video and its audio, optionally converting
        and merging them.

        Args:
            url (str): The URL of the YouTube video to download.

        Returns:
            bool: True if the download/processing was successful (or dry run),
                  False otherwise.
        """
        try:
            yt = YouTube(
                url=url,
                use_oauth=False,
                allow_oauth_cache=False,
                on_progress_callback=on_progress,
            )
            # Force update the YouTube object to fetch fresh data
            yt.check_availability()
        except (RegexMatchError, VideoUnavailable, LiveStreamError, ExtractError) as e:
            self.logger.error(
                f"Failed to initialize YouTube object for {url} due to "
                f"pytubefix error: {e}"
            )
            return False
        except Exception as e:
            self.logger.error(
                f"An unexpected error occurred while initializing YouTube "
                f"object for {url}: {e}"
            )
            return False

        self.logger.info(f"Title: {yt.title}")
        self.logger.info(f"Duration: {yt.length} sec")
        self.logger.info("---")
        if self.dry_run:
            self.logger.info("Dry run: No actual download will occur.")
            return True

        # Generate a safe filename from the video title
        filename_base = helpers.safe_filename(
            s=yt.title, max_length=self.max_file_length
        )
        video_full_filename = f"{filename_base}.{self.video_extension}"

        # Download captions if enabled
        if self.download_captions:
            for caption_code in yt.captions.keys():
                self.logger.debug(f"Available caption: {caption_code}")
                remote_caption_filepath = os.path.join(
                    self.video_destination_directory,
                    f"{video_full_filename}.{caption_code}.txt",
                )
                try:
                    caption_track = yt.captions[caption_code]
                    caption_track.save(remote_caption_filepath)
                    self.logger.info(
                        f"Caption for {caption_code} saved to "
                        f"{remote_caption_filepath}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Failed to save caption {caption_code} for {url}: {e}"
                    )

        temp_download_folder = "."  # Temporary folder for downloads
        remote_video_filepath = os.path.join(
            self.video_destination_directory, video_full_filename
        )

        if self.download_video:
            # Download video stream
            if not os.path.exists(remote_video_filepath):
                self.logger.info(
                    f"Attempting to download video to {remote_video_filepath}"
                )
                video_stream = (
                    yt.streams.filter(
                        progressive=self.progressive_streams,
                        mime_type=f"video/{self.video_mime_type}",
                        res=self.video_resolution,
                    )
                    .order_by(self.stream_order_by)
                    .desc()
                    .last()
                )
                if not video_stream:
                    # Fallback to highest resolution if specific filters yield no results
                    video_stream = yt.streams.get_highest_resolution(
                        progressive=self.progressive_streams
                    )
                    if not video_stream:
                        self.logger.error(
                            f"No suitable video stream found for {url} "
                            f"after fallback."
                        )
                        return False
                    self.logger.warning(
                        f"Specific video stream not found, downloading highest "
                        f"resolution: {video_stream}"
                    )

                self.logger.info(
                    f"Downloading video stream... itag={video_stream.itag} "
                    f"res={video_stream.resolution} "
                    f"video_code={video_stream.video_codec} "
                    f"abr={video_stream.abr} "
                    f"audio_code={video_stream.audio_codec}"
                )
                try:
                    video_stream.download(
                        output_path=temp_download_folder, filename=video_full_filename
                    )
                    self.logger.info(
                        f"Moving video file from "
                        f"{os.path.join(temp_download_folder, video_full_filename)} to "
                        f"{remote_video_filepath}"
                    )
                    shutil.move(
                        os.path.join(temp_download_folder, video_full_filename),
                        remote_video_filepath,
                    )
                except Exception as e:
                    self.logger.error(
                        f"Failed to download or move video for {url}: {e}"
                    )
                    return False
            else:
                self.logger.warning(
                    f"Remote video file [{remote_video_filepath}] already exists, "
                    f"skipping video download."
                )

        audio_full_filename = f"{filename_base}.{self.audio_extension}"
        original_audio_filename = f"{filename_base}.{self.audio_mime_type}"
        remote_audio_filepath = os.path.join(
            self.audio_destination_directory, audio_full_filename
        )
        temp_audio_filepath = os.path.join(
            temp_download_folder, original_audio_filename
        )

        if self.download_audio:
            # Download or extract audio stream
            if not os.path.exists(remote_audio_filepath):
                self.logger.info(
                    f"Attempting to download/convert audio to {remote_audio_filepath}"
                )
                audio_clip = None
                # If video was downloaded, try to extract audio from it first
                if os.path.exists(remote_video_filepath):
                    try:
                        video_clip_for_audio = VideoFileClip(remote_video_filepath)
                        audio_clip = video_clip_for_audio.audio
                        if audio_clip:
                            self.logger.info(
                                "Extracted audio from downloaded video file."
                            )
                        video_clip_for_audio.close()  # Close clip to release resources
                    except Exception as e:
                        self.logger.warning(
                            f"Could not extract audio from video file "
                            f"{remote_video_filepath}: {e}"
                        )

                # If no audio was extracted from video, download audio-only stream
                if not audio_clip:
                    self.logger.warning(
                        "No audio track found in original video or extraction failed, "
                        "downloading audio stream instead."
                    )
                    try:
                        audio_stream = (
                            yt.streams.filter(
                                mime_type=f"audio/{self.audio_mime_type}",
                                abr=self.audio_bitrate,
                            )
                            .asc()
                            .first()
                        )
                        if not audio_stream:
                            # Fallback to general audio-only stream if specific filter fails
                            audio_stream = yt.streams.get_audio_only(
                                subtype=self.audio_mime_type
                            )
                            if not audio_stream:
                                self.logger.error(
                                    f"No suitable audio stream found for {url} "
                                    f"after fallback."
                                )
                                return False
                            self.logger.warning(
                                f"Specific audio stream not found, downloading "
                                f"audio-only stream: {audio_stream}"
                            )

                        self.logger.info(
                            f"Downloading audio stream... itag={audio_stream.itag} "
                            f"res={audio_stream.resolution} "
                            f"video_code={audio_stream.video_codec} "
                            f"abr={audio_stream.abr} "
                            f"audio_code={audio_stream.audio_codec}"
                        )
                        audio_stream.download(
                            output_path=temp_download_folder,
                            filename=original_audio_filename,
                        )
                        audio_clip = AudioFileClip(temp_audio_filepath)
                    except Exception as e:
                        self.logger.error(
                            f"Failed to download audio stream for {url}: {e}"
                        )
                        return False

                # Write the audio clip to the final destination in the desired format
                if audio_clip:
                    try:
                        audio_clip.write_audiofile(
                            filename=remote_audio_filepath,
                            codec=None,  # Codec=None lets moviepy infer from extension
                        )
                        audio_clip.close()  # Close audio clip

                        # Handle original audio file (move or remove)
                        if (
                            self.keep_original_audio
                            and self.audio_mime_type != self.audio_extension
                            and os.path.exists(temp_audio_filepath)
                        ):
                            self.logger.info(
                                f"Moving original audio file from "
                                f"{temp_audio_filepath} to "
                                f"{os.path.join(self.audio_destination_directory, original_audio_filename)}"
                            )
                            shutil.move(
                                temp_audio_filepath,
                                os.path.join(
                                    self.audio_destination_directory,
                                    original_audio_filename,
                                ),
                            )
                        elif os.path.exists(temp_audio_filepath):
                            self.logger.info(
                                f"Removing temporary audio file {temp_audio_filepath}"
                            )
                            os.remove(temp_audio_filepath)
                    except Exception as e:
                        self.logger.error(
                            f"Failed to write or process audio file for {url}: {e}"
                        )
                        return False
                else:
                    self.logger.error(
                        f"No audio content available for {url} after all attempts."
                    )
                    return False
            else:
                self.logger.warning(
                    f"Remote audio file [{remote_audio_filepath}] already exists, "
                    f"skipping audio download."
                )

        # Merge video/audio if reconvert is enabled and both video and audio are present
        if (
            self.reconvert_media
            and self.download_video
            and self.download_audio
            and os.path.exists(remote_video_filepath)
            and os.path.exists(remote_audio_filepath)
        ):
            merged_video_temp_filename = (
                f"{filename_base}_merged.{self.video_extension}"
            )
            final_video_filepath_after_merge = os.path.join(
                self.video_destination_directory, video_full_filename
            )
            if self.keep_original_video:
                final_video_filepath_after_merge = os.path.join(
                    self.video_destination_directory, merged_video_temp_filename
                )

            # Check if the final merged file already exists and has audio
            if os.path.exists(final_video_filepath_after_merge):
                try:
                    existing_video_clip = VideoFileClip(
                        final_video_filepath_after_merge
                    )
                    if existing_video_clip.audio:
                        self.logger.warning(
                            f"Merged video file "
                            f"[{final_video_filepath_after_merge}] already exists "
                            f"with audio, skipping conversion."
                        )
                        existing_video_clip.close()
                        return True
                    existing_video_clip.close()
                except Exception as e:
                    self.logger.warning(
                        f"Could not check existing merged video for {url}: {e}"
                    )

            video_clip_to_merge = None
            audio_clip_to_merge = None
            final_merged_clip = None
            try:
                # Load the video and audio clips
                video_clip_to_merge = VideoFileClip(remote_video_filepath)
                self.logger.debug(
                    f"Video clip loaded: duration={video_clip_to_merge.duration}"
                )

                audio_clip_to_merge = AudioFileClip(remote_audio_filepath)
                self.logger.debug(
                    f"Audio clip loaded: duration={audio_clip_to_merge.duration}"
                )

                # Assign the audio to the video clip directly
                final_merged_clip = video_clip_to_merge
                final_merged_clip.audio = audio_clip_to_merge

                # Write the final video with the combined audio
                self.logger.info(
                    f"Writing final video with combined audio: "
                    f"temp={merged_video_temp_filename}, "
                    f"final={final_video_filepath_after_merge}"
                )
                final_merged_clip.write_videofile(
                    filename=merged_video_temp_filename,
                    codec=self.convert_video_codec,
                    audio_codec=self.convert_audio_codec,
                    temp_audiofile=os.path.join(
                        temp_download_folder, "_temp_audio.m4a"
                    ),
                    remove_temp=True,  # Remove temporary audio file created by moviepy
                    logger=self.logger,  # Pass custom logger to moviepy
                )
                self.logger.info("Video and audio merged successfully.")

                # Move the merged file to its final destination
                if not self.keep_original_video and os.path.exists(
                    remote_video_filepath
                ):
                    self.logger.info(
                        f"Removing original video file: {remote_video_filepath}"
                    )
                    os.remove(remote_video_filepath)

                self.logger.info(
                    f"Moving converted video from {merged_video_temp_filename} to "
                    f"{final_video_filepath_after_merge}"
                )
                shutil.move(
                    merged_video_temp_filename, final_video_filepath_after_merge
                )

            except Exception as e:
                self.logger.error(
                    f"An error occurred during video/audio merging for {url}: {e}"
                )
                return False

            finally:
                # Ensure all clips are closed to release resources
                if video_clip_to_merge is not None:
                    video_clip_to_merge.close()
                if audio_clip_to_merge is not None:
                    audio_clip_to_merge.close()
                if final_merged_clip is not None:
                    final_merged_clip.close()
        elif self.reconvert_media and self.download_video and self.download_audio:
            # Log cases where merging conditions are not met (e.g., file not found)
            if not os.path.exists(remote_video_filepath):
                self.logger.warning(
                    f"Cannot merge: Video file not found at "
                    f"{remote_video_filepath} for {url}"
                )
            if not os.path.exists(remote_audio_filepath):
                self.logger.warning(
                    f"Cannot merge: Audio file not found at "
                    f"{remote_audio_filepath} for {url}"
                )

        return True

    def _download_videos_from_list(self, videos: list) -> None:
        """Iterates through a list of video URLs or YouTube objects and
        downloads each one.

        Args:
            videos (list): A list containing video URLs (str) or YouTube objects.
        """
        if not videos:
            self.logger.info("No videos provided for download.")
            return

        for i, video_item in enumerate(videos):
            if isinstance(video_item, str):
                video_url = video_item
            elif isinstance(video_item, YouTube):
                video_url = video_item.watch_url
            else:
                self.logger.error(
                    f"Invalid video item type: {type(video_item)} encountered for "
                    f"item {i}. Skipping."
                )
                continue

            self.logger.info(f"Downloading video {video_url} [{i + 1}/{len(videos)}]")
            try:
                self._download_youtube_video(video_url)
            except BotDetection as e:
                self.logger.error(
                    f"Failed to download {video_url} due to bot detection: {e}. "
                    f"Consider changing client type or IP address."
                )
            except Exception as e:
                self.logger.error(
                    f"An unexpected error occurred while downloading {video_url}: {e}"
                )

    def _move_local_files_to_destinations(self) -> None:
        """Moves video and audio files from the current working directory to their
        respective destination folders, renaming them if necessary.

        This function is typically used after downloads if files are initially
        saved locally. It handles potential file system errors and logs them
        appropriately.
        """
        self.logger.debug(f"Current working directory: {os.getcwd()}")

        # Move video files
        videos_in_cwd = glob.glob(r"*.{ext}".format(ext=self.video_extension))
        self.logger.debug(f"Found video files in CWD: {videos_in_cwd}")
        for video_path in videos_in_cwd:
            try:
                base_name = os.path.basename(video_path)
                new_name = base_name[: self.max_file_length]
                final_destination_path = os.path.join(
                    self.video_destination_directory, new_name
                )

                # Rename in CWD first if necessary, then move
                if base_name != new_name:
                    os.rename(video_path, new_name)
                    video_path = new_name  # Update path for move operation

                shutil.move(video_path, final_destination_path)
                self.logger.info(
                    f"Moved video: {new_name} to {self.video_destination_directory}"
                )
            except FileNotFoundError:
                self.logger.warning(
                    f"Skipping move: Video file {video_path} not found in CWD."
                )
            except PermissionError:
                self.logger.error(
                    f"Permission denied when moving {video_path} to "
                    f"{self.video_destination_directory}. Check file permissions."
                )
            except Exception as e:
                self.logger.error(
                    f"An error occurred moving video file {video_path}: {e}"
                )

        # Move audio files
        audios_in_cwd = glob.glob(r"*.{ext}".format(ext=self.audio_extension))
        self.logger.debug(f"Found audio files in CWD: {audios_in_cwd}")
        for audio_path in audios_in_cwd:
            try:
                base_name = os.path.basename(audio_path)
                new_name = base_name[: self.max_file_length]
                final_destination_path = os.path.join(
                    self.audio_destination_directory, new_name
                )

                if base_name != new_name:
                    os.rename(audio_path, new_name)
                    audio_path = new_name

                shutil.move(audio_path, final_destination_path)
                self.logger.info(
                    f"Moved audio: {new_name} to {self.audio_destination_directory}"
                )
            except FileNotFoundError:
                self.logger.warning(
                    f"Skipping move: Audio file {audio_path} not found in CWD."
                )
            except PermissionError:
                self.logger.error(
                    f"Permission denied when moving {audio_path} to "
                    f"{self.audio_destination_directory}. Check file permissions."
                )
            except Exception as e:
                self.logger.error(
                    f"An error occurred moving audio file {audio_path}: {e}"
                )

    def _remove_double_extension_videos(self) -> None:
        """Renames files that have a double extension (e.g., .mp4.mp4) by removing
        the extra extension. This can occur if video files are downloaded with an
        added extension during a merge process.
        """
        self.logger.debug(f"Current working directory: {os.getcwd()}")
        videos_with_double_ext = glob.glob(
            os.path.join(
                self.video_destination_directory,
                r"*.{ext}.{ext}".format(ext=self.video_extension),
            )
        )
        self.logger.debug(
            f"Found original videos with double extension: {videos_with_double_ext}"
        )
        for video_path in videos_with_double_ext:
            try:
                source = video_path
                destination = video_path[: -len(f".{self.video_extension}")]
                self.logger.info(f"Moving original video: {source} to {destination}")
                shutil.move(source, destination)
            except FileNotFoundError:
                self.logger.warning(
                    f"Skipping rename: Original video file {video_path} not found."
                )
            except PermissionError:
                self.logger.error(
                    f"Permission denied when renaming {video_path}. "
                    f"Check file permissions."
                )
            except Exception as e:
                self.logger.error(
                    f"An error occurred renaming original video {video_path}: {e}"
                )

    def _compare_downloaded_audio_video_files(self) -> None:
        """Compares the list of video files with audio files in their respective
        destination folders. Logs any video files that do not have a corresponding
        audio file.
        """
        video_files = glob.glob(
            os.path.join(
                self.video_destination_directory,
                r"*.{ext}".format(ext=self.video_extension),
            )
        )
        video_basenames = sorted(
            [os.path.splitext(os.path.basename(video))[0] for video in video_files]
        )
        self.logger.info(
            f"Video files found in {self.video_destination_directory}: "
            f"{len(video_basenames)}"
        )

        audio_files = glob.glob(
            os.path.join(
                self.audio_destination_directory,
                r"*.{ext}".format(ext=self.audio_extension),
            )
        )
        audio_basenames = sorted(
            [os.path.splitext(os.path.basename(audio))[0] for audio in audio_files]
        )
        self.logger.info(
            f"Audio files found in {self.audio_destination_directory}: "
            f"{len(audio_basenames)}"
        )

        missing_audio_count = 0
        for video_name in video_basenames:
            if video_name not in audio_basenames:
                self.logger.warning(
                    f"Video file '{video_name}' in "
                    f"'{self.video_destination_directory}' "
                    f"has no matching audio file in "
                    f"'{self.audio_destination_directory}'."
                )
                missing_audio_count += 1
        if missing_audio_count == 0:
            self.logger.info(
                f"All video files in '{self.video_destination_directory}' have "
                f"matching audio files in '{self.audio_destination_directory}'."
            )

    def _compare_playlist_downloads(self) -> None:
        """Compares downloaded files (video and audio) against the titles in defined
        playlists. It normalizes names to account for subtle differences and reports
        any missing files.
        """
        for playlist_url in self.playlist_urls:
            try:
                playlist = Playlist(playlist_url)
                self.logger.info(
                    f"Processing Playlist for comparison: {playlist.title}"
                )

                # Set dynamic destination paths based on playlist title for comparison
                current_video_dst = os.path.join(
                    self.base_path,
                    helpers.safe_filename(
                        playlist.title, max_length=self.max_file_length
                    ),
                )
                current_audio_dst = os.path.join(
                    self.base_path,
                    f"{helpers.safe_filename(playlist.title, max_length=self.max_file_length)}-Audio",
                )

                # Get existing video filenames, normalized
                downloaded_videos = glob.glob(
                    os.path.join(
                        current_video_dst,
                        r"*.{ext}".format(ext=self.video_extension),
                    )
                )
                downloaded_video_basenames = sorted(
                    [
                        os.path.splitext(os.path.basename(v))[0]
                        for v in downloaded_videos
                    ]
                )
                normalized_downloaded_videos = sorted(
                    [
                        self._get_comparable_name(item)
                        for item in downloaded_video_basenames
                    ]
                )
                self.logger.debug(
                    f"Normalized video basenames for '{playlist.title}': "
                    f"{normalized_downloaded_videos}"
                )

                # Get existing audio filenames, normalized
                downloaded_audios = glob.glob(
                    os.path.join(
                        current_audio_dst,
                        r"*.{ext}".format(ext=self.audio_extension),
                    )
                )
                downloaded_audio_basenames = sorted(
                    [
                        os.path.splitext(os.path.basename(a))[0]
                        for a in downloaded_audios
                    ]
                )
                normalized_downloaded_audios = sorted(
                    [
                        self._get_comparable_name(item)
                        for item in downloaded_audio_basenames
                    ]
                )
                self.logger.debug(
                    f"Normalized audio basenames for '{playlist.title}': "
                    f"{normalized_downloaded_audios}"
                )

                self.logger.info(
                    f"Number of videos found for '{playlist.title}': "
                    f"{len(downloaded_video_basenames)}, "
                    f"Number of audios found: {len(downloaded_audio_basenames)}"
                )

                # Get YouTube video titles from the playlist, normalized
                youtube_titles = sorted(
                    [video_item.title for video_item in playlist.videos]
                )
                normalized_youtube_titles = sorted(
                    [self._get_comparable_name(title) for title in youtube_titles]
                )
                self.logger.debug(
                    f"Normalized YouTube titles from playlist '{playlist.title}': "
                    f"{normalized_youtube_titles}"
                )

                # Determine which set of downloaded files (video or audio) is larger
                # for comparison. This assumes if one is present, the other should be too.
                normalized_target_set = (
                    set(normalized_downloaded_videos)
                    if len(normalized_downloaded_videos)
                    >= len(normalized_downloaded_audios)
                    else set(normalized_downloaded_audios)
                )

                missing_count = 0
                for comparable_yt_title in normalized_youtube_titles:
                    if comparable_yt_title not in normalized_target_set:
                        missing_count += 1
                        original_title_idx = normalized_youtube_titles.index(
                            comparable_yt_title
                        )
                        original_yt_title = youtube_titles[original_title_idx]
                        self.logger.info(
                            f"Missing file detected in playlist "
                            f"'{playlist.title}': "
                            f"original_title='{original_yt_title!r}', "
                            f"comparable_title='{comparable_yt_title!r}'"
                        )
                if missing_count == 0:
                    self.logger.info(
                        f"All videos in playlist '{playlist.title}' found in downloads."
                    )
                else:
                    self.logger.warning(
                        f"Total missing files found for playlist '{playlist.title}': "
                        f"{missing_count}"
                    )
            except (
                RegexMatchError,
                VideoUnavailable,
                LiveStreamError,
                ExtractError,
            ) as e:
                self.logger.error(
                    f"Failed to load playlist {playlist_url} for comparison "
                    f"due to pytubefix error: {e}"
                )
            except Exception as e:
                self.logger.error(
                    f"An unexpected error occurred during playlist comparison "
                    f"for {playlist_url}: {e}"
                )

    def _find_duplicated_titles_in_playlists(self) -> list:
        """Checks for and logs duplicated video titles within each configured playlist.

        Returns:
            list: A list of duplicated original video titles found across all playlists.
                  Returns an empty list if no duplicates or if playlist cannot be processed.
        """
        all_duplicated_titles = []
        for playlist_url in self.playlist_urls:
            try:
                playlist = Playlist(playlist_url)
                self.logger.info(
                    f"Checking for duplicated titles in playlist: {playlist.title}"
                )
                video_titles = [video.title for video in playlist.videos]
                seen_normalized_titles = set()
                duplicated_original_titles_in_playlist = []

                for title in video_titles:
                    # Normalize the title before checking for duplicates
                    normalized_title = self._get_comparable_name(title)
                    if normalized_title in seen_normalized_titles:
                        duplicated_original_titles_in_playlist.append(title)
                    else:
                        seen_normalized_titles.add(normalized_title)

                if len(duplicated_original_titles_in_playlist) > 0:
                    self.logger.warning(
                        f"Found duplicated titles in playlist '{playlist.title}'!!!"
                    )
                    for duplicated_title in duplicated_original_titles_in_playlist:
                        self.logger.warning(f"  - Duplicated: {duplicated_title}")
                    all_duplicated_titles.extend(duplicated_original_titles_in_playlist)
                else:
                    self.logger.info(
                        f"No duplicated titles found in playlist '{playlist.title}'."
                    )
            except (
                RegexMatchError,
                VideoUnavailable,
                LiveStreamError,
                ExtractError,
            ) as e:
                self.logger.error(
                    f"Failed to load playlist {playlist_url} for duplication check "
                    f"due to pytubefix error: {e}"
                )
            except Exception as e:
                self.logger.error(
                    f"An unexpected error occurred during duplication check "
                    f"for playlist {playlist_url}: {e}"
                )
        return all_duplicated_titles

    def run(self) -> None:
        """Orchestrates the YouTube downloading process.

        This method processes individual videos, playlists, channels, and
        quick searches based on the instance's configuration lists (video_urls,
        playlist_urls, channel_urls, search_queries). It dynamically adjusts
        destination paths for playlists.
        """
        self.logger.info("Starting individual video downloads...")
        self._download_videos_from_list(self.video_urls)

        if self.enable_playlist_download:
            self.logger.info("Starting playlist downloads...")
            # Store original DST paths, as they change per playlist processing
            original_global_video_dst = self.video_destination_directory
            original_global_audio_dst = self.audio_destination_directory

            for playlist_url in self.playlist_urls:
                try:
                    playlist = Playlist(playlist_url)
                    self.logger.info(f"Processing Playlist: {playlist.title}")
                    # Set dynamic destination folders based on playlist title
                    self.video_destination_directory = os.path.join(
                        self.base_path,
                        helpers.safe_filename(
                            playlist.title, max_length=self.max_file_length
                        ),
                    )
                    self.audio_destination_directory = os.path.join(
                        self.base_path,
                        f"{helpers.safe_filename(playlist.title, max_length=self.max_file_length)}-Audio",
                    )
                    os.makedirs(self.video_destination_directory, exist_ok=True)
                    os.makedirs(self.audio_destination_directory, exist_ok=True)
                    self.logger.info(
                        f"Video destination: {self.video_destination_directory}, "
                        f"Audio destination: {self.audio_destination_directory}"
                    )
                    self._download_videos_from_list(playlist.videos)
                except (
                    RegexMatchError,
                    VideoUnavailable,
                    LiveStreamError,
                    ExtractError,
                ) as e:
                    self.logger.error(
                        f"Unable to process Playlist {playlist_url} "
                        f"due to pytubefix error: {e}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"An unexpected error occurred while processing Playlist "
                        f"{playlist_url}: {e}"
                    )

            # Restore original global DST paths after playlist processing
            self.video_destination_directory = original_global_video_dst
            self.audio_destination_directory = original_global_audio_dst

        if self.enable_channel_download:
            self.logger.info("Starting channel downloads...")
            for channel_url in self.channel_urls:
                try:
                    channel = Channel(channel_url)
                    self.logger.info(f"Processing Channel: {channel.channel_name}")
                    self._download_videos_from_list(channel.videos)
                except (
                    RegexMatchError,
                    VideoUnavailable,
                    LiveStreamError,
                    ExtractError,
                ) as e:
                    self.logger.error(
                        f"Unable to process Channel {channel_url} "
                        f"due to pytubefix error: {e}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"An unexpected error occurred while processing Channel "
                        f"{channel_url}: {e}"
                    )

        if self.enable_quick_search_download:
            self.logger.info("Starting quick search downloads...")
            for query_string, search_filter, top_n_results in self.search_queries:
                filters_obj = (
                    Filter.create()
                    .type(Filter.Type.VIDEO)
                    .sort_by(Filter.SortBy(search_filter))
                )
                try:
                    search_results = Search(query_string, filters=filters_obj)
                    if not search_results.videos:
                        self.logger.warning(
                            f"No videos found for search query '{query_string}'."
                        )
                        continue
                    for video in search_results.videos[:top_n_results]:
                        self.logger.info(f"Search result Title: {video.title}")
                        self.logger.info(f"Search result URL: {video.watch_url}")
                        self.logger.info(f"Search result Duration: {video.length} sec")
                        self.logger.info("---")
                        self._download_videos_from_list([video])
                except (
                    RegexMatchError,
                    VideoUnavailable,
                    LiveStreamError,
                    ExtractError,
                ) as e:
                    self.logger.error(
                        f"Unable to perform Search for '{query_string}' "
                        f"due to pytubefix error: {e}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"An unexpected error occurred during Search for "
                        f"'{query_string}': {e}"
                    )


# --- Pytest Fixtures and Test Cases --- #


@pytest.fixture
def downloader_instance():
    # Use a dummy logger for tests to avoid file I/O for logs
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.CRITICAL)  # Suppress output during tests
    # Create a fresh instance for each test
    downloader = YouTubeDownloader()
    downloader.logger = logger  # Override the logger with a dummy one
    # Ensure test directories are clean before and after each test
    test_video_dir = "./test_video_dst"
    test_audio_dir = "./test_audio_dst"
    if os.path.exists(test_video_dir):
        shutil.rmtree(test_video_dir)
    if os.path.exists(test_audio_dir):
        shutil.rmtree(test_audio_dir)
    os.makedirs(test_video_dir, exist_ok=True)
    os.makedirs(test_audio_dir, exist_ok=True)
    downloader.video_destination_directory = test_video_dir
    downloader.audio_destination_directory = test_audio_dir
    yield downloader
    if os.path.exists(test_video_dir):
        shutil.rmtree(test_video_dir)
    if os.path.exists(test_audio_dir):
        shutil.rmtree(test_audio_dir)


@pytest.fixture
def mock_youtube_object():
    with patch("pytubefix.YouTube") as mock_yt:
        mock_yt_instance = MagicMock()
        mock_yt_instance.title = "Test Video Title"
        mock_yt_instance.length = 120
        mock_yt_instance.check_availability.return_value = None
        mock_yt_instance.captions = {"en": MagicMock(save=MagicMock())}

        # Mock stream objects
        mock_video_stream = MagicMock(
            itag=22,
            resolution="720p",
            video_codec="avc1",
            abr="128kbps",
            audio_codec="aac",
            download=MagicMock(),
        )
        mock_audio_stream = MagicMock(
            itag=140, abr="128kbps", audio_codec="aac", download=MagicMock()
        )

        # Mock .streams.filter().order_by().desc().last()
        mock_yt_instance.streams.filter.return_value.order_by.return_value.desc.return_value.last.return_value = (
            mock_video_stream
        )

        # Mock .streams.get_highest_resolution()
        mock_yt_instance.streams.get_highest_resolution.return_value = mock_video_stream

        # Mock .streams.filter(mime_type=...).asc().first()
        mock_yt_instance.streams.filter.return_value.asc.return_value.first.return_value = (
            mock_audio_stream
        )

        # Mock .streams.get_audio_only()
        mock_yt_instance.streams.get_audio_only.return_value = mock_audio_stream

        mock_yt.return_value = mock_yt_instance
        yield mock_yt_instance


@pytest.fixture
def mock_playlist_object():
    with patch("pytubefix.Playlist") as mock_pl:
        mock_pl_instance = MagicMock()
        mock_pl_instance.title = "Test Playlist Title"
        mock_video1 = MagicMock(title="Video 1", watch_url="http://test.com/v1")
        mock_video2 = MagicMock(title="Video 2", watch_url="http://test.com/v2")
        mock_pl_instance.videos = [mock_video1, mock_video2]
        mock_pl.return_value = mock_pl_instance
        yield mock_pl_instance


@pytest.fixture
def mock_channel_object():
    with patch("pytubefix.Channel") as mock_ch:
        mock_ch_instance = MagicMock()
        mock_ch_instance.channel_name = "Test Channel Name"
        mock_video1 = MagicMock(
            title="Channel Video 1", watch_url="http://test.com/cv1"
        )
        mock_ch_instance.videos = [mock_video1]
        mock_ch.return_value = mock_ch_instance
        yield mock_ch_instance


@pytest.fixture
def mock_search_object():
    with patch("pytubefix.Search") as mock_s:
        mock_s_instance = MagicMock()
        mock_video1 = MagicMock(
            title="Search Result 1", watch_url="http://test.com/sr1", length=60
        )
        mock_s_instance.videos = [mock_video1]
        mock_s.return_value = mock_s_instance
        yield mock_s_instance


@pytest.fixture
def mock_moviepy_clips():
    with patch("moviepy.AudioFileClip") as mock_afc, patch(
        "moviepy.VideoFileClip"
    ) as mock_vfc:
        mock_audio_clip = MagicMock()
        mock_audio_clip.close.return_value = None
        mock_audio_clip.write_audiofile.return_value = None
        mock_afc.return_value = mock_audio_clip

        mock_video_clip = MagicMock()
        mock_video_clip.close.return_value = None
        mock_video_clip.audio = None  # Initially no audio
        mock_video_clip.set_audio.return_value = mock_video_clip
        mock_video_clip.write_videofile.return_value = None
        mock_vfc.return_value = mock_video_clip

        yield {"audio_clip": mock_audio_clip, "video_clip": mock_video_clip}


@pytest.fixture
def mock_filesystem(tmp_path):
    # Use tmp_path fixture for isolated file system operations
    with patch("os.path.exists") as mock_exists, patch(
        "os.remove"
    ) as mock_remove, patch("os.makedirs") as mock_makedirs, patch(
        "shutil.move"
    ) as mock_move, patch(
        "glob.glob"
    ) as mock_glob, patch(
        "pytubefix.helpers.safe_filename"
    ) as mock_safe_filename:

        mock_safe_filename.side_effect = lambda s, max_length: s.replace(" ", "_")[
            :max_length
        ]

        # Default behavior: files don't exist unless specified
        mock_exists.return_value = False

        # Allow os.makedirs to behave normally for the test directories
        mock_makedirs.side_effect = lambda path, exist_ok=False: None

        # Simulate initial directory creation by the downloader
        os.makedirs("./test_video_dst", exist_ok=True)
        os.makedirs("./test_audio_dst", exist_ok=True)

        # Mock glob to return empty lists by default, or specific files if needed
        mock_glob.return_value = []

        yield {
            "exists": mock_exists,
            "remove": mock_remove,
            "makedirs": mock_makedirs,
            "move": mock_move,
            "glob": mock_glob,
            "safe_filename": mock_safe_filename,
        }


# --- Test Cases for _remove_characters ---


def test_remove_characters_valid_string():
    assert (
        YouTubeDownloader._remove_characters("normal_file_name.mp4")
        == "normal_file_name.mp4"
    )


def test_remove_characters_with_illegal_chars():
    assert (
        YouTubeDownloader._remove_characters('file|name*with?illegal\\chars"')
        == "filenamewithillegalchars"
    )


def test_remove_characters_empty_string():
    assert YouTubeDownloader._remove_characters("") == ""


def test_remove_characters_only_illegal_chars():
    assert YouTubeDownloader._remove_characters('|*?"<>') == ""


def test_remove_characters_mixed_chars():
    assert (
        YouTubeDownloader._remove_characters("valid!file|name.mp4?")
        == "valid!filename.mp4"
    )


# --- Test Cases for _get_comparable_name ---


def test_get_comparable_name_non_string_input(
    downloader_instance, caplog, mock_filesystem
):
    with caplog.at_level(logging.WARNING):
        result = downloader_instance._get_comparable_name(123)
        assert result == 123
        assert "_get_comparable_name received non-string input" in caplog.text
    mock_filesystem[
        "safe_filename"
    ].assert_not_called()  # Should not call safe_filename for non-string


def test_get_comparable_name_valid_string(downloader_instance, mock_filesystem):
    test_string = "  My Awesome Video Title!  "
    expected_safe_filename_output = "My_Awesome_Video_Title!"
    mock_filesystem["safe_filename"].return_value = expected_safe_filename_output

    result = downloader_instance._get_comparable_name(test_string)
    assert result == expected_safe_filename_output
    # Verify unicodedata.normalize and replace('\u3000', ' ') implicitly by checking safe_filename call
    mock_filesystem["safe_filename"].assert_called_once_with(
        s="  My Awesome Video Title!  ", max_length=downloader_instance.max_file_length
    )


def test_get_comparable_name_unicode_and_ideographic_space(
    downloader_instance, mock_filesystem
):
    test_string = (
        "\u65e5\u672c\u8a9e \u306e \u30bf\u30a4\u30c8\u30eb\u3000(\u30c6\u30b9\u30c8)"
    )
    expected_safe_filename_output = (
        "\u65e5\u672c\u8a9e_\u306e_\u30bf\u30a4\u30c8\u30eb_(\u30c6\u30b9\u30c8)"
    )

    mock_filesystem["safe_filename"].return_value = expected_safe_filename_output

    result = downloader_instance._get_comparable_name(test_string)
    assert result == expected_safe_filename_output
    mock_filesystem["safe_filename"].assert_called_once_with(
        s="\u65e5\u672c\u8a9e \u306e \u30bf\u30a4\u30c8\u30eb (\u30c6\u30b9\u30c8)",  # Note: unicodedata.normalize & replace("\u3000", " ") happens before safe_filename
        max_length=downloader_instance.max_file_length,
    )


def test_get_comparable_name_truncation(downloader_instance, mock_filesystem):
    long_title = "A very very very very very very very very very very very very very very very very very long video title"
    downloader_instance.max_file_length = 20
    expected_safe_filename_output = "A_very_very_very_v"

    mock_filesystem["safe_filename"].side_effect = lambda s, max_length: s.replace(
        " ", "_"
    )[:max_length]

    result = downloader_instance._get_comparable_name(long_title)
    assert result == expected_safe_filename_output
    mock_filesystem["safe_filename"].assert_called_once_with(
        s="A very very very very very very very very very very very very very very very very very long video title",
        max_length=20,
    )


# --- Test Cases for _download_youtube_video ---


def test_download_youtube_video_success_with_mocks(
    downloader_instance,
    mock_youtube_object,
    mock_moviepy_clips,
    mock_filesystem,
    caplog,
):
    # Set up specific file system mock behaviors for this test
    # Sequence for os.path.exists calls:
    # 1. remote_video_filepath (initially False to trigger download)
    # 2. remote_audio_filepath (initially False to trigger download)
    # 3. existing_video_clip check (False, as we just downloaded)
    mock_filesystem["exists"].side_effect = [False, False, False, False] + [
        True
    ] * 10  # More True for subsequent checks

    # Ensure moviepy clips are returned by the mocks
    mock_audio_clip = mock_moviepy_clips["audio_clip"]
    mock_video_clip = mock_moviepy_clips["video_clip"]

    # Configure downloader instance for full download and merge
    downloader_instance.download_captions = True
    downloader_instance.download_video = True
    downloader_instance.download_audio = True
    downloader_instance.reconvert_media = True
    downloader_instance.video_resolution = "720p"
    downloader_instance.audio_bitrate = "128kbps"

    with caplog.at_level(logging.INFO):
        result = downloader_instance._download_youtube_video(
            "http://test.youtube.com/watch?v=test_id"
        )
        assert result is True
        assert "Title: Test Video Title" in caplog.text
        assert "Caption for en saved to" in caplog.text
        assert "Downloading video stream" in caplog.text
        assert "Downloading audio stream" in caplog.text
        assert "Video and audio merged successfully." in caplog.text

    # Verify mocks were called appropriately
    mock_youtube_object.check_availability.assert_called_once()
    mock_youtube_object.captions["en"].save.assert_called_once()

    # Ensure download was called on stream mocks
    mock_youtube_object.streams.filter.return_value.order_by.return_value.desc.return_value.last.return_value.download.assert_called_once_with(
        output_path=".", filename="Test_Video_Title.mp4"
    )
    mock_youtube_object.streams.filter.return_value.asc.return_value.first.return_value.download.assert_called_once_with(
        output_path=".", filename="Test_Video_Title.mp4"
    )

    # Verify moviepy operations for merging
    mock_moviepy_clips["video_clip"].audio = mock_moviepy_clips["audio_clip"]
    mock_moviepy_clips["video_clip"].write_videofile.assert_called_once()

    # Verify shutil.move was called for video and merged file
    assert mock_filesystem["move"].call_count >= 2
    mock_filesystem["move"].assert_any_call(
        os.path.join(".", "Test_Video_Title.mp4"),
        os.path.join(
            downloader_instance.video_destination_directory, "Test_Video_Title.mp4"
        ),
    )
    mock_filesystem["move"].assert_any_call(
        "Test_Video_Title_merged.mp4",
        os.path.join(
            downloader_instance.video_destination_directory, "Test_Video_Title.mp4"
        ),
    )

    # Verify os.remove was called for the original video file (without audio)
    mock_filesystem["remove"].assert_any_call(
        os.path.join(
            downloader_instance.video_destination_directory, "Test_Video_Title.mp4"
        )
    )


def test_download_youtube_video_video_only(
    downloader_instance,
    mock_youtube_object,
    mock_moviepy_clips,
    mock_filesystem,
    caplog,
):
    downloader_instance.download_video = True
    downloader_instance.download_audio = False
    downloader_instance.reconvert_media = False
    downloader_instance.download_captions = False

    # exists calls: remote_video_filepath (False to trigger download)
    mock_filesystem["exists"].side_effect = [False] + [True] * 10

    with caplog.at_level(logging.INFO):
        result = downloader_instance._download_youtube_video(
            "http://test.youtube.com/watch?v=video_only"
        )
        assert result is True
        assert "Downloading video stream" in caplog.text
        assert "Attempting to download/convert audio" not in caplog.text
        assert "Video and audio merged successfully." not in caplog.text

    mock_youtube_object.streams.filter.return_value.order_by.return_value.desc.return_value.last.return_value.download.assert_called_once()
    mock_youtube_object.streams.filter.return_value.asc.return_value.first.return_value.download.assert_not_called()
    mock_youtube_object.captions["en"].save.assert_not_called()
    mock_moviepy_clips["video_clip"].write_videofile.assert_not_called()
    mock_moviepy_clips["audio_clip"].write_audiofile.assert_not_called()


def test_download_youtube_video_audio_only(
    downloader_instance,
    mock_youtube_object,
    mock_moviepy_clips,
    mock_filesystem,
    caplog,
):
    downloader_instance.download_video = False
    downloader_instance.download_audio = True
    downloader_instance.reconvert_media = False
    downloader_instance.download_captions = False

    # exists calls: remote_audio_filepath (False to trigger download)
    mock_filesystem["exists"].side_effect = [False] * 10

    with caplog.at_level(logging.INFO):
        result = downloader_instance._download_youtube_video(
            "http://test.youtube.com/watch?v=audio_only"
        )
        assert result is True
        assert "Downloading audio stream" in caplog.text
        assert "Attempting to download video" not in caplog.text
        assert "Video and audio merged successfully." not in caplog.text

    mock_youtube_object.streams.filter.return_value.order_by.return_value.desc.return_value.last.return_value.download.assert_not_called()
    mock_youtube_object.streams.filter.return_value.asc.return_value.first.return_value.download.assert_called_once()
    mock_youtube_object.captions["en"].save.assert_not_called()
    mock_moviepy_clips["video_clip"].write_videofile.assert_not_called()
    mock_moviepy_clips["audio_clip"].write_audiofile.assert_called_once()


def test_download_youtube_video_caption_only(
    downloader_instance, mock_youtube_object, mock_filesystem, caplog
):
    downloader_instance.download_video = False
    downloader_instance.download_audio = False
    downloader_instance.download_captions = True
    downloader_instance.reconvert_media = False

    # No file system exists checks for video/audio to worry about, just captions
    mock_filesystem["exists"].return_value = False

    with caplog.at_level(logging.INFO):
        result = downloader_instance._download_youtube_video(
            "http://test.youtube.com/watch?v=caption_only"
        )
        assert result is True
        assert "Caption for en saved to" in caplog.text
        assert "Downloading video stream" not in caplog.text
        assert "Downloading audio stream" not in caplog.text

    mock_youtube_object.captions["en"].save.assert_called_once()


def test_download_youtube_video_dry_run(
    downloader_instance, caplog, mock_youtube_object
):
    downloader_instance.dry_run = True
    with caplog.at_level(logging.INFO):
        result = downloader_instance._download_youtube_video(
            "http://test.youtube.com/watch?v=dry_run_id"
        )
        assert result is True
        assert "Dry run: No actual download will occur." in caplog.text
    mock_youtube_object.streams.filter.return_value.order_by.return_value.desc.return_value.last.return_value.download.assert_not_called()


def test_download_youtube_video_pytubefix_error_init(
    downloader_instance, mock_youtube_object, caplog
):
    mock_youtube_object.check_availability.side_effect = VideoUnavailable(
        "Video unavailable"
    )
    with caplog.at_level(logging.ERROR):
        result = downloader_instance._download_youtube_video(
            "http://test.youtube.com/watch?v=error_init"
        )
        assert result is False
        assert (
            "Failed to initialize YouTube object for http://test.youtube.com/watch?v=error_init due to pytubefix error: Video unavailable"
            in caplog.text
        )


def test_download_youtube_video_bot_detection_download(
    downloader_instance, mock_youtube_object, mock_filesystem, caplog
):
    mock_filesystem["exists"].side_effect = [False, False] + [True] * 10
    mock_youtube_object.streams.filter.return_value.order_by.return_value.desc.return_value.last.return_value.download.side_effect = BotDetection(
        "Bot detected"
    )

    with caplog.at_level(logging.ERROR):
        result = downloader_instance._download_youtube_video(
            "http://test.youtube.com/watch?v=bot_detect"
        )
        assert result is False
        assert (
            "Failed to download or move video for http://test.youtube.com/watch?v=bot_detect: Bot detected"
            in caplog.text
        )


def test_download_youtube_video_moviepy_error_merge(
    downloader_instance,
    mock_youtube_object,
    mock_moviepy_clips,
    mock_filesystem,
    caplog,
):
    downloader_instance.download_video = True
    downloader_instance.download_audio = True
    downloader_instance.reconvert_media = True
    downloader_instance.download_captions = False

    # Simulate files exist to allow merging to be attempted
    mock_filesystem["exists"].side_effect = [False, False] + [True] * 10
    mock_youtube_object.streams.filter.return_value.order_by.return_value.desc.return_value.last.return_value.download.return_value = (
        None
    )
    mock_youtube_object.streams.filter.return_value.asc.return_value.first.return_value.download.return_value = (
        None
    )

    # Mock moviepy's write_videofile to raise an exception
    mock_moviepy_clips["video_clip"].write_videofile.side_effect = Exception(
        "MoviePy merging error"
    )

    with caplog.at_level(logging.ERROR):
        result = downloader_instance._download_youtube_video(
            "http://test.youtube.com/watch?v=moviepy_error"
        )
        assert result is False
        assert (
            "An error occurred during video/audio merging for http://test.youtube.com/watch?v=moviepy_error: MoviePy merging error"
            in caplog.text
        )


def test_download_youtube_video_no_video_stream(
    downloader_instance, mock_youtube_object, caplog, mock_filesystem
):
    # Ensure video stream is None, triggering fallback, and then fallback also fails
    mock_youtube_object.streams.filter.return_value.order_by.return_value.desc.return_value.last.return_value = (
        None
    )
    mock_youtube_object.streams.get_highest_resolution.return_value = None
    mock_filesystem["exists"].return_value = False  # Ensure it tries to download

    with caplog.at_level(logging.ERROR):
        result = downloader_instance._download_youtube_video(
            "http://test.youtube.com/watch?v=no_video"
        )
        assert result is False
        assert (
            "No suitable video stream found for http://test.youtube.com/watch?v=no_video after fallback."
            in caplog.text
        )


def test_download_youtube_video_no_audio_stream_after_fallback(
    downloader_instance, mock_youtube_object, mock_filesystem, caplog
):
    downloader_instance.download_video = False
    downloader_instance.download_audio = True
    downloader_instance.reconvert_media = False
    downloader_instance.download_captions = False

    mock_filesystem["exists"].side_effect = [False] * 10
    mock_youtube_object.streams.filter.return_value.asc.return_value.first.return_value = (
        None
    )
    mock_youtube_object.streams.get_audio_only.return_value = (
        None  # Fallback also fails
    )

    with caplog.at_level(logging.ERROR):
        result = downloader_instance._download_youtube_video(
            "http://test.youtube.com/watch?v=no_audio_stream"
        )
        assert result is False
        assert (
            "No suitable audio stream found for http://test.youtube.com/watch?v=no_audio_stream after fallback."
            in caplog.text
        )


def test_download_youtube_video_video_already_exists(
    downloader_instance, mock_youtube_object, mock_filesystem, caplog
):
    downloader_instance.download_video = True
    downloader_instance.download_audio = False
    downloader_instance.reconvert_media = False

    # Simulate video file already exists
    mock_filesystem["exists"].side_effect = [True] * 10

    with caplog.at_level(logging.WARNING):
        result = downloader_instance._download_youtube_video(
            "http://test.youtube.com/watch?v=existing_video"
        )
        assert result is True
        assert (
            f"Remote video file [{os.path.join(downloader_instance.video_destination_directory, 'Test_Video_Title.mp4')}] already exists, skipping video download."
            in caplog.text
        )
    mock_youtube_object.streams.filter.return_value.order_by.return_value.desc.return_value.last.return_value.download.assert_not_called()


def test_download_youtube_video_audio_already_exists(
    downloader_instance, mock_youtube_object, mock_filesystem, caplog
):
    downloader_instance.download_video = False
    downloader_instance.download_audio = True
    downloader_instance.reconvert_media = False

    # Simulate audio file already exists
    mock_filesystem["exists"].side_effect = [True] * 10

    with caplog.at_level(logging.WARNING):
        result = downloader_instance._download_youtube_video(
            "http://test.youtube.com/watch?v=existing_audio"
        )
        assert result is True
        assert (
            f"Remote audio file [{os.path.join(downloader_instance.audio_destination_directory, 'Test_Video_Title.mp3')}] already exists, skipping audio download."
            in caplog.text
        )
    mock_youtube_object.streams.filter.return_value.asc.return_value.first.return_value.download.assert_not_called()


# --- Test Cases for _download_videos_from_list ---


def test_download_videos_from_list_empty(downloader_instance, caplog):
    # 1. Add a test case for `_download_videos_from_list` that verifies it handles an empty list of videos gracefully, logging an informational message. Use `caplog` to assert the log message.
    videos_list = []
    with caplog.at_level(logging.INFO):
        downloader_instance._download_videos_from_list(videos_list)
        assert "No videos provided for download." in caplog.text


@patch(
    "test_youtube_downloader.YouTubeDownloader._download_youtube_video",
    return_value=True,
)
def test_download_videos_from_list_multiple(
    mock_download_youtube_video, downloader_instance
):
    # 2. Add a test case for `_download_videos_from_list` that verifies it iterates through a list of URLs and calls `_download_youtube_video` for each valid video URL. Mock `_download_youtube_video` to assert its calls.
    video_urls = [
        MagicMock(title="Video 1", watch_url="http://test.com/v1"),
        MagicMock(title="Video 2", watch_url="http://test.com/v2"),
    ]
    downloader_instance._download_videos_from_list(video_urls)
    assert mock_download_youtube_video.call_count == 2
    mock_download_youtube_video.assert_any_call(video_urls[0].watch_url)
    mock_download_youtube_video.assert_any_call(video_urls[1].watch_url)


@patch(
    "test_youtube_downloader.YouTubeDownloader._download_youtube_video",
    return_value=True,
)
@patch(
    "pytubefix.helpers.safe_filename",
    side_effect=lambda s, max_length: s.replace(" ", "_"),
)
@patch("os.makedirs")
def test_run_playlist_download_success(
    mock_os_makedirs,
    mock_safe_filename,
    mock_download_youtube_video,
    downloader_instance,
    mock_playlist_object,
    caplog,
):
    # 3. Add a test case for `run` to verify that when `enable_playlist_download` is True, it correctly processes a playlist: instantiates `pytubefix.Playlist`, dynamically sets `video_destination_directory` and `audio_destination_directory` based on the playlist title, creates these directories (mock `os.makedirs`), and calls `_download_videos_from_list` with the playlist's videos. Ensure original destination directories are restored afterwards.
    downloader_instance.enable_playlist_download = True
    downloader_instance.playlist_urls = ["http://test.com/playlist"]
    mock_playlist_object.title = "My Awesome Playlist"
    mock_video1 = MagicMock(title="Playlist Video 1", watch_url="http://test.com/plv1")
    mock_video2 = MagicMock(title="Playlist Video 2", watch_url="http://test.com/plv2")
    mock_playlist_object.videos = [mock_video1, mock_video2]

    original_video_dst = downloader_instance.video_destination_directory
    original_audio_dst = downloader_instance.audio_destination_directory

    with caplog.at_level(logging.INFO):
        downloader_instance.run()
        assert "Starting playlist downloads..." in caplog.text
        assert "Processing Playlist: My Awesome Playlist" in caplog.text

        # Verify dynamic directory setting
        expected_video_dir = os.path.join(
            downloader_instance.base_path, "My_Awesome_Playlist"
        )
        expected_audio_dir = os.path.join(
            downloader_instance.base_path, "My_Awesome_Playlist-Audio"
        )
        mock_os_makedirs.assert_any_call(expected_video_dir, exist_ok=True)
        mock_os_makedirs.assert_any_call(expected_audio_dir, exist_ok=True)
        assert f"Video destination: {expected_video_dir}" in caplog.text
        assert f"Audio destination: {expected_audio_dir}" in caplog.text

        # Verify _download_videos_from_list was called with playlist videos
        mock_download_videos_from_list.call_count == 2
        mock_download_videos_from_list.assert_any_call(mock_video1.watch_url)
        mock_download_videos_from_list.assert_any_call(mock_video2.watch_url)

    # Verify original DST paths are restored
    assert downloader_instance.video_destination_directory == original_video_dst
    assert downloader_instance.audio_destination_directory == original_audio_dst


def test_run_playlist_download_pytubefix_error(
    downloader_instance, mock_playlist_object, caplog
):
    downloader_instance.enable_playlist_download = True
    downloader_instance.playlist_urls = ["http://invalid.playlist.com"]
    mock_playlist_object.side_effect = RegexMatchError("Could not parse playlist URL")

    with caplog.at_level(logging.ERROR):
        downloader_instance.run()
        assert (
            "Unable to process Playlist http://invalid.playlist.com due to pytubefix error: Could not parse playlist URL"
            in caplog.text
        )


def test_move_local_files_to_destinations(downloader_instance, mock_filesystem):
    # Simulate files existing in CWD
    mock_filesystem["glob"].side_effect = [
        ["file1.mp4", "file2.mp4"],  # for video_extension
        ["audio1.mp3", "audio2.mp3"],  # for audio_extension
    ]

    # Ensure files don't exist in destination initially (for shutil.move)
    mock_filesystem["exists"].return_value = False

    downloader_instance._move_local_files_to_destinations()

    assert mock_filesystem["move"].call_count == 4  # 2 videos + 2 audios
    mock_filesystem["move"].assert_any_call(
        "file1.mp4",
        os.path.join(downloader_instance.video_destination_directory, "file1.mp4"),
    )
    mock_filesystem["move"].assert_any_call(
        "audio1.mp3",
        os.path.join(downloader_instance.audio_destination_directory, "audio1.mp3"),
    )


def test_remove_double_extension_videos(downloader_instance, mock_filesystem):
    mock_filesystem["glob"].return_value = ["video.mp4.mp4"]
    mock_filesystem["exists"].return_value = True  # Simulate file exists to be moved

    downloader_instance._remove_double_extension_videos()

    mock_filesystem["move"].assert_called_once_with("video.mp4.mp4", "video.mp4")


def test_compare_downloaded_audio_video_files_missing_audio(
    downloader_instance, mock_filesystem, caplog
):
    mock_filesystem["glob"].side_effect = [
        ["video1.mp4", "video2.mp4"],  # videos
        ["video1.mp3"],  # audios
    ]

    with caplog.at_level(logging.WARNING):
        downloader_instance._compare_downloaded_audio_video_files()
        assert "Video file 'video2' in" in caplog.text
        assert "has no matching audio file." in caplog.text


@patch(
    "pytubefix.helpers.safe_filename",
    side_effect=lambda s, max_length: s.replace(" ", "_"),
)
def test_compare_playlist_downloads_missing_video(
    mock_safe_filename,
    downloader_instance,
    mock_playlist_object,
    mock_filesystem,
    caplog,
):
    downloader_instance.playlist_urls = ["http://test.com/playlist"]
    mock_playlist_object.title = "Test Playlist"
    mock_playlist_object.videos = [
        MagicMock(title="Video 1"),
        MagicMock(title="Video 2"),
    ]

    # Mock glob for downloaded files
    mock_filesystem["glob"].side_effect = [
        [
            f"{downloader_instance.base_path}Test_Playlist/Video_1.mp4"
        ],  # Downloaded videos
        [
            f"{downloader_instance.base_path}Test_Playlist-Audio/Video_1.mp3"
        ],  # Downloaded audios
    ]

    with caplog.at_level(logging.INFO):
        downloader_instance._compare_playlist_downloads()
        assert (
            "Missing file detected in playlist 'Test Playlist': original_title='Video 2', comparable_title='Video_2'"
            in caplog.text
        )
        assert (
            "Total missing files found for playlist 'Test Playlist': 1" in caplog.text
        )


@patch(
    "pytubefix.helpers.safe_filename",
    side_effect=lambda s, max_length: s.replace(" ", "_"),
)
def test_find_duplicated_titles_in_playlists(
    mock_safe_filename, downloader_instance, mock_playlist_object, caplog
):
    downloader_instance.playlist_urls = ["http://test.com/playlist"]
    mock_playlist_object.title = "Test Playlist"
    mock_playlist_object.videos = [
        MagicMock(title="Duplicate Video"),
        MagicMock(title="Unique Video"),
        MagicMock(title="Duplicate Video"),
    ]

    with caplog.at_level(logging.WARNING):
        duplicates = downloader_instance._find_duplicated_titles_in_playlists()
        assert "Found duplicated titles in playlist 'Test Playlist'!!!" in caplog.text
        assert "Duplicated: Duplicate Video" in caplog.text
        assert "Duplicate Video" in duplicates
        assert len(duplicates) == 1  # Only one original title is duplicated


@patch("test_youtube_downloader.YouTubeDownloader._download_videos_from_list")
@patch(
    "pytubefix.helpers.safe_filename",
    side_effect=lambda s, max_length: s.replace(" ", "_")[:max_length],
)
@patch("os.makedirs")
def test_run_playlist_download_success(
    mock_os_makedirs,
    mock_safe_filename,
    mock_download_videos_from_list,
    downloader_instance,
    mock_playlist_object,
    caplog,
):
    downloader_instance.enable_playlist_download = True
    downloader_instance.playlist_urls = ["http://test.com/playlist"]
    mock_playlist_object.title = "My Awesome Playlist"
    mock_video1 = MagicMock(title="Playlist Video 1", watch_url="http://test.com/plv1")
    mock_video2 = MagicMock(title="Playlist Video 2", watch_url="http://test.com/plv2")
    mock_playlist_object.videos = [mock_video1, mock_video2]

    original_video_dst = downloader_instance.video_destination_directory
    original_audio_dst = downloader_instance.audio_destination_directory

    with caplog.at_level(logging.INFO):
        downloader_instance.run()
        assert "Starting playlist downloads..." in caplog.text
        assert "Processing Playlist: My Awesome Playlist" in caplog.text

        # Verify dynamic directory setting
        expected_video_dir = os.path.join(
            downloader_instance.base_path, "My_Awesome_Playlist"
        )
        expected_audio_dir = os.path.join(
            downloader_instance.base_path, "My_Awesome_Playlist-Audio"
        )
        mock_os_makedirs.assert_any_call(expected_video_dir, exist_ok=True)
        mock_os_makedirs.assert_any_call(expected_audio_dir, exist_ok=True)
        assert f"Video destination: {expected_video_dir}" in caplog.text
        assert f"Audio destination: {expected_audio_dir}" in caplog.text

        # Verify _download_videos_from_list was called with playlist videos
        mock_download_videos_from_list.call_count == 2
        mock_download_videos_from_list.assert_any_call(mock_video1.watch_url)
        mock_download_videos_from_list.assert_any_call(mock_video2.watch_url)

    # Verify original DST paths are restored
    assert downloader_instance.video_destination_directory == original_video_dst
    assert downloader_instance.audio_destination_directory == original_audio_dst


def test_run_playlist_download_pytubefix_error(
    downloader_instance, mock_playlist_object, caplog
):
    downloader_instance.enable_playlist_download = True
    downloader_instance.playlist_urls = ["http://invalid.playlist.com"]
    mock_playlist_object.side_effect = RegexMatchError("Could not parse playlist URL")

    with caplog.at_level(logging.ERROR):
        downloader_instance.run()
        assert (
            "Unable to process Playlist http://invalid.playlist.com due to pytubefix error: Could not parse playlist URL"
            in caplog.text
        )
