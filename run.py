import hashlib
import os
import sys
import glob
import shutil
import time
import logging
import unicodedata
from functools import wraps
from logging.handlers import TimedRotatingFileHandler

import sqlite3
from datetime import datetime
import re

from moviepy import AudioFileClip, VideoFileClip

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

LOGGING_LEVEL = logging.DEBUG
DOWNLOAD_CAPTIONS = True
DOWNLOAD_VIDEO = False
DOWNLOAD_AUDIO = True
RECOVERT_MEDIA = False

PLAYLIST_DOWNLOAD = True
CHANNEL_DOWNLOAD = False
SEARCH_DOWNLOAD = False


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
            "%(asctime)s | %(levelname)s : %(message)s"  # Corrected logging format
        )
        self.log_level = LOGGING_LEVEL

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
        self.max_file_length = (
            255  # Maximum length for filenames (Increased from 63 to 255)
        )
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
        self.download_captions = DOWNLOAD_CAPTIONS  # Download captions if available

        self.download_video = DOWNLOAD_VIDEO  # Enable video download
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

        self.download_audio = DOWNLOAD_AUDIO  # Enable audio download
        self.audio_extension = "mp3"  # Desired audio file extension
        self.audio_mime_type = "mp4"  # Audio MIME type filter (e.g., 'mp4' for m4a)
        self.audio_bitrate = "128kbps"  # Desired audio bitrate
        self.audio_codec = "abr"  # Desired audio codec (not strictly enforced)
        self.keep_original_audio = False  # Keep original audio file after conversion

        self.reconvert_media = (
            RECOVERT_MEDIA  # Reconvert video/audio to merge or re-encode
        )
        self.convert_video_codec = (
            None  # Codec for video re-encoding (moviepy) - None for auto
        )
        self.convert_audio_codec = (
            "aac"  # Codec for audio re-encoding (moviepy) - None for auto
        )

        # --- Modes of Operation --- #
        self.enable_playlist_download = PLAYLIST_DOWNLOAD  # Enable playlist downloads
        self.enable_channel_download = CHANNEL_DOWNLOAD  # Enable channel downloads
        self.enable_quick_search_download = (
            SEARCH_DOWNLOAD  # Enable quick search downloads
        )

        # Create destination directories if they don't exist
        os.makedirs(self.video_destination_directory, exist_ok=True)
        os.makedirs(self.audio_destination_directory, exist_ok=True)

        # --- Lists of URLs for individual videos, playlists, channels,
        # and search queries --- #
        self.video_urls = [
            # Example video URLs (commented out)
            # "https://www.youtube.com/watch?v=zb3nAoJJGYo",
            # "https://www.youtube.com/watch?v=BmtYHnvQcqw",
            # "https://www.youtube.com/watch?v=6F-fAlGA0q0&list=PLf8MTi2c_8X-TLNg6tAjLaeb0jvmSQoX5",
            # "https://www.youtube.com/watch?v=SQmVb9wcMP0",
            # "https://www.youtube.com/watch?v=HLhHMsh5a94",
            # "https://youtube.com/watch?v=cFoXcO8llNI",
            # "https://youtube.com/watch?v=YsKKuCUYUMU",
            # "https://youtube.com/watch?v=bvymTFYfRmk",
            # "https://youtube.com/watch?v=TZkg2SL-dkY",
            # "https://youtube.com/watch?v=00T_pojzqpw",
            # "https://www.youtube.com/watch?v=AiIBKcd4m5Q",
            # "https://www.youtube.com/watch?v=7CgbJGUxRJg",
            # "https://www.youtube.com/watch?v=qQzdAsjWGPg",
            # "https://youtu.be/w019MzRosmk?si=-e4I9b3XNUE-W4nA",
            # "https://youtu.be/ixbcvKCl4Jc?si=InmzDQxhoSjjXuLA",
            # "https://www.youtube.com/watch?v=-tJtsKngXJU",
            # "https://www.youtube.com/watch?v=oKGkr1bd6-c",
            # "https://www.youtube.com/watch?v=I5PI1i2npGQ&list=RDI5PI1i2npGQ&start_radio=1",
            # "https://www.youtube.com/watch?v=fcVHGZVCkDI&list=RDI5PI1i2npGQ&index=2",
            # "https://youtu.be/KCy_5nhiXs0?si=jibLq1eGh6si4fD-",
            # "https://youtube.com/watch?v=7g9xcCMdwns",
            # "https://www.youtube.com/watch?v=YAnjSN9hhyM&list=RDYAnjSN9hhyM&start_radio=1",  # JOLIN 蔡依林 PLEASURE世界巡迴演唱會 TAIPEI 20260101 Full version
        ]

        self.playlist_urls = [
            # Example playlist URLs
            # "https://youtube.com/playlist?list=PLf8MTi2c_8X8Vz5JGI57tNy2BlbjZkMxC&si=PliaxKExX5U48kPV",
            # "https://youtube.com/playlist?list=PLf8MTi2c_8X-TLNg6tAjLaeb0jvmSQoX5",
            # "https://www.youtube.com/playlist?list=PLf8MTi2c_8X9XM74Pk2PuTKNo39C8bqTJ",
            # "https://www.youtube.com/playlist?list=PLf8MTi2c_8X9CEJU-Unr7Gs6I3RYh6r1Y",
            # "https://www.youtube.com/playlist?list=PLm390xdh7__Kp-7I-0uCjnYRaff79DaS-",  # Okinawa
            # "https://www.youtube.com/playlist?list=PLf8MTi2c_8X8IJcb11DCWoqTUH0QgsAOk",  # Okinawa
            # "https://www.youtube.com/playlist?list=PLf8MTi2c_8X9Kmoz_rwLVYe18TOrJ9T2P",  # My HiFi
            # "https://youtube.com/playlist?list=OLAK5uy_neh80RHNGYi1gPdfpaoGWpwhTzq-YLZP4",  # Le Roi Est Mort, Vive Le Roi!
            # "https://www.youtube.com/playlist?list=PLgQLKhyDqSaP0IPDJ7eXWvsGqOOH3_mqQ",  # 測試喇叭高HiFi音質音樂檔
            # "https://www.youtube.com/playlist?list=PLf8MTi2c_8X9IUHdNR6Busq_uZmsmXbv8",  # Christmas
            # "https://www.youtube.com/playlist?list=PL12UaAf_xzfpfxj4siikK9CW8idyZo2",  # 【日語】SPY×FAMILY間諜家家酒(全部集數)
            # "https://www.youtube.com/watch?v=7cQzvmJvLpU&list=PL1H2dev3GUtgYGOiJFWjZe2mX29VpraJN",  # 聽歌學英文
            # "https://www.youtube.com/playlist?list=PLwPx6OD5gb4imniZyKp7xo7pXew3QRTuq",  # QWER 1ST WORLDTOUR Setlist (Rockation, 2025)
            # "https://youtube.com/playlist?list=PLhkqiApN_VYay4opZamqmnHIeKQtR9l-T&si=KYV2DqljMbF0W4mQ",  # 日本演歌
            # "https://www.youtube.com/playlist?list=PLf8MTi2c_8X9IYfTrHA_fCb2Q7R72wtKZ",  # 投資
            "https://www.youtube.com/playlist?list=PLf8GXxJN5qee681F2CR1zxhEJTktJE7BT",  # QWER Color Coded Lyrics
        ]

        self.channel_urls = [
            # Example channel URLs
            # "https://www.youtube.com/@ProgrammingKnowledge/featured",
            # "https://www.youtube.com/@LillianChiu101",
            # "https://www.youtube.com/@kellytsaii",
        ]

        self.search_queries = [
            # Example search queries: (query string, filter, top N results)
            # ("Programming Knowledge", self.view_count_filter, 1),
            # ("GitHub Issue Best Practices", self.view_count_filter, 1),
            # ("global news", self.upload_date_filter, 5),
            # ("breaking news, 台灣 新聞", self.upload_date_filter, 3),
            ("learn english", self.relevance_filter, 3),
            # (
            #     "Tee TA Cote Jay Park) SURL  CY NRE 18  (C1) seaRseREa at  mbit  cram  ars Snow eae? SS . ey Me ",
            #     self.relevance_filter,
            #     2,
            # ),  # test garbled characters
        ]

    def _calculate_file_hash(
        self, filepath: str, hash_algorithm=hashlib.sha256, block_size=4096
    ) -> str:
        """Calculates the hash of a file to check for content duplication.

        Args:
            filepath (str): The path to the file.
            hash_algorithm: The hashing algorithm to use (e.g., hashlib.sha256).
            block_size (int): The size of chunks to read from the file.

        Returns:
            str: The hexadecimal representation of the file's hash, or an empty string
                 if the file does not exist or an error occurs.
        """
        if not os.path.exists(filepath):
            self.logger.warning(f"File not found for hash calculation: {filepath}")
            return ""

        hasher = hash_algorithm()
        try:
            with open(filepath, "rb") as f:
                for block in iter(lambda: f.read(block_size), b""):
                    hasher.update(block)
            return hasher.hexdigest()
        except Exception as e:
            self.logger.error(f"Error calculating hash for {filepath}: {e}")
            return ""

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
        illegal_chars_set = {"｜", ",", "/", "\\", ":", "*", "?", "<", ">", '"'}
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
        # 1. Unicode Normalization (NFKC for compatibility, e.g., 'ジ' to 'ジ')
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
            for caption in yt.captions.keys():
                caption_code = caption.code
                caption_name = caption.name
                self.logger.debug(
                    f"Available caption: {caption_code} name: {caption_name}"
                )
                remote_caption_filepath = os.path.join(
                    self.video_destination_directory,
                    f"{video_full_filename}.{caption_code}.txt",
                )
                try:
                    caption_track = yt.captions[caption_code]
                    caption_track.save_captions(remote_caption_filepath)
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
                video_stream = None

                # Attempt 1: Specific resolution and mime type
                self.logger.info(
                    f"Attempting to find specific video stream: res={self.video_resolution}, mime_type=video/{self.video_mime_type}"
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
                    # Attempt 2: Fallback to highest resolution progressive/non-progressive
                    self.logger.warning(
                        "Specific video stream not found. Trying fallback to highest resolution."
                    )
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
                    audio_stream = None

                    # Attempt 1: Specific audio mime type and bitrate
                    self.logger.info(
                        f"Attempting to find specific audio stream: mime_type=audio/{self.audio_mime_type}, abr={self.audio_bitrate}"
                    )
                    audio_stream = (
                        yt.streams.filter(
                            mime_type=f"audio/{self.audio_mime_type}",
                            abr=self.audio_bitrate,
                        )
                        .asc()
                        .first()
                    )

                    if not audio_stream:
                        # Attempt 2: Fallback to highest bitrate audio-only stream with preferred mime_type
                        self.logger.warning(
                            f"Specific audio stream not found. Trying highest bitrate audio-only with mime_type=audio/{self.audio_mime_type}."
                        )
                        audio_stream = yt.streams.get_audio_only(
                            subtype=self.audio_mime_type
                        )

                    if not audio_stream:
                        # Attempt 3: Fallback to any available audio-only stream
                        self.logger.warning(
                            "Highest bitrate audio-only stream not found. Trying any available audio-only stream."
                        )
                        audio_stream = yt.streams.get_audio_only()

                    if not audio_stream:
                        self.logger.error(
                            f"No suitable audio stream found for {url} after all fallbacks."
                        )
                        return False

                    self.logger.info(
                        f"Downloading audio stream... itag={audio_stream.itag} "
                        f"res={audio_stream.resolution} "
                        f"video_code={audio_stream.video_codec} "
                        f"abr={audio_stream.abr} "
                        f"audio_code={audio_stream.audio_codec}"
                    )
                    try:
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
                self.logger.info(f"Original Playlist Title: {playlist.title}")
                self.logger.info(
                    f"Safe Playlist Title (for directory): {helpers.safe_filename(playlist.title, max_length=self.max_file_length)}"
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
                self.logger.info(
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
                self.logger.info(
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
                self.logger.info(
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


class YouTubeTaskManager:
    """Manages YouTube video download tasks and their metadata in an SQLite database."""

    def __init__(self, db_name='youtube_tasks.db', video_dst_dir=None, audio_dst_dir=None):
        """Initializes the database connection and creates the tasks table if it doesn't exist."""
        self.db_name = db_name
        self.video_destination_directory = video_dst_dir
        self.audio_destination_directory = audio_dst_dir
        self.conn = None
        self.cursor = None
        self._connect()
        self.create_table()

    def _connect(self):
        """Establishes a connection to the SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            logging.info(f"Connected to database: {self.db_name}")
        except sqlite3.Error as e:
            logging.error(f"Error connecting to database {self.db_name}: {e}")
            sys.exit(1)

    def create_table(self):
        """Creates the 'tasks' table with required columns if it doesn't already exist."""
        try:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    youtube_id TEXT NOT NULL UNIQUE,
                    video_url TEXT NOT NULL,
                    suggested_filename_base TEXT,
                    final_filename_on_disk TEXT UNIQUE,
                    added_date TEXT,
                    last_updated_date TEXT
                )
            ''')
            self.conn.commit()
            logging.info("Table 'tasks' checked/created successfully.")
        except sqlite3.Error as e:
            logging.error(f"Error creating table 'tasks': {e}")
            sys.exit(1)

    @staticmethod
    def _extract_youtube_id(video_url: str) -> str:
        """Extracts the YouTube video ID from a given URL."""
        # Regex for various YouTube URL formats
        match = re.search(r"(?:v=|youtu\.be/|embed/|watch\?v=)([0-9A-Za-z_-]{11}).*", video_url)
        if match:
            return match.group(1)
        return ""

    def task_exists(self, youtube_id: str) -> bool:
        """Checks if a task with the given YouTube ID already exists in the database."""
        self.cursor.execute('SELECT 1 FROM tasks WHERE youtube_id = ?', (youtube_id,))
        return self.cursor.fetchone() is not None

    def add_task(self, video_url: str, video_title: str, max_file_length: int) -> dict or None:
        """Adds a new download task to the database, ensuring unique YouTube ID and filename."""
        youtube_id = self._extract_youtube_id(video_url)
        if not youtube_id:
            logging.error(f"Could not extract YouTube ID from URL: {video_url}")
            return None

        if self.task_exists(youtube_id):
            logging.info(f"Task for YouTube ID {youtube_id} already exists. Skipping addition.")
            return None

        # Generate initial suggested filename base
        suggested_filename_base = helpers.safe_filename(video_title, max_length=max_file_length)
        
        base_name_for_uniqueness = suggested_filename_base
        counter = 0
        final_filename_on_disk = suggested_filename_base

        # Check for filename collisions (on disk and in DB)
        while self._filename_collision_exists(final_filename_on_disk):
            counter += 1
            # Recalculate candidate filename for uniqueness
            suffix = f"_{counter}"
            # Ensure the name with suffix does not exceed max_file_length
            if len(base_name_for_uniqueness) + len(suffix) > max_file_length:
                # Truncate original base name to make space for the suffix
                truncated_base = base_name_for_uniqueness[:max_file_length - len(suffix)]
                final_filename_on_disk = f"{truncated_base}{suffix}"
            else:
                final_filename_on_disk = f"{base_name_for_uniqueness}{suffix}"

            if counter > 999: # Arbitrary high counter, if it still collides, give up to prevent infinite loop
                logging.warning(f"Could not generate unique filename for {video_title} within max_file_length {max_file_length} after many attempts. Skipping.")
                return None

        current_time = datetime.now().isoformat()
        try:
            self.cursor.execute(
                '''INSERT INTO tasks (youtube_id, video_url, suggested_filename_base, final_filename_on_disk, added_date, last_updated_date) VALUES (?, ?, ?, ?, ?, ?)''',
                (
                    youtube_id,
                    video_url,
                    suggested_filename_base,
                    final_filename_on_disk,
                    current_time,
                    current_time,
                ),
            )
            self.conn.commit()
            logging.info(f"Task added: {video_title} with unique filename {final_filename_on_disk}")
            return {
                "youtube_id": youtube_id,
                "video_url": video_url,
                "suggested_filename_base": suggested_filename_base,
                "final_filename_on_disk": final_filename_on_disk,
                "added_date": current_time,
                "last_updated_date": current_time,
            }
        except sqlite3.IntegrityError as e:
            logging.error(f"Database integrity error when adding task for {video_url}: {e}")
            return None
        except sqlite3.Error as e:
            logging.error(f"Error adding task for {video_url}: {e}")
            return None

    def _filename_collision_exists(self, filename_base: str) -> bool:
        """Checks if a filename (base name) already exists on disk or in the database."""
        # Check on disk (for both video and audio extensions)
        video_exists = False
        if self.video_destination_directory:
            video_exists = os.path.exists(os.path.join(self.video_destination_directory, f"{filename_base}.mp4"))
        
        audio_exists = False
        if self.audio_destination_directory:
            audio_exists = os.path.exists(os.path.join(self.audio_destination_directory, f"{filename_base}.mp3"))

        if video_exists or audio_exists:
            return True

        # Check in database (for final_filename_on_disk)
        self.cursor.execute('SELECT 1 FROM tasks WHERE final_filename_on_disk = ?', (filename_base,))
        if self.cursor.fetchone() is not None:
            return True
        return False

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")


def _main():
    """An alternative main function, likely for testing individual pytubefix functionalities.
    This function is not called in the main execution block but serves as an example.
    """
    playlist_list = [
        "https://youtube.com/playlist?list=PLhkqiApN_VYay4opZamqmnHIeKQtR9l-T&si=KYV2DqljMbF0W4mQ",  # 日本演歌
    ]
    example_url = "https://www.youtube.com/watch?v=7g9xcCMdwns"
    yt_obj = YouTube(example_url, on_progress_callback=on_progress)
    logging.info(f"YouTube Title: {yt_obj.title}")
    highest_res_stream = yt_obj.streams.get_highest_resolution(
        progressive=False, mime_type="video/mp4"
    )
    highest_res_stream.download(output_path="download/mtv/歌心りえ/")

    logging.info(f"Captions for video: {yt_obj.captions.keys()}")
    for caption_code_key in yt_obj.captions.keys():
        try:
            caption_track = yt_obj.captions[caption_code_key]
            caption_track.save(
                f"download/mtv/歌心りえ/{yt_obj.title}.{caption_code_key}.txt"
            )
            logging.info(f"Caption {caption_code_key} saved.")
        except Exception as e:
            logging.error(f"Failed to save caption {caption_code_key}: {e}")

    audio_only_stream = yt_obj.streams.get_audio_only()
    audio_only_stream.download(f"download/mtv/歌心りえ/{yt_obj.title}.m4a")
    logging.info("Audio-only stream downloaded.")

    playlist = Playlist(url=playlist_list[0])
    logging.info(f"Playlist Title: {playlist.title}")
    for video_in_playlist in playlist.videos:
        logging.info(f"Downloading audio for playlist video: {video_in_playlist.title}")
        audio_stream_playlist = video_in_playlist.streams.get_audio_only()
        audio_stream_playlist.download(output_path="download/mtv")

    channel_a = Channel("https://www.youtube.com/@ProgrammingKnowledge/featured")
    logging.info(f"Channel name: {channel_a.channel_name}")

    channel_b = Channel("https://www.youtube.com/@LillianChiu101")
    logging.info(f"Channel name: {channel_b.channel_name}")

    search_res = Search("GitHub Issue Best Practices")
    logging.info("Search results for 'GitHub Issue Best Practices':")
    for video_search_result in search_res.videos:
        logging.info(f"  Title: {video_search_result.title}")
        logging.info(f"  URL: {video_search_result.watch_url}")
        logging.info(f"  Duration: {video_search_result.length} sec")
        logging.info("---")

    search_filters = {
        "upload_date": Filter.get_upload_date("Today"),
        "type": Filter.get_type("Video"),
        "duration": Filter.get_duration("Under 4 minutes"),
        "features": [
            Filter.get_features("4K"),
            Filter.get_features("Creative Commons"),
        ],
        "sort_by": Filter.get_sort_by("Upload date"),
    }

    music_search_results = Search("music", filters=search_filters)
    logging.info("Search results for 'music' with filters:")
    for video_music_result in music_search_results.videos:
        logging.info(f"  Title: {video_music_result.title}")
        logging.info(f"  URL: {video_music_result.watch_url}")
        logging.info(f"  Duration: {video_music_result.length} sec")
        logging.info("---")


if __name__ == "__main__":
    # Instantiate the downloader and run the main process
    downloader = YouTubeDownloader()
    # Set arbitrary destination directories for demonstration if not already set globally
    if not downloader.video_destination_directory:
        downloader.video_destination_directory = os.path.join(
            downloader.base_path, "VideoDownloads"
        )
        os.makedirs(downloader.video_destination_directory, exist_ok=True)
    if not downloader.audio_destination_directory:
        downloader.audio_destination_directory = os.path.join(
            downloader.base_path, "AudioDownloads"
        )
        os.makedirs(downloader.audio_destination_directory, exist_ok=True)

    downloader.run()
    downloader._compare_playlist_downloads()

    # Example usage of YouTubeTaskManager
    task_manager = YouTubeTaskManager(
        video_dst_dir=downloader.video_destination_directory,
        audio_dst_dir=downloader.audio_destination_directory,
    )
    # Add a dummy task to demonstrate the new methods
    dummy_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    dummy_title = "Never Gonna Give You Up"
    task_manager.add_task(dummy_url, dummy_title, downloader.max_file_length)

    # Attempt to add the same task again to see duplicate handling
    task_manager.add_task(dummy_url, dummy_title, downloader.max_file_length)

    # Test filename collision (assuming a file named 'Test_1.mp3' might exist for demonstration)
    dummy_url_2 = "https://www.youtube.com/watch?v=TESTID123"
    dummy_title_2 = "Test"
    task_manager.add_task(dummy_url_2, dummy_title_2, downloader.max_file_length)
    task_manager.close()
