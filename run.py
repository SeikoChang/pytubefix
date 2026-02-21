import os
import sys
import glob
import shutil
import time
import logging
import unicodedata
from functools import wraps
from logging.handlers import TimedRotatingFileHandler

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


class YouTubeDownloader:
    def __init__(self):
        # --- Constants and Configuration --- #
        self.RELEVANCE = Filter.SortBy.RELEVANCE
        self.UPLOAD_DATE = Filter.SortBy.UPLOAD_DATE
        self.VIEW_COUNT = Filter.SortBy.VIEW_COUNT
        self.RATING = Filter.SortBy.RATING

        # Logging configuration (using the global logger)
        # Define date format separately for logging.Formatter
        self.LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
        # LOG_FORMAT now uses %(asctime)s, which will be formatted by LOG_DATEFMT
        self.LOG_FORMAT = "%(asctime)s | %(levelname)s : %(message)s"
        self.LOG_LEVEL = logging.INFO

        # Configure logging to display INFO level and above, and use the custom format
        # 'stream=sys.stdout' ensures output goes to the standard Colab output cell
        logging.basicConfig(
            level=self.LOG_LEVEL,
            format=self.LOG_FORMAT,
            datefmt=self.LOG_DATEFMT,  # Pass date format here
            stream=sys.stdout,
            force=True,  # This argument forces Colab to use this config, overriding defaults
        )
        self.logger: logging.Logger = logging.getLogger()
        # Add a TimedRotatingFileHandler to save logs to a file, rotating daily
        self.handler = TimedRotatingFileHandler(
            filename="pytub.log",
            when="midnight",  # Rotate at midnight every day
            interval=1,  # Every 1 day
            backupCount=7,  # Keep 7 backup log files (for a week)
        )
        # The formatter for TimedRotatingFileHandler also needs the datefmt argument
        self.handler.suffix = "_%Y%m%d.log"  # Suffix for rotated log files
        self.formatter = logging.Formatter(
            fmt=self.LOG_FORMAT, datefmt=self.LOG_DATEFMT
        )  # Pass date format here
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

        # Environment variable check (e.g., for development vs. production)
        self.ENV = os.getenv("ENV", "DEV")

        # File and directory settings
        self.MAX_FILE_LENGTH = 63  # Maximum length for filenames
        self.DRY_RUN = False  # If True, no actual downloads will occur
        self.DOWNLOAD_ALL = False  # If True, download all available streams (not currently used extensively)
        self.PATH = "./drive/MyDrive/"  # Base path for saving files
        self.DST = os.path.join(
            f"{self.PATH}", "日本演歌"
        )  # Destination directory for videos
        self.DST_AUDIO = os.path.join(
            f"{self.PATH}", "日本演歌-Audio"
        )  # Destination directory for audio files

        # Download preferences
        self.CAPTION = True  # Download captions if available

        self.VIDEO = True  # Enable video download
        self.VIDEO_EXT = "mp4"  # Desired video file extension
        self.VIDEO_MIME = "mp4"  # Video MIME type filter
        self.VIDEO_RES = "1080p"  # Desired video resolution
        self.VIDEO_CODE = "av1"  # Desired video codec (not strictly enforced by pytube)
        self.VIDEO_KEEP_ORI = False  # Keep original video file after conversion/merging
        self.PROGRESSIVE = (
            False  # Filter for progressive streams (video and audio combined)
        )
        self.ORDER_BY = "itag"  # Stream sorting order

        self.AUDIO = True  # Enable audio download
        self.AUDIO_EXT = "mp3"  # Desired audio file extension
        self.AUDIO_MIME = "mp4"  # Audio MIME type filter (e.g., 'mp4' for m4a)
        self.AUDIO_BITRATE = "128kbps"  # Desired audio bitrate
        self.AUDIO_CODE = "abr"  # Desired audio codec (not strictly enforced by pytube)
        self.AUDIO_KEEP_ORI = False  # Keep original audio file after conversion

        self.RECONVERT = True  # Reconvert video/audio to merge or re-encode
        self.CONVERT_VIDEO_CODE = None  # "libx264" by default for .mp4, leave None for auto detection  # Codec for video re-encoding (moviepy)
        self.CONVERT_AUDIO_CODE = "aac"  # "libmp3lame" by default for .mp4, leave None for auto detection  # Codec for audio re-encoding (moviepy)

        # Modes of operation
        self.PLS = True  # Enable playlist downloads
        self.CLS = False  # Enable channel downloads
        self.QLS = False  # Enable quick search downloads

        # Create destination directories if they don't exist
        os.makedirs(self.DST, exist_ok=True)
        os.makedirs(self.DST_AUDIO, exist_ok=True)

        # Lists of URLs for individual videos, playlists, channels, and search queries
        self.vs: list[str] = [
            # Example video URLs (commented out)
            # "https://www.youtube.com/watch?v=zb3nAoJJGYo",
        ]

        self.pls = [
            # Example playlist URLs
            # "https://youtube.com/playlist?list=PLhkqiApN_VYay4opZamqmnHIeKQtR9l-T&si=KYV2DqljMbF0W4mQ",  # 日本演歌
        ]

        self.cls: list = [
            # Example channel URLs
            # "https://www.youtube.com/@ProgrammingKnowledge/featured",
        ]

        self.qls = [
            # Example search queries: (query string, filter, top N results)
            # ("learn english", self.RELEVANCE, 3),
        ]

    def _retry_function(self, retries: int = 1, delay: int = 1):
        """
        A decorator to retry a function multiple times with a delay between retries.

        Args:
            retries (int): The number of times to retry the function.
            delay (int): The delay in seconds between retries.

        Returns:
            Callable: A decorator function.
        """

        def outer_d_f(func):
            @wraps(func)
            def wraps_func(*args, **kargs):
                err = ""
                for i in range(1, retries + 1):
                    try:
                        return func(*args, **kargs)
                    except Exception as e:
                        self.logger.error(
                            "retry [fun: {}.{}] [{}/{}] ] delay [{}] secs, reason: {}".format(
                                func.__module__, func.__name__, i, retries, delay, e
                            )
                        )
                        time.sleep(delay)
                        err = str(e)
                else:
                    # If all retries fail, re-raise the last exception
                    raise Exception(
                        "[fun: {}.{}] {}".format(func.__module__, func.__name__, err)
                    )

            return wraps_func

        return outer_d_f

    @staticmethod
    def _remove_characters(filename: str) -> str:
        """
        Removes illegal characters from a filename string.

        Args:
            filename (str): The original filename string.

        Returns:
            str: The cleaned filename string.
        """
        _filename = ""
        # Using a set for efficient lookup of illegal characters.
        illegal_chars_set = {"｜", ",", "/", "\\", ":", "*", "?", "<", ">", '"'}
        for c in filename:
            if c not in illegal_chars_set:
                _filename += c
        return _filename

    def _get_comparable_name(self, original_string: str):
        """
        Normalizes a string for consistent comparison, especially for filenames.
        This involves Unicode normalization, standardizing spaces, and applying safe_filename.

        Args:
            original_string (str): The input string (e.g., YouTube video title or existing filename).

        Returns:
            str: The normalized and safe string for comparison.
        """
        if not isinstance(original_string, str):
            # Log a warning if the input is not a string, indicating a potential issue.
            self.logger.warning(
                f"_get_comparable_name received non-string input: {type(original_string)}"
            )
            return original_string
        # 1. Unicode Normalization (NFKC for compatibility, e.g., 'ジ' to 'ジ')
        normalized_s = unicodedata.normalize("NFKC", original_string)
        # 2. Replace ideographic space (U+3000) with standard space (U+0020)
        normalized_s = normalized_s.replace("\u3000", " ")
        # 3. Apply helpers.safe_filename to sanitize for filename compatibility and length
        return helpers.safe_filename(s=normalized_s, max_length=self.MAX_FILE_LENGTH)

    def _download_yt(self, url: str) -> bool:
        """
        Downloads a YouTube video and its audio, optionally converting and merging them.

        Args:
            url (str): The URL of the YouTube video to download.

        Returns:
            bool: True if the download/processing was successful (or dry run), False otherwise.
        """
        try:
            yt = YouTube(
                url=url,
                use_oauth=False,  # Do not use OAuth
                allow_oauth_cache=False,  # Do not allow OAuth caching
                on_progress_callback=on_progress,  # Callback for progress updates
            )
            # Force update the YouTube object to fetch fresh data, sometimes required by pytubefix
            yt.check_availability()
        except (RegexMatchError, VideoUnavailable, LiveStreamError, ExtractError) as e:
            self.logger.error(
                f"Failed to initialize YouTube object for {url} due to pytubefix error: {e}"
            )
            return False
        except Exception as e:
            self.logger.error(
                f"An unexpected error occurred while initializing YouTube object for {url}: {e}"
            )
            return False

        self.logger.info(f"Title: {yt.title}")
        self.logger.info(f"Duration: {yt.length} sec")
        self.logger.info("---")
        if self.DRY_RUN:
            self.logger.info("Dry run: No actual download will occur.")
            return True  # For dry run, indicate success without actual download

        # Generate a safe filename from the video title
        filename = helpers.safe_filename(s=yt.title, max_length=self.MAX_FILE_LENGTH)
        full_filename = f"{filename}.{self.VIDEO_EXT}"

        # Download captions if enabled
        if self.CAPTION:
            for caption_code in yt.captions.keys():
                self.logger.debug(f"Available caption: {caption_code}")
                remote_full_captionname = os.path.join(
                    self.DST, f"{full_filename}.{caption_code}.txt"
                )
                try:
                    caption_track = yt.captions[caption_code]
                    caption_track.save(remote_full_captionname)
                    self.logger.info(
                        f"Caption for {caption_code} saved to {remote_full_captionname}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Failed to save caption {caption_code} for {url}: {e}"
                    )

        video_download_folder = "."  # Temporary download folder
        remote_full_filename = os.path.join(self.DST, full_filename)  # Final video path
        if self.VIDEO:
            # Download video stream
            if not os.path.exists(remote_full_filename):
                self.logger.info(
                    f"Attempting to download video to {remote_full_filename}"
                )
                stream = (
                    yt.streams.filter(
                        progressive=self.PROGRESSIVE,
                        mime_type=f"video/{self.VIDEO_MIME}",
                        res=self.VIDEO_RES,
                    )
                    .order_by(self.ORDER_BY)
                    .desc()
                    .last()
                )
                if not stream:
                    # Fallback to highest resolution if specific filters yield no results
                    stream = yt.streams.get_highest_resolution(
                        progressive=self.PROGRESSIVE
                    )
                    if not stream:
                        self.logger.error(
                            f"No suitable video stream found for {url} after fallback."
                        )
                        return False
                    self.logger.warning(
                        f"Specific video stream not found, downloading highest resolution: {stream}"
                    )

                self.logger.info(
                    f"Downloading video stream... itag={stream.itag} res={stream.resolution} video_code={stream.video_codec} abr={stream.abr} audio_code={stream.audio_codec}"
                )
                try:
                    stream.download(
                        output_path=video_download_folder, filename=full_filename
                    )
                    self.logger.info(
                        f"Moving video file from {os.path.join(video_download_folder, full_filename)} to {remote_full_filename}"
                    )
                    shutil.move(
                        os.path.join(video_download_folder, full_filename),
                        remote_full_filename,
                    )
                except Exception as e:
                    self.logger.error(
                        f"Failed to download or move video for {url}: {e}"
                    )
                    return False
            else:
                self.logger.warning(
                    f"Remote video file [{remote_full_filename}] already exists, skipping video download."
                )

        audio_download_folder = "."  # Temporary download folder
        full_audioname = f"{filename}.{self.AUDIO_EXT}"  # Final audio filename
        full_audioname_ori = f"{filename}.{self.AUDIO_MIME}"  # Original audio filename (before conversion)
        remote_full_audioname = os.path.join(
            self.DST_AUDIO, full_audioname
        )  # Final audio path
        audio_download_fullname = os.path.join(
            audio_download_folder, full_audioname_ori
        )  # Temp audio path

        if self.AUDIO:
            # Download or extract audio stream
            if not os.path.exists(remote_full_audioname):
                self.logger.info(
                    f"Attempting to download/convert audio to {remote_full_audioname}"
                )
                audio_clip = None
                # If video was downloaded, try to extract audio from it first
                if os.path.exists(remote_full_filename):
                    try:
                        video_clip_for_audio = VideoFileClip(remote_full_filename)
                        audio_clip = video_clip_for_audio.audio
                        if audio_clip:
                            self.logger.info(
                                "Extracted audio from downloaded video file."
                            )
                        video_clip_for_audio.close()  # Close clip to release resources
                    except Exception as e:
                        self.logger.warning(
                            f"Could not extract audio from video file {remote_full_filename}: {e}"
                        )

                # If no audio was extracted from video, download audio-only stream
                if not audio_clip:
                    self.logger.warning(
                        "No audio track found in original video or extraction failed, downloading audio stream instead."
                    )
                    try:
                        stream = (
                            yt.streams.filter(
                                mime_type=f"audio/{self.AUDIO_MIME}",
                                abr=self.AUDIO_BITRATE,
                            )
                            .asc()
                            .first()
                        )
                        if not stream:
                            # Fallback to general audio-only stream if specific filter fails
                            stream = yt.streams.get_audio_only(subtype=self.AUDIO_MIME)
                            if not stream:
                                self.logger.error(
                                    f"No suitable audio stream found for {url} after fallback."
                                )
                                return False
                            self.logger.warning(
                                f"Specific audio stream not found, downloading audio-only stream: {stream}"
                            )

                        self.logger.info(
                            f"Downloading audio stream... itag={stream.itag} res={stream.resolution} video_code={stream.video_codec} abr={stream.abr} audio_code={stream.audio_codec}"
                        )
                        stream.download(
                            output_path=audio_download_folder,
                            filename=full_audioname_ori,
                        )
                        audio_clip = AudioFileClip(audio_download_fullname)
                    except Exception as e:
                        self.logger.error(
                            f"Failed to download audio stream for {url}: {e}"
                        )
                        return False

                # Write the audio clip to the final destination in the desired format
                if audio_clip:
                    try:
                        audio_clip.write_audiofile(
                            filename=remote_full_audioname,
                            codec=None,  # Codec=None lets moviepy infer from extension
                        )
                        audio_clip.close()  # Close audio clip

                        # Handle original audio file (move or remove)
                        if (
                            self.AUDIO_KEEP_ORI
                            and self.AUDIO_MIME != self.AUDIO_EXT
                            and os.path.exists(audio_download_fullname)
                        ):
                            self.logger.info(
                                f"Moving original audio file from {audio_download_fullname} to {os.path.join(self.DST_AUDIO, full_audioname_ori)}"
                            )
                            shutil.move(
                                audio_download_fullname,
                                os.path.join(self.DST_AUDIO, full_audioname_ori),
                            )
                        elif os.path.exists(audio_download_fullname):
                            self.logger.info(
                                f"Removing temporary audio file {audio_download_fullname}"
                            )
                            os.remove(audio_download_fullname)
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
                    f"Remote audio file [{remote_full_audioname}] already exists, skipping audio download."
                )

        # Merge video/audio if RECONVERT is enabled and video was downloaded/extracted
        if (
            self.RECONVERT
            and self.VIDEO
            and self.AUDIO
            and os.path.exists(remote_full_filename)
            and os.path.exists(remote_full_audioname)
        ):
            converted_full_filename_temp = (
                f"{filename}_merged.{self.VIDEO_EXT}"  # Temporary name for merged file
            )
            final_video_path_after_merge = os.path.join(
                self.DST, full_filename
            )  # Overwrite original video
            if self.VIDEO_KEEP_ORI:
                final_video_path_after_merge = os.path.join(
                    self.DST, converted_full_filename_temp
                )  # Save as new file if keeping original

            # Check if the final merged file already exists and has audio
            if os.path.exists(final_video_path_after_merge):
                try:
                    existing_video_clip = VideoFileClip(final_video_path_after_merge)
                    if existing_video_clip.audio:
                        self.logger.warning(
                            f"Merged video file [{final_video_path_after_merge}] already exists with audio, skipping conversion."
                        )
                        existing_video_clip.close()
                        return True
                    existing_video_clip.close()
                except Exception as e:
                    self.logger.warning(
                        f"Could not check existing merged video for {url}: {e}"
                    )

            video_clip = None
            audio_clip_to_merge = None
            final_clip = None
            try:
                # Load the video and audio clips
                video_clip = VideoFileClip(remote_full_filename)
                self.logger.debug(f"Video clip loaded: duration={video_clip.duration}")

                audio_clip_to_merge = AudioFileClip(remote_full_audioname)
                self.logger.debug(
                    f"Audio clip loaded: duration={audio_clip_to_merge.duration}"
                )

                # Assign the audio to the video clip
                final_clip = video_clip.set_audio(audio_clip_to_merge)

                # Write the final video with the combined audio
                self.logger.info(
                    f"Writing final video with combined audio: temp={converted_full_filename_temp}, final={final_video_path_after_merge}"
                )
                final_clip.write_videofile(
                    filename=converted_full_filename_temp,
                    codec=self.CONVERT_VIDEO_CODE,
                    audio_codec=self.CONVERT_AUDIO_CODE,
                    temp_audiofile=os.path.join(
                        audio_download_folder, "_temp_audio.m4a"
                    ),  # Specify temp audio file
                    remove_temp=True,  # Remove temporary audio file created by moviepy
                    logger=self.logger,  # Pass custom logger to moviepy
                )
                self.logger.info("Video and audio merged successfully.")

                # Move the merged file to its final destination
                if not self.VIDEO_KEEP_ORI and os.path.exists(remote_full_filename):
                    self.logger.info(
                        f"Removing original video file: {remote_full_filename}"
                    )
                    os.remove(
                        remote_full_filename
                    )  # Remove the original video without audio

                self.logger.info(
                    f"Moving converted video from {converted_full_filename_temp} to {final_video_path_after_merge}"
                )
                shutil.move(converted_full_filename_temp, final_video_path_after_merge)

            except Exception as e:
                self.logger.error(
                    f"An error occurred during video/audio merging for {url}: {e}"
                )
                return False

            finally:
                # Ensure all clips are closed to release resources
                if video_clip is not None:
                    video_clip.close()
                if audio_clip_to_merge is not None:
                    audio_clip_to_merge.close()
                if final_clip is not None:
                    final_clip.close()
        elif (
            self.RECONVERT and self.VIDEO and self.AUDIO
        ):  # Log cases where merging conditions are not met
            if not os.path.exists(remote_full_filename):
                self.logger.warning(
                    f"Cannot merge: Video file not found at {remote_full_filename} for {url}"
                )
            if not os.path.exists(remote_full_audioname):
                self.logger.warning(
                    f"Cannot merge: Audio file not found at {remote_full_audioname} for {url}"
                )

        return True

    def _download_videos(self, videos: list):
        """
        Iterates through a list of video URLs or YouTube objects and downloads each one.

        Args:
            videos (list):
                A list containing video URLs (str) or YouTube objects.
        """
        if not videos:  # Handle empty list gracefully
            self.logger.info("No videos provided for download.")
            return

        for i, video in enumerate(videos):
            if isinstance(video, str):
                url = video
            elif isinstance(video, YouTube):
                url = video.watch_url
            else:
                self.logger.error(
                    f"Invalid video item type: {type(video)} encountered for item {i}. Skipping."
                )
                continue

            self.logger.info(f"Downloading video {url} [{i + 1}/{len(videos)}]")
            try:
                self._download_yt(url)
            except BotDetection as e:
                self.logger.error(
                    f"Failed to download {url} due to bot detection: {e}. Consider changing client type or IP address."
                )
            except Exception as e:
                self.logger.error(
                    f"An unexpected error occurred while downloading {url}: {e}"
                )

    def _move_files(self):
        """
        Moves video and audio files from the current working directory to their
        respective destination folders (self.DST and self.DST_AUDIO), renaming them if necessary.
        This function is typically used after downloads if files are initially saved locally.
        """
        self.logger.debug(f"Current working directory: {os.getcwd()}")

        # Move video files
        videos_in_cwd = glob.glob(r"*.{ext}".format(ext=self.VIDEO_EXT))
        self.logger.debug(f"Found video files in CWD: {videos_in_cwd}")
        for video_path in videos_in_cwd:
            try:
                base_name = os.path.basename(video_path)
                new_name = base_name[
                    : self.MAX_FILE_LENGTH
                ]  # Truncate name if too long
                final_dest_path = os.path.join(self.DST, new_name)

                # Rename in CWD first if necessary, then move
                if base_name != new_name:  # Only rename if truncated
                    os.rename(video_path, new_name)
                    video_path = new_name  # Update path for move operation

                shutil.move(video_path, final_dest_path)
                self.logger.info(f"Moved video: {new_name} to {self.DST}")
            except FileNotFoundError:
                self.logger.warning(
                    f"Skipping move: Video file {video_path} not found in CWD."
                )
            except PermissionError:
                self.logger.error(
                    f"Permission denied when moving {video_path} to {self.DST}. Check file permissions."
                )
            except Exception as e:
                self.logger.error(
                    f"An error occurred moving video file {video_path}: {e}"
                )

        # Move audio files
        audios_in_cwd = glob.glob(r"*.{ext}".format(ext=self.AUDIO_EXT))
        self.logger.debug(f"Found audio files in CWD: {audios_in_cwd}")
        for audio_path in audios_in_cwd:
            try:
                base_name = os.path.basename(audio_path)
                new_name = base_name[
                    : self.MAX_FILE_LENGTH
                ]  # Truncate name if too long
                final_dest_path = os.path.join(self.DST_AUDIO, new_name)

                if base_name != new_name:  # Only rename if truncated
                    os.rename(audio_path, new_name)
                    audio_path = new_name  # Update path for move operation

                shutil.move(audio_path, final_dest_path)
                self.logger.info(f"Moved audio: {new_name} to {self.DST_AUDIO}")
            except FileNotFoundError:
                self.logger.warning(
                    f"Skipping move: Audio file {audio_path} not found in CWD."
                )
            except PermissionError:
                self.logger.error(
                    f"Permission denied when moving {audio_path} to {self.DST_AUDIO}. Check file permissions."
                )
            except Exception as e:
                self.logger.error(
                    f"An error occurred moving audio file {audio_path}: {e}"
                )

    def _remove_origional_video(self):
        """
        Renames files that have a double extension (e.g., .mp4.mp4) by removing the extra extension.
        This can occur if video files are downloaded with an added extension during a merge process.
        """
        self.logger.debug(f"Current working directory: {os.getcwd()}")
        videos_with_double_ext = glob.glob(
            os.path.join(self.DST, r"*.{ext}.{ext}".format(ext=self.VIDEO_EXT))
        )
        self.logger.debug(
            f"Found original videos with double extension: {videos_with_double_ext}"
        )
        for video_path in videos_with_double_ext:
            try:
                source = video_path
                destination = video_path[
                    : -len(f".{self.VIDEO_EXT}")
                ]  # Remove the last extension
                self.logger.info(f"Moving original video: {source} to {destination}")
                shutil.move(source, destination)
            except FileNotFoundError:
                self.logger.warning(
                    f"Skipping rename: Original video file {video_path} not found."
                )
            except PermissionError:
                self.logger.error(
                    f"Permission denied when renaming {video_path}. Check file permissions."
                )
            except Exception as e:
                self.logger.error(
                    f"An error occurred renaming original video {video_path}: {e}"
                )

    def _compare_audio_video(self):
        """
        Compares the list of video files with audio files in their respective destination folders.
        Prints the names of video files that do not have a corresponding audio file.
        """
        videos = glob.glob(
            os.path.join(self.DST, r"*.{ext}".format(ext=self.VIDEO_EXT))
        )
        videos = sorted(
            [os.path.splitext(os.path.basename(video))[0] for video in videos]
        )
        self.logger.info(f"Video files found in {self.DST}: {len(videos)}")

        audios = glob.glob(
            os.path.join(self.DST_AUDIO, r"*.{ext}".format(ext=self.AUDIO_EXT))
        )
        audios = sorted(
            [os.path.splitext(os.path.basename(audio))[0] for audio in audios]
        )
        self.logger.info(f"Audio files found in {self.DST_AUDIO}: {len(audios)}")

        missing_audio_count = 0
        for video_name in videos:
            if video_name not in audios:
                self.logger.warning(
                    f"Video file '{video_name}' in '{self.DST}' has no matching audio file in '{self.DST_AUDIO}'."
                )
                missing_audio_count += 1
        if missing_audio_count == 0:
            self.logger.info(
                f"All video files in '{self.DST}' have matching audio files in '{self.DST_AUDIO}'."
            )

    def _compare_playlist(self):
        """
        Compares downloaded files (video and audio) against the titles in defined playlists.
        It normalizes names to account for subtle differences and reports any missing files.
        """
        for pl_url in self.pls:
            try:
                p = Playlist(pl_url)
                self.logger.info(f"Processing Playlist for comparison: {p.title}")

                # Set dynamic destination paths based on playlist title
                current_dst = os.path.join(f"{self.PATH}", p.title)
                current_dst_audio = os.path.join(f"{self.PATH}", f"{p.title}-Audio")

                # Get existing video filenames, normalized
                videos = glob.glob(
                    os.path.join(current_dst, r"*.{ext}".format(ext=self.VIDEO_EXT))
                )
                videos_base_names = sorted(
                    [os.path.splitext(os.path.basename(video))[0] for video in videos]
                )
                normalized_videos = sorted(
                    [self._get_comparable_name(item) for item in videos_base_names]
                )
                self.logger.debug(
                    f"Normalized video base names for '{p.title}': {normalized_videos}"
                )

                # Get existing audio filenames, normalized
                audios = glob.glob(
                    os.path.join(
                        current_dst_audio, r"*.{ext}".format(ext=self.AUDIO_EXT)
                    )
                )
                audios_base_names = sorted(
                    [os.path.splitext(os.path.basename(audio))[0] for audio in audios]
                )
                normalized_audios = sorted(
                    [self._get_comparable_name(item) for item in audios_base_names]
                )
                self.logger.debug(
                    f"Normalized audio base names for '{p.title}': {normalized_audios}"
                )

                self.logger.info(
                    f"Number of videos found for '{p.title}': {len(videos_base_names)}, Number of audios found: {len(audios_base_names)}"
                )

                # Get YouTube video titles from the playlist, normalized
                youtube_titles = sorted([video_item.title for video_item in p.videos])
                # Normalize YouTube titles for consistent comparison
                normalized_youtube_titles = sorted(
                    [self._get_comparable_name(title) for title in youtube_titles]
                )
                self.logger.debug(
                    f"Normalized YouTube titles from playlist '{p.title}': {normalized_youtube_titles}"
                )

                # Determine which set of downloaded files (video or audio) is larger for target comparison
                # This assumes if one is present, the other should be too, and uses the larger set for more comprehensive checking
                normalized_target_set = (
                    set(normalized_videos)
                    if len(normalized_videos) >= len(normalized_audios)
                    else set(normalized_audios)
                )

                missing_count = 0
                for comparable_yt_title in normalized_youtube_titles:
                    if comparable_yt_title not in normalized_target_set:
                        missing_count += 1
                        # Find the original title for better reporting
                        original_title_idx = normalized_youtube_titles.index(
                            comparable_yt_title
                        )
                        original_yt_title = youtube_titles[original_title_idx]
                        self.logger.info(
                            f"Missing file detected in playlist '{p.title}': original_title='{original_yt_title!r}', comparable_title='{comparable_yt_title!r}'"
                        )
                if missing_count == 0:
                    self.logger.info(
                        f"All videos in playlist '{p.title}' found in downloads."
                    )
                else:
                    self.logger.warning(
                        f"Total missing files found for playlist '{p.title}': {missing_count}"
                    )
            except (
                RegexMatchError,
                VideoUnavailable,
                LiveStreamError,
                ExtractError,
            ) as e:
                self.logger.error(
                    f"Failed to load playlist {pl_url} for comparison due to pytubefix error: {e}"
                )
            except Exception as e:
                self.logger.error(
                    f"An unexpected error occurred during playlist comparison for {pl_url}: {e}"
                )

    def _find_duplicated_title_in_playlist(self):
        """
        Checks for and logs duplicated video titles within each configured playlist.

        Returns:
            list: A list of duplicated video titles found in the playlist. Returns an empty list if no duplicates or if playlist cannot be processed.
        """
        all_duplicated_titles = []
        for pl_url in self.pls:
            try:
                p = Playlist(pl_url)
                self.logger.info(
                    f"Checking for duplicated titles in playlist: {p.title}"
                )
                titles = [video_item.title for video_item in p.videos]
                seen_normalized_titles = set()
                duplicated_original_titles = []

                for title in titles:
                    # Normalize the title before checking for duplicates
                    normalized_title = self._get_comparable_name(title)
                    if normalized_title in seen_normalized_titles:
                        duplicated_original_titles.append(
                            title
                        )  # Append original title to duplicated list
                    else:
                        seen_normalized_titles.add(
                            normalized_title
                        )  # Add normalized title to seen set

                if len(duplicated_original_titles) > 0:
                    self.logger.warning(
                        f"Found duplicated titles in playlist '{p.title}'!!!"
                    )
                    for d_title in duplicated_original_titles:
                        self.logger.warning(f"  - Duplicated: {d_title}")
                    all_duplicated_titles.extend(duplicated_original_titles)
                else:
                    self.logger.info(
                        f"No duplicated titles found in playlist '{p.title}'."
                    )
            except (
                RegexMatchError,
                VideoUnavailable,
                LiveStreamError,
                ExtractError,
            ) as e:
                self.logger.error(
                    f"Failed to load playlist {pl_url} for duplication check due to pytubefix error: {e}"
                )
            except Exception as e:
                self.logger.error(
                    f"An unexpected error occurred during duplication check for playlist {pl_url}: {e}"
                )
        return all_duplicated_titles

    def run(self):
        """
        Main function to orchestrate the downloading process based on configured lists (vs, pls, cls, qls).
        It processes individual videos, playlists, channels, and search queries.
        """
        self.logger.info("Starting individual video downloads...")
        self._download_videos(self.vs)

        if self.PLS:
            self.logger.info("Starting playlist downloads...")
            # Store original DST paths, as they change per playlist
            original_global_dst = self.DST
            original_global_dst_audio = self.DST_AUDIO

            for pl_url in self.pls:
                try:
                    p = Playlist(pl_url)
                    self.logger.info(f"Processing Playlist: {p.title}")
                    # Set dynamic destination folders based on playlist title for each playlist
                    self.DST = os.path.join(
                        f"{self.PATH}",
                        helpers.safe_filename(p.title, max_length=self.MAX_FILE_LENGTH),
                    )
                    self.DST_AUDIO = os.path.join(
                        f"{self.PATH}",
                        f"{helpers.safe_filename(p.title, max_length=self.MAX_FILE_LENGTH)}-Audio",
                    )
                    os.makedirs(self.DST, exist_ok=True)
                    os.makedirs(self.DST_AUDIO, exist_ok=True)
                    self.logger.info(
                        f"Video destination: {self.DST}, Audio destination: {self.DST_AUDIO}"
                    )
                    self._download_videos(p.videos)
                except (
                    RegexMatchError,
                    VideoUnavailable,
                    LiveStreamError,
                    ExtractError,
                ) as e:
                    self.logger.error(
                        f"Unable to process Playlist {pl_url} due to pytubefix error: {e}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"An unexpected error occurred while processing Playlist {pl_url}: {e}"
                    )

            # Restore original global DST paths after playlist processing
            self.DST = original_global_dst
            self.DST_AUDIO = original_global_dst_audio

        if self.CLS:
            self.logger.info("Starting channel downloads...")
            # Channel downloads might also benefit from dynamic DST, but following original structure for now.
            for ch_url in self.cls:
                try:
                    c = Channel(ch_url)
                    self.logger.info(f"Processing Channel: {c.channel_name}")
                    self._download_videos(c.videos)
                except (
                    RegexMatchError,
                    VideoUnavailable,
                    LiveStreamError,
                    ExtractError,
                ) as e:
                    self.logger.error(
                        f"Unable to process Channel {ch_url} due to pytubefix error: {e}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"An unexpected error occurred while processing Channel {ch_url}: {e}"
                    )

        if self.QLS:
            self.logger.info("Starting quick search downloads...")
            for qs, search_filter, top_n in self.qls:
                filters_obj = (
                    Filter.create()
                    .type(Filter.Type.VIDEO)
                    .sort_by(Filter.SortBy(search_filter))
                )
                try:
                    res = Search(qs, filters=filters_obj)
                    if not res.videos:
                        self.logger.warning(f"No videos found for search query '{qs}'.")
                        continue
                    for video in res.videos[:top_n]:
                        self.logger.info(f"Search result Title: {video.title}")
                        self.logger.info(f"Search result URL: {video.watch_url}")
                        self.logger.info(f"Search result Duration: {video.length} sec")
                        self.logger.info("---")
                        self._download_videos([video])
                except (
                    RegexMatchError,
                    VideoUnavailable,
                    LiveStreamError,
                    ExtractError,
                ) as e:
                    self.logger.error(
                        f"Unable to perform Search for '{qs}' due to pytubefix error: {e}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"An unexpected error occurred during Search for '{qs}': {e}"
                    )


def _main():
    """
    An alternative main function, likely for testing individual pytubefix functionalities.
    Currently not called in the main execution block.
    """
    # Example usage for individual video download, caption, and audio
    plst = [
        "https://youtube.com/playlist?list=PLhkqiApN_VYay4opZamqmnHIeKQtR9l-T&si=KYV2DqljMbF0W4mQ",  # 日本演歌
    ]
    url = "https://youtube.com/watch?v=7g9xcCMdwns"
    yt = YouTube(url, on_progress_callback=on_progress)
    logging.info(f"YouTube Title: {yt.title}")
    ys = yt.streams.get_highest_resolution(progressive=False, mime_type="video/mp4")
    ys.download(output_path="download/mtv/歌心りえ/")

    logging.info(f"Captions for video: {yt.captions.keys()}")
    for caption_code in yt.captions.keys():
        try:
            caption_track = yt.captions[caption_code]
            caption_track.save(f"download/mtv/歌心りえ/{yt.title}.{caption_code}.txt")
            logging.info(f"Caption {caption_code} saved.")
        except Exception as e:
            logging.error(f"Failed to save caption {caption_code}: {e}")

    ya = yt.streams.get_audio_only()
    ya.download(f"download/mtv/歌心りえ/{yt.title}.m4a")
    logging.info("Audio-only stream downloaded.")

    pl = Playlist(url=plst[0])
    logging.info(f"Playlist Title: {pl.title}")
    for video in pl.videos:
        logging.info(f"Downloading audio for playlist video: {video.title}")
        ys = video.streams.get_audio_only()
        ys.download(output_path="download/mtv")

    c = Channel("https://www.youtube.com/@ProgrammingKnowledge/featured")
    logging.info(f"Channel name: {c.channel_name}")

    c1 = Channel("https://www.youtube.com/@LillianChiu101")
    logging.info(f"Channel name: {c1.channel_name}")

    res = Search("GitHub Issue Best Practices")
    logging.info("Search results for 'GitHub Issue Best Practices':")
    for video in res.videos:
        logging.info(f"  Title: {video.title}")
        logging.info(f"  URL: {video.watch_url}")
        logging.info(f"  Duration: {video.length} sec")
        logging.info("  ---")

    filters = {
        "upload_date": Filter.get_upload_date("Today"),
        "type": Filter.get_type("Video"),
        "duration": Filter.get_duration("Under 4 minutes"),
        "features": [
            Filter.get_features("4K"),
            Filter.get_features("Creative Commons"),
        ],
        "sort_by": Filter.get_sort_by("Upload date"),
    }

    res = Search("music", filters=filters)
    logging.info("Search results for 'music' with filters:")
    for video in res.videos:
        logging.info(f"  Title: {video.title}")
        logging.info(f"  URL: {video.watch_url}")
        logging.info(f"  Duration: {video.length} sec")
        logging.info("  ---")


if __name__ == "__main__":
    downloader = YouTubeDownloader()
    downloader.run()
