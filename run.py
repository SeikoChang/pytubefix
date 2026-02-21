import sys
from functools import wraps
import glob
import os
import shutil
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import unicodedata
from pytubefix import YouTube
from pytubefix import Playlist
from pytubefix import Channel
from pytubefix import Search
from pytubefix import helpers
from pytubefix.exceptions import BotDetection
from pytubefix.contrib.search import Filter
from pytubefix.cli import on_progress
from moviepy import AudioFileClip, VideoFileClip

# --- Constants and Configuration --- #
RELEVANCE = Filter.SortBy.RELEVANCE
UPLOAD_DATE = Filter.SortBy.UPLOAD_DATE
VIEW_COUNT = Filter.SortBy.VIEW_COUNT
RATING = Filter.SortBy.RATING

# Logging configuration
LOG_FORMAT = "%(asctime)s | %(levelname)s : %(message)s"
LOG_FORMAT_DATE = "%Y-%m-%d %H:%M:%S"
LOG_LEVEL = logging.INFO

# Configure logging to display INFO level and above, and use the custom format
# 'stream=sys.stdout' ensures output goes to the standard Colab output cell
logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    stream=sys.stdout,
    force=True,  # This argument forces Colab to use this config, overriding defaults
)
logger: logging.Logger = logging.getLogger()
# Add a TimedRotatingFileHandler to save logs to a file, rotating daily
handler = TimedRotatingFileHandler(
    filename="pytub.log",
    when="midnight",  # Rotate at midnight every day
    interval=1,  # Every 1 day
    backupCount=7,  # Keep 7 backup log files (for a week)
)
handler.suffix = "_%Y%m%d.log"  # Suffix for rotated log files
formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_FORMAT_DATE)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Environment variable check (e.g., for development vs. production)
ENV = os.getenv("ENV", "DEV")

# File and directory settings
MAX_FILE_LENGTH = 63  # Maximum length for filenames
DRY_RUN = False  # If True, no actual downloads will occur
DOWNLOAD_ALL = (
    False  # If True, download all available streams (not currently used extensively)
)
PATH = "./drive/MyDrive/"  # Base path for saving files
DST = os.path.join(f"{PATH}", "日本演歌")  # Destination directory for videos
DST_AUDIO = os.path.join(
    f"{PATH}", "日本演歌-Audio"
)  # Destination directory for audio files

# Download preferences
CAPTION = True  # Download captions if available

VIDEO = True  # Enable video download
VIDEO_EXT = "mp4"  # Desired video file extension
VIDEO_MIME = "mp4"  # Video MIME type filter
VIDEO_RES = "1080p"  # Desired video resolution
VIDEO_CODE = "av1"  # Desired video codec (not strictly enforced by pytube)
VIDEO_KEEP_ORI = False  # Keep original video file after conversion/merging
PROGRESSIVE = False  # Filter for progressive streams (video and audio combined)
# ADAPTIVE = True # Filter for adaptive streams (separate video/audio)
ORDER_BY = "itag"  # Stream sorting order

AUDIO = True  # Enable audio download
AUDIO_EXT = "mp3"  # Desired audio file extension
AUDIO_MIME = "mp4"  # Audio MIME type filter (e.g., 'mp4' for m4a)
AUDIO_BITRATE = "128kbps"  # Desired audio bitrate
AUDIO_CODE = "abr"  # Desired audio codec (not strictly enforced by pytube)
AUDIO_KEEP_ORI = False  # Keep original audio file after conversion

RECONVERT = True  # Reconvert video/audio to merge or re-encode
CONVERT_VIDEO_CODE = None  # "libx264" by default for .mp4, leave None for auto detection  # Codec for video re-encoding (moviepy)
CONVERT_AUDIO_CODE = "aac"  # "libmp3lame" by default for .mp4, leave None for auto detection  # Codec for audio re-encoding (moviepy)

# Modes of operation
PLS = True  # Enable playlist downloads
CLS = False  # Enable channel downloads
QLS = False  # Enable quick search downloads

# Create destination directories if they don't exist
os.makedirs(DST, exist_ok=True)
os.makedirs(DST_AUDIO, exist_ok=True)

# Lists of URLs for individual videos, playlists, channels, and search queries
vs: list[str] = [
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

pls = [
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
    # "https://www.youtube.com/playlist?list=PL12UaAf_xzfpfxj4siikK9CW8idyJyZo2",  # 【日語】SPY×FAMILY間諜家家酒(全部集數)
    # "https://www.youtube.com/watch?v=7cQzvmJvLpU&list=PL1H2dev3GUtgYGOiJFWjZe2mX29VpraJN",  # 聽歌學英文
    # "https://www.youtube.com/playlist?list=PLwPx6OD5gb4imniZyKp7xo7pXew3QRTuq",  # QWER 1ST WORLDTOUR Setlist (Rockation, 2025)
    "https://youtube.com/playlist?list=PLhkqiApN_VYay4opZamqmnHIeKQtR9l-T&si=KYV2DqljMbF0W4mQ",  # 日本演歌
    # "https://www.youtube.com/playlist?list=PLf8MTi2c_8X9IYfTrHA_fCb2Q7R72wtKZ",  # 投資
]

cls: list = [
    # Example channel URLs
    # "https://www.youtube.com/@ProgrammingKnowledge/featured",
    # "https://www.youtube.com/@LillianChiu101",
    # "https://www.youtube.com/@kellytsaii",
]

qls = [
    # Example search queries: (query string, filter, top N results)
    # ("Programming Knowledge", VIEW_COUNT, 1),
    # ("GitHub Issue Best Practices", VIEW_COUNT, 1),
    # ("global news", UPLOAD_DATE, 5),
    # ("breaking news, 台灣 新聞", UPLOAD_DATE, 3),
    ("learn english", RELEVANCE, 3),
    # (
    #     "Tee TA Cote Jay Park) SURL  CY NRE 18  (C1) seaRseREa at  mbit  cram  ars Snow eae? SS . ey Me ",
    #     RELEVANCE,
    #     2,
    # ),  # test garbled characters
]

# --- Helper Functions --- #


def retry_function(retries: int = 1, delay: int = 1):
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
                    logger.error(
                        "retry [fun: {}.{}] [{}/{}]] delay [{}] secs, reason: {}".format(
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


def remove_characters(filename: str) -> str:
    """
    Removes illegal characters from a filename string.

    Args:
        filename (str): The original filename string.

    Returns:
        str: The cleaned filename string.
    """
    _filename = ""
    for c in filename:
        # List of characters typically illegal in Windows/Unix filenames
        if c not in "｜|,/\\:*?<>":
            _filename += c
    return _filename
    # Alternative (commented out) approach using replace for specific characters:
    # filename = filename.replace('"' , " ")
    # filename = filename.replace('|', " ")


# Define a helper function for consistent string normalization
def get_comparable_name(original_string: str):
    """
    Normalizes a string for consistent comparison, especially for filenames.
    This involves Unicode normalization, standardizing spaces, and applying safe_filename.

    Args:
        original_string (str): The input string (e.g., YouTube video title or existing filename).

    Returns:
        str: The normalized and safe string for comparison.
    """
    if not isinstance(original_string, str):
        return original_string
    # 1. Unicode Normalization (NFKC for compatibility, e.g., 'ジ' to 'ジ')
    normalized_s = unicodedata.normalize("NFKC", original_string)
    # 2. Replace ideographic space (U+3000) with standard space (U+0020)
    normalized_s = normalized_s.replace("\u3000", " ")
    # 3. Apply helpers.safe_filename to sanitize for filename compatibility and length
    return helpers.safe_filename(s=normalized_s, max_length=MAX_FILE_LENGTH)


# @retry_function(retries=3, delay=30) # Example usage of retry decorator
def download_yt(url: str) -> bool:
    """
    Downloads a YouTube video and its audio, optionally converting and merging them.

    Args:
        url (str): The URL of the YouTube video to download.

    Returns:
        bool: True if the download/processing was successful (or dry run), False otherwise.
    """
    yt = YouTube(
        url=url,
        use_oauth=False,  # Do not use OAuth
        allow_oauth_cache=False,  # Do not allow OAuth caching
        on_progress_callback=on_progress,  # Callback for progress updates
        # client='ANDROID',  # 'WEB' # Specify client type if needed
    )

    logger.info(f"Title: {yt.title}")
    logger.info(f"Duration: {yt.length} sec")
    logger.info("---")
    if DRY_RUN:
        logger.info("Dry run: No actual download will occur.")
        return True  # For dry run, indicate success without actual download

    # Generate a safe filename from the video title
    filename = helpers.safe_filename(s=yt.title, max_length=MAX_FILE_LENGTH)
    full_filename = f"{filename}.{VIDEO_EXT}"

    # Download captions if enabled
    if CAPTION:
        for caption in yt.captions.keys():
            logger.debug(f"Available caption: {caption}")
            remote_full_captionname = os.path.join(
                DST, f"{full_filename}.{caption}.txt"  # caption.code was causing issues
            )
            try:
                caption_track = yt.captions[caption]
                caption_track.save(remote_full_captionname)
                logger.info(f"Caption saved to {remote_full_captionname}")
            except Exception as e:
                logger.error(f"Failed to save caption {caption}: {e}")

    video_download_folder = "."  # Temporary download folder
    remote_full_filename = os.path.join(DST, full_filename)  # Final video path
    if VIDEO:
        # Download video stream
        if not os.path.exists(remote_full_filename):
            logger.info(f"Attempting to download video to {remote_full_filename}")
            stream = (
                yt.streams.filter(
                    progressive=PROGRESSIVE,
                    mime_type=f"video/{VIDEO_MIME}",
                    res=VIDEO_RES,
                )
                .order_by(ORDER_BY)
                .desc()
                .last()  # Get the last (highest quality matching) stream
            )
            if not stream:
                # Fallback to highest resolution if specific filters yield no results
                stream = yt.streams.get_highest_resolution(progressive=PROGRESSIVE)
                logger.warning(
                    f"Specific video stream not found, downloading highest resolution: {stream}"
                )

            logger.info(
                f"Downloading video stream... itag={stream.itag} res={stream.resolution} video_code={stream.video_codec} abr={stream.abr} audio_code={stream.audio_codec}"
            )
            # Download the video to a temporary location
            stream.download(output_path=video_download_folder, filename=full_filename)
            logger.info(
                f"Moving video file from {os.path.join(video_download_folder, full_filename)} to {remote_full_filename}"
            )
            # Move the downloaded video to its final destination
            shutil.move(
                os.path.join(video_download_folder, full_filename), remote_full_filename
            )
        else:
            logger.warning(
                f"Remote video file [{remote_full_filename}] already exists, skipping video download."
            )

    audio_download_folder = "."  # Temporary download folder
    full_audioname = f"{filename}.{AUDIO_EXT}"  # Final audio filename
    full_audioname_ori = (
        f"{filename}.{AUDIO_MIME}"  # Original audio filename (before conversion)
    )
    remote_full_audioname = os.path.join(DST_AUDIO, full_audioname)  # Final audio path
    audio_download_fullname = os.path.join(
        audio_download_folder, full_audioname_ori
    )  # Temp audio path

    if AUDIO:
        # Download or extract audio stream
        if not os.path.exists(remote_full_audioname):
            logger.info(
                f"Attempting to download/convert audio to {remote_full_audioname}"
            )
            audio_clip = None
            # If video was downloaded, try to extract audio from it first
            if os.path.exists(remote_full_filename):
                try:
                    video_clip_for_audio = VideoFileClip(remote_full_filename)
                    audio_clip = video_clip_for_audio.audio
                    if audio_clip:
                        logger.info("Extracted audio from downloaded video file.")
                    video_clip_for_audio.close()  # Close video clip to release resources
                except Exception as e:
                    logger.warning(f"Could not extract audio from video file: {e}")

            # If no audio was extracted from video, download audio-only stream
            if not audio_clip:
                logger.warning(
                    "No audio track found in original video or extraction failed, downloading audio stream instead."
                )
                try:
                    stream = (
                        yt.streams.filter(
                            mime_type=f"audio/{AUDIO_MIME}", abr=AUDIO_BITRATE
                        )
                        .asc()
                        .first()  # Get the first (lowest quality matching) stream
                    )
                except Exception as e:
                    logger.debug(f"Failed to find specific audio stream: {e}")
                    # Fallback to general audio-only stream
                    stream = yt.streams.get_audio_only(subtype=AUDIO_MIME)
                    logger.warning(
                        f"Specific audio stream not found, downloading audio-only stream: {stream}"
                    )

                logger.info(
                    f"Downloading audio stream... itag={stream.itag} res={stream.resolution} video_code={stream.video_codec} abr={stream.abr} audio_code={stream.audio_codec}"
                )
                # Download audio to a temporary location
                stream.download(
                    output_path=audio_download_folder, filename=full_audioname_ori
                )
                audio_clip = AudioFileClip(audio_download_fullname)

            # Write the audio clip to the final destination in the desired format
            if audio_clip:
                audio_clip.write_audiofile(
                    filename=remote_full_audioname, codec=None
                )  # Codec=None lets moviepy infer from extension
                audio_clip.close()  # Close audio clip

                # Handle original audio file (move or remove)
                if (
                    AUDIO_KEEP_ORI
                    and AUDIO_MIME != AUDIO_EXT
                    and os.path.exists(audio_download_fullname)
                ):
                    logger.info(
                        f"Moving original audio file from {audio_download_fullname} to {os.path.join(DST_AUDIO, full_audioname_ori)}"
                    )
                    shutil.move(
                        audio_download_fullname,
                        os.path.join(DST_AUDIO, full_audioname_ori),
                    )
                elif os.path.exists(audio_download_fullname):
                    logger.info(
                        f"Removing temporary audio file {audio_download_fullname}"
                    )
                    os.remove(audio_download_fullname)
            else:
                logger.error(f"No audio content available for {url}")
        else:
            logger.warning(
                f"Remote audio file [{remote_full_audioname}] already exists, skipping audio download."
            )

    # Merge video/audio if RECONVERT is enabled and video was downloaded/extracted
    if RECONVERT and VIDEO and AUDIO:
        converted_full_filename_temp = (
            f"{filename}_merged.{VIDEO_EXT}"  # Temporary name for merged file
        )
        final_video_path_after_merge = os.path.join(
            DST, full_filename
        )  # Overwrite original video
        if VIDEO_KEEP_ORI:
            final_video_path_after_merge = os.path.join(
                DST, converted_full_filename_temp
            )  # Save as new file if keeping original

        # Check if the final merged file already exists
        if os.path.exists(final_video_path_after_merge):
            # If exists and has audio, assume it's already processed
            try:
                existing_video_clip = VideoFileClip(final_video_path_after_merge)
                if existing_video_clip.audio:
                    logger.warning(
                        f"Merged video file [{final_video_path_after_merge}] already exists with audio, skipping conversion."
                    )
                    existing_video_clip.close()
                    return True
                existing_video_clip.close()
            except Exception as e:
                logger.warning(f"Could not check existing merged video: {e}")

        try:
            # Load the video and audio clips
            video_clip = VideoFileClip(remote_full_filename)
            logger.debug(f"Video clip loaded: duration={video_clip.duration}")

            audio_clip = AudioFileClip(remote_full_audioname)
            logger.debug(f"Audio clip loaded: duration={audio_clip.duration}")

            # Assign the audio to the video clip
            final_clip = video_clip.set_audio(audio_clip)

            # Write the final video with the combined audio
            logger.info(
                f"Writing final video with combined audio: temp={converted_full_filename_temp}, final={final_video_path_after_merge}"
            )
            final_clip.write_videofile(
                filename=converted_full_filename_temp,
                codec=CONVERT_VIDEO_CODE,
                audio_codec=CONVERT_AUDIO_CODE,
                temp_audiofile=os.path.join(
                    audio_download_folder, "_temp_audio.m4a"
                ),  # Specify temp audio file
                remove_temp=True,  # Remove temporary audio file created by moviepy
            )
            logger.info("Video and audio merged successfully.")

            # Move the merged file to its final destination
            if not VIDEO_KEEP_ORI and os.path.exists(remote_full_filename):
                logger.info(f"Removing original video file: {remote_full_filename}")
                os.remove(
                    remote_full_filename
                )  # Remove the original video without audio

            logger.info(
                f"Moving converted video from {converted_full_filename_temp} to {final_video_path_after_merge}"
            )
            shutil.move(converted_full_filename_temp, final_video_path_after_merge)

        except Exception as e:
            logger.error(f"An error occurred during video/audio merging for {url}: {e}")
            return False

        finally:
            # Ensure all clips are closed to release resources
            if "video_clip" in locals() and video_clip is not None:
                video_clip.close()
            if "audio_clip" in locals() and audio_clip is not None:
                audio_clip.close()
            if "final_clip" in locals() and final_clip is not None:
                final_clip.close()

    return True


def download_videos(videos: list):
    """
    Iterates through a list of video URLs or YouTube objects and downloads each one.

    Args:
        videos (list): A list containing video URLs (str) or YouTube objects.
    """
    for i, video in enumerate(videos):
        if isinstance(video, str):
            url = video
        elif isinstance(video, YouTube):
            url = video.watch_url
        else:
            logger.error(f"Invalid video item type: {type(video)}")
            continue

        logger.info(f"Downloading video {url} [{i + 1}/{len(videos)}]")
        try:
            download_yt(url)
        except BotDetection as e:
            logger.error(f"Failed to download {url} due to bot detection: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while downloading {url}: {e}")


def move_files():
    """
    Moves video and audio files from the current working directory to their
    respective destination folders (DST and DST_AUDIO), renaming them if necessary.
    This function is typically used after downloads if files are initially saved locally.
    """
    logger.debug(f"Current working directory: {os.getcwd()}")
    # Move video files
    videos = glob.glob(r"*.{ext}".format(ext=VIDEO_EXT))
    logger.debug(f"Found video files: {videos}")
    for video in videos:
        new_name = video[:MAX_FILE_LENGTH]  # Truncate name if too long
        os.rename(video, new_name)
        shutil.move(new_name, os.path.join(DST, new_name))
        logger.info(f"Moved video: {new_name} to {DST}")

    # Move audio files
    audios = glob.glob(r"*.{ext}".format(ext=AUDIO_EXT))
    logger.debug(f"Found audio files: {audios}")
    for audio in audios:
        new_name = audio[:MAX_FILE_LENGTH]  # Truncate name if too long
        os.rename(audio, new_name)
        shutil.move(new_name, os.path.join(DST_AUDIO, new_name))
        logger.info(f"Moved audio: {new_name} to {DST_AUDIO}")


def remove_origional_video():
    """
    Renames files that have a double extension (e.g., .mp4.mp4) by removing the extra extension.
    This can occur if video files are downloaded with an added extension during a merge process.
    """
    logger.debug(f"Current working directory: {os.getcwd()}")
    # Find files with double extension (e.g., video.mp4.mp4)
    videos = glob.glob(os.path.join(DST, r"*.{ext}.{ext}".format(ext=VIDEO_EXT)))
    logger.debug(f"Found original videos with double extension: {videos}")
    for video in videos:
        source = video
        destination = video[: -len(f".{VIDEO_EXT}")]  # Remove the last extension
        logger.info(f"Moving original video: {source} to {destination}")
        shutil.move(source, destination)


def compare_audio_video():
    """
    Compares the list of video files with audio files in their respective destination folders.
    Prints the names of video files that do not have a corresponding audio file.
    """
    # Get base filenames for videos
    videos = glob.glob(os.path.join(DST, r"*.{ext}".format(ext=VIDEO_EXT)))
    videos = sorted([os.path.splitext(os.path.basename(video))[0] for video in videos])
    logger.info(f"Video files found: {len(videos)}")

    # Get base filenames for audios
    audios = glob.glob(os.path.join(DST_AUDIO, r"*.{ext}".format(ext=AUDIO_EXT)))
    audios = sorted([os.path.splitext(os.path.basename(audio))[0] for audio in audios])
    logger.info(f"Audio files found: {len(audios)}")

    # Identify videos without a matching audio file
    for video_name in videos:
        if video_name not in audios:
            print(f"Video file '{video_name}' has no matching audio file.")


def compare_playlist():
    """
    Compares downloaded files (video and audio) against the titles in defined playlists.
    It normalizes names to account for subtle differences and reports any missing files.
    """
    for pl_url in pls:
        p = Playlist(pl_url)
        logger.info(f"Processing Playlist: {p.title}")

        # Set dynamic destination paths based on playlist title
        current_dst = os.path.join(f"{PATH}", p.title)
        current_dst_audio = os.path.join(f"{PATH}", f"{p.title}-Audio")

        # Get existing video filenames, normalized
        videos = glob.glob(os.path.join(current_dst, r"*.{ext}".format(ext=VIDEO_EXT)))
        videos_base_names = sorted(
            [os.path.splitext(os.path.basename(video))[0] for video in videos]
        )
        normalized_videos = sorted(
            [get_comparable_name(item) for item in videos_base_names]
        )
        logger.debug(f"Normalized video base names: {normalized_videos}")

        # Get existing audio filenames, normalized
        audios = glob.glob(
            os.path.join(current_dst_audio, r"*.{ext}".format(ext=AUDIO_EXT))
        )
        audios_base_names = sorted(
            [os.path.splitext(os.path.basename(audio))[0] for audio in audios]
        )
        normalized_audios = sorted(
            [get_comparable_name(item) for item in audios_base_names]
        )
        logger.debug(f"Normalized audio base names: {normalized_audios}")

        logger.info(
            f"Number of videos found: {len(videos_base_names)}, Number of audios found: {len(audios_base_names)}"
        )

        # Get YouTube video titles from the playlist, normalized
        youtube_titles = sorted([url.title for url in p.videos])
        logger.debug(f"Original YouTube titles: {youtube_titles}")

        # Determine which set of downloaded files (video or audio) is larger for target comparison
        # This assumes if one is present, the other should be too, and uses the larger set for more comprehensive checking
        normalized_target_set = (
            set(normalized_videos)
            if len(normalized_videos) >= len(normalized_audios)
            else set(normalized_audios)
        )

        missing_count = 0
        for original_yt_title in youtube_titles:
            comparable_yt_title = get_comparable_name(original_yt_title)

            if comparable_yt_title not in normalized_target_set:
                missing_count += 1
                logger.info(
                    f"Missing file detected: original_title='{original_yt_title!r}', comparable_title='{comparable_yt_title!r}'"
                )
        logger.info(
            f"Total missing files found for playlist '{p.title}': {missing_count}"
        )


def find_duplicated_title_in_playlist():
    """
    Checks for and logs duplicated video titles within each configured playlist.

    Returns:
        list: A list of duplicated video titles found in the playlist.
    """
    for pl in pls:
        p = Playlist(pl)
        logger.info(f"Playlist ... {p.title}")
        titles = sorted([url.title for url in p.videos])
        seen = set()
        duplicated = []
        for title in titles:
            if title in seen:
                duplicated.append(title)
            else:
                seen.add(title)

        if len(duplicated) > 0:
            logger.warning("Found duplicated title in playlist !!!")
            for d in duplicated:
                print(d)
        else:
            logger.info("No duplicated title in playlist.")

        return duplicated


def main():
    """
    Main function to orchestrate the downloading process based on configured lists (vs, pls, cls, qls).
    It processes individual videos, playlists, channels, and search queries.
    """
    logger.info("Starting individual video downloads...")
    download_videos(vs)

    if PLS:
        logger.info("Starting playlist downloads...")
        for pl_url in pls:
            global DST
            global DST_AUDIO
            try:
                p = Playlist(pl_url)
                logger.info(f"Processing Playlist: {p.title}")
                # Set destination folders based on playlist title for each playlist
                DST = os.path.join(f"{PATH}", p.title)
                DST_AUDIO = os.path.join(f"{PATH}", f"{p.title}-Audio")
                os.makedirs(DST, exist_ok=True)
                os.makedirs(DST_AUDIO, exist_ok=True)
                logger.info(f"Video destination: {DST}, Audio destination: {DST_AUDIO}")
                download_videos(p.videos)
            except Exception as e:
                logger.error(f"Unable to process Playlist {pl_url}: {e}")

    if CLS:
        logger.info("Starting channel downloads...")
        for ch_url in cls:
            try:
                c = Channel(ch_url)
                logger.info(f"Processing Channel: {c.channel_name}")
                # Note: Channel downloads might need separate DST/DST_AUDIO handling if desired
                download_videos(c.videos)
            except Exception as e:
                logger.error(f"Unable to process Channel {ch_url}: {e}")

    if QLS:
        logger.info("Starting quick search downloads...")
        for qs, search_filter, top_n in qls:
            # Construct filters for the search query
            filters_obj = (
                Filter.create()
                .type(Filter.Type.VIDEO)
                .sort_by(Filter.SortBy(search_filter))
            )
            try:
                res = Search(qs, filters=filters_obj)
                # Download only the top N videos from the search results
                for video in res.videos[:top_n]:
                    logger.info(f"Search result Title: {video.title}")
                    logger.info(f"Search result URL: {video.watch_url}")
                    logger.info(f"Search result Duration: {video.length} sec")
                    logger.info("---")
                    download_videos([video])  # Download each video individually
            except Exception as e:
                logger.error(f"Unable to perform Search for '{qs}': {e}")


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
    logger.info(f"YouTube Title: {yt.title}")
    ys = yt.streams.get_highest_resolution(progressive=False, mime_type="video/mp4")
    ys.download(output_path="download/mtv/歌心りえ/")

    logger.info(f"Captions for video: {yt.captions.keys()}")
    for caption_code in yt.captions.keys():
        try:
            caption_track = yt.captions[caption_code]
            caption_track.save(f"download/mtv/歌心りえ/{yt.title}.{caption_code}.txt")
            logger.info(f"Caption {caption_code} saved.")
        except Exception as e:
            logger.error(f"Failed to save caption {caption_code}: {e}")

    ya = yt.streams.get_audio_only()
    ya.download(
        f"download/mtv/歌心りえ/{yt.title}.m4a"
    )  # Assuming m4a for audio-only from YT
    logger.info("Audio-only stream downloaded.")

    # Example for playlist processing
    pl = Playlist(url=plst[0])  # Use the first playlist from the list
    logger.info(f"Playlist Title: {pl.title}")
    for video in pl.videos:
        logger.info(f"Downloading audio for playlist video: {video.title}")
        ys = video.streams.get_audio_only()
        ys.download(output_path="download/mtv")

    # Example for channel processing
    c = Channel("https://www.youtube.com/@ProgrammingKnowledge/featured")
    logger.info(f"Channel name: {c.channel_name}")

    c1 = Channel("https://www.youtube.com/@LillianChiu101")
    logger.info(f"Channel name: {c1.channel_name}")

    # Example for search functionality
    res = Search("GitHub Issue Best Practices")
    logger.info("Search results for 'GitHub Issue Best Practices':")
    for video in res.videos:
        logger.info(f"  Title: {video.title}")
        logger.info(f"  URL: {video.watch_url}")
        logger.info(f"  Duration: {video.length} sec")
        logger.info("  ---")

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
    logger.info("Search results for 'music' with filters:")
    for video in res.videos:
        logger.info(f"  Title: {video.title}")
        logger.info(f"  URL: {video.watch_url}")
        logger.info(f"  Duration: {video.length} sec")
        logger.info("  ---")


if __name__ == "__main__":
    # This block executes when the script is run directly
    main()  # Call the main function to start the download process
    # The following functions are commented out but can be called for specific tasks:
    # move_files()
    # remove_origional_video()
    # compare_audio_video()
    # compare_playlist()
    # find_duplicated_title_in_playlist()
