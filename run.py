import sys
from functools import wraps
import glob
import os
import shutil
import time
import logging
from logging.handlers import TimedRotatingFileHandler
from pytubefix import YouTube
from pytubefix import Playlist
from pytubefix import Channel
from pytubefix import Search
from pytubefix import helpers
from pytubefix.exceptions import BotDetection
from pytubefix.contrib.search import Filter
from pytubefix.cli import on_progress
from moviepy import AudioFileClip, CompositeAudioClip, VideoFileClip

RELEVANCE = Filter.SortBy.RELEVANCE
UPLOAD_DATE = Filter.SortBy.UPLOAD_DATE
VIEW_COUNT = Filter.SortBy.VIEW_COUNT
RATING = Filter.SortBy.RATING

LOG_FORMAT = "[%(asctime)s.%(msecs)03d] [%(levelname)s]: %(message)s"
LOG_FORMAT_DATE = "%Y-%m-%d %H:%M:%S"
LOG_LEVEL = logging.INFO

# Configure logging to display INFO level and above, and use the custom format
# 'stream=sys.stdout' ensures output goes to the standard Colab output cell
logging.basicConfig(
    format=LOG_FORMAT,
    datefmt=LOG_FORMAT_DATE,
    stream=sys.stdout,
    level=logging.getLevelName(LOG_LEVEL),
    force=True,  # This argument forces Colab to use this config, overriding defaults
)
logger: logging.Logger = logging.getLogger()
handler = TimedRotatingFileHandler(
    filename="pytub.log",
    when="midnight",  # Rotate at midnight every day
    interval=1,  # Every 1 day
    backupCount=7,  # Keep 7 backup log files (for a week)
)
handler.suffix = "_%Y%m%d.log"
formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_FORMAT_DATE)
handler.setFormatter(formatter)
logger.addHandler(handler)

ENV = os.getenv("ENV", "DEV")

MAX_FILE_LENGTH = 63
DRY_RUN = False
DOWNLOAD_ALL = False
PATH = "./drive/MyDrive/"
DST = os.path.join(f"{PATH}", "日本演歌")
DST_AUDIO = os.path.join(f"{PATH}", "日本演歌-Audio")

CAPTION = True

VIDEO = True
VIDEO_EXT = "mp4"
VIDEO_MIME = "mp4"
VIDEO_RES = "1080p"
VIDEO_CODE = "av1"
VIDEO_KEEP_ORI = False
PROGRESSIVE = False
# ADAPTIVE = True
ORDER_BY = "itag"

AUDIO = True
AUDIO_EXT = "mp3"
AUDIO_MIME = "mp4"
AUDIO_BITRATE = "128kbps"
AUDIO_CODE = "abr"
AUDIO_KEEP_ORI = False

RECONVERT = True
CONVERT_VIDEO_CODE = (
    None  # "libx264" by fefault for .mp4, leave None for auto detection
)
CONVERT_AUDIO_CODE = (
    "aac"  # "libmp3lame" by default for .mp4, leave None for auto detection
)

PLS = True
CLS = False
QLS = False

os.makedirs(DST, exist_ok=True)
os.makedirs(DST_AUDIO, exist_ok=True)

vs: list[str] = [
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
    # "https://www.youtube.com/@ProgrammingKnowledge/featured",
    # "https://www.youtube.com/@LillianChiu101",
    # "https://www.youtube.com/@kellytsaii",
]

qls = [
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


def retry_function(retries=1, delay=1):
    def outer_d_f(func):
        @wraps(func)
        def wraps_func(*args, **kargs):
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
                raise Exception(
                    "[fun: {}.{}] {}".format(func.__module__, func.__name__, err)
                )

        return wraps_func

    return outer_d_f


def remove_characters(filename):
    _filename = ""
    for c in filename:
        if c not in "｜|,/\\:*?<>":
            _filename += c
    return _filename

    # filename = filename.replace('\"' , " ")
    # filename = filename.replace('|', " ")
    # filename = filename.replace(',', " ")
    # filename = filename.replace('/"' , " ")
    # filename = filename.replace('\\', " ")
    # filename = filename.replace(':', " ")
    # filename = filename.replace('*"' , " ")
    # filename = filename.replace('?', " ")
    # filename = filename.replace('<', " ")
    # filename = filename.replace('>"' , " ")


# @retry_function(retries=3, delay=30)
def download_yt(url):
    yt = YouTube(
        url=url,
        use_oauth=False,
        allow_oauth_cache=False,
        on_progress_callback=on_progress,
        # client='ANDROID',  # 'WEB'
    )

    # logger.info(f"URL: {yt.watch_url}")
    logger.info(f"Title: {yt.title}")
    logger.info(f"Duration: {yt.length} sec")
    logger.info("---")
    if DRY_RUN:
        return True  # for dry run

    # vids = yt.streams
    # for i, vid in enumerate(vids):
    #   logger.info(i, vid)

    filename = helpers.safe_filename(s=yt.title, max_length=MAX_FILE_LENGTH)
    full_filename = f"{filename}.{VIDEO_EXT}"

    # if DOWNLOAD_ALL:
    #     for stream in yt.streams:
    #         stream.download(
    #             output_path=".",
    #             filename=f"{filename[:16]}_{stream.itag}_{stream.subtype}_{stream.resolution}_{stream.video_codec}_{stream.abr}_{stream.audio_codec}.mp4"
    #         )

    if CAPTION:
        # download caption
        for caption in yt.captions.keys():
            logger.debug(caption.name)
            remote_full_captionname = os.path.join(
                DST, f"{full_filename}.{caption.code}.txt"
            )
            caption.save_captions(remote_full_captionname)

    video_download_folder = "."
    remote_full_filename = os.path.join(DST, full_filename)
    if VIDEO:
        # download video
        if not os.path.exists(remote_full_filename):
            stream = (
                yt.streams.filter(
                    progressive=PROGRESSIVE,
                    mime_type=f"video/{VIDEO_MIME}",
                    res=VIDEO_RES,
                )
                .order_by(ORDER_BY)
                .desc()
                .last()
            )
            if not stream:
                stream = yt.streams.get_highest_resolution(progressive=PROGRESSIVE)
            logger.info(
                f"downloading video ... itag = {stream.itag} res = {stream.resolution} video_code = {stream.video_codec} abr = {stream.abr} audio_code = {stream.audio_codec}"
            )
            stream.download(output_path=video_download_folder, filename=full_filename)
            logger.info(
                f"moving video file from = {full_filename} to = {remote_full_filename}"
            )
            shutil.move(full_filename, remote_full_filename)
        else:
            logger.warning(
                f"remote file = [{remote_full_filename}] already exists, skip downloading video this time"
            )

    audio_download_folder = "."
    full_audioname = f"{filename}.{AUDIO_EXT}"
    full_audioname_ori = f"{filename}.{AUDIO_MIME}"
    remote_full_audioname = os.path.join(DST_AUDIO, full_audioname)
    audio_download_fullname = os.path.join(audio_download_folder, full_audioname_ori)
    if AUDIO:
        # download audio
        if not os.path.exists(remote_full_audioname):
            logger.info(f"converting audio = {full_audioname}")
            audio = None
            if os.path.exists(remote_full_filename):
                video = VideoFileClip(remote_full_filename)
                audio = video.audio
            if not audio:
                logger.warning(
                    "no audio track found from origional video, downloading audio stream instead ..."
                )
                try:
                    stream = (
                        yt.streams.filter(
                            mime_type=f"audio/{AUDIO_MIME}", abr=AUDIO_BITRATE
                        )
                        .asc()
                        .first()
                    )
                except Exception as e:
                    logger.debug(f"err = {e}")
                    stream = yt.streams.get_audio_only(subtype=AUDIO_MIME)
                logger.info(
                    f"downloading audio ... itag = {stream.itag} res = {stream.resolution} video_code = {stream.video_codec} abr = {stream.abr} audio_code = {stream.audio_codec}"
                )
                stream.download(
                    output_path=audio_download_folder, filename=full_audioname_ori
                )
                audio = AudioFileClip(audio_download_fullname)
            audio.write_audiofile(filename=remote_full_audioname, codec=None)
            if AUDIO_KEEP_ORI and AUDIO_MIME != AUDIO_EXT:
                logger.info(
                    f"moving aideo file from = {audio_download_fullname} to = {os.path.join(DST_AUDIO, full_audioname_ori)}"
                )
                shutil.move(
                    audio_download_fullname, os.path.join(DST_AUDIO, full_audioname_ori)
                )
            else:
                logger.info(f"remove aideo file = {audio_download_fullname}")
                os.remove(audio_download_fullname)
            # codec="pcm_s16le" for '.wav' ="libmp3lame" for '.mp3',
            # default to detect by file extension name
        else:
            logger.warning(
                f"remote file = [{remote_full_audioname}] already exists, skip downloading audio this time"
            )

    # merge video/audio if needed
    if RECONVERT:
        converted_full_filename = f"{full_filename}.{VIDEO_EXT}"
        if VIDEO_KEEP_ORI:
            coverted_remote_full_filename = os.path.join(DST, converted_full_filename)
        else:
            coverted_remote_full_filename = os.path.join(DST, full_filename)

        if os.path.exists(coverted_remote_full_filename):
            # Load the video clip
            video_clip = VideoFileClip(coverted_remote_full_filename)
            logger.debug(
                f"video length = {video_clip.duration} audio = {video_clip.audio} fps = {video_clip.fps} h = {video_clip.h}"
            )
            if video_clip.audio:
                logger.warning(
                    f"remote file = [{coverted_remote_full_filename}] already exists, skip converting video this time"
                )
                return True

        try:
            # Load the video clip
            video_clip = VideoFileClip(remote_full_filename)
            logger.debug(f"video length = {video_clip.duration}")

            # Load the audio clip
            audio_clip = AudioFileClip(remote_full_audioname)
            logger.debug(f"audio length = {audio_clip.duration}")

            # Assign the audio to the video clip
            final_clip = video_clip
            final_clip.audio = audio_clip

            # Write the final video with the combined audio
            logger.info(
                f"Write the final video with the combined audio, \
                    local = {converted_full_filename}, remote = {coverted_remote_full_filename}"
            )
            final_clip.write_videofile(
                filename=converted_full_filename,
                codec=CONVERT_VIDEO_CODE,
                audio_codec=CONVERT_AUDIO_CODE,
            )
            # videoclip = VideoFileClip(remote_full_filename)
            # audioclip = AudioFileClip(remote_full_audioname)
            # if not videoclip.audio:
            #     # videoclip = videoclip.set_audio(audioclip)
            #     new_audioclip = CompositeAudioClip([audioclip])
            #     videoclip.audio = new_audioclip
            #     videoclip.write_videofile(remote_full_filename)

            # Load the video clip
            video_clip = VideoFileClip(converted_full_filename)
            logger.debug(
                f"video length = {video_clip.duration} audio = {video_clip.audio} fps = {video_clip.fps} h = {video_clip.h}"
            )
            logger.info(
                f"moving converted video from = {converted_full_filename} to = {coverted_remote_full_filename}"
            )
            shutil.move(converted_full_filename, coverted_remote_full_filename)
            logger.info(
                f"Video and audio combined successfully and saved to {remote_full_filename}"
            )

        except Exception as e:
            logger.error(f"An error occurred: {e}")

        finally:
            # Close the clips to release resources
            if "video_clip" in locals() and video_clip is not None:
                video_clip.close()
            if "audio_clip" in locals() and audio_clip is not None:
                audio_clip.close()
            if "final_clip" in locals() and final_clip is not None:
                final_clip.close()


def download_videos(videos):
    for i, video in enumerate(videos):
        if isinstance(video, str):
            url = video
        elif isinstance(video, YouTube):
            url = video.watch_url
        logger.info(f"Downloading url = {url} [{i + 1}/{len(videos)}]")
        try:
            download_yt(url)
        except BotDetection as e:
            logger.error(
                f"fail to download url = {url} due to detected as a bot err = {e}"
            )
        except Exception as e:
            logger.error(f"fail to download url = {url} err = {e}")


def move_files():
    logger.debug(os.getcwd())
    videos = glob.glob(r"*.{ext}".format(ext=VIDEO_EXT))
    logger.debug(videos)
    for video in videos:
        os.rename(video, video[:MAX_FILE_LENGTH])
        video = video[:MAX_FILE_LENGTH]
        shutil.move(video, os.path.join(DST, video))

    audios = glob.glob(r"*.{ext}".format(ext=AUDIO_EXT))
    logger.debug(audios)
    for audio in audios:
        os.rename(audio, audio[:MAX_FILE_LENGTH])
        audio = audio[:MAX_FILE_LENGTH]
        shutil.move(audio, os.path.join(DST_AUDIO, audio))


def remove_origional_video():
    logger.debug(os.getcwd())
    videos = glob.glob(os.path.join(DST, r"*.{ext}.{ext}".format(ext=VIDEO_EXT)))
    logger.debug(videos)
    for video in videos:
        source = video
        destination = video[:-4]
        logger.info(msg=f"moving... {source=} to {destination=}")
        shutil.move(source, destination)


def compare_audio_video():
    videos = glob.glob(os.path.join(DST, r"*.{ext}".format(ext=VIDEO_EXT)))
    videos = sorted([os.path.splitext(os.path.basename(video))[0] for video in videos])

    audios = glob.glob(os.path.join(DST_AUDIO, r"*.{ext}".format(ext=AUDIO_EXT)))
    audios = sorted([os.path.splitext(os.path.basename(audio))[0] for audio in audios])

    for video in videos:
        if video not in audios:
            print(video)


def compare_video_playlist():
    for pl in pls:
        p = Playlist(pl)
        logger.info(f"Playlist ... {p.title}")
        DST = os.path.join(f"{PATH}", p.title)
        videos = glob.glob(os.path.join(DST, r"*.{ext}".format(ext=VIDEO_EXT)))
        videos = sorted([os.path.splitext(os.path.basename(video))[0] for video in videos])

        titles = sorted([url.title for url in p.videos])
        for title in titles:
            if (
                helpers.safe_filename(s=title, max_length=MAX_FILE_LENGTH)
                not in videos
            ):
                print(title)


def main():
    logger.info("Individual Video ...")
    download_videos(vs)

    if PLS:
        logger.info("Playlist ...")
        for pl in pls:
            global DST
            global DST_AUDIO
            try:
                p = Playlist(pl)
                logger.info(f"Playlist ... {p.title}")
                DST = os.path.join(f"{PATH}", p.title)
                DST_AUDIO = os.path.join(f"{PATH}", f"{p.title}-Audio")
                os.makedirs(DST, exist_ok=True)
                os.makedirs(DST_AUDIO, exist_ok=True)
                logger.info(f"{DST=} {DST_AUDIO=}")
                download_videos(p.videos)
            except Exception:
                logger.error(f"unable to handle Playlist = {pl}")

    if CLS:
        logger.info("Channel ...")
        for cl in cls:
            try:
                c = Channel(cl)
                logger.info(f"Channel name ... {c.channel_name}")
                download_videos(c.videos)
            except Exception:
                logger.error(f"unable to handle Channel = {cl}")

    if QLS:
        logger.info("Search ...")

        for qs, filter, top in qls:
            # filters = {
            #     # "upload_date": Filter.get_upload_date(
            #     #     "This Week"
            #     # ),  # Today, Last Hour, This Week
            #     "type": Filter.get_type("Video"),
            #     # "duration": Filter.get_duration("Under 4 minutes"),
            #     # "features": [Filter.get_features("4K"), Filter.get_features("Creative Commons")],
            #     "sort_by": Filter.get_sort_by(filter),
            # }
            filters = (
                Filter.create()
                # .upload_date(Filter.UploadDate.TODAY)
                .type(Filter.Type.VIDEO)
                # .duration(Filter.Duration.UNDER_4_MINUTES)
                # .feature([Filter.Features.CREATIVE_COMMONS, Filter.Features._4K])
                .sort_by(Filter.SortBy(filter))
            )
            try:
                res = Search(qs, filters=filters)
                for video in res.videos[
                    :top
                ]:  # only download the top N videos from the search
                    logger.info(f"Title: {video.title}")
                    logger.info(f"URL: {video.watch_url}")
                    logger.info(f"Duration: {video.length} sec")
                    logger.info("---")
                    download_videos([video])
            except Exception:
                logger.error(f"unable to handle Search = {qs}")


def _main():
    plst = [
        "https://youtube.com/playlist?list=PLhkqiApN_VYay4opZamqmnHIeKQtR9l-T&si=KYV2DqljMbF0W4mQ",  # 日本演歌
    ]
    url = "https://youtube.com/watch?v=7g9xcCMdwns"
    yt = YouTube(url, on_progress_callback=on_progress)
    logger.info(yt.title)
    ys = yt.streams.get_highest_resolution(progressive=False, mime_type="video/mp4")
    ys.download(output_path="download/mtv/歌心りえ/")

    logger.info(yt.captions)
    for caption in yt.captions.keys():
        # logger.info(caption.generate_srt_captions())
        caption.save_captions(f"download/mtv/歌心りえ/{yt.title}.txt")

    ya = yt.streams.get_audio_only()
    ya.download(f"download/mtv/歌心りえ/{yt.title}.txt")

    pl = Playlist(url=plst)

    for video in pl.videos:
        ys = video.streams.get_audio_only()
        ys.download(output_path="download/mtv")

    c = Channel("https://www.youtube.com/@ProgrammingKnowledge/featured")
    logger.info(f"Channel name: {c.channel_name}")

    c1 = Channel("https://www.youtube.com/@LillianChiu101")
    logger.info(f"Channel name: {c1.channel_name}")
    # for video in c1.videos:
    #     video.streams.get_highest_resolution().download()

    res = Search("GitHub Issue Best Practices")
    for video in res.videos:
        logger.info(f"Title: {video.title}")
        logger.info(f"URL: {video.watch_url}")
        logger.info(f"Duration: {video.length} sec")
        logger.info("---")

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
    for video in res.videos:
        logger.info(f"Title: {video.title}")
        logger.info(f"URL: {video.watch_url}")
        logger.info(f"Duration: {video.length} sec")
        logger.info("---")


if __name__ == "__main__":
    main()
    # move_files()
    # remove_origional_video()
    # compare_audio_video()
    # compare_video_playlist()
