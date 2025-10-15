import glob
import os
import shutil
from pytubefix import YouTube
from pytubefix import Playlist
from pytubefix import Channel
from pytubefix import Search
from pytubefix import helpers
from pytubefix.exceptions import BotDetection
from pytubefix.contrib.search import Filter
from pytubefix.cli import on_progress
from moviepy import AudioFileClip, CompositeAudioClip, VideoFileClip

MAX_FILE_LENGTH = 63
DRY_RUN = False
EXT = "mp4"
DST = "./content/drive/MyDrive/Tokyo"
DST_AUDIO = "./content/drive/MyDrive/Tokyo-Audio"

VIDEO_MIME = "mp4"
VIDEO_RES = "1080p"
PROGRESSIVE = False
ORDER_BY = "resolution"

AUDIO = True
RECONVERT = True

PLS = True
CLS = False
QLS = False


os.makedirs(DST, exist_ok=True)
os.makedirs(DST_AUDIO, exist_ok=True)

url = "https://www.youtube.com/watch?v=6F-fAlGA0q0&list=PLf8MTi2c_8X-TLNg6tAjLaeb0jvmSQoX5"
plst = "https://www.youtube.com/playlist?list=PLf8MTi2c_8X-TLNg6tAjLaeb0jvmSQoX5"


vs = [
    # "https://www.youtube.com/watch?v=zb3nAoJJGYo",
    # "https://www.youtube.com/watch?v=BmtYHnvQcqw",
    # "https://www.youtube.com/watch?v=6F-fAlGA0q0&list=PLf8MTi2c_8X-TLNg6tAjLaeb0jvmSQoX5",
]

pls = [
    # "https://youtube.com/playlist?list=PLf8MTi2c_8X8Vz5JGI57tNy2BlbjZkMxC&si=PliaxKExX5U48kPV",
    # "https://youtube.com/playlist?list=PLf8MTi2c_8X-TLNg6tAjLaeb0jvmSQoX5",
    "https://www.youtube.com/playlist?list=PLf8MTi2c_8X9XM74Pk2PuTKNo39C8bqTJ",
]

cls = [
    # "https://www.youtube.com/@ProgrammingKnowledge/featured",
    "https://www.youtube.com/@LillianChiu101",
]

qls = [
    # "Programming Knowledge",
    # "GitHub Issue Best Practices",
    "global news"
]


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


def download_yt(url):
    yt = YouTube(
        url=url,
        use_oauth=False,
        allow_oauth_cache=False,
        on_progress_callback=on_progress,
        # client='ANDROID',  # 'WEB'
    )

    print(yt.title)
    print(f"Title: {yt.title}")
    print(f"URL: {yt.watch_url}")
    print(f"Duration: {yt.length} sec")
    print("---")
    if DRY_RUN:
        return True  # for dry run

    # vids = yt.streams
    # for i, vid in enumerate(vids):
    #   print(i, vid)

    filename = helpers.safe_filename(s=yt.title, max_length=MAX_FILE_LENGTH)
    full_filename = f"{filename}.{EXT}"

    # download caption
    for caption in yt.captions.keys():
        print(caption.name)
        remote_full_captionname = os.path.join(DST, f"{full_filename}.{caption.code}.txt")
        caption.save_captions(remote_full_captionname)

    # download video
    remote_full_filename = os.path.join(DST, full_filename)
    if not os.path.exists(remote_full_filename):
        yt.streams.filter(
            progressive=PROGRESSIVE, mime_type=f"video/{VIDEO_MIME}", res=VIDEO_RES
        ).order_by(ORDER_BY).desc().first().download(
            output_path=".", filename=full_filename
        )
        print(f"moving file from = {full_filename} to = {remote_full_filename}")
        shutil.move(full_filename, remote_full_filename)
    else:
        print(
            f"remote file = [{remote_full_filename}] already exists, skip download video this time"
        )

    # download audio
    full_audioname = f"{filename}.mp3"
    remote_full_audioname = os.path.join(DST_AUDIO, full_audioname)
    if not os.path.exists(remote_full_audioname):
        if AUDIO:
            if RECONVERT and not PROGRESSIVE:
                print(f"converting audio = {full_audioname}")
                video = VideoFileClip(remote_full_filename)
                audio = video.audio
                if audio:
                    audio.write_audiofile(full_audioname)  # , codec="pcm_s16le"
                    shutil.move(full_audioname, remote_full_audioname)
                else:
                    yt.streams.filter(
                        mime_type="audio/mp4", abr="128kbps"
                    ).last().download(output_path=DST_AUDIO, filename=full_audioname)
                    yt.streams.get_audio_only().download(
                        output_path=DST_AUDIO, filename=full_audioname
                    )
            else:
                yt.streams.filter(
                    mime_type="audio/webm", abr="160kbps"
                ).last().download(output_path=DST_AUDIO, filename=full_audioname)
    else:
        print(
            f"remote file = [{remote_full_audioname}] already exists, skip download audio this time"
        )

    # merge video/audio if needed
    if not PROGRESSIVE:
        try:
            # Load the video clip
            video_clip = VideoFileClip(remote_full_filename)
            print(video_clip.duration)

            # Load the audio clip
            audio_clip = AudioFileClip(remote_full_audioname)
            print(audio_clip.duration)

            # Assign the audio to the video clip
            final_clip = video_clip
            final_clip.audio = audio_clip
            print(final_clip.duration)

            if not final_clip.audio or RECONVERT:
                # Write the final video with the combined audio
                print(
                    f"Write the final video with the combined audio = {remote_full_filename}.mp4"
                )
                final_clip.write_videofile(
                    f"{remote_full_filename}.mp4", codec="libx264", audio_codec="aac"
                )
                shutil.move(f"{remote_full_filename}.mp4", remote_full_filename)
                print(
                    f"Video and audio combined successfully and saved to {remote_full_filename}"
                )
            else:
                print(
                    f"remote file = [{remote_full_filename}] already exists, skip merge video/audio this time"
                )

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            # Close the clips to release resources
            if "video_clip" in locals() and video_clip is not None:
                video_clip.close()
            if "audio_clip" in locals() and audio_clip is not None:
                audio_clip.close()
            if "final_clip" in locals() and final_clip is not None:
                final_clip.close()
        # videoclip = VideoFileClip(remote_full_filename)
        # audioclip = AudioFileClip(remote_full_audioname)
        # if not videoclip.audio:
        #     # videoclip = videoclip.set_audio(audioclip)
        #     new_audioclip = CompositeAudioClip([audioclip])
        #     videoclip.audio = new_audioclip
        #     videoclip.write_videofile(remote_full_filename)


def download_videos(videos):
    for video in videos:
        if isinstance(video, str):
            url = video
        elif isinstance(video, YouTube):
            url = video.watch_url
        print(f"Downloading url = {url}")
        try:
            download_yt(url)
        except BotDetection:
            print(f"fail to download url = {url} due to detected as a bot")
        except Exception:
            print(f"fail to download url = {url}")


def move_files():
    print(os.getcwd())
    videos = glob.glob(r"*.{ext}".format(ext=EXT))
    print(videos)
    for video in videos:
        os.rename(video, video[:MAX_FILE_LENGTH])
        video = video[:MAX_FILE_LENGTH]
        shutil.move(video, os.path.join(DST, video))

    audios = glob.glob(r"*.{ext}".format(ext="mp3"))
    print(audios)
    for audio in audios:
        os.rename(audio, audio[:MAX_FILE_LENGTH])
        audio = audio[:MAX_FILE_LENGTH]
        shutil.move(audio, os.path.join(DST_AUDIO, audio))


def main():
    print("Individual Video ...")
    download_videos(vs)

    if PLS:
        print("Playlist ...")
        for pl in pls:
            try:
                p = Playlist(pl)
                print(f"Playlist ... {p.title}")
                download_videos(p.videos)
            except Exception:
                print(f"unable to handle Playlist = {pl}")

    if CLS:
        print("Channel ...")
        for cl in cls:
            try:
                c = Channel(cl)
                print(f"Channel name ... {c.channel_name}")
                download_videos(c.videos)
            except Exception:
                print(f"unable to handle Channel = {cl}")

    if QLS:
        print("Search ...")
        filters = {
            "upload_date": Filter.get_upload_date("Last Hour"),
            "type": Filter.get_type("Video"),
            # "duration": Filter.get_duration("Under 4 minutes"),
            # "features": [Filter.get_features("4K"), Filter.get_features("Creative Commons")],
            "sort_by": Filter.get_sort_by("Upload date"),
        }
        for ql in qls:
            try:
                q = Search(ql, filters=filters)
                download_videos(q.videos)
            except Exception:
                print(f"unable to handle Search = {ql}")


def _main():
    yt = YouTube(url, on_progress_callback=on_progress)
    print(yt.title)
    ys = yt.streams.get_highest_resolution(progressive=False, mime_type="video/mp4")
    ys.download(output_path="download/mtv/歌心りえ/")

    print(yt.captions)
    for caption in yt.captions.keys():
        # print(caption.generate_srt_captions())
        caption.save_captions(f"download/mtv/歌心りえ/{yt.title}.txt")

    ya = yt.streams.get_audio_only()
    ya.download(f"download/mtv/歌心りえ/{yt.title}.txt")

    pl = Playlist(url=plst)

    for video in pl.videos:
        ys = video.streams.get_audio_only()
        ys.download(output_path="download/mtv")

    c = Channel("https://www.youtube.com/@ProgrammingKnowledge/featured")
    print(f"Channel name: {c.channel_name}")

    c1 = Channel("https://www.youtube.com/@LillianChiu101")
    print(f"Channel name: {c1.channel_name}")
    # for video in c1.videos:
    #     video.streams.get_highest_resolution().download()

    res = Search("GitHub Issue Best Practices")
    for video in res.videos:
        print(f"Title: {video.title}")
        print(f"URL: {video.watch_url}")
        print(f"Duration: {video.length} sec")
        print("---")

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
        print(f"Title: {video.title}")
        print(f"URL: {video.watch_url}")
        print(f"Duration: {video.length} sec")
        print("---")


if __name__ == "__main__":
    main()
    move_files()
