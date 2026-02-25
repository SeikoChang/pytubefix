# To-Do List

- [x] Expand the database schema in `YouTubeTaskManager` to track the full lifecycle of a download.
- [x] Implement CRUD methods (`get_task`, `update_task`, modified `add_task`) in `YouTubeTaskManager`.
- [x] Integrate `YouTubeTaskManager` into `YouTubeDownloader`, instantiating it and ensuring its database connection is closed.
- [x] Refactor `_download_youtube_video` to use the task manager, preventing duplicate downloads and tracking task status.
- [x] Debug script execution issues, including missing dependencies (`moviepy`) and database schema mismatches (`sqlite3.OperationalError`).
- [x] Clean up test data from the script.
- [x] Separate `final_filename_on_disk` into `final_video_filename` and `final_audio_filename` in the database and corresponding code.
- [x] Modify the `_download_videos_from_list` method to ensure video download occurs when `DOWNLOAD_VIDEO` is True, even if the corresponding audio file already exists on disk.
- [x] Ensure that the global `DOWNLOAD_VIDEO` constant is correctly defined and accessible.
- [x] Resolve the `'RootLogger' object is not callable` error during video/audio merging by adjusting how the logger is passed to MoviePy.
- [x] Format the `run.py` file according to PEP8 guidelines using a linter/formatter.
- [x] Run, revise, and update unit tests for `YouTubeTaskManager` and `YouTubeDownloader` in `test_run_script.py` to ensure robust coverage and path-aware mocking.
- [x] Output the current project context and memories to a local `GEMINI.md` file.
- [x] Refine `GEMINI.md` with usage examples and a system flow chart.
- [x] Comprehensive revision of `test_run_script.py`: implemented path-aware mocking, improved fixture isolation, and added coverage for failure modes and batch processing.
- [x] Document project-specific Best Practices in `GEMINI.md`.
- [x] Refactor `run.py` to use the asynchronous `AsyncYouTube` interface and follow modern `pytubefix` best practices (await metadata, streams, etc.).
- [x] Update `test_run_script.py` to support asynchronous testing using `pytest-asyncio` and `AsyncMock`.
